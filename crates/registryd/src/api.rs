//! The HTTP write API — docs/api.md. Small by design: submit records
//! (single or batch), read one back, health, and the read ticket. All
//! write endpoints require `Authorization: Bearer <token>`.

use crate::gc::GcState;
use crate::index::{IndexRecord, RecordIndex, SubmitOutcome};
use crate::publisher::PublisherStatus;
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use registry_core::{
    content_hash, is_valid_record_key, iscc, partition_id_for_key, BlobStore, RecordStatus,
};
use registry_node::IrohBlobStore;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub index: Arc<RecordIndex>,
    pub blob_store: IrohBlobStore,
    pub gc_state: Arc<GcState>,
    pub publisher_status: Arc<PublisherStatus>,
    pub api_tokens: Arc<HashSet<String>>,
    pub read_ticket: Arc<String>,
    pub endpoint_id: Arc<String>,
    pub max_value_bytes: usize,
    pub batch_max_records: usize,
    pub top_level_partitions: u32,
}

pub fn router(state: AppState) -> Router {
    // A full batch of maximum-size values must fit in one request body —
    // axum's 2 MB default rejects real-world imports with 413.
    let body_limit = state
        .batch_max_records
        .saturating_mul(state.max_value_bytes)
        .saturating_add(1024 * 1024);
    let writes = Router::new()
        .route("/v1/records", post(submit_record))
        .route("/v1/records/batch", post(submit_batch))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_api_token,
        ));

    Router::new()
        .merge(writes)
        .route("/v1/records/:key", get(get_record))
        .route("/health", get(health))
        .route("/ticket", get(ticket))
        .layer(axum::extract::DefaultBodyLimit::max(body_limit))
        .with_state(state)
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("invalid record key: {0}")]
    InvalidKey(String),
    #[error("invalid value: {0}")]
    InvalidValue(String),
    #[error("value too large (limit {limit} bytes)")]
    ValueTooLarge { limit: usize },
    #[error("missing or unknown bearer token")]
    Unauthorized,
    #[error("key already exists with different content")]
    Conflict { existing_hash: String },
    #[error("too many records in batch (limit {limit})")]
    BatchTooLarge { limit: usize },
    #[error("internal error: {0}")]
    Internal(String),
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            ApiError::InvalidKey(_) => "invalid_key",
            ApiError::InvalidValue(_) => "invalid_value",
            ApiError::ValueTooLarge { .. } => "value_too_large",
            ApiError::Unauthorized => "unauthorized",
            ApiError::Conflict { .. } => "conflict",
            ApiError::BatchTooLarge { .. } => "batch_too_large",
            ApiError::Internal(_) => "internal_error",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            ApiError::InvalidKey(_) | ApiError::InvalidValue(_) => StatusCode::BAD_REQUEST,
            ApiError::ValueTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Conflict { .. } => StatusCode::CONFLICT,
            ApiError::BatchTooLarge { .. } => StatusCode::BAD_REQUEST,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Internal detail stays in the log; the client gets the category.
        if let ApiError::Internal(detail) = &self {
            tracing::error!(detail, "internal error serving API request");
        }
        let mut body = json!({ "error": self.code(), "message": self.to_string() });
        if let ApiError::Conflict { existing_hash } = &self {
            body["existing_hash"] = json!(existing_hash);
        }
        (self.status(), Json(body)).into_response()
    }
}

async fn require_api_token(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    match token {
        Some(t) if state.api_tokens.contains(t) => Ok(next.run(request).await),
        _ => Err(ApiError::Unauthorized),
    }
}

#[derive(Deserialize)]
pub struct SubmitRecordRequest {
    pub key: String,
    pub value: serde_json::Value,
}

/// Normalize the submitted value to its canonical stored bytes. Accepts a
/// JSON object directly, or a string containing serialized JSON (kept
/// verbatim, so imports of an existing dump preserve the original bytes
/// and therefore the original content hashes).
fn canonical_value(value: &serde_json::Value) -> Result<String, ApiError> {
    match value {
        serde_json::Value::Object(_) => {
            Ok(serde_json::to_string(value).expect("object always serializes"))
        }
        serde_json::Value::String(raw) => {
            let parsed: serde_json::Value = serde_json::from_str(raw)
                .map_err(|e| ApiError::InvalidValue(format!("string value is not JSON: {e}")))?;
            if !parsed.is_object() {
                return Err(ApiError::InvalidValue(
                    "value must be a JSON object".to_string(),
                ));
            }
            Ok(raw.clone())
        }
        _ => Err(ApiError::InvalidValue(
            "value must be a JSON object".to_string(),
        )),
    }
}

/// Validation and canonicalization only — no I/O. Returns the canonical
/// value bytes and the index record they would claim.
fn validate_one(
    state: &AppState,
    key: &str,
    value: &serde_json::Value,
) -> Result<(String, IndexRecord), ApiError> {
    if !is_valid_record_key(key) {
        return Err(ApiError::InvalidKey(format!(
            "'{key}' is not a valid CIDv1 or declaration id"
        )));
    }
    let value_str = canonical_value(value)?;
    if value_str.len() > state.max_value_bytes {
        return Err(ApiError::ValueTooLarge {
            limit: state.max_value_bytes,
        });
    }

    let hash = content_hash(&value_str);
    let partition_id = partition_id_for_key(key, state.top_level_partitions);
    let record = IndexRecord {
        content_hash: hash,
        size: value_str.len() as u64,
        partition_id,
        status: RecordStatus::Pending,
        created_at: crate::index::declaration_timestamp(&value_str)
            .unwrap_or_else(chrono::Utc::now),
        content_code: iscc::extract_from_json(&value_str),
        published_at: None,
    };
    Ok((value_str, record))
}

/// Validation + blob write for one record; returns the index submission
/// it still needs. The blob is written before the index claim — a record
/// must never be enqueued without its value bytes being durable — and a
/// conflict just orphans a tiny blob for GC to sweep.
async fn prepare_one(
    state: &AppState,
    key: &str,
    value: &serde_json::Value,
) -> Result<(String, IndexRecord), ApiError> {
    let (value_str, record) = validate_one(state, key, value)?;
    let hash = record.content_hash;
    let stored = state
        .blob_store
        .put(bytes::Bytes::from(value_str))
        .await
        .map_err(|e| ApiError::Internal(format!("failed to store value blob: {e}")))?;
    debug_assert_eq!(stored, hash, "iroh-blobs must agree with content_hash");
    Ok((key.to_string(), record))
}

async fn ingest_one(
    state: &AppState,
    key: &str,
    value: &serde_json::Value,
) -> Result<(SubmitOutcome, u32), ApiError> {
    // Keep GC away while the fresh value blob is not yet index-referenced.
    let _gc_guard = state.gc_state.write_guard();
    let (key_owned, record) = prepare_one(state, key, value).await?;
    let partition_id = record.partition_id;
    let index = state.index.clone();
    let outcome = tokio::task::spawn_blocking(move || index.submit(&key_owned, &record))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok((outcome, partition_id))
}

pub async fn submit_record(
    State(state): State<AppState>,
    Json(req): Json<SubmitRecordRequest>,
) -> Result<Response, ApiError> {
    let (outcome, partition) = ingest_one(&state, &req.key, &req.value).await?;
    let (status_code, body) = match outcome {
        SubmitOutcome::Queued => (
            StatusCode::ACCEPTED,
            json!({ "key": req.key, "status": "queued", "partition": partition }),
        ),
        SubmitOutcome::DuplicateIdentical => (
            StatusCode::OK,
            json!({ "key": req.key, "status": "queued", "partition": partition, "duplicate": true }),
        ),
        SubmitOutcome::Conflict { existing_hash } => {
            return Err(ApiError::Conflict {
                existing_hash: existing_hash.to_prefixed(),
            })
        }
    };
    Ok((status_code, Json(body)).into_response())
}

#[derive(Deserialize)]
pub struct BatchSubmitRequest {
    pub records: Vec<SubmitRecordRequest>,
}

pub async fn submit_batch(
    State(state): State<AppState>,
    Json(req): Json<BatchSubmitRequest>,
) -> Result<Response, ApiError> {
    if req.records.len() > state.batch_max_records {
        return Err(ApiError::BatchTooLarge {
            limit: state.batch_max_records,
        });
    }
    // Phases: validate (no I/O) → duplicate/conflict precheck against the
    // index (one read transaction — known-identical records answer here
    // and never touch the blob store, which is what makes re-imports of a
    // mostly-loaded dump cheap and orphan-free) → bounded-concurrency
    // blob writes for the genuinely new records → ONE index write
    // transaction for the batch.
    let _gc_guard = state.gc_state.write_guard();
    let mut results: Vec<Option<serde_json::Value>> = Vec::new();
    let mut candidates: Vec<(usize, String, String, IndexRecord)> = Vec::new();
    for (i, record) in req.records.into_iter().enumerate() {
        match validate_one(&state, &record.key, &record.value) {
            Ok((value_str, rec)) => {
                results.push(None);
                candidates.push((i, record.key, value_str, rec));
            }
            Err(err) => results.push(Some(json!({
                "key": record.key,
                "status": "error",
                "error": err.code(),
                "message": err.to_string(),
            }))),
        }
    }

    let keys: Vec<String> = candidates.iter().map(|(_, k, _, _)| k.clone()).collect();
    let index_for_check = state.index.clone();
    let existing = tokio::task::spawn_blocking(move || index_for_check.get_many(&keys))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let blob_concurrency = Arc::new(tokio::sync::Semaphore::new(32));
    let mut tasks = Vec::new();
    for ((slot, key, value_str, rec), known) in candidates.into_iter().zip(existing) {
        match known {
            Some(prior) if prior.content_hash == rec.content_hash => {
                results[slot] = Some(json!({
                    "key": key,
                    "status": "queued",
                    "partition": rec.partition_id,
                    "duplicate": true,
                }));
            }
            Some(prior) => {
                results[slot] = Some(json!({
                    "key": key,
                    "status": "error",
                    "error": "conflict",
                    "existing_hash": prior.content_hash.to_prefixed(),
                }));
            }
            None => {
                let blob_store = state.blob_store.clone();
                let permits = blob_concurrency.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = permits.acquire().await;
                    let outcome = blob_store
                        .put(bytes::Bytes::from(value_str))
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            ApiError::Internal(format!("failed to store value blob: {e}"))
                        });
                    (slot, key, rec, outcome)
                }));
            }
        }
    }

    let mut prepared: Vec<(usize, String, IndexRecord)> = Vec::new();
    for task in tasks {
        let (slot, key, rec, outcome) =
            task.await.map_err(|e| ApiError::Internal(e.to_string()))?;
        match outcome {
            Ok(()) => prepared.push((slot, key, rec)),
            Err(err) => {
                results[slot] = Some(json!({
                    "key": key,
                    "status": "error",
                    "error": err.code(),
                    "message": err.to_string(),
                }))
            }
        }
    }

    let submissions: Vec<(String, IndexRecord)> = prepared
        .iter()
        .map(|(_, key, rec)| (key.clone(), rec.clone()))
        .collect();
    let index = state.index.clone();
    let outcomes = tokio::task::spawn_blocking(move || index.submit_many(&submissions))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    for ((slot, key, record), outcome) in prepared.into_iter().zip(outcomes) {
        results[slot] = Some(match outcome {
            SubmitOutcome::Conflict { existing_hash } => json!({
                "key": key,
                "status": "error",
                "error": "conflict",
                "existing_hash": existing_hash.to_prefixed(),
            }),
            outcome => json!({
                "key": key,
                "status": "queued",
                "partition": record.partition_id,
                "duplicate": matches!(outcome, SubmitOutcome::DuplicateIdentical),
            }),
        });
    }
    Ok((
        StatusCode::MULTI_STATUS,
        Json(json!({ "results": results })),
    )
        .into_response())
}

/// Convenience read-through (index row + value bytes from the local blob
/// store) so demos can verify a write without standing up a reader node.
/// The canonical read path is still `storectl` over iroh.
pub async fn get_record(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, ApiError> {
    let index = state.index.clone();
    let key_for_index = key.clone();
    let record = tokio::task::spawn_blocking(move || index.get(&key_for_index))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let Some(record) = record else {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({ "key": key, "status": "not_found" })),
        )
            .into_response());
    };

    let mut body = json!({
        "key": key,
        "status": record.status.as_str(),
        "hash": record.content_hash.to_prefixed(),
        "partition": record.partition_id,
        "size": record.size,
        "created_at": record.created_at.to_rfc3339(),
    });
    if let Some(published_at) = record.published_at {
        body["published_at"] = json!(published_at.to_rfc3339());
    }
    if let Ok(Some(bytes)) = state.blob_store.get(&record.content_hash).await {
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            body["value"] = value;
        }
    }
    Ok(Json(body).into_response())
}

pub async fn health(State(state): State<AppState>) -> Response {
    let index = state.index.clone();
    let stats = tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((index.queue_depth()?, index.records_total()?))
    })
    .await;
    let (queue_depth, records_total) = match stats {
        Ok(Ok(pair)) => pair,
        _ => (0, 0),
    };
    let last_publish = state
        .publisher_status
        .last_publish_unix
        .load(Ordering::Relaxed);
    let body = json!({
        "status": "ok",
        "queue_depth": queue_depth,
        "records_total": records_total,
        "partitions": state.top_level_partitions,
        "endpoint_id": *state.endpoint_id,
        "last_publish_at": if last_publish > 0 {
            chrono::DateTime::from_timestamp(last_publish, 0).map(|t| t.to_rfc3339())
        } else {
            None
        },
        "cycles_completed": state.publisher_status.cycles_completed.load(Ordering::Relaxed),
        "records_published": state.publisher_status.records_published.load(Ordering::Relaxed),
        "last_publish_error": *state.publisher_status.last_error.lock().expect("status lock"),
    });
    Json(body).into_response()
}

/// The read ticket is public by design: it grants read-only access to the
/// pointer document, which is the point of running a registry.
pub async fn ticket(State(state): State<AppState>) -> Response {
    state.read_ticket.as_str().to_owned().into_response()
}
