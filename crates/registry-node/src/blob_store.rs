//! `registry_core::BlobStore` implementation backed by a real `iroh-blobs`
//! store — docs/data-model.md, "iroh-blobs content store".
//! The writer (`registryd`) only ever needs the local store, since it
//! produced every blob it reads back. Readers (`storectl` and other reader
//! nodes) may need to fetch a blob they don't have cached yet from one
//! of the known bootstrap providers embedded in the root pointer document's
//! ticket — that fetch-on-miss behavior is what distinguishes
//! [`IrohBlobStore`] from a bare local-only wrapper.

use async_trait::async_trait;
use bytes::Bytes;
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::api::Store as BlobsStore;
use registry_core::{BlobStore, BlobStoreError, Hash as CommonHash};

#[derive(Clone)]
pub struct IrohBlobStore {
    store: BlobsStore,
    /// One shared downloader for every fetch this store ever makes. A
    /// `Downloader` owns its own connection pool; constructing a fresh one
    /// per `get` call meant N concurrent gets opened N pools (observed as
    /// a connection stampede that starved every transfer once readers
    /// started fetching in parallel). Built once, cloned cheaply.
    downloader: Option<iroh_blobs::api::downloader::Downloader>,
    endpoint: Option<Endpoint>,
    providers: Vec<EndpointAddr>,
    /// Global cap on in-flight batched prefetch requests across every
    /// clone of this store. The origin may be a small box; a burst of
    /// large batch responses is exactly what OOM-killed a 2 GB production
    /// node — clients must pace themselves.
    prefetch_limit: std::sync::Arc<tokio::sync::Semaphore>,
}

impl IrohBlobStore {
    /// A store with no network fallback — used by `registryd`, which
    /// only ever reads blobs it produced itself locally.
    pub fn local_only(store: BlobsStore) -> Self {
        Self {
            store,
            downloader: None,
            endpoint: None,
            providers: Vec::new(),
            prefetch_limit: std::sync::Arc::new(tokio::sync::Semaphore::new(PREFETCH_CONCURRENCY)),
        }
    }

    /// A store that falls back to fetching from `providers` (typically the
    /// root pointer document ticket's bootstrap nodes) on a local cache
    /// miss — used by `storectl` and other reader nodes.
    pub fn with_providers(
        store: BlobsStore,
        endpoint: Endpoint,
        providers: Vec<EndpointAddr>,
    ) -> Self {
        let downloader = Some(store.downloader(&endpoint));
        Self {
            store,
            downloader,
            endpoint: Some(endpoint),
            providers,
            prefetch_limit: std::sync::Arc::new(tokio::sync::Semaphore::new(PREFETCH_CONCURRENCY)),
        }
    }

    pub fn inner(&self) -> &BlobsStore {
        &self.store
    }
}

/// See [`IrohBlobStore::prefetch_limit`].
const PREFETCH_CONCURRENCY: usize = 2;

fn to_iroh_hash(hash: &CommonHash) -> iroh_blobs::Hash {
    iroh_blobs::Hash::from_bytes(hash.0)
}

fn from_iroh_hash(hash: iroh_blobs::Hash) -> CommonHash {
    CommonHash(*hash.as_bytes())
}

#[async_trait]
impl BlobStore for IrohBlobStore {
    async fn put(&self, bytes: Bytes) -> Result<CommonHash, BlobStoreError> {
        let tag = self
            .store
            .blobs()
            .add_bytes(bytes)
            .await
            .map_err(|e| BlobStoreError::Io(e.to_string()))?;
        Ok(from_iroh_hash(tag.hash))
    }

    /// Pull every blob in `hashes` that is missing locally with batched
    /// `GetMany` requests over a single connection per provider — one
    /// round-trip per batch instead of one per blob, which is what makes
    /// bulk index walks over a WAN link fast. Best-effort: any blob still
    /// missing afterwards is picked up by the per-blob `get` fallback.
    async fn prefetch(&self, hashes: &[CommonHash], budget: std::time::Duration) {
        /// Bound one request's size; a HAMT level can hold tens of
        /// thousands of nodes, and index leaves run ~200 KB each. Sized
        /// so one response stays in the low megabytes.
        const BATCH: usize = 32;
        let deadline = tokio::time::Instant::now() + budget;
        let (Some(endpoint), false) = (&self.endpoint, self.providers.is_empty()) else {
            return;
        };
        // Presence filtering is read-only, so it is safe to abandon on
        // deadline — unlike a transfer, which is always run to completion.
        let missing = match tokio::time::timeout_at(deadline, async {
            let mut missing = Vec::new();
            for hash in hashes {
                let iroh_hash = to_iroh_hash(hash);
                match self.store.blobs().has(iroh_hash).await {
                    Ok(true) => {}
                    Ok(false) | Err(_) => missing.push(iroh_hash),
                }
            }
            missing
        })
        .await
        {
            Ok(missing) => missing,
            Err(_) => return,
        };
        if missing.is_empty() {
            return;
        }
        for provider in &self.providers {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            let Ok(Ok(conn)) = tokio::time::timeout_at(
                deadline,
                endpoint.connect(provider.clone(), iroh_blobs::protocol::ALPN),
            )
            .await
            else {
                continue;
            };
            let remote = self.store.remote();
            let mut fetched_all = true;
            for chunk in missing.chunks(BATCH) {
                // Deadline enforced BETWEEN requests only: waiting for a
                // pacing permit is cancel-safe, the transfer itself never
                // is.
                let Ok(Ok(_permit)) =
                    tokio::time::timeout_at(deadline, self.prefetch_limit.acquire()).await
                else {
                    return;
                };
                let mut request = iroh_blobs::protocol::GetManyRequest::builder();
                for hash in chunk {
                    request = request.hash(*hash, iroh_blobs::protocol::ChunkRanges::all());
                }
                if remote
                    .execute_get_many(conn.clone(), request.build())
                    .complete()
                    .await
                    .is_err()
                {
                    fetched_all = false;
                    break;
                }
            }
            if fetched_all {
                return;
            }
        }
    }

    async fn get(&self, hash: &CommonHash) -> Result<Option<Bytes>, BlobStoreError> {
        let iroh_hash = to_iroh_hash(hash);

        // iroh-blobs conflates "missing" and transient IO errors in
        // get_bytes. Treating a transient error as a miss is catastrophic
        // for writers: a false miss on a current root makes the rebuild
        // path silently reconstruct the partition FROM EMPTY, discarding
        // its accumulated records (observed live as shrinking partition
        // counts under sustained storage load). A local-only store must
        // therefore retry before concluding "miss". A provider-backed
        // store must NOT: its next step is the downloader, which either
        // confirms the blob (fetching it if genuinely absent) or fails —
        // and paying 300 ms of retry sleeps on every one of the millions
        // of cache misses in a bulk sync dwarfs the walk itself.
        let can_fetch = self.downloader.is_some() && !self.providers.is_empty();
        let attempts: u32 = if can_fetch { 1 } else { 3 };
        for attempt in 1..=attempts {
            match self.store.blobs().get_bytes(iroh_hash).await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(_) if attempt < attempts => {
                    tokio::time::sleep(std::time::Duration::from_millis(100 * attempt as u64))
                        .await;
                }
                Err(_) => {}
            }
        }

        let (Some(downloader), false) = (&self.downloader, self.providers.is_empty()) else {
            return Ok(None);
        };

        let provider_ids = self.providers.iter().map(|addr| addr.id).collect();
        let providers = iroh_blobs::api::downloader::Shuffled::new(provider_ids);
        if downloader.download(iroh_hash, providers).await.is_err() {
            return Ok(None);
        }

        match self.store.blobs().get_bytes(iroh_hash).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(_) => Ok(None),
        }
    }
}
