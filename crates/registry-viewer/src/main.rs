//! registry-viewer — a micro web app for reviewing a registry through a
//! reader node's eyes. It joins the network with the same read ticket any
//! `storectl` user would (no privileged access, no daemon APIs), then
//! serves a small local dashboard: per-partition counts, record listings,
//! and key lookup. Point it at a local `registryd` for demos, or at any
//! remote deployment.
//!
//!   registry-viewer --ticket <ticket>          # then open http://127.0.0.1:8090
//!   registry-viewer --ticket-url http://<origin>:8080/ticket   # fresh machine
//!
//! The ticket resolves exactly like storectl's (flag, env, config file),
//! so a machine already set up for `storectl get` needs no flags at all.
//! `--ticket-url` fetches a fresh ticket from a running registryd's public
//! `/ticket` endpoint — the one-command bootstrap for a brand-new machine,
//! and immune to the stale direct-address problem of long-stored tickets.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::get;
use axum::Router;
use n0_future::StreamExt;
use registry_core::{hamt, partition_id_for_key, BlobStore, Hash, LeafEntry};
use registry_node::{identity, IrohBlobStore, PointerDoc, RegistryNode};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Cap on fully cached partition listings — a partition's entry list is
/// kept in memory keyed by its root hash so paging and re-sorting are
/// instant, but a full registry can hold millions of entries; the viewer
/// is a review tool, not a database.
const MAX_CACHED_PARTITIONS: usize = 256;

/// Give up on a partition whose index blobs no peer serves within this
/// window when a *request handler* is doing the walk. Local nodes answer
/// in milliseconds; WAN reads of a populated partition take seconds with
/// the parallel walker.
const WALK_TIMEOUT: Duration = Duration::from_secs(30);

/// The background sync loop's per-partition budget against the network —
/// generous, because a first-ever sync of a large partition fetches its
/// entire index.
const SYNC_WALK_TIMEOUT: Duration = Duration::from_secs(300);

/// Partitions synced concurrently by the background loop. Network reads
/// are batched and globally paced inside the blob store (see
/// `IrohBlobStore::prefetch`), so this mostly governs how many partitions
/// overlap their local reads and decode work.
const PARTITION_CONCURRENCY: usize = 4;

/// In-flight blob reads within one partition walk.
const FETCH_CONCURRENCY: usize = 24;

/// Idle delay between sync passes when nothing changed and no doc event
/// arrived. Doc events (a publish at the origin) wake the loop instantly,
/// so this is only the fallback cadence.
const PASS_IDLE: Duration = Duration::from_secs(15);

/// Every Nth pass re-walks every partition even when its root looks
/// unchanged, healing any local-store loss the cheap root comparison
/// cannot see.
const FULL_PASS_EVERY: u64 = 120;

#[derive(clap::Parser)]
#[command(name = "registry-viewer", version, about)]
struct Cli {
    /// Read ticket (falls back to STORECTL_READ_TICKET, then storectl's
    /// config file — same resolution as the CLI reader).
    #[arg(long)]
    ticket: Option<String>,

    /// Fetch the read ticket from a running registryd's public /ticket
    /// endpoint (e.g. http://origin-host:8080/ticket). The zero-config
    /// path for a fresh machine; ignored when --ticket is given.
    #[arg(long, env = "VIEWER_TICKET_URL")]
    ticket_url: Option<String>,

    /// Where the dashboard listens.
    #[arg(long, default_value = "127.0.0.1:8090")]
    bind: SocketAddr,

    /// Local cache directory (identity + synced blobs). Defaults to a
    /// viewer-specific directory so it never fights storectl over locks.
    #[arg(long)]
    storage_dir: Option<PathBuf>,

    /// Throwaway node identity, nothing persisted.
    #[arg(long)]
    ephemeral: bool,

    /// Also replicate every record's value bytes locally (full seeder
    /// behavior). Off by default: listings are always kept synced, but
    /// values are fetched on demand — a full value replica of a large
    /// registry can be hundreds of GB.
    #[arg(long)]
    warm_values: bool,

    /// `auto` keeps every partition's index replicated continuously.
    /// `manual` replicates only the (tiny) pointer document — partition
    /// index blobs are fetched exclusively when a sync is requested via
    /// the UI or `POST /api/sync[/:id]`, so a node can stay
    /// near-zero-footprint and pull data on demand.
    #[arg(long, value_enum, default_value_t = SyncMode::Auto)]
    sync_mode: SyncMode,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum SyncMode {
    Auto,
    Manual,
}

/// One cached partition listing: the root it was walked at plus its
/// sorted entries.
type CachedListing = (Hash, Arc<Vec<LeafEntry>>);

/// A pointer-document root read, bounded: the underlying blob fetch can
/// otherwise hang indefinitely on an unreachable provider and wedge the
/// caller.
async fn get_root_bounded(state: &Viewer, partition_id: u32) -> anyhow::Result<Option<Hash>> {
    tokio::time::timeout(
        Duration::from_secs(10),
        state.pointer.get_partition_root(partition_id),
    )
    .await
    .map_err(|_| anyhow::anyhow!("root read timed out"))?
}

/// Continuously maintained per-partition sync state — what makes every
/// partition answerable locally, all the time.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct PartitionSync {
    root: String,
    records: usize,
    synced_at: chrono::DateTime<chrono::Utc>,
    /// Newest `added_at` among the partition's records — when data was
    /// last inserted into the REGISTRY's storage (the write daemon's
    /// index), as opposed to when this node happened to sync it.
    #[serde(default)]
    latest_added_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Set when the latest warm attempt failed; the previous good state
    /// stays served.
    stale_error: Option<String>,
}

/// Aggregate progress of the background loop, served verbatim by the API
/// so the UI can show live sync state without touching the network.
#[derive(Clone, Default, serde::Serialize)]
struct SyncStats {
    /// Partitions whose current root is present in the (locally
    /// replicated) pointer document.
    roots_known: u32,
    /// Partitions whose doc root differs from the last walked root —
    /// known to exist but not yet (re)synced.
    behind: u32,
    pass: u64,
    last_pass_at: Option<chrono::DateTime<chrono::Utc>>,
    last_pass_secs: Option<f64>,
}

struct Viewer {
    state_path: std::path::PathBuf,
    pointer: PointerDoc,
    /// Bootstrap peers from the ticket — re-dialed periodically so doc
    /// sync recovers whenever the origin comes (back) online.
    ticket_nodes: Vec<registry_node::iroh::EndpointAddr>,
    blob_store: IrohBlobStore,
    /// Same store, no network fallback — walks against it are instant and
    /// can never hang on an unreachable provider.
    local_blob_store: IrohBlobStore,
    endpoint_id: String,
    ticket_source: String,
    /// The resolved read ticket, verbatim — public by design (the origin
    /// serves it unauthenticated on /ticket), surfaced so the UI can show
    /// it for handing to other machines.
    read_ticket: String,
    providers: usize,
    partitions: u32,
    warm_values: bool,
    sync_mode: SyncMode,
    /// Partitions whose sync was explicitly requested (UI button / API).
    forced: std::sync::Mutex<std::collections::BTreeSet<u32>>,
    /// A full forced pass was requested.
    force_all: std::sync::atomic::AtomicBool,
    started_at: chrono::DateTime<chrono::Utc>,
    /// partition → [`CachedListing`]. Invalidated automatically when the
    /// partition's root moves.
    listing_cache: tokio::sync::Mutex<HashMap<u32, CachedListing>>,
    /// Background sync loop's per-partition state; served instantly by
    /// `/api/partitions`.
    sync_state: std::sync::RwLock<HashMap<u32, PartitionSync>>,
    /// Latest snapshot of the pointer document's roots — what the origin
    /// currently publishes, as opposed to what has been walked locally.
    doc_roots: std::sync::RwLock<HashMap<u32, Hash>>,
    stats: std::sync::RwLock<SyncStats>,
    /// Partitions currently being walked by the sync loop.
    syncing: AtomicU32,
    /// Pinged by the doc-event subscription whenever the pointer document
    /// changes, so a publish at the origin triggers a sync pass
    /// immediately instead of waiting out the poll interval.
    wake: tokio::sync::Notify,
    /// Set when the process is shutting down: the sync loop must stop
    /// starting work and drain quickly so the node can close its store
    /// cleanly. Closing under in-flight blob writes is what turns a
    /// routine restart into a full store verification on the next start.
    stopping: std::sync::atomic::AtomicBool,
    /// Bearer key for the central CommonsDB metadata API
    /// (`COMMONSDB_API_KEY`, e.g. via a `.env` file — see `.env.example`).
    /// Kept server-side: the browser talks only to this viewer, which
    /// proxies the call so the key is never exposed to the page.
    commonsdb_api_key: Option<String>,
    /// Base URL for that API (`COMMONSDB_METADATA_URL`), `<base><cid>`.
    commonsdb_metadata_url: String,
    /// Compact similarity-search index: all content codes in one
    /// contiguous array (~16 bytes/record). The listing cache's scattered,
    /// string-heavy entries get compressed/paged out under memory
    /// pressure, turning a scan into seconds of page-ins; this array
    /// keeps NNS fast regardless. Rebuilt lazily after any partition
    /// update; `None` means stale.
    code_index: std::sync::RwLock<Option<Arc<CodeIndex>>>,
}

/// Self-consistent snapshot for similarity search: `codes` refers into
/// `listings` by (slot, entry index), and `listings` pins the exact entry
/// vectors the codes were extracted from — a concurrent partition update
/// invalidates the shared index but can never skew an in-flight search.
struct CodeIndex {
    /// (content code, listing slot, entry index within that listing)
    codes: Vec<(u64, u32, u32)>,
    /// (partition id, entries) per slot.
    listings: Vec<(u32, Arc<Vec<LeafEntry>>)>,
}

/// Snapshot of every partition root the pointer doc currently carries,
/// bounded so an unreachable provider can only delay a pass, not wedge it.
async fn doc_roots_snapshot(state: &Viewer) -> Option<HashMap<u32, Hash>> {
    match tokio::time::timeout(Duration::from_secs(120), state.pointer.partition_roots()).await {
        Ok(Ok(roots)) => Some(roots),
        Ok(Err(err)) => {
            tracing::debug!(error = %err, "sync: root snapshot failed");
            None
        }
        Err(_) => {
            tracing::debug!("sync: root snapshot timed out");
            None
        }
    }
}

fn note_doc_roots(state: &Viewer, roots: &HashMap<u32, Hash>) {
    *state.doc_roots.write().expect("doc roots lock") = roots.clone();
    state.stats.write().expect("stats lock").roots_known = roots.len() as u32;
}

/// Partitions whose walked root differs from the doc root right now.
fn behind_count(state: &Viewer) -> u32 {
    let synced = state.sync_state.read().expect("sync lock");
    state
        .doc_roots
        .read()
        .expect("doc roots lock")
        .iter()
        .filter(|(id, root)| {
            synced.get(id).map(|s| s.root != root.to_hex()).unwrap_or(true)
        })
        .count() as u32
}

/// Walk one partition and refresh its caches. Local store first (instant,
/// cannot hang), network only for whatever is genuinely missing. `force`
/// bypasses the listing cache so an explicit user request re-verifies the
/// local blobs even when the root has not moved.
async fn sync_partition(state: &Viewer, partition_id: u32, root: Hash, force: bool) {
    state.syncing.fetch_add(1, Ordering::Relaxed);
    let result: anyhow::Result<Arc<Vec<LeafEntry>>> = async {
        if !force {
            let cache = state.listing_cache.lock().await;
            if let Some((cached_root, entries)) = cache.get(&partition_id) {
                if *cached_root == root {
                    return Ok(entries.clone());
                }
            }
        }
        let mut outcome = hamt::walk_entries_parallel(
            &state.local_blob_store,
            root,
            Duration::from_secs(20),
            FETCH_CONCURRENCY,
        )
        .await;
        if !outcome.complete {
            outcome = hamt::walk_entries_parallel(
                &state.blob_store,
                root,
                SYNC_WALK_TIMEOUT,
                FETCH_CONCURRENCY,
            )
            .await;
        }
        if !outcome.complete {
            anyhow::bail!(
                "index incomplete within {}s (origin busy or unreachable)",
                SYNC_WALK_TIMEOUT.as_secs()
            );
        }
        Ok(Arc::new(outcome.entries))
    }
    .await;

    match result {
        Ok(entries) => {
            if state.warm_values {
                // Full seeder mode: values replicated locally too — via
                // the batched prefetch (already-present blobs are
                // filtered, so re-warms are cheap and interrupted warms
                // resume where they left off). Checked per chunk so a
                // shutdown drains within one small request.
                let hashes: Vec<Hash> = entries.iter().map(|e| e.hash).collect();
                for chunk in hashes.chunks(256) {
                    if state.stopping.load(Ordering::Relaxed) {
                        break;
                    }
                    state
                        .blob_store
                        .prefetch(chunk, Duration::from_secs(600))
                        .await;
                }
            }
            let count = entries.len();
            let latest_added_at = entries.iter().map(|e| e.added_at).max();
            {
                let mut cache = state.listing_cache.lock().await;
                if cache.len() >= MAX_CACHED_PARTITIONS && !cache.contains_key(&partition_id) {
                    cache.clear();
                }
                cache.insert(partition_id, (root, entries));
            }
            state.sync_state.write().expect("sync lock").insert(
                partition_id,
                PartitionSync {
                    root: root.to_hex(),
                    records: count,
                    synced_at: chrono::Utc::now(),
                    latest_added_at,
                    stale_error: None,
                },
            );
            *state.code_index.write().expect("code index lock") = None;
        }
        Err(err) => mark_stale(state, partition_id, err.to_string()),
    }
    state.syncing.fetch_sub(1, Ordering::Relaxed);
}

/// Any pointer-document activity (a publish landing, sync completing)
/// wakes the sync loop immediately. The subscription is re-established if
/// the stream ever ends.
async fn doc_event_wake(state: AppState) {
    loop {
        match state.pointer.doc().subscribe().await {
            Ok(mut events) => {
                while let Some(event) = events.next().await {
                    if event.is_err() {
                        break;
                    }
                    state.wake.notify_one();
                }
            }
            Err(err) => {
                tracing::debug!(error = %err, "doc event subscription failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Background loop: keep every partition's index nodes and value blobs
/// present in the local store, and its listing cached, re-checking roots
/// continuously — the origin being busy or unreachable then only delays
/// fresh data, it never makes partitions unavailable. Changed partitions
/// sync [`PARTITION_CONCURRENCY`] at a time, each walking its index with
/// [`FETCH_CONCURRENCY`] blob reads in flight.
async fn sync_loop(state: AppState) {
    // Last-known state from disk: the UI always has SOMETHING to show,
    // immediately, regardless of restarts, root moves, or origin health.
    let mut persisted: HashMap<u32, PartitionSync> = std::fs::read_to_string(&state.state_path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default();
    // Entries without a root never held data (written by older builds'
    // failure paths) — restoring them would render empty partitions.
    persisted.retain(|_, v| !v.root.is_empty());
    if !persisted.is_empty() {
        let mut map = state.sync_state.write().expect("sync lock");
        for (k, mut v) in persisted.clone() {
            v.stale_error = Some("restored from disk; refreshing".into());
            map.insert(k, v);
        }
        tracing::info!(
            partitions = map.len(),
            "startup: overview restored from disk"
        );
    }

    tokio::spawn(doc_event_wake(state.clone()));

    // Then upgrade whatever the LOCAL store can actually walk — current
    // root first, last-known root as fallback (root may have moved to a
    // tree we cannot fetch yet). Purely local, so failures are cheap and
    // the walks can run wide.
    let roots = doc_roots_snapshot(&state).await.unwrap_or_default();
    note_doc_roots(&state, &roots);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
    let mut walks = tokio::task::JoinSet::new();
    for partition_id in 0..state.partitions {
        let current = roots.get(&partition_id).copied();
        let fallback = persisted
            .get(&partition_id)
            .and_then(|s| Hash::from_hex(&s.root).ok())
            .filter(|f| Some(*f) != current);
        let local = state.local_blob_store.clone();
        let semaphore = semaphore.clone();
        walks.spawn(async move {
            let _permit = semaphore.acquire().await;
            for root in [current, fallback].into_iter().flatten() {
                let outcome = hamt::walk_entries_parallel(
                    &local,
                    root,
                    Duration::from_secs(20),
                    FETCH_CONCURRENCY,
                )
                .await;
                if outcome.complete {
                    return Some((partition_id, root, outcome.entries));
                }
            }
            None
        });
    }
    let mut restored = 0u32;
    while let Some(walked) = walks.join_next().await {
        let Ok(Some((partition_id, root, entries))) = walked else {
            continue;
        };
        let count = entries.len();
        let latest_added_at = entries.iter().map(|e| e.added_at).max();
        state
            .listing_cache
            .lock()
            .await
            .insert(partition_id, (root, Arc::new(entries)));
        state.sync_state.write().expect("sync lock").insert(
            partition_id,
            PartitionSync {
                root: root.to_hex(),
                records: count,
                synced_at: chrono::Utc::now(),
                latest_added_at,
                stale_error: None,
            },
        );
        restored += 1;
    }
    tracing::info!(restored, "startup: partitions walked from local store");
    *state.code_index.write().expect("code index lock") = None;
    persist_state(&state);

    let mut pass = 0u64;
    loop {
        if state.stopping.load(Ordering::Relaxed) {
            tracing::info!("sync loop drained for shutdown");
            return;
        }
        // Re-nudge doc sync every pass: a broken session (origin restart,
        // network blip, loader windows) must recover without a viewer
        // restart.
        if let Err(err) = state
            .pointer
            .doc()
            .start_sync(state.ticket_nodes.clone())
            .await
        {
            tracing::debug!(error = %err, "sync: start_sync nudge failed");
        }

        let pass_started = tokio::time::Instant::now();
        let Some(roots) = doc_roots_snapshot(&state).await else {
            tokio::time::sleep(PASS_IDLE).await;
            continue;
        };
        note_doc_roots(&state, &roots);

        // Explicit requests (UI buttons, POST /api/sync) always sync and
        // always force a re-walk; in auto mode changed partitions join
        // them, in manual mode nothing else does — the pointer document
        // stays replicated either way, so `behind`/`pending` keep
        // reporting exactly how far the local replica lags.
        let force_all = state.force_all.swap(false, Ordering::Relaxed);
        let forced: std::collections::BTreeSet<u32> =
            std::mem::take(&mut *state.forced.lock().expect("forced lock"));
        let auto = state.sync_mode == SyncMode::Auto;
        let full = auto && pass > 0 && pass.is_multiple_of(FULL_PASS_EVERY);
        let work: Vec<(u32, Hash, bool)> = {
            let synced = state.sync_state.read().expect("sync lock");
            roots
                .iter()
                .filter_map(|(id, root)| {
                    let requested = force_all || forced.contains(id);
                    let changed = synced
                        .get(id)
                        .map(|s| s.root != root.to_hex() || s.stale_error.is_some())
                        .unwrap_or(true);
                    if requested || (auto && (full || changed)) {
                        Some((*id, *root, requested))
                    } else {
                        None
                    }
                })
                .collect()
        };
        state.stats.write().expect("stats lock").behind = behind_count(&state);

        if !work.is_empty() {
            tracing::info!(
                partitions = work.len(),
                pass,
                "sync: walking changed partitions"
            );
            let semaphore = Arc::new(tokio::sync::Semaphore::new(PARTITION_CONCURRENCY));
            let mut tasks = tokio::task::JoinSet::new();
            for (partition_id, root, requested) in work.iter().copied() {
                let state = state.clone();
                let semaphore = semaphore.clone();
                tasks.spawn(async move {
                    let _permit = semaphore.acquire().await;
                    if state.stopping.load(Ordering::Relaxed) {
                        return;
                    }
                    sync_partition(&state, partition_id, root, requested).await;
                });
            }
            while tasks.join_next().await.is_some() {}
            persist_state(&state);
        }
        let progressed = {
            let synced = state.sync_state.read().expect("sync lock");
            work.iter()
                .filter(|(id, root, _)| {
                    synced
                        .get(id)
                        .map(|s| s.root == root.to_hex() && s.stale_error.is_none())
                        .unwrap_or(false)
                })
                .count()
        };

        pass += 1;
        {
            let behind = behind_count(&state);
            let mut stats = state.stats.write().expect("stats lock");
            stats.pass = pass;
            stats.behind = behind;
            stats.last_pass_at = Some(chrono::Utc::now());
            stats.last_pass_secs = Some(pass_started.elapsed().as_secs_f64());
        }

        // A pass that made progress is immediately followed by another —
        // during a bulk catch-up new roots keep landing while we walk. A
        // pass that only failed (origin down, partitions unfetchable)
        // waits like an idle one, so persistent failures never turn into
        // a hot retry loop — and manual mode always waits, since being
        // behind is its normal state. A doc event or an explicit sync
        // request cuts any wait short.
        if state.sync_mode == SyncMode::Manual
            || behind_count(&state) == 0
            || progressed == 0
        {
            tokio::select! {
                _ = state.wake.notified() => {
                    // Debounce: a publish updates many partitions in quick
                    // succession; let the batch land before snapshotting.
                    tokio::time::sleep(Duration::from_millis(750)).await;
                }
                _ = tokio::time::sleep(PASS_IDLE) => {}
            }
        }
    }
}

fn persist_state(state: &Viewer) {
    let map = state.sync_state.read().expect("sync lock").clone();
    if let Ok(raw) = serde_json::to_string(&map) {
        // Write-then-rename: a crash mid-write must never truncate the
        // only copy of the restore state.
        let tmp = state.state_path.with_extension("json.tmp");
        if std::fs::write(&tmp, raw).is_ok() {
            let _ = std::fs::rename(&tmp, &state.state_path);
        }
    }
}

fn mark_stale(state: &Viewer, partition_id: u32, error: String) {
    // Only annotate partitions that have served data before. A partition
    // that was never walked stays absent here — the overview reports it
    // as pending (root known, sync queued), which is the truth; a
    // zero-record placeholder would render as an empty partition.
    let mut map = state.sync_state.write().expect("sync lock");
    if let Some(entry) = map.get_mut(&partition_id) {
        entry.stale_error = Some(error);
    }
    tracing::debug!(
        partition_id,
        "sync: partition warm failed; serving last good state"
    );
}

type AppState = Arc<Viewer>;

/// Fetch a read ticket from a registryd /ticket endpoint — plain text,
/// public by design. Hand-rolled HTTP/1.0 GET over a plain socket: the
/// endpoint is a one-line text response on a LAN/WAN port, and pulling
/// in a full HTTP client (plus a TLS stack) for that is not worth it.
/// HTTP/1.0 with `Connection: close` also rules out chunked encoding, so
/// the body is simply everything after the header terminator.
async fn fetch_ticket(url: &str) -> anyhow::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        anyhow::anyhow!("--ticket-url must be a plain http:// URL, got {url}")
    })?;
    let (host_port, path) = match rest.split_once('/') {
        Some((host_port, path)) => (host_port, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    let addr = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:80")
    };
    let mut stream = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connecting to {addr} timed out"))?
    .map_err(|e| anyhow::anyhow!("connecting to {addr}: {e}"))?;
    let host = host_port.split(':').next().unwrap_or(host_port);
    stream
        .write_all(
            format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(15), stream.read_to_end(&mut response))
        .await
        .map_err(|_| anyhow::anyhow!("reading ticket from {url} timed out"))??;
    let response = String::from_utf8_lossy(&response);
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response from {url}"))?;
    let status_line = head.lines().next().unwrap_or_default();
    let ok = status_line
        .split_whitespace()
        .nth(1)
        .map(|code| code.starts_with('2'))
        .unwrap_or(false);
    if !ok {
        anyhow::bail!("{url} answered: {status_line}");
    }
    let ticket = body.trim().to_string();
    if ticket.is_empty() {
        anyhow::bail!("{url} returned an empty ticket");
    }
    Ok(ticket)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        "info,iroh=error,noq_proto=error,iroh_docs=error,iroh_blobs=error,\
         iroh_gossip=error,iroh_relay=error,iroh_util=error,netwatch=error,portmapper=error"
            .to_string()
    });
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .init();

    registry_core::config::load_dotenv();
    let cli = <Cli as clap::Parser>::parse();
    let storage_dir = cli.storage_dir.or_else(|| {
        dirs_storage_default() // viewer's own cache dir
    });
    let (cli_ticket, fetched_source) = match (cli.ticket, &cli.ticket_url) {
        (Some(ticket), _) => (Some(ticket), None),
        (None, Some(url)) => {
            let ticket = fetch_ticket(url).await?;
            tracing::info!(url = %url, "read ticket fetched from origin");
            (Some(ticket), Some("--ticket-url"))
        }
        (None, None) => (None, None),
    };
    let cfg = storectl::config::resolve(cli_ticket, storage_dir)?;
    let ticket_source = fetched_source.unwrap_or(cfg.read_ticket_source).to_string();
    let ticket: registry_node::iroh_docs::DocTicket = cfg
        .read_ticket
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid read ticket: {e}"))?;

    let node = if cli.ephemeral {
        RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?
    } else {
        let secret_key =
            identity::load_or_generate_secret_key(&cfg.storage_dir.join("identity")).await?;
        RegistryNode::spawn_persistent(
            secret_key,
            cfg.storage_dir.join("blobs"),
            cfg.storage_dir.join("docs"),
        )
        .await?
    };

    let doc = node.docs.import(ticket.clone()).await?;
    let blob_store = node.blob_store_with_providers(ticket.nodes.clone());
    let local_blob_store = node.blob_store();
    let pointer = PointerDoc::new(doc, blob_store.clone(), None);

    let ticket_nodes = ticket.nodes.clone();
    let state_path = cfg.storage_dir.join("viewer-state.json");
    let state: AppState = Arc::new(Viewer {
        state_path,
        pointer,
        ticket_nodes,
        blob_store,
        local_blob_store,
        endpoint_id: node.endpoint.id().to_string(),
        ticket_source,
        read_ticket: cfg.read_ticket.clone(),
        providers: ticket.nodes.len(),
        partitions: cfg.top_level_partitions,
        warm_values: cli.warm_values,
        sync_mode: cli.sync_mode,
        forced: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        force_all: std::sync::atomic::AtomicBool::new(false),
        started_at: chrono::Utc::now(),
        listing_cache: tokio::sync::Mutex::new(HashMap::new()),
        sync_state: std::sync::RwLock::new(HashMap::new()),
        doc_roots: std::sync::RwLock::new(HashMap::new()),
        stats: std::sync::RwLock::new(SyncStats::default()),
        syncing: AtomicU32::new(0),
        wake: tokio::sync::Notify::new(),
        stopping: std::sync::atomic::AtomicBool::new(false),
        commonsdb_api_key: std::env::var("COMMONSDB_API_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty()),
        commonsdb_metadata_url: std::env::var("COMMONSDB_METADATA_URL")
            .ok()
            .filter(|u| !u.trim().is_empty())
            .unwrap_or_else(|| "https://api.commonsdb.org/v1/metadata/".to_string()),
        code_index: std::sync::RwLock::new(None),
    });

    let sync_handle = tokio::spawn(sync_loop(state.clone()));

    let app = Router::new()
        .route("/", get(index_html))
        .route("/api/status", get(status))
        .route("/api/partitions", get(partitions_overview))
        .route("/api/partition/:id", get(partition))
        .route("/api/record/:key", get(record))
        .route("/api/sync", axum::routing::post(sync_all))
        .route("/api/sync/:id", axum::routing::post(sync_one))
        .route("/api/similar", get(similar))
        .route("/api/external-metadata/:key", get(external_metadata))
        .layer(axum::middleware::from_fn(cors_for_reads))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
    tracing::info!(addr = %cli.bind, "registry-viewer listening — open http://{}", cli.bind);

    #[cfg(unix)]
    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    };
    #[cfg(not(unix))]
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    // Drain the sync loop BEFORE closing the node: closing the store
    // under in-flight blob writes costs a full verification scan on the
    // next start. Warm transfers check the flag between small chunks, so
    // the drain is normally a few seconds.
    state.stopping.store(true, Ordering::Relaxed);
    state.wake.notify_waiters();
    if tokio::time::timeout(Duration::from_secs(90), sync_handle)
        .await
        .is_err()
    {
        tracing::warn!("sync loop did not drain within 90s; closing the store anyway");
    }
    node.shutdown().await.ok();
    Ok(())
}

fn dirs_storage_default() -> Option<PathBuf> {
    // Mirrors storectl's platform-default cache location, one directory
    // over, so the two never contend for store locks.
    Some(
        storectl::config::default_storage_dir()
            .parent()
            .map(|p| p.join("registry-viewer"))
            .unwrap_or_else(|| PathBuf::from(".registry-viewer")),
    )
}

async fn index_html() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

fn error_response(status: StatusCode, message: String) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// Open the read-only endpoints to cross-origin browser callers (e.g. a
/// search frontend querying `/api/similar` on a deployed reader node).
/// Only GET gets the header — the mutating routes (`POST /api/sync…`)
/// stay same-origin as far as browsers are concerned.
async fn cors_for_reads(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use axum::http::{header, HeaderValue, Method};
    if request.method() == Method::OPTIONS {
        return (
            StatusCode::NO_CONTENT,
            [
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                (header::ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS"),
                (header::ACCESS_CONTROL_ALLOW_HEADERS, "content-type"),
                (header::ACCESS_CONTROL_MAX_AGE, "86400"),
            ],
        )
            .into_response();
    }
    let is_get = request.method() == Method::GET;
    let mut response = next.run(request).await;
    if is_get {
        response.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
    }
    response
}

/// Instant overview from the background sync loop — no network I/O, no
/// doc reads; safe to poll aggressively.
async fn partitions_overview(State(state): State<AppState>) -> Response {
    let map = state.sync_state.read().expect("sync lock").clone();
    let doc_roots = state.doc_roots.read().expect("doc roots lock").clone();
    let stats = state.stats.read().expect("stats lock").clone();
    let total_records: usize = map.values().map(|s| s.records).sum();
    let detail: HashMap<u32, serde_json::Value> = map
        .iter()
        .map(|(id, s)| {
            let behind = doc_roots
                .get(id)
                .map(|r| r.to_hex() != s.root)
                .unwrap_or(false);
            (
                *id,
                json!({
                    "records": s.records,
                    "synced_at": s.synced_at,
                    "latest_added_at": s.latest_added_at,
                    "stale_error": s.stale_error,
                    "behind": behind,
                }),
            )
        })
        .collect();
    // Newest record insertion (into the registry's storage, not sync
    // time) across everything replicated locally.
    let data_updated_at = map.values().filter_map(|s| s.latest_added_at).max();
    // Partitions the doc knows about but the loop has not walked at all
    // yet — the UI renders them as pending rather than empty.
    let pending: Vec<u32> = doc_roots
        .keys()
        .filter(|id| !map.contains_key(id))
        .copied()
        .collect();
    Json(json!({
        "partitions": state.partitions,
        "synced": map.len(),
        "total_records": total_records,
        "roots_known": stats.roots_known,
        "behind": stats.behind,
        "syncing": state.syncing.load(Ordering::Relaxed),
        "pass": stats.pass,
        "last_pass_at": stats.last_pass_at,
        "last_pass_secs": stats.last_pass_secs,
        "data_updated_at": data_updated_at,
        "sync_mode": match state.sync_mode {
            SyncMode::Auto => "auto",
            SyncMode::Manual => "manual",
        },
        "detail": detail,
        "pending": pending,
    }))
    .into_response()
}

/// Cheap by construction: everything served here is maintained by the
/// background loop; this handler never touches the doc, the store, or the
/// network.
async fn status(State(state): State<AppState>) -> Response {
    let stats = state.stats.read().expect("stats lock").clone();
    Json(json!({
        "endpoint_id": state.endpoint_id,
        "ticket_source": state.ticket_source,
        "read_ticket": state.read_ticket,
        "providers": state.providers,
        "partitions": state.partitions,
        "roots_synced": stats.roots_known,
        "warm_values": state.warm_values,
        "sync_mode": match state.sync_mode {
            SyncMode::Auto => "auto",
            SyncMode::Manual => "manual",
        },
        "started_at": state.started_at.to_rfc3339(),
        "sync": stats,
        "syncing_now": state.syncing.load(Ordering::Relaxed),
    }))
    .into_response()
}

/// Queue an explicit sync of every published partition. In manual mode
/// this is THE way index data arrives; in auto mode it doubles as a
/// full-replica verification (forced walks bypass the listing cache).
async fn sync_all(State(state): State<AppState>) -> Response {
    state.force_all.store(true, Ordering::Relaxed);
    state.wake.notify_one();
    Json(json!({ "queued": "all" })).into_response()
}

/// Queue an explicit sync of one partition.
async fn sync_one(State(state): State<AppState>, Path(id): Path<u32>) -> Response {
    if id >= state.partitions {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("partition must be 0..{}", state.partitions),
        );
    }
    state.forced.lock().expect("forced lock").insert(id);
    state.wake.notify_one();
    Json(json!({ "queued": id })).into_response()
}

#[derive(Deserialize)]
struct PartitionQuery {
    #[serde(default)]
    offset: usize,
    /// 0 = counts only, no entry rows.
    #[serde(default)]
    limit: usize,
}

/// One partition's full listing, cache-first: the background loop keeps
/// the cache current, so paging through a listing costs a map lookup. The
/// slow path (root read + walk) only runs for a partition the loop has
/// not reached yet.
async fn partition_entries(
    state: &Viewer,
    partition_id: u32,
) -> anyhow::Result<Option<CachedListing>> {
    {
        let cache = state.listing_cache.lock().await;
        if let Some((root, entries)) = cache.get(&partition_id) {
            return Ok(Some((*root, entries.clone())));
        }
    }
    let Some(root) = get_root_bounded(state, partition_id).await? else {
        return Ok(None);
    };
    // Everything already replicated answers instantly and offline; only a
    // partition with genuinely missing nodes goes to the network.
    let mut outcome = hamt::walk_entries_parallel(
        &state.local_blob_store,
        root,
        Duration::from_secs(5),
        FETCH_CONCURRENCY,
    )
    .await;
    if !outcome.complete {
        outcome =
            hamt::walk_entries_parallel(&state.blob_store, root, WALK_TIMEOUT, FETCH_CONCURRENCY)
                .await;
    }
    if !outcome.complete {
        anyhow::bail!(
            "no peer served partition {partition_id}'s index within {}s",
            WALK_TIMEOUT.as_secs()
        );
    }
    let entries = Arc::new(outcome.entries);
    {
        let mut cache = state.listing_cache.lock().await;
        if cache.len() >= MAX_CACHED_PARTITIONS && !cache.contains_key(&partition_id) {
            // Simple bounded cache: drop everything rather than tracking LRU.
            cache.clear();
        }
        cache.insert(partition_id, (root, entries.clone()));
    }
    *state.code_index.write().expect("code index lock") = None;
    Ok(Some((root, entries)))
}

async fn partition(
    State(state): State<AppState>,
    Path(id): Path<u32>,
    Query(query): Query<PartitionQuery>,
) -> Response {
    if id >= state.partitions {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("partition must be 0..{}", state.partitions),
        );
    }
    match partition_entries(&state, id).await {
        Ok(None) => Json(json!({
            "partition": id, "root": null, "total": 0, "entries": []
        }))
        .into_response(),
        Ok(Some((root, entries))) => {
            let page: Vec<_> = entries
                .iter()
                .skip(query.offset)
                .take(query.limit)
                .map(|e| {
                    json!({
                        "key": e.key,
                        "hash": e.hash.to_prefixed(),
                        "size": e.size,
                        "added_at": e.added_at.to_rfc3339(),
                        "content_code": e.content_code.map(|c| format!("{c:#018x}")),
                    })
                })
                .collect();
            Json(json!({
                "partition": id,
                "root": root.to_hex(),
                "total": entries.len(),
                "offset": query.offset,
                "entries": page,
            }))
            .into_response()
        }
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err.to_string()),
    }
}

#[derive(Deserialize)]
struct SimilarQuery {
    iscc: String,
    /// Maximum Hamming distance over the 64-bit Content-Code (0..=16).
    max_distance: Option<u32>,
    limit: Option<usize>,
}

/// Exact nearest-neighbor search over ISCC Content-Codes, served entirely
/// from the locally replicated index: every leaf entry already carries
/// its decoded 64-bit code, so a query is one linear scan with popcount
/// distances — exact (no LSH candidate misses), tens of milliseconds for
/// millions of records, and it works with the origin offline.
async fn similar(State(state): State<AppState>, Query(query): Query<SimilarQuery>) -> Response {
    let code = match registry_core::iscc::decode_content_code(query.iscc.trim()) {
        Ok(code) => code,
        Err(err) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("not a decodable ISCC (full code or Content-Code unit): {err}"),
            )
        }
    };
    let max_distance = query.max_distance.unwrap_or(8).min(16);
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let started = std::time::Instant::now();

    // Reuse the compact index when current; rebuild once after any
    // partition update. The build is the only part that touches the big
    // listing cache — a query scans just the ~16-byte-per-record array.
    let index = state.code_index.read().expect("code index lock").clone();
    let index = match index {
        Some(index) => index,
        None => {
            let listings: Vec<(u32, Arc<Vec<LeafEntry>>)> = state
                .listing_cache
                .lock()
                .await
                .iter()
                .map(|(id, (_, entries))| (*id, entries.clone()))
                .collect();
            let built = tokio::task::spawn_blocking(move || {
                let total: usize = listings.iter().map(|(_, e)| e.len()).sum();
                let mut codes = Vec::with_capacity(total);
                for (slot, (_, entries)) in listings.iter().enumerate() {
                    for (entry_index, entry) in entries.iter().enumerate() {
                        if let Some(entry_code) = entry.content_code {
                            codes.push((entry_code, slot as u32, entry_index as u32));
                        }
                    }
                }
                Arc::new(CodeIndex { codes, listings })
            })
            .await;
            match built {
                Ok(index) => {
                    // An index built while the listing cache is still
                    // filling (startup walks in progress) must not stick:
                    // cache only a non-empty build; empty ones are
                    // rebuilt per query until data arrives.
                    if !index.codes.is_empty() {
                        *state.code_index.write().expect("code index lock") =
                            Some(index.clone());
                    }
                    index
                }
                Err(err) => {
                    return error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
                }
            }
        }
    };

    let scan = tokio::task::spawn_blocking(move || {
        const MAX_HITS: usize = 10_000;
        let mut hits: Vec<(u32, u32, u32, u64)> = Vec::new();
        for (entry_code, slot, entry_index) in &index.codes {
            let distance = registry_core::similarity::hamming(code, *entry_code);
            if distance <= max_distance && hits.len() < MAX_HITS {
                hits.push((distance, *slot, *entry_index, *entry_code));
            }
        }
        hits.sort_by_key(|(distance, slot, entry_index, _)| (*distance, *slot, *entry_index));
        hits.truncate(limit);
        let total = index.codes.len() as u64;
        let matches: Vec<serde_json::Value> = hits
            .into_iter()
            .map(|(distance, slot, entry_index, entry_code)| {
                let (partition_id, entries) = &index.listings[slot as usize];
                json!({
                    "key": entries[entry_index as usize].key,
                    "distance": distance,
                    "partition": partition_id,
                    "content_code": format!("{entry_code:#018x}"),
                })
            })
            .collect();
        (total, matches)
    })
    .await;
    match scan {
        Ok((with_code, matches)) => Json(json!({
            "query_code": format!("{code:#018x}"),
            "max_distance": max_distance,
            "with_content_code": with_code,
            "scanned": with_code,
            "elapsed_ms": started.elapsed().as_millis() as u64,
            "matches": matches,
        }))
        .into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

/// Proxy to the central CommonsDB metadata API — the browser never sees
/// the bearer key. Shelled out to `curl` deliberately: it is the only
/// HTTPS call in the binary, and the system curl avoids adding an entire
/// TLS stack to the dependency tree for one endpoint.
async fn external_metadata(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    if !registry_core::is_valid_record_key(&key) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "not a valid CIDv1 or declaration id".to_string(),
        );
    }
    let Some(api_key) = &state.commonsdb_api_key else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "COMMONSDB_API_KEY is not configured on this viewer — set it in the \
             environment or a .env file (see .env.example) and restart"
                .to_string(),
        );
    };
    let url = format!("{}{key}", state.commonsdb_metadata_url);
    let output = tokio::time::timeout(
        Duration::from_secs(25),
        tokio::process::Command::new("curl")
            .args([
                "-s",
                "-m",
                "20",
                "-w",
                "\n%{http_code}",
                "-H",
                &format!("Authorization: Bearer {api_key}"),
                &url,
            ])
            .output(),
    )
    .await;
    let output = match output {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                format!("could not invoke curl for the metadata API: {err}"),
            )
        }
        Err(_) => {
            return error_response(StatusCode::GATEWAY_TIMEOUT, "metadata API timed out".into())
        }
    };
    let raw = String::from_utf8_lossy(&output.stdout);
    let (body, status_line) = raw.rsplit_once('\n').unwrap_or(("", ""));
    let upstream_status = status_line.trim().parse::<u16>().unwrap_or(0);
    if upstream_status != 200 {
        return error_response(
            StatusCode::BAD_GATEWAY,
            format!("metadata API answered HTTP {upstream_status} for {key}"),
        );
    }
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(metadata) => Json(json!({ "key": key, "source": "api.commonsdb.org", "metadata": metadata }))
            .into_response(),
        Err(_) => error_response(
            StatusCode::BAD_GATEWAY,
            "metadata API returned a non-JSON body".to_string(),
        ),
    }
}

async fn record(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    if !registry_core::is_valid_record_key(&key) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "not a valid CIDv1 or declaration id".to_string(),
        );
    }
    let partition_id = partition_id_for_key(&key, state.partitions);
    let result: anyhow::Result<Response> = async {
        // On a fresh cache the pointer document may still be syncing in
        // from the providers; an instant "not found" would be misleading.
        // Poll briefly, like storectl's get does.
        let mut root = state.pointer.get_partition_root(partition_id).await?;
        if root.is_none() {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while root.is_none() && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(300)).await;
                root = state.pointer.get_partition_root(partition_id).await?;
            }
        }
        let entry = match root {
            Some(root) => tokio::time::timeout(
                WALK_TIMEOUT,
                hamt::lookup(&state.blob_store, Some(root), &key),
            )
            .await
            .map_err(|_| anyhow::anyhow!("lookup timed out"))??,
            None => None,
        };
        let Some(entry) = entry else {
            // A valid key that is simply absent is a normal answer, not an
            // HTTP error — 200 with found=false keeps the UI's handling
            // uniform (a 404 here surfaced as a raw "HTTP 404" message).
            return Ok(
                Json(json!({ "key": key, "partition": partition_id, "found": false }))
                    .into_response(),
            );
        };
        // The index entry is local and verified; the value bytes may only
        // exist at the origin (values are fetched on demand unless
        // --warm-values). Bound the fetch and say precisely what failed —
        // "not fetchable" with no reason reads as a bug when the origin
        // is merely offline.
        let (value, value_error) = match tokio::time::timeout(
            Duration::from_secs(15),
            state.blob_store.get(&entry.hash),
        )
        .await
        {
            Ok(Ok(Some(bytes))) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(v) => (Some(v), None),
                Err(_) => (
                    None,
                    Some("value bytes fetched but are not valid JSON".to_string()),
                ),
            },
            Ok(Ok(None)) => (
                None,
                Some(
                    "value bytes are not replicated locally and no provider served them \
                     just now — the index entry itself is present and verified. If the \
                     origin is offline or restarting, retry once it is back."
                        .to_string(),
                ),
            ),
            Ok(Err(err)) => (None, Some(format!("value fetch failed: {err}"))),
            Err(_) => (
                None,
                Some("value fetch timed out — origin unreachable right now".to_string()),
            ),
        };
        Ok(Json(json!({
            "key": entry.key,
            "partition": partition_id,
            "found": true,
            "hash": entry.hash.to_prefixed(),
            "size": entry.size,
            "added_at": entry.added_at.to_rfc3339(),
            "content_code": entry.content_code.map(|c| format!("{c:#018x}")),
            "value": value,
            "value_error": value_error,
        }))
        .into_response())
    }
    .await;
    result.unwrap_or_else(|err| error_response(StatusCode::BAD_GATEWAY, err.to_string()))
}
