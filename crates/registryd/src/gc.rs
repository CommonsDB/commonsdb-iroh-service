//! Blob-store garbage collection wiring for the daemon.
//!
//! Every publish path-copies: superseded HAMT nodes become unreachable
//! orphans. iroh-blobs sweeps anything not protected, so the protect
//! callback here enumerates the FULL live set before each run:
//!
//! 1. every partition's current HAMT nodes and the value blobs their leaf
//!    entries reference (`hamt::collect_reachable`),
//! 2. every value hash the record index knows about — a pending record's
//!    value blob is referenced by no tree until its first publish,
//! 3. the ISCC similarity index, when a deployment publishes one
//!    (`iscc_store::collect_reachable`),
//! 4. the root pointer document's own entry content blobs.
//!
//! Two safety rails:
//! - while a publish cycle or a record submission is in flight, the
//!   callback aborts the GC run (freshly written blobs are unreferenced
//!   until the cycle publishes its roots);
//! - any error during enumeration aborts the run — an incomplete protect
//!   set must never reach the sweep.

use crate::index::RecordIndex;
use registry_core::{hamt, iscc_store};
use registry_node::{IrohBlobStore, PointerDoc, ISCC_INDEX_ROOT_KEY};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Everything the protect callback needs; installed once the pointer doc
/// exists (the blob store is created before it, so the callback starts out
/// unwired and aborts runs until then).
#[derive(Clone)]
pub struct GcSources {
    pub pointer_doc: Arc<PointerDoc>,
    /// Local-only view (no network providers): the walk must only read
    /// what this store already holds.
    pub blob_store: IrohBlobStore,
    pub index: Arc<RecordIndex>,
    pub top_level_partitions: u32,
}

#[derive(Default)]
pub struct GcState {
    /// std RwLock: written once at startup, read synchronously by the
    /// protect callback (whose future must be `Sync`, which rules out
    /// holding async lock guards across awaits).
    pub sources: std::sync::RwLock<Option<GcSources>>,
    /// Number of publish cycles / submissions currently writing blobs
    /// that are not yet referenced anywhere.
    active_writes: AtomicUsize,
}

impl GcState {
    pub fn write_guard(self: &Arc<Self>) -> WriteGuard {
        self.active_writes.fetch_add(1, Ordering::SeqCst);
        WriteGuard {
            state: self.clone(),
        }
    }
}

pub struct WriteGuard {
    state: Arc<GcState>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        self.state.active_writes.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Builds the `GcConfig` for `RegistryNode::spawn_persistent_with_gc`.
pub fn config(state: Arc<GcState>, interval_seconds: u64) -> Option<iroh_blobs::store::GcConfig> {
    if interval_seconds == 0 {
        return None;
    }
    let add_protected: iroh_blobs::store::ProtectCb = Arc::new(move |live| {
        let state = state.clone();
        let sources_snapshot = state
            .sources
            .read()
            .expect("gc sources lock poisoned")
            .clone();
        let writes_active = state.active_writes.load(Ordering::SeqCst) > 0;
        // The walk uses async-recursion (boxed Send-but-not-Sync futures),
        // while the ProtectCb contract wants a Sync future — running the
        // walk in its own task and awaiting the (Sync) JoinHandle bridges
        // the two.
        let walk = tokio::spawn(async move {
            let Some(sources) = sources_snapshot else {
                tracing::info!("gc: sources not wired yet; skipping run");
                return None;
            };
            if writes_active {
                tracing::info!("gc: publish or submission in flight; skipping run");
                return None;
            }
            match collect_live(&sources).await {
                Ok(protected) => {
                    tracing::info!(protected = protected.len(), "gc: protect set collected");
                    Some(protected)
                }
                Err(err) => {
                    tracing::warn!(error = %err, "gc: protect-set enumeration failed; aborting run");
                    None
                }
            }
        });
        Box::pin(async move {
            match walk.await {
                Ok(Some(protected)) => {
                    live.extend(
                        protected
                            .into_iter()
                            .map(|h| iroh_blobs::Hash::from_bytes(h.0)),
                    );
                    iroh_blobs::store::ProtectOutcome::Continue
                }
                _ => iroh_blobs::store::ProtectOutcome::Abort,
            }
        })
    });
    Some(iroh_blobs::store::GcConfig {
        interval: std::time::Duration::from_secs(interval_seconds),
        add_protected: Some(add_protected),
    })
}

async fn collect_live(sources: &GcSources) -> anyhow::Result<HashSet<registry_core::Hash>> {
    let mut live: HashSet<registry_core::Hash> = HashSet::new();

    // Index-known value hashes first: this is what keeps pending (not yet
    // published) values alive.
    let index = sources.index.clone();
    let hashes = tokio::task::spawn_blocking(move || index.all_content_hashes()).await??;
    live.extend(hashes);

    // Partition roots come from the writer's local table first — the
    // pointer document's entry blobs are themselves GC-managed and must
    // not be a single point of failure for the protect walk.
    let index_for_roots = sources.index.clone();
    let mut roots: std::collections::HashMap<u32, registry_core::Hash> =
        tokio::task::spawn_blocking(move || index_for_roots.all_local_roots())
            .await??
            .into_iter()
            .collect();
    for partition_id in 0..sources.top_level_partitions {
        if let std::collections::hash_map::Entry::Vacant(slot) = roots.entry(partition_id) {
            if let Some(root) = sources.pointer_doc.get_partition_root(partition_id).await? {
                slot.insert(root);
            }
        }
    }
    for (partition_id, root) in roots {
        // A root whose blobs are gone (recovery scenarios) contributes
        // what it can; the missing subtree is dead by definition.
        if let Err(err) = hamt::collect_reachable(&sources.blob_store, root, &mut live).await {
            tracing::debug!(partition_id, error = %err, "gc: partial partition walk (stale root)");
        }
    }

    if let Some(root) = sources
        .pointer_doc
        .get_named_root(ISCC_INDEX_ROOT_KEY)
        .await?
    {
        if let Err(err) = iscc_store::collect_reachable(&sources.blob_store, root, &mut live).await
        {
            tracing::debug!(error = %err, "gc: partial iscc-index walk (stale root)");
        }
    }

    // The pointer document's entry contents are themselves blobs in this
    // store; losing them would corrupt the doc replica.
    for hash in sources.pointer_doc.entry_content_hashes().await? {
        live.insert(hash);
    }

    Ok(live)
}
