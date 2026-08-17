//! The publisher loop — the single writer task that turns pending queue
//! entries into published HAMT partitions (docs/data-model.md,
//! "Publishing").
//!
//! One cycle:
//! 1. take the oldest pending batch from the queue,
//! 2. group entries by partition,
//! 3. per partition: `hamt::insert_batch` on the current root, write the
//!    new root to the pointer document (the durability boundary), then
//!    mark that partition's records published and drop their queue rows,
//! 4. records on the denylist are excluded from the tree and marked
//!    denylisted instead.
//!
//! Failure anywhere leaves the queue rows in place; the next cycle
//! re-derives the same entries and the HAMT insert tolerates the replay
//! idempotently. A current root that resolves in the pointer document but
//! whose node blob cannot be read is a HARD error (`hamt` fails on the
//! missing node) — never a signal to rebuild from empty, which would
//! silently discard the partition's accumulated records.
//!
//! Single writer by construction: one task, partitions processed
//! sequentially. If this is ever made concurrent, rebuilds of the SAME
//! partition must be serialized (per-partition locks) — concurrent
//! rebuilds from the same old root lose the loser's records while they
//! are already marked published.

use crate::gc::GcState;
use crate::index::RecordIndex;
use registry_core::hamt;
use registry_node::{IrohBlobStore, PointerDoc};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Shared with `/health` — see docs/api.md.
#[derive(Default)]
pub struct PublisherStatus {
    /// Unix seconds of the last successful publish cycle; 0 = none yet.
    pub last_publish_unix: AtomicI64,
    pub cycles_completed: AtomicU64,
    pub records_published: AtomicU64,
    pub last_error: std::sync::Mutex<Option<String>>,
}

pub struct Publisher {
    pub index: Arc<RecordIndex>,
    pub blob_store: IrohBlobStore,
    pub pointer_doc: Arc<PointerDoc>,
    pub gc_state: Arc<GcState>,
    pub status: Arc<PublisherStatus>,
    pub leaf_max_entries: usize,
    pub max_pending: usize,
    pub max_interval: Duration,
    pub denylist_path: Option<PathBuf>,
    pub top_level_partitions: u32,
}

/// How often the publisher re-checks the pointer document against the
/// local roots and repairs damaged entries.
const HEAL_INTERVAL: Duration = Duration::from_secs(600);

impl Publisher {
    /// Run until `shutdown` flips to true, then drain what is already due
    /// and return. The caller awaits this before closing the node.
    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        match self.seed_local_roots().await {
            Ok(n) if n > 0 => tracing::info!(partitions = n, "seeded local roots from pointer doc"),
            Err(err) => tracing::warn!(error = %err, "local-root seeding failed"),
            _ => {}
        }
        match self.heal_pointer_doc().await {
            Ok(n) if n > 0 => tracing::warn!(healed = n, "pointer-doc entries healed on startup"),
            Err(err) => tracing::warn!(error = %err, "pointer-doc heal failed"),
            _ => {}
        }
        let mut last_cycle = Instant::now();
        let mut last_heal = Instant::now();
        loop {
            if last_heal.elapsed() >= HEAL_INTERVAL {
                last_heal = Instant::now();
                match self.heal_pointer_doc().await {
                    Ok(n) if n > 0 => {
                        tracing::warn!(healed = n, "pointer-doc entries healed")
                    }
                    Err(err) => tracing::warn!(error = %err, "pointer-doc heal failed"),
                    _ => {}
                }
            }
            let depth = self.queue_depth().await;
            let due = depth > 0
                && (depth as usize >= self.max_pending
                    || last_cycle.elapsed() >= self.max_interval);

            if due {
                match self.publish_cycle().await {
                    Ok(published) => {
                        last_cycle = Instant::now();
                        self.status
                            .last_publish_unix
                            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
                        self.status.cycles_completed.fetch_add(1, Ordering::Relaxed);
                        self.status
                            .records_published
                            .fetch_add(published as u64, Ordering::Relaxed);
                        *self.status.last_error.lock().expect("status lock") = None;
                        // With a backlog (bulk import), cycle again
                        // immediately instead of waiting out the timer.
                        continue;
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "publish cycle failed; queue preserved, retrying");
                        *self.status.last_error.lock().expect("status lock") =
                            Some(err.to_string());
                        // Back off a little so a persistent fault does not
                        // spin the loop.
                        last_cycle = Instant::now();
                    }
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => {}
            }
            if *shutdown.borrow() {
                // Final best-effort drain of records already accepted, so a
                // clean stop never strands a small queue tail. Bounded to
                // one pass; anything left publishes after the next start.
                if self.queue_depth().await > 0 {
                    if let Err(err) = self.publish_cycle().await {
                        tracing::warn!(error = %err, "final drain failed; records stay queued for next start");
                    }
                }
                tracing::info!("publisher stopped");
                return;
            }
        }
    }

    async fn queue_depth(&self) -> u64 {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || index.queue_depth())
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(0)
    }

    /// The partition's current root as the writer knows it: the local
    /// index first (immune to blob-store accidents), the pointer document
    /// only for partitions that predate local root tracking.
    async fn current_root(&self, partition_id: u32) -> anyhow::Result<Option<registry_core::Hash>> {
        let index = self.index.clone();
        if let Some(root) =
            tokio::task::spawn_blocking(move || index.get_local_root(partition_id)).await??
        {
            return Ok(Some(root));
        }
        self.pointer_doc.get_partition_root(partition_id).await
    }

    /// Re-assert every locally known root into the pointer document when
    /// its entry is missing or unreadable. A GC sweep racing a publish (or
    /// a torn write on a full disk) can destroy an entry's tiny content
    /// blob; without healing that wedges the partition forever. Also seeds
    /// local roots from the doc on first run after an upgrade.
    pub async fn heal_pointer_doc(&self) -> anyhow::Result<usize> {
        let index = self.index.clone();
        let local_roots = tokio::task::spawn_blocking(move || index.all_local_roots()).await??;
        let mut healed = 0usize;
        for (partition_id, local_root) in local_roots {
            match self.pointer_doc.get_partition_root(partition_id).await {
                Ok(Some(_)) => {} // readable — doc may be equal or newer; leave it
                Ok(None) | Err(_) => {
                    self.pointer_doc
                        .set_partition_root(partition_id, local_root)
                        .await?;
                    tracing::warn!(
                        partition_id,
                        root = %local_root,
                        "healed unreadable/missing pointer-doc entry from local root"
                    );
                    healed += 1;
                }
            }
        }
        Ok(healed)
    }

    /// Seed the local-roots table from the pointer document for partitions
    /// that predate local tracking (one-time migration on startup).
    pub async fn seed_local_roots(&self) -> anyhow::Result<usize> {
        let mut seeded = 0usize;
        for partition_id in 0..self.top_level_partitions {
            let index = self.index.clone();
            let have =
                tokio::task::spawn_blocking(move || index.get_local_root(partition_id)).await??;
            if have.is_some() {
                continue;
            }
            if let Ok(Some(root)) = self.pointer_doc.get_partition_root(partition_id).await {
                let index = self.index.clone();
                tokio::task::spawn_blocking(move || index.set_local_root(partition_id, root))
                    .await??;
                seeded += 1;
            }
        }
        Ok(seeded)
    }

    /// One publish cycle over at most `max_pending` queue rows. Returns
    /// the number of records published into trees.
    pub async fn publish_cycle(&self) -> anyhow::Result<usize> {
        // Pause GC while this cycle's fresh blobs are unreferenced.
        let _gc_guard = self.gc_state.write_guard();

        let index = self.index.clone();
        let max = self.max_pending;
        let batch = tokio::task::spawn_blocking(move || index.pending_batch(max)).await??;
        if batch.is_empty() {
            return Ok(0);
        }

        let denylist = load_denylist(self.denylist_path.as_deref());

        // Group by partition, preserving queue order within each group.
        let mut per_partition: BTreeMap<u32, Vec<(u64, String, crate::index::IndexRecord)>> =
            BTreeMap::new();
        for item in batch {
            per_partition
                .entry(item.2.partition_id)
                .or_default()
                .push(item);
        }

        let mut published_total = 0usize;
        let partitions = per_partition.len();
        for (partition_id, items) in per_partition {
            let mut publishable: Vec<(u64, String)> = Vec::new();
            let mut denylisted: Vec<(u64, String)> = Vec::new();
            let mut entries = Vec::new();
            for (seq, key, record) in items {
                if denylist.contains(&key) {
                    denylisted.push((seq, key));
                } else {
                    entries.push(record.leaf_entry(&key));
                    publishable.push((seq, key));
                }
            }

            if !entries.is_empty() {
                let current_root = self.current_root(partition_id).await?;
                let new_root = hamt::insert_batch(
                    &self.blob_store,
                    current_root,
                    entries,
                    self.leaf_max_entries,
                    &HashSet::new(),
                )
                .await?;
                // The pointer-document write is the durability boundary:
                // anything built above but not yet referenced from here is
                // harmless orphaned data on retry.
                self.pointer_doc
                    .set_partition_root(partition_id, new_root)
                    .await?;
                // Mirror the root into the local index — the writer's own
                // source of truth, from which a damaged doc entry heals.
                let index = self.index.clone();
                tokio::task::spawn_blocking(move || index.set_local_root(partition_id, new_root))
                    .await??;
                tracing::info!(
                    partition_id,
                    new_root = %new_root,
                    records = publishable.len(),
                    "partition published"
                );
            }

            let index = self.index.clone();
            let done = publishable.clone();
            let denied = denylisted.clone();
            tokio::task::spawn_blocking(move || {
                index.mark_published(&done)?;
                index.mark_denylisted(&denied)
            })
            .await??;
            published_total += publishable.len();
        }

        tracing::info!(
            records = published_total,
            partitions,
            "publish cycle complete"
        );
        Ok(published_total)
    }
}

/// One key per line; `#` starts a comment; blank lines ignored. A missing
/// or unreadable file is an empty denylist (logged, not fatal) — takedowns
/// are an operator convenience, not a startup dependency.
pub fn load_denylist(path: Option<&std::path::Path>) -> HashSet<String> {
    let Some(path) = path else {
        return HashSet::new();
    };
    match std::fs::read_to_string(path) {
        Ok(raw) => raw
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(String::from)
            .collect(),
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "denylist unreadable; treating as empty");
            HashSet::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denylist_parses_comments_and_blanks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("denylist.txt");
        std::fs::write(&path, "# taken down 2026-01-01\nkey-a\n\n  key-b  \n").unwrap();
        let list = load_denylist(Some(&path));
        assert_eq!(list.len(), 2);
        assert!(list.contains("key-a"));
        assert!(list.contains("key-b"));
    }

    #[test]
    fn missing_denylist_is_empty() {
        assert!(load_denylist(Some(std::path::Path::new("/nonexistent/denylist"))).is_empty());
        assert!(load_denylist(None).is_empty());
    }
}
