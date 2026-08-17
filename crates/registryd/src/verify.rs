//! `registryd verify` — tree-level audit (docs/operator-guide.md,
//! "Verify"). Index status flags say what SHOULD be in the published
//! trees; the only honest audit is to walk what actually IS there. For
//! every partition, diff the set of `published` keys in the record index
//! against the keys reachable from the partition's current HAMT root, and
//! (with `--fix`) re-queue whatever is missing so the next publish cycles
//! repair it.
//!
//! Runs offline: it opens the same data directory as the daemon, and both
//! stores are single-process — stop `registryd` first. On a single-writer
//! node this should always report zero missing, which is exactly what
//! makes it a meaningful post-import health check.

use crate::config::Config;
use crate::index::RecordIndex;
use registry_core::hamt;
use std::collections::HashSet;
use std::sync::Arc;

pub struct VerifyReport {
    pub partitions_checked: u32,
    pub partitions_with_gaps: u32,
    pub expected_total: u64,
    pub in_tree_total: u64,
    pub missing_total: u64,
    /// Keys present in a tree but not `published` in the index — benign
    /// after a restore or re-import, but worth surfacing.
    pub foreign_total: u64,
    pub requeued_total: u64,
}

/// Disaster recovery: overwrite one partition's root pointer with a
/// known-good hash (e.g. the last `partition published` line in the
/// journal) after the pointer entry's content blob was lost to a disk
/// failure. Refuses hashes whose node blob is not present and readable
/// in the local store, so it can never point a partition at nothing.
pub async fn set_root(cfg: Config, partition_id: u32, hash_hex: &str) -> anyhow::Result<()> {
    let hash = registry_core::Hash::from_hex(hash_hex)
        .map_err(|e| anyhow::anyhow!("'{hash_hex}' is not a 32-byte hex hash: {e}"))?;
    let node = crate::node_setup::open_node(&cfg, None).await?;
    let result = async {
        let bytes = registry_core::BlobStore::get(&node.blob_store, &hash)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| {
                anyhow::anyhow!("refusing: node blob {hash} is not readable in the local store")
            })?;
        // Must decode as a HAMT node, not just exist.
        registry_core::hamt::HamtNode::decode(&bytes, hash)
            .map_err(|e| anyhow::anyhow!("refusing: {hash} is not a valid HAMT node: {e}"))?;
        node.pointer_doc
            .set_partition_root(partition_id, hash)
            .await?;
        // Keep the writer's local source of truth in step.
        let index = RecordIndex::open(&cfg.index_path())?;
        index.set_local_root(partition_id, hash)?;
        println!("partition {partition_id} root pointer set to {hash}");
        Ok(())
    }
    .await;
    node.node.shutdown().await?;
    result
}

pub async fn run(
    cfg: Config,
    only_partition: Option<u32>,
    fix: bool,
) -> anyhow::Result<VerifyReport> {
    let index = Arc::new(RecordIndex::open(&cfg.index_path())?);
    let node = crate::node_setup::open_node(&cfg, None).await?;

    let mut report = VerifyReport {
        partitions_checked: 0,
        partitions_with_gaps: 0,
        expected_total: 0,
        in_tree_total: 0,
        missing_total: 0,
        foreign_total: 0,
        requeued_total: 0,
    };

    let partitions: Vec<u32> = match only_partition {
        Some(p) => vec![p],
        None => (0..cfg.top_level_partitions).collect(),
    };

    // Single index pass for the full audit; the per-partition scan is
    // only proportionate when auditing one partition.
    let spill_dir = if only_partition.is_none() {
        let dir = cfg.data_dir.join("verify-spill");
        let index_for_scan = index.clone();
        let dir_for_scan = dir.clone();
        let total = tokio::task::spawn_blocking(move || {
            index_for_scan.spill_published_keys(&dir_for_scan, cfg.top_level_partitions)
        })
        .await??;
        tracing::info!(published = total, "index scanned once; auditing partitions");
        Some(dir)
    } else {
        None
    };

    for partition_id in partitions {
        let expected: HashSet<String> = match &spill_dir {
            Some(dir) => std::fs::read_to_string(dir.join(format!("{partition_id:03}.keys")))?
                .lines()
                .map(String::from)
                .collect(),
            None => {
                let index_for_scan = index.clone();
                tokio::task::spawn_blocking(move || {
                    index_for_scan.published_keys_for_partition(partition_id)
                })
                .await??
                .into_iter()
                .collect()
            }
        };

        // Local root first — the audit must reflect the writer's truth
        // even when a pointer-doc entry blob is damaged.
        let root = match index.get_local_root(partition_id)? {
            Some(root) => Some(root),
            None => node.pointer_doc.get_partition_root(partition_id).await?,
        };
        let mut in_tree: HashSet<String> = HashSet::new();
        if let Some(root) = root {
            let mut entries = Vec::new();
            hamt::walk_entries(&node.blob_store, root, &mut entries).await?;
            in_tree = entries.into_iter().map(|e| e.key).collect();
        } else if !expected.is_empty() {
            tracing::warn!(
                partition_id,
                expected = expected.len(),
                "partition has published records but no root at all"
            );
        }

        let missing: Vec<&String> = expected.iter().filter(|k| !in_tree.contains(*k)).collect();
        let foreign = in_tree.iter().filter(|k| !expected.contains(*k)).count();

        report.partitions_checked += 1;
        report.expected_total += expected.len() as u64;
        report.in_tree_total += in_tree.len() as u64;
        report.foreign_total += foreign as u64;

        if missing.is_empty() {
            tracing::debug!(partition_id, records = in_tree.len(), "partition complete");
            continue;
        }
        report.partitions_with_gaps += 1;
        report.missing_total += missing.len() as u64;
        tracing::warn!(
            partition_id,
            expected = expected.len(),
            in_tree = in_tree.len(),
            missing = missing.len(),
            "partition has gaps{}",
            if fix {
                ""
            } else {
                " (report only; use --fix to re-queue)"
            }
        );
        if fix {
            for key in missing {
                let index_for_fix = index.clone();
                let key = key.clone();
                let requeued =
                    tokio::task::spawn_blocking(move || index_for_fix.requeue(&key)).await??;
                if requeued {
                    report.requeued_total += 1;
                }
            }
        }
    }

    if let Some(dir) = spill_dir {
        let _ = std::fs::remove_dir_all(dir);
    }
    node.node.shutdown().await?;
    Ok(report)
}
