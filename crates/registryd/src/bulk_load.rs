//! `registryd bulk-load` — load a full NDJSON dump directly into the
//! store, bypassing the HTTP API and the incremental publisher.
//!
//! The per-record publish path rewrites a whole leaf per insert (path
//! copying), which is the right durability trade for continuous operation
//! and catastrophically wasteful for bulk loads: importing millions of
//! records through it costs O(records × leaf size) in writes and orphans.
//! This command instead: writes each value blob once, spills entries per
//! partition, then builds every partition's tree bottom-up with
//! `hamt::build_from_entries` (each node written exactly once) and
//! publishes the 256 roots. Total writes ≈ final data size.
//!
//! Run with the daemon STOPPED (both stores are single-process). Records
//! already present in the index keep their original metadata; records
//! present in the index but absent from the dump are NOT removed from the
//! index — run `registryd verify --fix` afterwards to re-queue anything
//! the rebuilt trees do not cover.

use crate::config::Config;
use crate::index::{IndexRecord, RecordIndex};
use registry_core::{
    content_hash, hamt, is_valid_record_key, iscc, partition_id_for_key, RecordStatus,
};
use serde::Deserialize;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Deserialize)]
struct DumpLine {
    key: String,
    value: serde_json::Value,
}

fn open_dump(path: &PathBuf) -> anyhow::Result<Box<dyn BufRead + Send>> {
    let file = std::fs::File::open(path)?;
    if path.extension().is_some_and(|e| e == "gz") {
        // MultiGzDecoder: a dump plus appended extra members is one file.
        Ok(Box::new(std::io::BufReader::new(
            flate2::read::MultiGzDecoder::new(file),
        )))
    } else {
        Ok(Box::new(std::io::BufReader::new(file)))
    }
}

fn canonical_value_str(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(_) => Some(serde_json::to_string(value).expect("serializes")),
        serde_json::Value::String(raw) => {
            let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
            parsed.is_object().then(|| raw.clone())
        }
        _ => None,
    }
}

/// One resumable ingest chunk: stream `[skip, skip+limit)` dump lines —
/// value blobs + index rows + append-mode spill files. Run repeatedly
/// (100k at a time works well) until it reports `lines_read=0`, then run
/// `bulk-build`. Each run is independently observable and safely
/// re-runnable (duplicates are detected against the index and collapse
/// first-wins at tree build).
pub async fn ingest(
    cfg: Config,
    dump: PathBuf,
    spill_dir: PathBuf,
    skip: u64,
    limit: u64,
) -> anyhow::Result<u64> {
    let started = Instant::now();
    let index = Arc::new(RecordIndex::open(&cfg.index_path())?);
    let node = crate::node_setup::open_node(&cfg, None).await?;
    std::fs::create_dir_all(&spill_dir)?;

    let mut spills: Vec<std::io::BufWriter<std::fs::File>> = (0..cfg.top_level_partitions)
        .map(|p| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(spill_dir.join(format!("{p:03}.ndjson")))
                .map(std::io::BufWriter::new)
        })
        .collect::<Result<_, _>>()?;

    let mut total = 0u64;
    let mut invalid = 0u64;
    let mut conflicts = 0u64;
    let mut new_records = 0u64;
    let reader = open_dump(&dump)?;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
    let mut blob_tasks = tokio::task::JoinSet::new();
    let mut batch: Vec<(String, String, IndexRecord)> = Vec::with_capacity(2000);

    let flush_batch = |batch: &mut Vec<(String, String, IndexRecord)>,
                       spills: &mut Vec<std::io::BufWriter<std::fs::File>>,
                       new_records: &mut u64,
                       conflicts: &mut u64|
     -> anyhow::Result<Vec<(String, IndexRecord)>> {
        let keys: Vec<String> = batch.iter().map(|(k, _, _)| k.clone()).collect();
        let existing = index.get_many(&keys)?;
        let mut to_insert: Vec<(String, IndexRecord)> = Vec::new();
        let mut to_put: Vec<(String, IndexRecord)> = Vec::new();
        for ((key, value_str, record), known) in batch.drain(..).zip(existing) {
            let entry_record = match known {
                Some(prior) if prior.content_hash == record.content_hash => {
                    // Known record — but the value blob must still be
                    // (re)written: bulk loads may target a fresh store.
                    to_put.push((value_str, prior.clone()));
                    prior
                }
                Some(prior) => {
                    // Immutability: the index's existing binding wins; the
                    // dump's differing bytes are not stored.
                    *conflicts += 1;
                    prior
                }
                None => {
                    *new_records += 1;
                    to_insert.push((key.clone(), record.clone()));
                    to_put.push((value_str, record.clone()));
                    record
                }
            };
            let leaf = entry_record.leaf_entry(&key);
            let line = serde_json::to_vec(&leaf)?;
            let spill = &mut spills[entry_record.partition_id as usize];
            spill.write_all(&line)?;
            spill.write_all(b"\n")?;
        }
        index.bulk_mark_published(&to_insert)?;
        Ok(to_put)
    };

    let mut seen_lines = 0u64;
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        seen_lines += 1;
        if seen_lines <= skip {
            continue;
        }
        if total >= limit {
            break;
        }
        total += 1;
        let parsed: DumpLine = match serde_json::from_str(&line) {
            Ok(p) => p,
            Err(_) => {
                invalid += 1;
                continue;
            }
        };
        let Some(value_str) = canonical_value_str(&parsed.value) else {
            invalid += 1;
            continue;
        };
        if !is_valid_record_key(&parsed.key) || value_str.len() > cfg.max_value_bytes {
            invalid += 1;
            continue;
        }
        let hash = content_hash(&value_str);
        let record = IndexRecord {
            content_hash: hash,
            size: value_str.len() as u64,
            partition_id: partition_id_for_key(&parsed.key, cfg.top_level_partitions),
            status: RecordStatus::Published,
            created_at: crate::index::declaration_timestamp(&value_str)
                .unwrap_or_else(chrono::Utc::now),
            content_code: iscc::extract_from_json(&value_str),
            published_at: Some(chrono::Utc::now()),
        };
        batch.push((parsed.key, value_str, record));

        if batch.len() >= 2000 {
            let puts = flush_batch(&mut batch, &mut spills, &mut new_records, &mut conflicts)?;
            for (value_str, _) in puts {
                let blobs = node.blob_store.clone();
                let permit = semaphore.clone().acquire_owned().await?;
                blob_tasks.spawn(async move {
                    let _permit = permit;
                    registry_core::BlobStore::put(&blobs, bytes::Bytes::from(value_str)).await
                });
            }
            // Reap finished blob writes so the set stays bounded.
            while let Some(done) = blob_tasks.try_join_next() {
                done??;
            }
            if total.is_multiple_of(200_000) {
                tracing::info!(
                    total,
                    new_records,
                    conflicts,
                    invalid,
                    elapsed_secs = started.elapsed().as_secs(),
                    "bulk-load ingest progress"
                );
            }
        }
    }
    let puts = flush_batch(&mut batch, &mut spills, &mut new_records, &mut conflicts)?;
    for (value_str, _) in puts {
        let blobs = node.blob_store.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        blob_tasks.spawn(async move {
            let _permit = permit;
            registry_core::BlobStore::put(&blobs, bytes::Bytes::from(value_str)).await
        });
    }
    while let Some(done) = blob_tasks.join_next().await {
        done??;
    }
    for spill in &mut spills {
        spill.flush()?;
    }
    println!(
        "ingest chunk done in {}s: skip={skip} lines_read={total} new_records={new_records} \
         conflicts={conflicts} invalid={invalid}",
        started.elapsed().as_secs(),
    );
    node.node.shutdown().await?;
    Ok(total)
}

/// Final phase: build every partition's tree once from the accumulated
/// spill files and publish the roots. Run after the last ingest chunk.
/// Re-stamp every indexed record's `created_at` with the declaration
/// timestamp embedded in its value (sourced from the bulk dump — the
/// index itself does not store values), then rebuild and republish every
/// partition tree so published `added_at` reflects declaration time.
/// Requires exclusive access: stop the daemon first.
pub async fn retimestamp(cfg: Config, dump: PathBuf, spill_dir: PathBuf) -> anyhow::Result<()> {
    let started = Instant::now();
    let index = RecordIndex::open(&cfg.index_path())?;
    let reader = open_dump(&dump)?;

    let (mut updated, mut missing, mut no_timestamp, mut lines) = (0u64, 0u64, 0u64, 0u64);
    let mut batch: Vec<(String, chrono::DateTime<chrono::Utc>)> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        lines += 1;
        let parsed: DumpLine = match serde_json::from_str(&line) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };
        let Some(value_str) = canonical_value_str(&parsed.value) else {
            continue;
        };
        let Some(created_at) = crate::index::declaration_timestamp(&value_str) else {
            no_timestamp += 1;
            continue;
        };
        batch.push((parsed.key, created_at));
        if batch.len() >= 10_000 {
            let (u, m) = index.update_created_at(&batch)?;
            updated += u;
            missing += m;
            batch.clear();
        }
        if lines.is_multiple_of(500_000) {
            println!(
                "retimestamp: {lines} dump lines, {updated} re-stamped, \
                 {missing} not in index, {no_timestamp} without timestamp \
                 ({}s)",
                started.elapsed().as_secs()
            );
        }
    }
    let (u, m) = index.update_created_at(&batch)?;
    updated += u;
    missing += m;
    println!(
        "retimestamp: dump pass done in {}s — {lines} lines, {updated} re-stamped, \
         {missing} not in index, {no_timestamp} without timestamp",
        started.elapsed().as_secs()
    );

    let spilled = index.spill_published_entries(&spill_dir, cfg.top_level_partitions)?;
    println!("retimestamp: {spilled} published entries spilled; rebuilding trees");
    drop(index); // build_trees reopens the index (and needs the store exclusively)
    build_trees(cfg, spill_dir).await
}

pub async fn build_trees(cfg: Config, spill_dir: PathBuf) -> anyhow::Result<()> {
    let started = Instant::now();
    let index = Arc::new(RecordIndex::open(&cfg.index_path())?);
    let node = crate::node_setup::open_node(&cfg, None).await?;

    for partition_id in 0..cfg.top_level_partitions {
        let path = spill_dir.join(format!("{partition_id:03}.ndjson"));
        let mut entries = Vec::new();
        for line in std::io::BufReader::new(std::fs::File::open(&path)?).lines() {
            let line = line?;
            if !line.is_empty() {
                entries.push(serde_json::from_str(&line)?);
            }
        }
        let count = entries.len();
        let root =
            hamt::build_from_entries(&node.blob_store, entries, cfg.leaf_max_entries).await?;
        node.pointer_doc
            .set_partition_root(partition_id, root)
            .await?;
        index.set_local_root(partition_id, root)?;
        tracing::info!(partition_id, records = count, root = %root, "partition built");
    }

    println!(
        "build-trees done in {}s: {} partitions rebuilt and published. \
         Run `registryd verify --fix` next.",
        started.elapsed().as_secs(),
        cfg.top_level_partitions,
    );
    node.node.shutdown().await?;
    Ok(())
}
