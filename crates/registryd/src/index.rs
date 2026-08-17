//! The embedded record index and pending queue, both `redb` tables in one
//! database file — the single-node replacement for the reference
//! architecture's managed existence index and ingestion stream
//! (docs/data-model.md, "Existence and duplicate detection").
//!
//! Tables:
//! - `records`: key → JSON-encoded [`IndexRecord`]. Immutability rule:
//!   a key, once accepted, is bound to its content hash forever —
//!   identical resubmission is an idempotent no-op, different content is
//!   a conflict.
//! - `queue`: monotonic sequence number → key. The publisher drains this
//!   in order; rows are only removed after the record is durably
//!   published (at-least-once semantics, tolerated by the HAMT's
//!   idempotent insert).
//! - `meta`: small counters (the queue's next sequence number).
//!
//! All methods are synchronous (redb transactions fsync on commit); async
//! callers wrap them in `spawn_blocking`.

use chrono::{DateTime, Utc};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use registry_core::{Hash, LeafEntry, RecordStatus};
use serde::{Deserialize, Serialize};
use std::path::Path;

const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("records");
const QUEUE: TableDefinition<u64, &str> = TableDefinition::new("queue");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
/// The writer's own record of each partition's current root. The pointer
/// document is the *replication* mechanism; this table is the local source
/// of truth, immune to blob-store accidents (a swept or torn doc-entry
/// blob is healed FROM here rather than wedging the writer).
const ROOTS: TableDefinition<u32, &[u8]> = TableDefinition::new("local_roots");

const META_NEXT_SEQ: &str = "next_seq";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRecord {
    pub content_hash: Hash,
    pub size: u64,
    pub partition_id: u32,
    pub status: RecordStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_code: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
}

impl IndexRecord {
    pub fn leaf_entry(&self, key: &str) -> LeafEntry {
        LeafEntry {
            key: key.to_string(),
            hash: self.content_hash,
            size: self.size,
            added_at: self.created_at,
            content_code: self.content_code,
        }
    }
}

/// The declaration's own timestamp, when the value carries one: the
/// source registry embeds it as a top-level `timestamp` field (epoch
/// milliseconds). Ingestion dates records by this — `created_at` /
/// published `added_at` mean "when the declaration entered the
/// registry", not "when this daemon imported it" — falling back to
/// import time only for values without one.
pub fn declaration_timestamp(value_str: &str) -> Option<DateTime<Utc>> {
    let value: serde_json::Value = serde_json::from_str(value_str).ok()?;
    DateTime::from_timestamp_millis(value.get("timestamp")?.as_i64()?)
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// New key: claimed in the index and appended to the pending queue.
    Queued,
    /// Same key, same content hash: idempotent no-op.
    DuplicateIdentical,
    /// Same key, different content hash: immutability violation.
    Conflict { existing_hash: Hash },
}

pub struct RecordIndex {
    db: Database,
}

impl RecordIndex {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        // Materialize every table so later read transactions never hit
        // "table does not exist" on a fresh database.
        let txn = db.begin_write()?;
        {
            txn.open_table(RECORDS)?;
            txn.open_table(QUEUE)?;
            txn.open_table(META)?;
            txn.open_table(ROOTS)?;
        }
        txn.commit()?;
        Ok(Self { db })
    }

    /// Claim `key` and enqueue it for publishing — one atomic transaction,
    /// so a crash can never leave a claimed-but-unqueued record.
    pub fn submit(&self, key: &str, record: &IndexRecord) -> anyhow::Result<SubmitOutcome> {
        Ok(self
            .submit_many(std::slice::from_ref(&(key.to_string(), record.clone())))?
            .pop()
            .expect("one submission yields one outcome"))
    }

    /// [`submit`](Self::submit) for many records in ONE transaction (one
    /// fsync per batch instead of per record) — the difference between a
    /// bulk import taking hours and taking all night. Outcomes are
    /// positional. A duplicate key WITHIN the batch resolves like a
    /// resubmission: first occurrence wins, later ones compare against it.
    pub fn submit_many(
        &self,
        submissions: &[(String, IndexRecord)],
    ) -> anyhow::Result<Vec<SubmitOutcome>> {
        let txn = self.db.begin_write()?;
        let mut outcomes = Vec::with_capacity(submissions.len());
        {
            let mut records = txn.open_table(RECORDS)?;
            let mut meta = txn.open_table(META)?;
            let mut queue = txn.open_table(QUEUE)?;
            let mut next_seq = meta.get(META_NEXT_SEQ)?.map(|v| v.value()).unwrap_or(0);
            for (key, record) in submissions {
                let existing: Option<IndexRecord> = records
                    .get(key.as_str())?
                    .map(|raw| serde_json::from_slice(raw.value()))
                    .transpose()?;
                match existing {
                    Some(existing) => {
                        outcomes.push(if existing.content_hash == record.content_hash {
                            SubmitOutcome::DuplicateIdentical
                        } else {
                            SubmitOutcome::Conflict {
                                existing_hash: existing.content_hash,
                            }
                        });
                    }
                    None => {
                        let encoded = serde_json::to_vec(record)?;
                        records.insert(key.as_str(), encoded.as_slice())?;
                        queue.insert(next_seq, key.as_str())?;
                        next_seq += 1;
                        outcomes.push(SubmitOutcome::Queued);
                    }
                }
            }
            meta.insert(META_NEXT_SEQ, next_seq)?;
        }
        txn.commit()?;
        Ok(outcomes)
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Option<IndexRecord>> {
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        match records.get(key)? {
            Some(raw) => Ok(Some(serde_json::from_slice(raw.value())?)),
            None => Ok(None),
        }
    }

    /// Batch point-lookups in one read transaction — the write path's
    /// duplicate precheck. Results are positional.
    pub fn get_many(&self, keys: &[String]) -> anyhow::Result<Vec<Option<IndexRecord>>> {
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(match records.get(key.as_str())? {
                Some(raw) => Some(serde_json::from_slice(raw.value())?),
                None => None,
            });
        }
        Ok(out)
    }

    /// The oldest `max` queue rows, in sequence order, joined with their
    /// index records. A queue row whose record has vanished (impossible
    /// outside manual tampering) is skipped and dropped.
    pub fn pending_batch(&self, max: usize) -> anyhow::Result<Vec<(u64, String, IndexRecord)>> {
        let txn = self.db.begin_read()?;
        let queue = txn.open_table(QUEUE)?;
        let records = txn.open_table(RECORDS)?;
        let mut out = Vec::new();
        for row in queue.iter()? {
            if out.len() >= max {
                break;
            }
            let (seq, key) = row?;
            let key = key.value().to_string();
            if let Some(raw) = records.get(key.as_str())? {
                let record: IndexRecord = serde_json::from_slice(raw.value())?;
                out.push((seq.value(), key, record));
            } else {
                tracing::warn!(key, "queue row references a missing index record; skipping");
            }
        }
        Ok(out)
    }

    /// Mark a published partition batch done: set each record's status,
    /// stamp `published_at`, and remove the queue rows — one transaction,
    /// committed only after the new root is durably referenced from the
    /// pointer document.
    pub fn mark_published(&self, items: &[(u64, String)]) -> anyhow::Result<()> {
        self.finish(items, RecordStatus::Published)
    }

    /// Same as [`mark_published`](Self::mark_published) for records the
    /// denylist excluded from the tree.
    pub fn mark_denylisted(&self, items: &[(u64, String)]) -> anyhow::Result<()> {
        self.finish(items, RecordStatus::Denylisted)
    }

    fn finish(&self, items: &[(u64, String)], status: RecordStatus) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let txn = self.db.begin_write()?;
        {
            let mut records = txn.open_table(RECORDS)?;
            let mut queue = txn.open_table(QUEUE)?;
            for (seq, key) in items {
                let record: Option<IndexRecord> = records
                    .get(key.as_str())?
                    .map(|raw| serde_json::from_slice(raw.value()))
                    .transpose()?;
                if let Some(mut record) = record {
                    record.status = status;
                    if status == RecordStatus::Published {
                        record.published_at = Some(now);
                    }
                    let encoded = serde_json::to_vec(&record)?;
                    records.insert(key.as_str(), encoded.as_slice())?;
                }
                queue.remove(*seq)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Re-enqueue an existing record for publishing (used by
    /// `registryd verify --fix`). Returns false if the key is unknown.
    pub fn requeue(&self, key: &str) -> anyhow::Result<bool> {
        let txn = self.db.begin_write()?;
        let known;
        {
            let records = txn.open_table(RECORDS)?;
            known = records.get(key)?.is_some();
            if known {
                let mut meta = txn.open_table(META)?;
                let seq = meta.get(META_NEXT_SEQ)?.map(|v| v.value()).unwrap_or(0);
                meta.insert(META_NEXT_SEQ, seq + 1)?;
                let mut queue = txn.open_table(QUEUE)?;
                queue.insert(seq, key)?;
            }
        }
        txn.commit()?;
        Ok(known)
    }

    /// Bulk-load path: insert records directly as `published`, with NO
    /// queue rows (the caller builds and publishes the trees itself).
    /// One transaction per call — callers batch.
    pub fn bulk_mark_published(&self, records: &[(String, IndexRecord)]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RECORDS)?;
            for (key, record) in records {
                let encoded = serde_json::to_vec(record)?;
                table.insert(key.as_str(), encoded.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Rewrite `created_at` for the given keys (one transaction). Keys not
    /// in the index are counted, not errors — a dump may contain records
    /// that were filtered at ingest time.
    pub fn update_created_at(
        &self,
        batch: &[(String, DateTime<Utc>)],
    ) -> anyhow::Result<(u64, u64)> {
        let (mut updated, mut missing) = (0u64, 0u64);
        let txn = self.db.begin_write()?;
        {
            let mut records = txn.open_table(RECORDS)?;
            for (key, created_at) in batch {
                let record: Option<IndexRecord> = records
                    .get(key.as_str())?
                    .map(|raw| serde_json::from_slice(raw.value()))
                    .transpose()?;
                let Some(mut record) = record else {
                    missing += 1;
                    continue;
                };
                if record.created_at != *created_at {
                    record.created_at = *created_at;
                    let encoded = serde_json::to_vec(&record)?;
                    records.insert(key.as_str(), encoded.as_slice())?;
                }
                updated += 1;
            }
        }
        txn.commit()?;
        Ok((updated, missing))
    }

    /// Write every published record's [`LeafEntry`] to fresh per-partition
    /// spill files (the `bulk-build` input format) — the bridge that lets
    /// partition trees be rebuilt straight from the index, no dump or
    /// value reads needed.
    pub fn spill_published_entries(
        &self,
        spill_dir: &Path,
        partitions: u32,
    ) -> anyhow::Result<u64> {
        use std::io::Write;
        std::fs::create_dir_all(spill_dir)?;
        let mut spills: Vec<std::io::BufWriter<std::fs::File>> = (0..partitions)
            .map(|p| {
                std::fs::File::create(spill_dir.join(format!("{p:03}.ndjson")))
                    .map(std::io::BufWriter::new)
            })
            .collect::<Result<_, _>>()?;
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        let mut spilled = 0u64;
        for row in records.iter()? {
            let (key, raw) = row?;
            let record: IndexRecord = serde_json::from_slice(raw.value())?;
            if record.status != RecordStatus::Published {
                continue;
            }
            let leaf = record.leaf_entry(key.value());
            let mut spill = &mut spills[record.partition_id as usize];
            serde_json::to_writer(&mut spill, &leaf)?;
            spill.write_all(b"\n")?;
            spilled += 1;
        }
        for mut spill in spills {
            spill.flush()?;
        }
        Ok(spilled)
    }

    pub fn set_local_root(&self, partition_id: u32, root: Hash) -> anyhow::Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut roots = txn.open_table(ROOTS)?;
            roots.insert(partition_id, root.0.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get_local_root(&self, partition_id: u32) -> anyhow::Result<Option<Hash>> {
        let txn = self.db.begin_read()?;
        let roots = txn.open_table(ROOTS)?;
        Ok(roots.get(partition_id)?.and_then(|raw| {
            let bytes: [u8; 32] = raw.value().try_into().ok()?;
            Some(Hash(bytes))
        }))
    }

    pub fn all_local_roots(&self) -> anyhow::Result<Vec<(u32, Hash)>> {
        let txn = self.db.begin_read()?;
        let roots = txn.open_table(ROOTS)?;
        let mut out = Vec::new();
        for row in roots.iter()? {
            let (partition_id, raw) = row?;
            if let Ok(bytes) = <[u8; 32]>::try_from(raw.value()) {
                out.push((partition_id.value(), Hash(bytes)));
            }
        }
        Ok(out)
    }

    pub fn queue_depth(&self) -> anyhow::Result<u64> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(QUEUE)?.len()?)
    }

    pub fn records_total(&self) -> anyhow::Result<u64> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(RECORDS)?.len()?)
    }

    /// Every published key of one partition — the expectation side of
    /// `registryd verify` (docs/operator-guide.md, "Verify").
    pub fn published_keys_for_partition(&self, partition_id: u32) -> anyhow::Result<Vec<String>> {
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        let mut out = Vec::new();
        for row in records.iter()? {
            let (key, raw) = row?;
            let record: IndexRecord = serde_json::from_slice(raw.value())?;
            if record.partition_id == partition_id && record.status == RecordStatus::Published {
                out.push(key.value().to_string());
            }
        }
        Ok(out)
    }

    /// ONE pass over the whole index, spilling published keys into one
    /// file per partition under `dir`. The full-audit path: iterating the
    /// index once per partition made verifying a multi-million-record
    /// registry take hours; this takes one scan.
    pub fn spill_published_keys(&self, dir: &Path, partitions: u32) -> anyhow::Result<u64> {
        use std::io::Write;
        std::fs::create_dir_all(dir)?;
        let mut files: Vec<std::io::BufWriter<std::fs::File>> = (0..partitions)
            .map(|p| {
                std::fs::File::create(dir.join(format!("{p:03}.keys"))).map(std::io::BufWriter::new)
            })
            .collect::<Result<_, _>>()?;
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        let mut total = 0u64;
        for row in records.iter()? {
            let (key, raw) = row?;
            let record: IndexRecord = serde_json::from_slice(raw.value())?;
            if record.status == RecordStatus::Published && record.partition_id < partitions {
                let file = &mut files[record.partition_id as usize];
                file.write_all(key.value().as_bytes())?;
                file.write_all(b"\n")?;
                total += 1;
            }
        }
        for mut file in files {
            file.flush()?;
        }
        Ok(total)
    }

    /// Every value hash the index knows about, regardless of status — the
    /// index side of the GC protect set: a pending record's value blob is
    /// unreferenced by any tree until its first publish, and must survive
    /// until then.
    pub fn all_content_hashes(&self) -> anyhow::Result<Vec<Hash>> {
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        let mut out = Vec::new();
        for row in records.iter()? {
            let (_, raw) = row?;
            let record: IndexRecord = serde_json::from_slice(raw.value())?;
            out.push(record.content_hash);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use registry_core::content_hash;

    fn record(value: &str, partition_id: u32) -> IndexRecord {
        IndexRecord {
            content_hash: content_hash(value),
            size: value.len() as u64,
            partition_id,
            status: RecordStatus::Pending,
            created_at: Utc::now(),
            content_code: None,
            published_at: None,
        }
    }

    fn open_temp() -> (tempfile::TempDir, RecordIndex) {
        let dir = tempfile::tempdir().unwrap();
        let index = RecordIndex::open(&dir.path().join("index.redb")).unwrap();
        (dir, index)
    }

    #[test]
    fn submit_is_immutable_per_key() {
        let (_dir, index) = open_temp();
        let first = record(r#"{"a":1}"#, 3);
        assert_eq!(index.submit("k1", &first).unwrap(), SubmitOutcome::Queued);
        assert_eq!(
            index.submit("k1", &first).unwrap(),
            SubmitOutcome::DuplicateIdentical
        );
        let different = record(r#"{"a":2}"#, 3);
        assert_eq!(
            index.submit("k1", &different).unwrap(),
            SubmitOutcome::Conflict {
                existing_hash: first.content_hash
            }
        );
        // The duplicate and the conflict must not have enqueued anything.
        assert_eq!(index.queue_depth().unwrap(), 1);
        assert_eq!(index.records_total().unwrap(), 1);
    }

    #[test]
    fn submit_many_is_atomic_and_positional() {
        let (_dir, index) = open_temp();
        let first = record(r#"{"a":1}"#, 1);
        let conflicting = record(r#"{"a":2}"#, 1);
        let outcomes = index
            .submit_many(&[
                ("x".to_string(), first.clone()),
                ("y".to_string(), first.clone()),
                // In-batch duplicate: first occurrence of "x" wins.
                ("x".to_string(), first.clone()),
                ("x".to_string(), conflicting.clone()),
            ])
            .unwrap();
        assert_eq!(outcomes[0], SubmitOutcome::Queued);
        assert_eq!(outcomes[1], SubmitOutcome::Queued);
        assert_eq!(outcomes[2], SubmitOutcome::DuplicateIdentical);
        assert_eq!(
            outcomes[3],
            SubmitOutcome::Conflict {
                existing_hash: first.content_hash
            }
        );
        assert_eq!(index.queue_depth().unwrap(), 2);
        assert_eq!(index.records_total().unwrap(), 2);
    }

    #[test]
    fn publish_cycle_drains_the_queue() {
        let (_dir, index) = open_temp();
        index.submit("a", &record(r#"{"n":1}"#, 1)).unwrap();
        index.submit("b", &record(r#"{"n":2}"#, 1)).unwrap();
        index.submit("c", &record(r#"{"n":3}"#, 2)).unwrap();

        let batch = index.pending_batch(10).unwrap();
        assert_eq!(batch.len(), 3);
        // Sequence order is submission order.
        let keys: Vec<&str> = batch.iter().map(|(_, k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);

        let done: Vec<(u64, String)> = batch
            .iter()
            .map(|(seq, key, _)| (*seq, key.clone()))
            .collect();
        index.mark_published(&done).unwrap();
        assert_eq!(index.queue_depth().unwrap(), 0);
        assert_eq!(
            index.get("a").unwrap().unwrap().status,
            RecordStatus::Published
        );
        assert!(index.get("a").unwrap().unwrap().published_at.is_some());
    }

    #[test]
    fn requeue_reenqueues_known_keys_only() {
        let (_dir, index) = open_temp();
        index.submit("a", &record(r#"{"n":1}"#, 1)).unwrap();
        let batch = index.pending_batch(10).unwrap();
        let done: Vec<(u64, String)> = batch
            .iter()
            .map(|(seq, key, _)| (*seq, key.clone()))
            .collect();
        index.mark_published(&done).unwrap();

        assert!(index.requeue("a").unwrap());
        assert!(!index.requeue("never-seen").unwrap());
        assert_eq!(index.queue_depth().unwrap(), 1);
    }

    #[test]
    fn partition_and_hash_enumeration() {
        let (_dir, index) = open_temp();
        index.submit("a", &record(r#"{"n":1}"#, 7)).unwrap();
        index.submit("b", &record(r#"{"n":2}"#, 7)).unwrap();
        index.submit("c", &record(r#"{"n":3}"#, 8)).unwrap();
        let batch = index.pending_batch(10).unwrap();
        let done: Vec<(u64, String)> = batch
            .iter()
            .filter(|(_, k, _)| k != "c")
            .map(|(seq, key, _)| (*seq, key.clone()))
            .collect();
        index.mark_published(&done).unwrap();

        let mut published = index.published_keys_for_partition(7).unwrap();
        published.sort();
        assert_eq!(published, vec!["a".to_string(), "b".to_string()]);
        assert!(index.published_keys_for_partition(8).unwrap().is_empty());
        assert_eq!(index.all_content_hashes().unwrap().len(), 3);
    }
}
