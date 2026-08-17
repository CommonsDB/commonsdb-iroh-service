//! The per-partition hash array mapped trie (HAMT) — docs/data-model.md
//! ("Sharding and the index structure") and docs/data-model.md
//! ("HAMT rebuild cycle"). Written purely against the `BlobStore` trait so its
//! correctness (insert, split, path-copying, lookup) is verified with plain
//! unit tests, independent of whatever backs blob storage in a given binary
//! (`iroh-blobs` in the real services, `MemoryBlobStore` here).
//!
//! Node wire format: each node is prefixed with a one-byte type tag
//! (`TAG_LEAF` / `TAG_INTERMEDIATE`) followed by the type-specific body. The
//! spec (docs/data-model.md) describes leaves as bare NDJSON and intermediates
//! as a bare 256x32-byte array; a leading tag byte is added here because
//! `iroh-blobs` blobs carry no out-of-band type metadata, so a node must be
//! self-describing to be decoded correctly during descent.

use crate::blobstore::{BlobStore, BlobStoreError};
use crate::hash::Hash;
use crate::partition::{descent_byte, key_digest};
use crate::record::LeafEntry;
use bytes::Bytes;
use std::collections::HashSet;
use thiserror::Error;

pub const INTERMEDIATE_FANOUT: usize = 256;

const TAG_LEAF: u8 = 0;
const TAG_INTERMEDIATE: u8 = 1;

#[derive(Debug, Error)]
pub enum HamtError {
    #[error(transparent)]
    BlobStore(#[from] BlobStoreError),
    #[error("corrupt HAMT node at {0}: {1}")]
    CorruptNode(Hash, String),
    #[error("node referenced by hash {0} is missing from the blob store")]
    MissingNode(Hash),
    #[error("HAMT descent exhausted digest bytes for key '{0}' at depth {1} (fanout too shallow for a sha256 digest? this should not happen with the default 256-way fanout)")]
    DepthExhausted(String, usize),
}

#[derive(Debug, Clone)]
pub enum HamtNode {
    Leaf(Vec<LeafEntry>),
    Intermediate(Box<[Option<Hash>; INTERMEDIATE_FANOUT]>),
}

impl HamtNode {
    fn empty_leaf() -> Self {
        HamtNode::Leaf(Vec::new())
    }

    fn encode(&self) -> Bytes {
        match self {
            HamtNode::Leaf(entries) => {
                let mut out = Vec::with_capacity(1 + entries.len() * 96);
                out.push(TAG_LEAF);
                for e in entries {
                    serde_json::to_writer(&mut out, e).expect("LeafEntry always serializes");
                    out.push(b'\n');
                }
                Bytes::from(out)
            }
            HamtNode::Intermediate(slots) => {
                let mut out = vec![0u8; 1 + INTERMEDIATE_FANOUT * 32];
                out[0] = TAG_INTERMEDIATE;
                for (i, slot) in slots.iter().enumerate() {
                    if let Some(h) = slot {
                        let start = 1 + i * 32;
                        out[start..start + 32].copy_from_slice(&h.0);
                    }
                }
                Bytes::from(out)
            }
        }
    }

    pub fn decode(bytes: &[u8], node_hash: Hash) -> Result<Self, HamtError> {
        match bytes.first() {
            Some(&TAG_LEAF) => {
                let mut entries = Vec::new();
                for line in bytes[1..].split(|&b| b == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    let entry: LeafEntry = serde_json::from_slice(line)
                        .map_err(|e| HamtError::CorruptNode(node_hash, e.to_string()))?;
                    entries.push(entry);
                }
                Ok(HamtNode::Leaf(entries))
            }
            Some(&TAG_INTERMEDIATE) => {
                let expected = 1 + INTERMEDIATE_FANOUT * 32;
                if bytes.len() != expected {
                    return Err(HamtError::CorruptNode(
                        node_hash,
                        format!(
                            "expected {expected} bytes for an intermediate node, got {}",
                            bytes.len()
                        ),
                    ));
                }
                let mut slots: Box<[Option<Hash>; INTERMEDIATE_FANOUT]> =
                    Box::new([None; INTERMEDIATE_FANOUT]);
                for (i, slot) in slots.iter_mut().enumerate() {
                    let start = 1 + i * 32;
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&bytes[start..start + 32]);
                    if h != [0u8; 32] {
                        *slot = Some(Hash(h));
                    }
                }
                Ok(HamtNode::Intermediate(slots))
            }
            _ => Err(HamtError::CorruptNode(
                node_hash,
                "unrecognized node tag byte".into(),
            )),
        }
    }
}

/// Resolve a single key against a partition's current HAMT root (or `None`
/// for a partition that has never been built). Cost is at most one blob
/// fetch per trie level plus the leaf — docs/data-model.md's "at most ~4 blob
/// fetches beyond the root" estimate at the trillion-record target.
pub async fn lookup(
    store: &dyn BlobStore,
    root: Option<Hash>,
    key: &str,
) -> Result<Option<LeafEntry>, HamtError> {
    let Some(mut node_hash) = root else {
        return Ok(None);
    };
    let digest = key_digest(key);
    let mut depth = 0usize;
    loop {
        let bytes = store
            .get(&node_hash)
            .await?
            .ok_or(HamtError::MissingNode(node_hash))?;
        match HamtNode::decode(&bytes, node_hash)? {
            HamtNode::Leaf(entries) => return Ok(entries.into_iter().find(|e| e.key == key)),
            HamtNode::Intermediate(slots) => {
                let b = descent_byte(&digest, depth)
                    .ok_or_else(|| HamtError::DepthExhausted(key.to_string(), depth))?;
                match slots[b as usize] {
                    Some(child) => {
                        node_hash = child;
                        depth += 1;
                    }
                    None => return Ok(None),
                }
            }
        }
    }
}

/// Recursively walks every node reachable from `root`, fetching each one
/// (and, for leaves, every referenced content blob) through `store`. Used
/// by `storectl seed` (docs/reader-guide.md) to proactively
/// warm a node's local cache for a partition so it can re-serve that
/// partition's data to other peers. Against a plain local `BlobStore` this
/// is a no-op re-read; against `registry_node::IrohBlobStore` (which
/// fetches from network providers on a local miss) this is what actually
/// pulls the data in. Returns the number of leaf entries visited.
#[async_recursion::async_recursion]
pub async fn warm_cache(store: &dyn BlobStore, root: Hash) -> Result<usize, HamtError> {
    let bytes = store
        .get(&root)
        .await?
        .ok_or(HamtError::MissingNode(root))?;
    match HamtNode::decode(&bytes, root)? {
        HamtNode::Leaf(entries) => {
            let count = entries.len();
            for entry in entries {
                store.get(&entry.hash).await?;
            }
            Ok(count)
        }
        HamtNode::Intermediate(slots) => {
            let mut total = 0;
            for child in slots.iter().flatten() {
                total += warm_cache(store, *child).await?;
            }
            Ok(total)
        }
    }
}

/// Collects every blob hash reachable from `root` — index node hashes AND
/// the value hashes leaf entries point at — into `out`. Backs the writer's
/// garbage-collection protect set: anything NOT collected by this walk (or
/// its ISCC-index counterpart) is an orphan from path-copying and may be
/// swept. Reads only index nodes; values are never fetched.
#[async_recursion::async_recursion]
pub async fn collect_reachable(
    store: &dyn BlobStore,
    root: Hash,
    out: &mut std::collections::HashSet<Hash>,
) -> Result<(), HamtError> {
    if !out.insert(root) {
        return Ok(());
    }
    let bytes = store
        .get(&root)
        .await?
        .ok_or(HamtError::MissingNode(root))?;
    match HamtNode::decode(&bytes, root)? {
        HamtNode::Leaf(entries) => {
            for entry in entries {
                out.insert(entry.hash);
            }
            Ok(())
        }
        HamtNode::Intermediate(slots) => {
            for child in slots.iter().flatten() {
                collect_reachable(store, *child, out).await?;
            }
            Ok(())
        }
    }
}

/// Recursively collects every leaf entry reachable from `root`, fetching
/// only index nodes (never the referenced content blobs — unlike
/// [`warm_cache`]). Backs `storectl list`'s debugging enumeration; entries
/// come back in the HAMT's deterministic traversal order.
#[async_recursion::async_recursion]
pub async fn walk_entries(
    store: &dyn BlobStore,
    root: Hash,
    out: &mut Vec<LeafEntry>,
) -> Result<(), HamtError> {
    let bytes = store
        .get(&root)
        .await?
        .ok_or(HamtError::MissingNode(root))?;
    match HamtNode::decode(&bytes, root)? {
        HamtNode::Leaf(entries) => {
            out.extend(entries);
            Ok(())
        }
        HamtNode::Intermediate(slots) => {
            for child in slots.iter().flatten() {
                walk_entries(store, *child, out).await?;
            }
            Ok(())
        }
    }
}

/// Result of a bounded, fault-tolerant walk: `complete` is false when the
/// deadline expired or nodes were unfetchable mid-walk — the entries
/// collected so far are still returned, because a partial listing beats
/// "unreadable" while the network is mid-republish and the writer is the
/// only peer serving the newest roots.
pub struct WalkOutcome {
    pub entries: Vec<LeafEntry>,
    pub complete: bool,
}

/// Breadth-first variant of [`walk_entries`] that fetches each level's
/// nodes concurrently (`concurrency` in-flight blob reads) under a hard
/// `deadline`. The serial walker descends one node at a time, so over a
/// network store every round-trip is paid in full; here a level's fetches
/// overlap, which is what makes walking a populated partition from a
/// remote provider take seconds instead of minutes. Entries are returned
/// sorted by key.
pub async fn walk_entries_parallel<S>(
    store: &S,
    root: Hash,
    deadline: std::time::Duration,
    concurrency: usize,
) -> WalkOutcome
where
    S: BlobStore + Clone + Send + Sync + 'static,
{
    let started = tokio::time::Instant::now();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut frontier: Vec<Hash> = vec![root];
    let mut entries: Vec<LeafEntry> = Vec::new();
    let mut complete = true;
    'levels: while !frontier.is_empty() {
        // One batched fetch for the whole level (no-op on local stores):
        // the per-node gets below then hit the local store instead of
        // paying a network round-trip each. The deadline is only checked
        // BETWEEN requests, never enforced by cancelling one: dropping a
        // batch future mid-transfer aborts a half-written blob and
        // poisons the iroh-blobs store (observed as cascading "poisoned
        // storage" panics killing a reader mid-sync). Each request is
        // small and bounded, so the overrun past the deadline is too.
        let Some(remaining) = deadline.checked_sub(started.elapsed()) else {
            complete = false;
            break 'levels;
        };
        store.prefetch(&frontier, remaining).await;
        let mut tasks = tokio::task::JoinSet::new();
        for hash in frontier.drain(..) {
            let store = store.clone();
            let semaphore = semaphore.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire().await;
                let bytes = store
                    .get(&hash)
                    .await?
                    .ok_or(HamtError::MissingNode(hash))?;
                HamtNode::decode(&bytes, hash)
            });
        }
        loop {
            let Some(remaining) = deadline.checked_sub(started.elapsed()) else {
                complete = false;
                tasks.abort_all();
                break 'levels;
            };
            let joined = match tokio::time::timeout(remaining, tasks.join_next()).await {
                Ok(Some(joined)) => joined,
                Ok(None) => break, // level finished
                Err(_) => {
                    complete = false;
                    tasks.abort_all();
                    break 'levels;
                }
            };
            match joined {
                Ok(Ok(HamtNode::Leaf(leaf_entries))) => entries.extend(leaf_entries),
                Ok(Ok(HamtNode::Intermediate(slots))) => {
                    frontier.extend(slots.iter().flatten().copied());
                }
                _ => complete = false, // unfetchable subtree; keep the rest
            }
        }
    }
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    WalkOutcome { entries, complete }
}

/// Insert (or idempotently no-op re-insert) a single entry, returning the
/// new partition root hash. Performs standard HAMT path-copying: only nodes
/// on the path from the modified leaf to the root are rewritten; every
/// sibling subtree is reused unchanged.
pub async fn insert_one(
    store: &dyn BlobStore,
    root: Option<Hash>,
    entry: LeafEntry,
    leaf_max_entries: usize,
) -> Result<Hash, HamtError> {
    let digest = key_digest(&entry.key);
    insert_at(store, root, 0, &digest, entry, leaf_max_entries).await
}

/// Apply many pending entries in one pass, as `registryd`'s publisher
/// does per batch (docs/data-model.md, "Publishing").
/// Entries already present in `denylist` are silently excluded, matching
/// the takedown workflow in docs/operator-guide.md.
///
/// Implementation note: entries are folded sequentially through
/// [`insert_one`] rather than grouped/parallelized per subtree. This is
/// correct (each fold step is a full, consistent path-copy) but not the
/// maximum-throughput strategy the spec's "batch many pending entries into
/// a single pass" note gestures at — fanning writes out per-subtree is a
/// worthwhile follow-up if rebuild throughput ever becomes the
/// bottleneck.
pub async fn insert_batch(
    store: &dyn BlobStore,
    root: Option<Hash>,
    entries: impl IntoIterator<Item = LeafEntry>,
    leaf_max_entries: usize,
    denylist: &HashSet<String>,
) -> Result<Hash, HamtError> {
    let mut current = root;
    for entry in entries {
        if denylist.contains(&entry.key) {
            continue;
        }
        current = Some(insert_one(store, current, entry, leaf_max_entries).await?);
    }
    match current {
        Some(h) => Ok(h),
        None => Ok(store.put(HamtNode::empty_leaf().encode()).await?),
    }
}

/// Build a partition's entire HAMT bottom-up from its full entry set,
/// writing every node exactly once. The result is byte-identical to what
/// incremental [`insert_one`] calls converge to (a node is a leaf iff its
/// subtree holds at most `leaf_max_entries` entries; leaves are sorted by
/// key; both facts are insert-order-independent), but the cost is O(final
/// tree size) instead of O(records × leaf size) — the difference between
/// bulk-loading millions of records in minutes versus days of rewriting
/// path-copied leaves.
///
/// Duplicate keys in the input collapse first-occurrence-wins, matching
/// the incremental path's first-write-wins.
pub async fn build_from_entries(
    store: &dyn BlobStore,
    entries: Vec<LeafEntry>,
    leaf_max_entries: usize,
) -> Result<Hash, HamtError> {
    let mut seen = HashSet::with_capacity(entries.len());
    let mut unique: Vec<(LeafEntry, [u8; 32])> = Vec::with_capacity(entries.len());
    for entry in entries {
        if seen.insert(entry.key.clone()) {
            let digest = key_digest(&entry.key);
            unique.push((entry, digest));
        }
    }
    build_subtree(store, unique, 0, leaf_max_entries).await
}

#[async_recursion::async_recursion]
async fn build_subtree(
    store: &dyn BlobStore,
    mut entries: Vec<(LeafEntry, [u8; 32])>,
    depth: usize,
    leaf_max_entries: usize,
) -> Result<Hash, HamtError> {
    if entries.len() <= leaf_max_entries {
        entries.sort_by(|a, b| a.0.key.cmp(&b.0.key));
        let leaf: Vec<LeafEntry> = entries.into_iter().map(|(e, _)| e).collect();
        return Ok(store.put(HamtNode::Leaf(leaf).encode()).await?);
    }
    let mut buckets: Vec<Vec<(LeafEntry, [u8; 32])>> =
        (0..INTERMEDIATE_FANOUT).map(|_| Vec::new()).collect();
    for (entry, digest) in entries {
        let byte = descent_byte(&digest, depth)
            .ok_or_else(|| HamtError::DepthExhausted(entry.key.clone(), depth))?;
        buckets[byte as usize].push((entry, digest));
    }
    let mut slots: Box<[Option<Hash>; INTERMEDIATE_FANOUT]> = Box::new([None; INTERMEDIATE_FANOUT]);
    for (i, bucket) in buckets.into_iter().enumerate() {
        if bucket.is_empty() {
            continue;
        }
        slots[i] = Some(build_subtree(store, bucket, depth + 1, leaf_max_entries).await?);
    }
    Ok(store.put(HamtNode::Intermediate(slots).encode()).await?)
}

#[async_recursion::async_recursion]
async fn insert_at(
    store: &dyn BlobStore,
    node_hash: Option<Hash>,
    depth: usize,
    digest: &[u8; 32],
    entry: LeafEntry,
    leaf_max_entries: usize,
) -> Result<Hash, HamtError> {
    let node = match node_hash {
        Some(h) => {
            let bytes = store.get(&h).await?.ok_or(HamtError::MissingNode(h))?;
            HamtNode::decode(&bytes, h)?
        }
        None => HamtNode::empty_leaf(),
    };

    match node {
        HamtNode::Leaf(mut entries) => {
            if let Some(pos) = entries.iter().position(|e| e.key == entry.key) {
                // Already present: at-least-once queue redelivery or a
                // genuinely duplicate submission that the record index
                // already accepted as an idempotent resubmission
                // (docs/data-model.md, "Existence and duplicate detection").
                // Either way this is first-write-wins and a structural no-op
                // — the path is left untouched rather than rewritten.
                let _ = pos;
                return node_hash.ok_or_else(|| {
                    HamtError::CorruptNode(
                        Hash::ZERO,
                        "leaf entry found without a node hash".into(),
                    )
                });
            }

            entries.push(entry);
            entries.sort_by(|a, b| a.key.cmp(&b.key));

            if entries.len() <= leaf_max_entries {
                return Ok(store.put(HamtNode::Leaf(entries).encode()).await?);
            }

            // Split: standard HAMT leaf-to-intermediate conversion,
            // redistributing every entry by its next descent byte.
            let mut buckets: Vec<Vec<LeafEntry>> =
                (0..INTERMEDIATE_FANOUT).map(|_| Vec::new()).collect();
            for e in entries {
                let d = key_digest(&e.key);
                let b = descent_byte(&d, depth)
                    .ok_or_else(|| HamtError::DepthExhausted(e.key.clone(), depth))?;
                buckets[b as usize].push(e);
            }
            let mut slots: Box<[Option<Hash>; INTERMEDIATE_FANOUT]> =
                Box::new([None; INTERMEDIATE_FANOUT]);
            for (i, bucket) in buckets.into_iter().enumerate() {
                if bucket.is_empty() {
                    continue;
                }
                slots[i] = Some(store.put(HamtNode::Leaf(bucket).encode()).await?);
            }
            Ok(store.put(HamtNode::Intermediate(slots).encode()).await?)
        }
        HamtNode::Intermediate(mut slots) => {
            let b = descent_byte(digest, depth)
                .ok_or_else(|| HamtError::DepthExhausted(entry.key.clone(), depth))?;
            let child = slots[b as usize];
            let new_child =
                insert_at(store, child, depth + 1, digest, entry, leaf_max_entries).await?;
            slots[b as usize] = Some(new_child);
            Ok(store.put(HamtNode::Intermediate(slots).encode()).await?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::MemoryBlobStore;
    use chrono::Utc;

    fn entry(key: &str) -> LeafEntry {
        LeafEntry {
            key: key.to_string(),
            hash: Hash::of(key.as_bytes()),
            size: key.len() as u64,
            added_at: Utc::now(),
            content_code: None,
        }
    }

    #[tokio::test]
    async fn insert_then_lookup_single_entry() {
        let store = MemoryBlobStore::new();
        let root = insert_one(&store, None, entry("bafyone"), 1024)
            .await
            .unwrap();
        let found = lookup(&store, Some(root), "bafyone").await.unwrap();
        assert_eq!(found.unwrap().key, "bafyone");
        assert!(lookup(&store, Some(root), "bafymissing")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn lookup_against_empty_root_is_none() {
        let store = MemoryBlobStore::new();
        assert!(lookup(&store, None, "anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn duplicate_insert_is_idempotent_noop() {
        let store = MemoryBlobStore::new();
        let e = entry("bafydup");
        let root1 = insert_one(&store, None, e.clone(), 1024).await.unwrap();
        let root2 = insert_one(&store, Some(root1), e, 1024).await.unwrap();
        assert_eq!(
            root1, root2,
            "re-inserting the same key+hash must not change the root"
        );
    }

    #[tokio::test]
    async fn leaf_splits_past_max_entries() {
        let store = MemoryBlobStore::new();
        let leaf_max = 4usize;
        let mut root = None;
        let keys: Vec<String> = (0..50).map(|i| format!("bafykey{i:04}")).collect();
        for k in &keys {
            root = Some(insert_one(&store, root, entry(k), leaf_max).await.unwrap());
        }
        // Every inserted key must still resolve correctly after however many
        // splits occurred.
        for k in &keys {
            let found = lookup(&store, root, k).await.unwrap();
            assert_eq!(
                found.map(|e| e.key),
                Some(k.clone()),
                "lookup failed for {k}"
            );
        }
        // A key that was never inserted must resolve to None even after splits.
        assert!(lookup(&store, root, "bafynotthere")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn insert_batch_applies_all_and_skips_denylisted() {
        let store = MemoryBlobStore::new();
        let mut denylist = HashSet::new();
        denylist.insert("bafyblocked".to_string());
        let entries = vec![entry("bafyok1"), entry("bafyblocked"), entry("bafyok2")];
        let root = insert_batch(&store, None, entries, 1024, &denylist)
            .await
            .unwrap();

        assert!(lookup(&store, Some(root), "bafyok1")
            .await
            .unwrap()
            .is_some());
        assert!(lookup(&store, Some(root), "bafyok2")
            .await
            .unwrap()
            .is_some());
        assert!(lookup(&store, Some(root), "bafyblocked")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn path_copying_reuses_untouched_siblings() {
        let store = MemoryBlobStore::new();
        let leaf_max = 2usize;
        let mut root = insert_one(&store, None, entry("a"), leaf_max)
            .await
            .unwrap();
        root = insert_one(&store, Some(root), entry("b"), leaf_max)
            .await
            .unwrap();
        root = insert_one(&store, Some(root), entry("c"), leaf_max)
            .await
            .unwrap();
        let count_after_three = store.len();

        // Inserting a fourth, unrelated key should only add nodes along its
        // own path, not rewrite the whole tree from scratch.
        root = insert_one(&store, Some(root), entry("d"), leaf_max)
            .await
            .unwrap();
        let count_after_four = store.len();
        assert!(
            count_after_four - count_after_three <= 3,
            "expected only a small number of new nodes on the modified path, got {}",
            count_after_four - count_after_three
        );
        assert!(lookup(&store, Some(root), "a").await.unwrap().is_some());
        assert!(lookup(&store, Some(root), "d").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn bulk_build_matches_incremental_inserts_exactly() {
        let store = MemoryBlobStore::new();
        let leaf_max = 4usize;
        let keys: Vec<String> = (0..300).map(|i| format!("bafybulk{i:04}")).collect();
        let entries: Vec<LeafEntry> = keys.iter().map(|k| entry(k)).collect();

        let mut incremental = None;
        for e in entries.clone() {
            incremental = Some(insert_one(&store, incremental, e, leaf_max).await.unwrap());
        }

        let bulk = build_from_entries(&store, entries, leaf_max).await.unwrap();
        assert_eq!(
            incremental.unwrap(),
            bulk,
            "bottom-up build must produce the identical root hash"
        );
        for k in &keys {
            assert!(lookup(&store, Some(bulk), k).await.unwrap().is_some());
        }
        assert!(lookup(&store, Some(bulk), "bafymissing")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn bulk_build_dedupes_first_occurrence_wins() {
        let store = MemoryBlobStore::new();
        let first = entry("bafydup");
        let mut second = entry("bafydup");
        second.size = 999;
        let root = build_from_entries(&store, vec![first.clone(), second], 4)
            .await
            .unwrap();
        let found = lookup(&store, Some(root), "bafydup")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.size, first.size);
    }

    #[tokio::test]
    async fn warm_cache_visits_every_leaf_entry() {
        let store = MemoryBlobStore::new();
        let leaf_max = 2usize;
        let mut root = None;
        let keys: Vec<String> = (0..20).map(|i| format!("bafykey{i:04}")).collect();
        for k in &keys {
            root = Some(insert_one(&store, root, entry(k), leaf_max).await.unwrap());
        }
        let visited = warm_cache(&store, root.unwrap()).await.unwrap();
        assert_eq!(visited, keys.len());
    }
}
