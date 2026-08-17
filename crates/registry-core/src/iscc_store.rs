//! Blob-backed, content-addressed banded-LSH index for ISCC similarity
//! search — the distributed form of [`crate::similarity::IsccIndex`],
//! docs/similarity-search.md.
//!
//! Like [`crate::hamt`], this is pure logic over the [`BlobStore`] trait, so
//! the *same* code builds the index on the writer and walks it on the
//! client. Structure (format v2, "segmented buckets"):
//!
//! - a **directory** blob (`{"v":2, "buckets": {bucket_key → manifest hash}}`):
//!   bounded at `ISCC_INDEX_BANDS × 2^band_width` entries (2 048 at the
//!   8-band default), one small blob.
//! - one **bucket manifest** blob per non-empty bucket: the ordered list of
//!   the bucket's segment hashes plus a member count.
//! - **segment** blobs: at most [`SEGMENT_MAX_MEMBERS`] `IsccMember`s each.
//!   Only the *tail* segment of a bucket is ever rewritten; full segments
//!   are immutable forever. This is what keeps a bucket's insert cost
//!   O(batch) instead of O(bucket): the v1 format stored each bucket as one
//!   monolithic posting list, so every insert rewrote the whole list — at
//!   3.8M records that meant ~2 GB of writes per rebuild cycle, and it only
//!   gets worse with corpus size.
//!
//! Insert-time deduplication checks only the tail segment (the only place a
//! same-batch redelivery can collide in practice). Duplicates that survive
//! across a segment rollover are tolerated in storage and removed at query
//! time — bounded, and far cheaper than scanning every segment per insert.
//!
//! The v1 (unsegmented) format is still readable: a legacy directory entry
//! is treated as a bucket whose single full segment is the old posting
//! list; the first insert into such a bucket migrates it to v2 layout.
//!
//! The index root (the directory hash) is published in the root pointer
//! document so readers discover it (docs/similarity-search.md). Updates are immutable
//! path-copying, exactly like the HAMT.

use crate::blobstore::{BlobStore, BlobStoreError};
use crate::hash::Hash;
use crate::similarity::{verify, BandParams, BucketId, IsccMember, SimilarityMatch};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// Members per segment blob. At ~80 serialized bytes per member this keeps
/// segments in the low hundreds of kilobytes — large enough that manifests
/// stay short, small enough that rewriting the tail is cheap.
pub const SEGMENT_MAX_MEMBERS: usize = 4096;

#[derive(Debug, Error)]
pub enum IsccStoreError {
    #[error(transparent)]
    BlobStore(#[from] BlobStoreError),
    #[error("index blob {0} is missing from the blob store")]
    MissingBlob(Hash),
    #[error("corrupt ISCC index blob {0}: {1}")]
    Corrupt(Hash, String),
}

/// v2 directory: bucket storage key → bucket manifest blob hash.
#[derive(Serialize, Deserialize)]
struct DirectoryV2 {
    v: u32,
    buckets: BTreeMap<String, Hash>,
}

/// Per-bucket manifest: ordered segment hashes, oldest first. Only the last
/// segment may be non-full.
#[derive(Serialize, Deserialize, Default, Clone)]
struct BucketManifest {
    count: u64,
    segments: Vec<Hash>,
}

/// A bucket reference as loaded from either format.
enum BucketRef {
    /// v2: manifest blob hash.
    Manifest(Hash),
    /// v1 legacy: the monolithic posting-list blob hash.
    LegacyList(Hash),
}

struct Directory {
    buckets: BTreeMap<String, BucketRef>,
}

async fn load_json<T: serde::de::DeserializeOwned>(
    store: &dyn BlobStore,
    hash: Hash,
) -> Result<T, IsccStoreError> {
    let bytes = store
        .get(&hash)
        .await?
        .ok_or(IsccStoreError::MissingBlob(hash))?;
    serde_json::from_slice(&bytes).map_err(|e| IsccStoreError::Corrupt(hash, e.to_string()))
}

async fn store_json<T: serde::Serialize>(
    store: &dyn BlobStore,
    value: &T,
) -> Result<Hash, IsccStoreError> {
    let bytes = serde_json::to_vec(value).expect("index structures always serialize");
    Ok(store.put(Bytes::from(bytes)).await?)
}

async fn load_directory(
    store: &dyn BlobStore,
    root: Option<Hash>,
) -> Result<Directory, IsccStoreError> {
    let Some(root) = root else {
        return Ok(Directory {
            buckets: BTreeMap::new(),
        });
    };
    let bytes = store
        .get(&root)
        .await?
        .ok_or(IsccStoreError::MissingBlob(root))?;
    // v2 first, then the v1 legacy plain map.
    if let Ok(v2) = serde_json::from_slice::<DirectoryV2>(&bytes) {
        if v2.v == 2 {
            return Ok(Directory {
                buckets: v2
                    .buckets
                    .into_iter()
                    .map(|(k, h)| (k, BucketRef::Manifest(h)))
                    .collect(),
            });
        }
    }
    let legacy: BTreeMap<String, Hash> =
        serde_json::from_slice(&bytes).map_err(|e| IsccStoreError::Corrupt(root, e.to_string()))?;
    Ok(Directory {
        buckets: legacy
            .into_iter()
            .map(|(k, h)| (k, BucketRef::LegacyList(h)))
            .collect(),
    })
}

async fn load_bucket_manifest(
    store: &dyn BlobStore,
    bucket: &BucketRef,
) -> Result<BucketManifest, IsccStoreError> {
    match bucket {
        BucketRef::Manifest(hash) => load_json(store, *hash).await,
        // Legacy bucket: its whole posting list acts as one (possibly
        // oversized) frozen segment.
        BucketRef::LegacyList(hash) => {
            let list: Vec<IsccMember> = load_json(store, *hash).await?;
            Ok(BucketManifest {
                count: list.len() as u64,
                segments: vec![*hash],
            })
        }
    }
}

async fn load_segment(
    store: &dyn BlobStore,
    hash: Hash,
) -> Result<Vec<IsccMember>, IsccStoreError> {
    load_json(store, hash).await
}

/// Apply a batch of members to the index, returning the new directory root.
/// Members whose content code is `None` are the caller's responsibility to
/// filter out; this takes concrete `IsccMember`s. Re-delivery of a recent
/// batch is deduplicated against the tail segment; older duplicates are
/// tolerated in storage and collapsed at query time.
pub async fn insert_batch(
    store: &dyn BlobStore,
    root: Option<Hash>,
    params: BandParams,
    members: &[IsccMember],
) -> Result<Hash, IsccStoreError> {
    let directory = load_directory(store, root).await?;
    let mut new_buckets: BTreeMap<String, Hash> = BTreeMap::new();

    // Group incoming members by bucket so each touched bucket is rewritten
    // exactly once per batch.
    let mut by_bucket: BTreeMap<String, Vec<IsccMember>> = BTreeMap::new();
    for member in members {
        for bucket in params.buckets(member.content_code) {
            by_bucket
                .entry(bucket.storage_key())
                .or_default()
                .push(member.clone());
        }
    }

    // Untouched buckets carry over; legacy entries are migrated lazily (a
    // manifest blob is only written for buckets this batch touches).
    for (key, bucket) in &directory.buckets {
        if by_bucket.contains_key(key) {
            continue;
        }
        let hash = match bucket {
            BucketRef::Manifest(h) => *h,
            BucketRef::LegacyList(h) => {
                let manifest = load_bucket_manifest(store, bucket).await?;
                let _ = h;
                store_json(store, &manifest).await?
            }
        };
        new_buckets.insert(key.clone(), hash);
    }

    for (bucket_key, new_members) in by_bucket {
        let mut manifest = match directory.buckets.get(&bucket_key) {
            Some(bucket) => load_bucket_manifest(store, bucket).await?,
            None => BucketManifest::default(),
        };

        // Work on the tail segment only: pop it if non-full, dedup incoming
        // members against it, then append and re-segment.
        let mut tail: Vec<IsccMember> = match manifest.segments.last() {
            Some(&hash) => {
                let seg = load_segment(store, hash).await?;
                if seg.len() < SEGMENT_MAX_MEMBERS {
                    manifest.segments.pop();
                    manifest.count -= seg.len() as u64;
                    seg
                } else {
                    Vec::new()
                }
            }
            None => Vec::new(),
        };

        for m in new_members {
            if !tail
                .iter()
                .any(|e| e.key == m.key && e.content_code == m.content_code)
            {
                tail.push(m);
            }
        }

        // Freeze full segments, keep at most one non-full tail.
        let mut start = 0usize;
        while start < tail.len() {
            let end = (start + SEGMENT_MAX_MEMBERS).min(tail.len());
            let segment: Vec<IsccMember> = tail[start..end].to_vec();
            manifest.count += segment.len() as u64;
            let hash = store_json(store, &segment).await?;
            manifest.segments.push(hash);
            start = end;
        }

        let manifest_hash = store_json(store, &manifest).await?;
        new_buckets.insert(bucket_key, manifest_hash);
    }

    store_json(
        store,
        &DirectoryV2 {
            v: 2,
            buckets: new_buckets,
        },
    )
    .await
}

/// Walk the index for `query_code` and return verified matches within
/// `radius`, sorted by ascending distance. `probe_radius` widens the per-
/// band lookup (docs/similarity-search.md); 0 = exact band match. Storage-level
/// duplicate postings (see [`insert_batch`]) are collapsed here.
pub async fn query(
    store: &dyn BlobStore,
    root: Option<Hash>,
    params: BandParams,
    query_code: u64,
    radius: u32,
    probe_radius: u32,
) -> Result<Vec<SimilarityMatch>, IsccStoreError> {
    let directory = load_directory(store, root).await?;
    if directory.buckets.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates: Vec<IsccMember> = Vec::new();
    let mut seen_buckets: std::collections::HashSet<String> = std::collections::HashSet::new();
    for bucket in params.query_buckets(query_code, probe_radius) {
        let key = bucket.storage_key();
        if !seen_buckets.insert(key.clone()) {
            continue;
        }
        if let Some(bucket_ref) = directory.buckets.get(&key) {
            let manifest = load_bucket_manifest(store, bucket_ref).await?;
            for segment_hash in manifest.segments {
                candidates.extend(load_segment(store, segment_hash).await?);
            }
        }
    }

    // Collapse duplicates from at-least-once inserts before verification.
    let mut seen: std::collections::HashSet<(u64, String)> =
        std::collections::HashSet::with_capacity(candidates.len());
    candidates.retain(|m| seen.insert((m.content_code, m.key.clone())));

    Ok(verify(query_code, &candidates, radius))
}

/// Collects every blob hash reachable from the index root — the directory,
/// every bucket manifest, and every segment hash (segments are recorded
/// without being fetched) — into `out`. Counterpart of
/// [`crate::hamt::collect_reachable`] for the writer's GC protect set.
pub async fn collect_reachable(
    store: &dyn BlobStore,
    root: Hash,
    out: &mut std::collections::HashSet<Hash>,
) -> Result<(), IsccStoreError> {
    if !out.insert(root) {
        return Ok(());
    }
    let directory = load_directory(store, Some(root)).await?;
    for bucket in directory.buckets.values() {
        match bucket {
            BucketRef::Manifest(hash) => {
                out.insert(*hash);
                let manifest: BucketManifest = load_json(store, *hash).await?;
                out.extend(manifest.segments);
            }
            BucketRef::LegacyList(hash) => {
                out.insert(*hash);
            }
        }
    }
    Ok(())
}

/// Bucket ids a member is indexed under — exposed for callers that want to
/// reason about placement without importing `similarity` directly.
pub fn member_buckets(params: BandParams, content_code: u64) -> Vec<BucketId> {
    params.buckets(content_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobstore::MemoryBlobStore;
    use crate::similarity::hamming;

    fn member(key: &str, code: u64) -> IsccMember {
        IsccMember {
            key: key.to_string(),
            content_code: code,
        }
    }

    #[tokio::test]
    async fn build_then_query_finds_near_duplicate() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let base = 0xdead_beef_cafe_babeu64;

        let root = insert_batch(&store, None, params, &[member("base", base)])
            .await
            .unwrap();

        // exact
        let hits = query(&store, Some(root), params, base, 4, 0).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].distance, 0);

        // near-dup with 2 flipped bits in the lowest band => collides there
        let near = base ^ 0b11;
        let hits = query(&store, Some(root), params, near, 4, 0).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "base");
        assert_eq!(hits[0].distance, 2);
    }

    #[tokio::test]
    async fn query_against_empty_index_is_empty() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let hits = query(&store, None, params, 123, 8, 0).await.unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn incremental_batches_accumulate_and_verify_exactly() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let query_code = 0u64;

        let mut root = insert_batch(&store, None, params, &[member("a", 0b1)])
            .await
            .unwrap();
        root = insert_batch(&store, Some(root), params, &[member("b", 0b11)])
            .await
            .unwrap();
        root = insert_batch(&store, Some(root), params, &[member("c", u64::MAX)])
            .await
            .unwrap();

        let hits = query(&store, Some(root), params, query_code, 3, 0)
            .await
            .unwrap();
        let keys: Vec<&str> = hits.iter().map(|h| h.key.as_str()).collect();
        assert_eq!(keys, ["a", "b"]); // c is distance 64, excluded
        for h in &hits {
            assert_eq!(hamming(query_code, h.content_code), h.distance);
        }
    }

    #[tokio::test]
    async fn duplicate_insert_is_idempotent() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let root1 = insert_batch(&store, None, params, &[member("x", 42)])
            .await
            .unwrap();
        let root2 = insert_batch(&store, Some(root1), params, &[member("x", 42)])
            .await
            .unwrap();
        // Re-inserting identical member must not duplicate it in results.
        let hits = query(&store, Some(root2), params, 42, 0, 0).await.unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn probing_recovers_spread_out_match() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let base = 0u64;
        let root = insert_batch(&store, None, params, &[member("base", base)])
            .await
            .unwrap();
        // one bit per band => no exact band match
        let spread: u64 = (0..8).map(|i| 1u64 << (i * 8)).sum();
        assert!(query(&store, Some(root), params, spread, 8, 0)
            .await
            .unwrap()
            .is_empty());
        let recovered = query(&store, Some(root), params, spread, 8, 1)
            .await
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].distance, 8);
    }

    #[tokio::test]
    async fn segments_roll_over_and_full_segments_are_reused() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        // All members share band values (same code) so they land in the same
        // buckets; unique keys keep them distinct members.
        let code = 0xabcd_ef01_2345_6789u64;
        let batch: Vec<IsccMember> = (0..SEGMENT_MAX_MEMBERS + 10)
            .map(|i| member(&format!("k{i}"), code))
            .collect();

        let root = insert_batch(&store, None, params, &batch).await.unwrap();
        let hits = query(&store, Some(root), params, code, 0, 0).await.unwrap();
        assert_eq!(hits.len(), SEGMENT_MAX_MEMBERS + 10);

        // Inserting one more member must not rewrite the frozen full
        // segment: the new root's bucket manifest must still reference it.
        let root2 = insert_batch(&store, Some(root), params, &[member("extra", code)])
            .await
            .unwrap();
        let hits = query(&store, Some(root2), params, code, 0, 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), SEGMENT_MAX_MEMBERS + 11);

        let dir = load_directory(&store, Some(root)).await.unwrap();
        let dir2 = load_directory(&store, Some(root2)).await.unwrap();
        let bucket_key = params.buckets(code)[0].storage_key();
        let m1 = load_bucket_manifest(&store, &dir.buckets[&bucket_key])
            .await
            .unwrap();
        let m2 = load_bucket_manifest(&store, &dir2.buckets[&bucket_key])
            .await
            .unwrap();
        assert_eq!(
            m1.segments[0], m2.segments[0],
            "full segment must be structurally shared"
        );
        assert_ne!(m1.segments.last(), m2.segments.last());
    }

    #[tokio::test]
    async fn legacy_v1_directory_is_readable_and_migrates_on_insert() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let code = 0x1111_2222_3333_4444u64;

        // Hand-build a v1 index: plain map of bucket key -> posting list.
        let list = vec![member("old", code)];
        let list_hash = store_json(&store, &list).await.unwrap();
        let mut legacy: BTreeMap<String, Hash> = BTreeMap::new();
        for bucket in params.buckets(code) {
            legacy.insert(bucket.storage_key(), list_hash);
        }
        let legacy_root = store_json(&store, &legacy).await.unwrap();

        // Readable as-is.
        let hits = query(&store, Some(legacy_root), params, code, 0, 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "old");

        // Insert migrates to v2 and keeps the old member.
        let root2 = insert_batch(&store, Some(legacy_root), params, &[member("new", code)])
            .await
            .unwrap();
        let hits = query(&store, Some(root2), params, code, 0, 0)
            .await
            .unwrap();
        let mut keys: Vec<&str> = hits.iter().map(|h| h.key.as_str()).collect();
        keys.sort();
        assert_eq!(keys, ["new", "old"]);
    }

    #[tokio::test]
    async fn cross_segment_duplicates_collapse_at_query_time() {
        let store = MemoryBlobStore::new();
        let params = BandParams::new(8).unwrap();
        let code = 0x5555_0000_aaaa_ffffu64;
        // Fill exactly one segment, then redeliver an old member: the tail
        // dedup cannot see it (it lives in the frozen segment), so storage
        // holds a duplicate — the query must still return it once.
        let batch: Vec<IsccMember> = (0..SEGMENT_MAX_MEMBERS)
            .map(|i| member(&format!("k{i}"), code))
            .collect();
        let root = insert_batch(&store, None, params, &batch).await.unwrap();
        let root2 = insert_batch(&store, Some(root), params, &[member("k0", code)])
            .await
            .unwrap();
        let hits = query(&store, Some(root2), params, code, 0, 0)
            .await
            .unwrap();
        assert_eq!(hits.len(), SEGMENT_MAX_MEMBERS);
    }
}
