//! Approximate Hamming-distance similarity search over 64-bit ISCC
//! Content-Codes — docs/similarity-search.md.
//!
//! This module is pure `u64` logic with no I/O: banding, bucketing,
//! candidate generation, and exact verification. The same functions drive
//! both the writer-side index construction and the client-side query walk,
//! so their agreement on bucket ids is guaranteed by construction. The
//! blob-backed distributed form (a HAMT keyed by [`BucketId::storage_key`]
//! with `{key, content_code}` posting lists) reuses these primitives; the
//! candidate-verification heart ([`verify`]) is identical whether members
//! come from memory or from fetched bucket blobs.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// ISCC Content-Codes are 64-bit similarity hashes (ISO 24138).
pub const CONTENT_CODE_BITS: u32 = 64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SimilarityError {
    #[error("ISCC_INDEX_BANDS ({0}) must be between 1 and 64 and divide 64 evenly")]
    InvalidBandCount(u32),
    #[error("requested radius {requested} exceeds the configured maximum {max}")]
    RadiusTooLarge { requested: u32, max: u32 },
}

/// Hamming distance between two Content-Codes — the number of differing bits.
#[inline]
pub fn hamming(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Banding configuration. The band count must divide 64 evenly so every band
/// has equal width; this keeps bucket math branch-free and the recall/
/// selectivity behaviour uniform across bands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandParams {
    num_bands: u32,
}

impl BandParams {
    pub fn new(num_bands: u32) -> Result<Self, SimilarityError> {
        if num_bands == 0
            || num_bands > CONTENT_CODE_BITS
            || !CONTENT_CODE_BITS.is_multiple_of(num_bands)
        {
            return Err(SimilarityError::InvalidBandCount(num_bands));
        }
        Ok(Self { num_bands })
    }

    pub fn num_bands(&self) -> u32 {
        self.num_bands
    }

    pub fn band_width(&self) -> u32 {
        CONTENT_CODE_BITS / self.num_bands
    }

    /// The value of band `band_index` (0-based, most-significant band first)
    /// within `code`.
    fn band_value(&self, code: u64, band_index: u32) -> u64 {
        let width = self.band_width();
        let shift = CONTENT_CODE_BITS - width * (band_index + 1);
        let mask = if width == CONTENT_CODE_BITS {
            u64::MAX
        } else {
            (1u64 << width) - 1
        };
        (code >> shift) & mask
    }

    /// The bucket id for each band of `code` — one per band. A record is
    /// indexed under all of these; a query looks up all of these (plus any
    /// probe neighbours).
    pub fn buckets(&self, code: u64) -> Vec<BucketId> {
        (0..self.num_bands)
            .map(|band_index| BucketId {
                band_index,
                band_value: self.band_value(code, band_index),
            })
            .collect()
    }

    /// Query buckets including per-band probing: for each band, the exact
    /// bucket plus every bucket whose band value is within `probe_radius`
    /// Hamming distance of the query's band value. `probe_radius = 0`
    /// reduces to [`buckets`]. Raising it boosts recall near the search-
    /// radius cap at the cost of more lookups (∑ C(width, i) buckets/band).
    pub fn query_buckets(&self, code: u64, probe_radius: u32) -> Vec<BucketId> {
        if probe_radius == 0 {
            return self.buckets(code);
        }
        let width = self.band_width();
        let mut out = Vec::new();
        for band_index in 0..self.num_bands {
            let base = self.band_value(code, band_index);
            for flip in bit_flip_masks(width, probe_radius) {
                out.push(BucketId {
                    band_index,
                    band_value: base ^ flip,
                });
            }
        }
        out
    }
}

/// Identifies one posting-list bucket: which band, and the band's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BucketId {
    pub band_index: u32,
    pub band_value: u64,
}

impl BucketId {
    /// Stable string key for addressing this bucket in the distributed ISCC
    /// index HAMT (docs/similarity-search.md). Zero-padded hex keeps the keyspace
    /// lexicographically ordered and fixed-width.
    pub fn storage_key(&self) -> String {
        format!("iscc/{:02}/{:016x}", self.band_index, self.band_value)
    }
}

/// One record's presence in a bucket: its key plus its full Content-Code, so
/// verification needs no blob fetch (docs/similarity-search.md, "Inline codes").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsccMember {
    pub key: String,
    pub content_code: u64,
}

/// A verified similarity match, sorted by ascending distance by [`verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimilarityMatch {
    pub key: String,
    pub content_code: u64,
    pub distance: u32,
}

/// Exact verification: from a candidate member set (deduplicated by key,
/// keeping the smallest distance), return those within `radius` of `query`,
/// sorted by ascending distance then key. This is exact — there are **no
/// false positives**; only recall (missing true matches) is approximate, and
/// that is governed entirely by which candidates the bucket lookup produced.
pub fn verify(query: u64, candidates: &[IsccMember], radius: u32) -> Vec<SimilarityMatch> {
    let mut best: BTreeMap<&str, u32> = BTreeMap::new();
    for m in candidates {
        let d = hamming(query, m.content_code);
        if d <= radius {
            best.entry(m.key.as_str())
                .and_modify(|cur| {
                    if d < *cur {
                        *cur = d;
                    }
                })
                .or_insert(d);
        }
    }
    let mut matches: Vec<SimilarityMatch> = candidates
        .iter()
        .filter_map(|m| {
            best.get(m.key.as_str()).and_then(|&d| {
                if hamming(query, m.content_code) == d {
                    Some(SimilarityMatch {
                        key: m.key.clone(),
                        content_code: m.content_code,
                        distance: d,
                    })
                } else {
                    None
                }
            })
        })
        .collect();
    matches.sort_by(|a, b| a.distance.cmp(&b.distance).then_with(|| a.key.cmp(&b.key)));
    matches.dedup_by(|a, b| a.key == b.key);
    matches
}

/// In-memory banded-LSH index. Used directly for small datasets and tests,
/// and as the reference the blob-backed distributed index mirrors bucket-for-
/// bucket (both use [`BandParams::buckets`] to place members and
/// [`BandParams::query_buckets`] + [`verify`] to query).
#[derive(Debug, Clone)]
pub struct IsccIndex {
    params: BandParams,
    buckets: BTreeMap<BucketId, Vec<IsccMember>>,
}

impl IsccIndex {
    pub fn new(params: BandParams) -> Self {
        Self {
            params,
            buckets: BTreeMap::new(),
        }
    }

    pub fn params(&self) -> BandParams {
        self.params
    }

    pub fn insert(&mut self, key: impl Into<String>, content_code: u64) {
        let member = IsccMember {
            key: key.into(),
            content_code,
        };
        for bucket in self.params.buckets(content_code) {
            let list = self.buckets.entry(bucket).or_default();
            if !list.iter().any(|m| m.key == member.key) {
                list.push(member.clone());
            }
        }
    }

    pub fn bucket(&self, id: &BucketId) -> Option<&[IsccMember]> {
        self.buckets.get(id).map(|v| v.as_slice())
    }

    /// Collect candidate members for `query` from the relevant buckets.
    pub fn candidates(&self, query: u64, probe_radius: u32) -> Vec<IsccMember> {
        let mut out = Vec::new();
        for bucket in self.params.query_buckets(query, probe_radius) {
            if let Some(members) = self.buckets.get(&bucket) {
                out.extend_from_slice(members);
            }
        }
        out
    }

    /// Full query: candidate generation + exact verification.
    pub fn query(&self, query: u64, radius: u32, probe_radius: u32) -> Vec<SimilarityMatch> {
        verify(query, &self.candidates(query, probe_radius), radius)
    }
}

/// All bit-flip masks over a `width`-bit field with popcount in `0..=radius`,
/// i.e. every value reachable within Hamming `radius` of zero. Used to
/// enumerate a band's probe neighbourhood.
fn bit_flip_masks(width: u32, radius: u32) -> Vec<u64> {
    let radius = radius.min(width);
    let mut masks = vec![0u64];
    let mut current = vec![0u64];
    for _ in 1..=radius {
        let mut next = Vec::new();
        for &m in &current {
            let highest = 64 - m.leading_zeros();
            for bit in highest..width {
                next.push(m | (1u64 << bit));
            }
        }
        masks.extend_from_slice(&next);
        current = next;
    }
    masks
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    #[test]
    fn hamming_basics() {
        assert_eq!(hamming(0, 0), 0);
        assert_eq!(hamming(0, 1), 1);
        assert_eq!(hamming(0, u64::MAX), 64);
        assert_eq!(hamming(0b1010, 0b0101), 4);
    }

    #[test]
    fn band_count_must_divide_64() {
        assert!(BandParams::new(8).is_ok());
        assert!(BandParams::new(16).is_ok());
        assert!(BandParams::new(1).is_ok());
        assert!(BandParams::new(64).is_ok());
        assert!(BandParams::new(0).is_err());
        assert!(BandParams::new(7).is_err());
        assert!(BandParams::new(65).is_err());
    }

    #[test]
    fn bands_cover_all_bits_without_overlap() {
        let p = BandParams::new(8).unwrap();
        // Reconstruct the code from its band values; must be lossless.
        let code = 0x0123_4567_89ab_cdefu64;
        let width = p.band_width();
        let mut reconstructed = 0u64;
        for b in p.buckets(code) {
            let shift = CONTENT_CODE_BITS - width * (b.band_index + 1);
            reconstructed |= b.band_value << shift;
        }
        assert_eq!(reconstructed, code);
    }

    #[test]
    fn identical_codes_are_distance_zero_and_share_all_buckets() {
        let p = BandParams::new(8).unwrap();
        let code = 0xdead_beef_cafe_babeu64;
        assert_eq!(p.buckets(code), p.buckets(code));
        let mut idx = IsccIndex::new(p);
        idx.insert("k", code);
        let hits = idx.query(code, 0, 0);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].distance, 0);
    }

    #[test]
    fn verify_has_no_false_positives_and_sorts_ascending() {
        let query = 0u64;
        let members = vec![
            IsccMember {
                key: "far".into(),
                content_code: u64::MAX,
            }, // dist 64
            IsccMember {
                key: "near".into(),
                content_code: 0b11,
            }, // dist 2
            IsccMember {
                key: "exact".into(),
                content_code: 0,
            }, // dist 0
            IsccMember {
                key: "edge".into(),
                content_code: 0xF,
            }, // dist 4
        ];
        let out = verify(query, &members, 4);
        assert_eq!(
            out.iter().map(|m| m.key.as_str()).collect::<Vec<_>>(),
            ["exact", "near", "edge"]
        );
        assert!(out.iter().all(|m| m.distance <= 4));
        assert!(!out.iter().any(|m| m.key == "far"));
    }

    #[test]
    fn verify_dedups_by_key_keeping_smallest_distance() {
        // A member can appear from multiple buckets; keep one row, best dist.
        let query = 0u64;
        let members = vec![
            IsccMember {
                key: "dup".into(),
                content_code: 0b11,
            },
            IsccMember {
                key: "dup".into(),
                content_code: 0b11,
            },
        ];
        let out = verify(query, &members, 8);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].distance, 2);
    }

    #[test]
    fn near_duplicate_is_found_via_banding() {
        // Flip a few bits inside a single band; other bands match exactly, so
        // the near-duplicate is guaranteed to collide there.
        let p = BandParams::new(8).unwrap();
        let base = 0x0000_0000_0000_0000u64;
        let near = 0b0000_0111u64; // 3 bits, all in the lowest band
        let mut idx = IsccIndex::new(p);
        idx.insert("base", base);
        let hits = idx.query(near, 3, 0);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "base");
        assert_eq!(hits[0].distance, 3);
    }

    #[test]
    fn probing_recovers_matches_that_exact_banding_misses() {
        // One differing bit in every band => no band matches exactly =>
        // exact-band query misses it, but probe_radius=1 recovers it.
        let p = BandParams::new(8).unwrap();
        let base = 0u64;
        // set the lowest bit of each 8-bit band: bits 0,8,16,...,56
        let spread: u64 = (0..8).map(|i| 1u64 << (i * 8)).sum();
        assert_eq!(hamming(base, spread), 8);
        let mut idx = IsccIndex::new(p);
        idx.insert("base", base);
        assert!(
            idx.query(spread, 8, 0).is_empty(),
            "exact banding should miss it"
        );
        let recovered = idx.query(spread, 8, 1);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].distance, 8);
    }

    #[test]
    fn recall_is_high_for_close_matches_property() {
        // Insert random codes, plus planted near-duplicates of a query at
        // small distances; assert the planted close ones are all recovered
        // and every returned match is truly within radius (exactness).
        let p = BandParams::new(8).unwrap();
        let mut rng = StdRng::seed_from_u64(42);
        let mut idx = IsccIndex::new(p);
        let query: u64 = rng.gen();

        // background noise
        for i in 0..5_000 {
            idx.insert(format!("noise-{i}"), rng.gen::<u64>());
        }
        // planted near-duplicates within a single band (guaranteed findable)
        let mut planted = Vec::new();
        for i in 0..50 {
            let bits_to_flip = (i % 5) + 1; // 1..=5 bits, all in lowest band
            let mask = (1u64 << bits_to_flip) - 1;
            let code = query ^ mask;
            let key = format!("planted-{i}");
            idx.insert(&key, code);
            planted.push((key, hamming(query, code)));
        }

        let radius = 8;
        let hits = idx.query(query, radius, 0);

        // exactness: no false positives
        for h in &hits {
            assert!(h.distance <= radius, "returned a match beyond radius");
            assert_eq!(hamming(query, h.content_code), h.distance);
        }
        // recall: every planted single-band near-duplicate within radius found
        let found: std::collections::HashSet<&str> = hits.iter().map(|h| h.key.as_str()).collect();
        for (key, dist) in &planted {
            if *dist <= radius {
                assert!(
                    found.contains(key.as_str()),
                    "missed planted match {key} at dist {dist}"
                );
            }
        }
    }
}
