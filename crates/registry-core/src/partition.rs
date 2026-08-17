//! Top-level partitioning and per-depth descent bytes for the HAMT —
//! docs/data-model.md, "Top-level partitions" and "HAMT node
//! structure".
//!
//! `partition_id = first_byte(sha256(key)) mod TOP_LEVEL_PARTITIONS`. Within
//! a partition's own trie, descent at depth `d` (0-indexed, starting
//! immediately under the partition root) uses `sha256(key)[1 + d]` — i.e.
//! the partition "consumes" byte 0 of the digest, and the trie underneath it
//! consumes the following bytes in order. This keeps a single sha256 digest
//! per key sufficient for both the partition assignment and the entire
//! descent path.

use sha2::{Digest, Sha256};

pub const TOP_LEVEL_PARTITIONS_DEFAULT: u32 = 256;
pub const LEAF_MAX_ENTRIES_DEFAULT: usize = 1024;

/// The full sha256 digest of a key, computed once and reused for both
/// partition assignment and HAMT descent.
pub fn key_digest(key: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.finalize().into()
}

pub fn partition_id(digest: &[u8; 32], top_level_partitions: u32) -> u32 {
    (digest[0] as u32) % top_level_partitions
}

pub fn partition_id_for_key(key: &str, top_level_partitions: u32) -> u32 {
    partition_id(&key_digest(key), top_level_partitions)
}

/// The byte of the digest used to select a child slot at HAMT depth `depth`
/// (0-indexed, first level under the partition root). Digest byte 0 is
/// reserved for partition assignment, so depth 0 uses byte 1, depth 1 uses
/// byte 2, and so on. Partitions default to 256 slots per level, so at most
/// 31 levels are addressable from one sha256 digest — vastly more than the
/// ~4 levels docs/data-model.md estimates are needed even at
/// the 1-trillion-record target.
pub fn descent_byte(digest: &[u8; 32], depth: usize) -> Option<u8> {
    digest.get(1 + depth).copied()
}

pub fn partition_key(partition_id: u32) -> String {
    format!("partition/{partition_id:03}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_is_stable_and_bounded() {
        for key in [
            "a",
            "b",
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
        ] {
            let p1 = partition_id_for_key(key, 256);
            let p2 = partition_id_for_key(key, 256);
            assert_eq!(p1, p2);
            assert!(p1 < 256);
        }
    }

    #[test]
    fn partition_key_format() {
        assert_eq!(partition_key(42), "partition/042");
        assert_eq!(partition_key(0), "partition/000");
    }
}
