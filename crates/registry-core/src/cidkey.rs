//! Validation of the record `key` field, which must be a syntactically valid
//! CIDv1 string (docs/api.md, docs/api.md).
//! The key is operator-assigned and is *not* required to relate to the hash
//! of `value` (docs/data-model.md) — we only validate syntax
//! here, we never derive it.

use cid::Cid;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CidKeyError {
    #[error("key is not a syntactically valid CID: {0}")]
    InvalidCid(String),
    #[error("key must be CID version 1, got version {0}")]
    WrongVersion(u64),
}

/// Validate that `s` parses as a CIDv1 string. Returns the parsed `Cid` for
/// callers that want it, but most call sites only need the syntax check.
pub fn validate_cidv1(s: &str) -> Result<Cid, CidKeyError> {
    let cid = Cid::from_str(s).map_err(|e| CidKeyError::InvalidCid(e.to_string()))?;
    if cid.version() != cid::Version::V1 {
        return Err(CidKeyError::WrongVersion(cid.version() as u64));
    }
    Ok(cid)
}

pub fn is_valid_cidv1(s: &str) -> bool {
    validate_cidv1(s).is_ok()
}

/// The record key syntax the write path accepts: a standard CIDv1, or the
/// legacy declaration-identifier flavor described below — the upstream
/// declaration tooling this registry ingests from keys its existing
/// declarations with it, and keys must match that system verbatim for
/// cross-lookups to work.
pub fn is_valid_record_key(s: &str) -> bool {
    is_valid_cidv1(s) || is_legacy_declaration_id(s)
}

/// The upstream declaration tooling produces a CIDv1-*like* identifier
/// that is not parseable as a real CID: bytes
/// `0x01` (version) · `0x0c` (its private "JSON" codec — not a registered
/// multicodec) · `0x12 0x20` (sha2-256 multihash header) · 32-byte digest,
/// encoded as a base-x big integer over the RFC-4648 base32 alphabet with
/// NO multibase prefix. Validated here byte-for-byte, nothing looser.
fn is_legacy_declaration_id(s: &str) -> bool {
    // The 36-byte payload always starts 0x01, i.e. a 281-bit big integer:
    // exactly 57 base-32 digits. Reject other lengths before big-int work.
    if s.len() != 57 {
        return false;
    }
    let Some(bytes) = base_x_decode_b32(s) else {
        return false;
    };
    bytes.len() == 36 && bytes[..4] == [0x01, 0x0c, 0x12, 0x20]
}

/// Big-integer base decode matching the JS `base-x` package (which is NOT
/// RFC 4648: no bit-grouping, no padding; leading first-alphabet chars are
/// leading zero bytes).
fn base_x_decode_b32(input: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut bytes: Vec<u8> = Vec::with_capacity(40);
    for ch in input.bytes() {
        let mut carry = ALPHABET.iter().position(|&a| a == ch)? as u32;
        for b in bytes.iter_mut() {
            carry += (*b as u32) << 5;
            *b = (carry & 0xff) as u8;
            carry >>= 8;
        }
        while carry > 0 {
            bytes.push((carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let leading_zeros = input.bytes().take_while(|&c| c == ALPHABET[0]).count();
    bytes.extend(std::iter::repeat_n(0, leading_zeros));
    bytes.reverse();
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_cidv1() {
        // A real CIDv1 (dag-pb, sha2-256) sample.
        let sample = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        assert!(is_valid_cidv1(sample), "expected {sample} to be valid");
    }

    #[test]
    fn rejects_garbage() {
        assert!(!is_valid_cidv1("not-a-cid"));
        assert!(!is_valid_cidv1(""));
    }

    #[test]
    fn rejects_cidv0() {
        // A CIDv0 (base58btc sha2-256, always starts with "Qm").
        let v0 = "QmY7Yh4UquoXHLPFo2XbhXkhBvFoPwmQUSa92pxnxjQuPU";
        assert!(!is_valid_cidv1(v0));
    }

    #[test]
    fn record_key_accepts_both_flavors() {
        // Real identifiers produced by the upstream declaration tooling.
        let legacy = "bbqjcbw6mxhqxyl2tkvnxkud4osyy5wcpgkrowp7vp2qxgms2mvrkfxam";
        let legacy2 = "bbqjcb3t5id2vmqyfn3baag3wamgbjkjc7s325tv63kteisvgqbe6mwqf";
        let standard = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        assert!(is_valid_record_key(legacy));
        assert!(is_valid_record_key(legacy2));
        assert!(is_valid_record_key(standard));
        // They are NOT standard CIDs — if this ever starts passing, the
        // legacy arm of is_valid_record_key has become redundant.
        assert!(!is_valid_cidv1(legacy));
    }

    #[test]
    fn record_key_rejects_near_misses() {
        assert!(!is_valid_record_key("not-a-cid"));
        assert!(!is_valid_record_key(""));
        // Right length, wrong leading bytes once decoded.
        assert!(!is_valid_record_key(
            "zzqjcbw6mxhqxyl2tkvnxkud4osyy5wcpgkrowp7vp2qxgms2mvrkfxam"
        ));
        // Uppercase / non-alphabet chars.
        assert!(!is_valid_record_key(
            "BBQJCBW6MXHQXYL2TKVNXKUD4OSYY5WCPGKROWP7VP2QXGMS2MVRKFXAM"
        ));
    }
}
