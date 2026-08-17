//! Minimal ISCC (ISO 24138) decoding — just enough to recover the 64-bit
//! **Content-Code** unit that drives similarity search
//! (docs/similarity-search.md). The similarity engine
//! ([`crate::similarity`]) works purely on the resulting `u64`, so this
//! module's only job is string → code.
//!
//! Supported inputs:
//!   - a bare 64-bit Content-Code unit (`MainType = CONTENT`), and
//!   - the canonical 256-bit composite ISCC-CODE (`MainType = ISCC`,
//!     Meta+Content+Data+Instance × 64 bits), from which the Content-Code
//!     is extracted.
//!
//! **Conformance caveat (docs/similarity-search.md):** the header parse assumes the
//! common single-nibble encoding of MainType/SubType/Version/Length (true
//! for these types) and the standard composite layout. Full ISO 24138
//! coverage (wide units, all composite subtypes) should be validated against
//! official `iscc-core` vectors before production reliance. Decode failures
//! are non-fatal at the call site — the record is simply not indexed for
//! similarity.

use data_encoding::BASE32_NOPAD;
use thiserror::Error;

const MAINTYPE_CONTENT: u8 = 2;
const MAINTYPE_ISCC: u8 = 5;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IsccError {
    #[error("empty ISCC string")]
    Empty,
    #[error("invalid base32 encoding: {0}")]
    Base32(String),
    #[error("ISCC too short to contain a header and body")]
    TooShort,
    #[error("unsupported ISCC MainType {0} (need CONTENT or a standard composite ISCC-CODE)")]
    UnsupportedMainType(u8),
    #[error("unexpected body length {0} for the declared type")]
    UnexpectedBodyLength(usize),
}

/// Decode an ISCC string to its 64-bit Content-Code. Accepts an optional
/// `ISCC:` scheme prefix and is case-insensitive.
pub fn decode_content_code(iscc: &str) -> Result<u64, IsccError> {
    let trimmed = iscc.trim();
    if trimmed.is_empty() {
        return Err(IsccError::Empty);
    }
    let without_scheme = trimmed
        .strip_prefix("ISCC:")
        .or_else(|| trimmed.strip_prefix("iscc:"))
        .unwrap_or(trimmed);
    let normalized = without_scheme.trim().to_ascii_uppercase();

    let bytes = BASE32_NOPAD
        .decode(normalized.as_bytes())
        .map_err(|e| IsccError::Base32(e.to_string()))?;

    if bytes.len() < 2 {
        return Err(IsccError::TooShort);
    }
    let maintype = bytes[0] >> 4;
    let body = &bytes[2..];

    match maintype {
        MAINTYPE_CONTENT => {
            if body.len() < 8 {
                return Err(IsccError::UnexpectedBodyLength(body.len()));
            }
            Ok(u64::from_be_bytes(body[..8].try_into().unwrap()))
        }
        MAINTYPE_ISCC => {
            // Canonical composite: Meta(8) Content(8) Data(8) Instance(8).
            if body.len() != 32 {
                return Err(IsccError::UnexpectedBodyLength(body.len()));
            }
            Ok(u64::from_be_bytes(body[8..16].try_into().unwrap()))
        }
        other => Err(IsccError::UnsupportedMainType(other)),
    }
}

/// Encode a 64-bit Content-Code as a bare Content-Code unit ISCC string.
/// Used by tooling and round-trip tests; `subtype` is the ISCC content
/// subtype (TEXT=0, IMAGE=1, AUDIO=2, VIDEO=3, MIXED=4), which does not
/// affect the similarity code itself.
pub fn encode_content_code_unit(code: u64, subtype: u8) -> String {
    let mut buf = Vec::with_capacity(10);
    // header nibbles: [MainType=CONTENT][SubType][Version=0][Length=1 (64-bit)]
    buf.push((MAINTYPE_CONTENT << 4) | (subtype & 0x0F));
    buf.push(0x01);
    buf.extend_from_slice(&code.to_be_bytes());
    format!("ISCC:{}", BASE32_NOPAD.encode(&buf))
}

/// Extract the Content-Code from a record's serialized JSON value by reading
/// its ISCC field — matched case-insensitively (`ISCC`, `iscc`, ...)
/// because upstream producers differ: some emit lowercase `iscc`, the ISO
/// examples use uppercase. An exact-case `ISCC` match wins if both
/// are somehow present. Returns `None` on any problem (no field, wrong type,
/// undecodable) — callers treat that as "not indexable for similarity",
/// never as an error.
pub fn extract_from_json(value: &str) -> Option<u64> {
    let parsed: serde_json::Value = serde_json::from_str(value).ok()?;
    let object = parsed.as_object()?;
    let iscc = object
        .get("ISCC")
        .or_else(|| {
            object
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("iscc"))
                .map(|(_, v)| v)
        })?
        .as_str()?;
    decode_content_code(iscc).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_content_code_roundtrips() {
        for code in [
            0u64,
            1,
            0xdead_beef_cafe_babe,
            u64::MAX,
            0x0123_4567_89ab_cdef,
        ] {
            for subtype in 0..=4u8 {
                let s = encode_content_code_unit(code, subtype);
                assert!(s.starts_with("ISCC:"));
                assert_eq!(
                    decode_content_code(&s).unwrap(),
                    code,
                    "roundtrip failed for {code:#x}"
                );
            }
        }
    }

    #[test]
    fn decode_accepts_missing_scheme_and_lowercase() {
        let code = 0x1122_3344_5566_7788u64;
        let full = encode_content_code_unit(code, 1);
        let no_scheme = full.strip_prefix("ISCC:").unwrap();
        assert_eq!(decode_content_code(no_scheme).unwrap(), code);
        assert_eq!(decode_content_code(&full.to_lowercase()).unwrap(), code);
    }

    #[test]
    fn composite_iscc_code_content_unit_is_extracted() {
        // Build a canonical 256-bit composite: Meta, Content, Data, Instance.
        let meta = 0x1111_1111_1111_1111u64;
        let content = 0x2222_2222_2222_2222u64;
        let data = 0x3333_3333_3333_3333u64;
        let instance = 0x4444_4444_4444_4444u64;
        // header: MainType=ISCC in the high nibble, SubType 0; version/length 0
        let mut buf = vec![MAINTYPE_ISCC << 4, 0x00];
        for unit in [meta, content, data, instance] {
            buf.extend_from_slice(&unit.to_be_bytes());
        }
        let s = format!("ISCC:{}", BASE32_NOPAD.encode(&buf));
        assert_eq!(decode_content_code(&s).unwrap(), content);
    }

    #[test]
    fn rejects_garbage_and_empty() {
        assert_eq!(decode_content_code(""), Err(IsccError::Empty));
        assert!(matches!(
            decode_content_code("!!!!not-base32!!!!"),
            Err(IsccError::Base32(_))
        ));
    }

    #[test]
    fn extract_from_json_reads_the_field() {
        let code = 0xabcd_ef01_2345_6789u64;
        let iscc = encode_content_code_unit(code, 0);
        let value = format!(r#"{{"title":"x","ISCC":"{iscc}"}}"#);
        assert_eq!(extract_from_json(&value), Some(code));

        assert_eq!(extract_from_json(r#"{"no_iscc":true}"#), None);
        assert_eq!(extract_from_json(r#"{"ISCC":123}"#), None); // wrong type
        assert_eq!(extract_from_json("not json"), None);
    }

    #[test]
    fn extract_from_json_matches_field_case_insensitively() {
        // Some upstream producers emit lowercase `iscc`.
        let code = 0x5555_6666_7777_8888u64;
        let iscc = encode_content_code_unit(code, 2);
        let lower = format!(r#"{{"publicMetadata":{{}},"iscc":"{iscc}","companyId":"c1"}}"#);
        assert_eq!(extract_from_json(&lower), Some(code));

        let mixed = format!(r#"{{"Iscc":"{iscc}"}}"#);
        assert_eq!(extract_from_json(&mixed), Some(code));

        // exact-case ISCC wins over a case-variant when both exist
        let other = encode_content_code_unit(0x1u64, 2);
        let both = format!(r#"{{"iscc":"{other}","ISCC":"{iscc}"}}"#);
        assert_eq!(extract_from_json(&both), Some(code));
    }
}
