use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash as StdHash;

/// A 32-byte BLAKE3 content hash, used to address every blob in the system
/// (record values, HAMT nodes). Matches the hash `iroh-blobs` itself produces
/// for the same bytes, so a hash computed at the HTTP tier and the hash
/// `iroh-blobs` assigns on import are guaranteed to be the same value.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, StdHash)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub const ZERO: Hash = Hash([0u8; 32]);

    pub fn of(bytes: &[u8]) -> Self {
        Hash(*blake3::hash(bytes).as_bytes())
    }

    pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
        let s = s.strip_prefix("b3-").unwrap_or(s);
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out)?;
        Ok(Hash(out))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// The `b3-<hex>` display form used in the write API's JSON responses
    /// (see docs/api.md).
    pub fn to_prefixed(&self) -> String {
        format!("b3-{}", self.to_hex())
    }

    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 32]
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", self.to_hex())
    }
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Hash::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

impl From<blake3::Hash> for Hash {
    fn from(h: blake3::Hash) -> Self {
        Hash(*h.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_hex() {
        let h = Hash::of(b"hello world");
        let hex = h.to_hex();
        let back = Hash::from_hex(&hex).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn prefixed_roundtrip() {
        let h = Hash::of(b"hello world");
        let prefixed = h.to_prefixed();
        assert!(prefixed.starts_with("b3-"));
        let back = Hash::from_hex(&prefixed).unwrap();
        assert_eq!(h, back);
    }
}
