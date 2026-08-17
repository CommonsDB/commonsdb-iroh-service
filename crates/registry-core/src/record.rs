use crate::hash::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One entry inside a HAMT leaf node — docs/data-model.md,
/// "HAMT node structure".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeafEntry {
    pub key: String,
    pub hash: Hash,
    pub size: u64,
    pub added_at: DateTime<Utc>,
    /// The record's 64-bit ISCC Content-Code, if one could be decoded from
    /// its value (docs/similarity-search.md). Carried here
    /// so a reader resolving a *known* key gets the code for free — the "I
    /// have a key, is the value worth downloading?" path — without fetching
    /// the value. `None` when the value had no decodable ISCC. `#[serde]`
    /// defaults keep older leaves (written before this field existed)
    /// readable and omit the field entirely when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_code: Option<u64>,
}

/// Lifecycle status of a record as tracked in the record index —
/// docs/data-model.md, "Existence and duplicate detection".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStatus {
    Pending,
    Published,
    Denylisted,
}

impl RecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordStatus::Pending => "pending",
            RecordStatus::Published => "published",
            RecordStatus::Denylisted => "denylisted",
        }
    }
}

/// A submitted record as accepted by the write API — docs/api.md.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordSubmission {
    pub key: String,
    pub value: String,
}

/// Compute the same BLAKE3 content hash both the write API's existence
/// check and `iroh-blobs` will independently arrive at for a given
/// value's UTF-8 bytes.
pub fn content_hash(value: &str) -> Hash {
    Hash::of(value.as_bytes())
}
