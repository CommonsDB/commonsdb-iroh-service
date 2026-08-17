//! Shared, dependency-light building blocks used by every service in the
//! workspace: CIDv1 validation, the record/leaf-entry types, the
//! partitioning scheme, the `BlobStore` abstraction, and the HAMT engine
//! itself. Nothing in this crate depends on iroh — see `registry-node` for
//! that integration. This separation is what lets the HAMT engine (the
//! trickiest correctness-critical piece of the whole system) be unit
//! tested with zero I/O.

pub mod blobstore;
pub mod cidkey;
pub mod config;
pub mod hamt;
pub mod hash;
pub mod iscc;
pub mod iscc_store;
pub mod partition;
pub mod record;
pub mod similarity;

pub use blobstore::{BlobStore, BlobStoreError, MemoryBlobStore};
pub use cidkey::{is_valid_cidv1, is_valid_record_key, validate_cidv1, CidKeyError};
pub use hamt::HamtError;
pub use hash::Hash;
pub use partition::{
    descent_byte, key_digest, partition_id_for_key, partition_key, LEAF_MAX_ENTRIES_DEFAULT,
    TOP_LEVEL_PARTITIONS_DEFAULT,
};
pub use record::{content_hash, LeafEntry, RecordStatus, RecordSubmission};
pub use similarity::{
    hamming, BandParams, BucketId, IsccIndex, IsccMember, SimilarityError, SimilarityMatch,
    CONTENT_CODE_BITS,
};
