use crate::hash::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::RwLock;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobStoreError {
    #[error("blob I/O error: {0}")]
    Io(String),
}

/// Content-addressed blob storage abstraction. The HAMT engine and all its
/// tests are written purely against this trait so their correctness can be
/// verified with zero network/iroh dependency (see `MemoryBlobStore` below);
/// `registry-node` provides the real `iroh-blobs`-backed implementation
/// used by the actual services.
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(&self, bytes: Bytes) -> Result<Hash, BlobStoreError>;
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>, BlobStoreError>;

    async fn put_and_hash(&self, bytes: Bytes) -> Result<Hash, BlobStoreError> {
        self.put(bytes).await
    }

    /// Hint that these blobs are about to be read. A purely local store has
    /// nothing to do; a network-backed store can pull every missing blob in
    /// one batched request instead of paying a full round-trip per `get` —
    /// the difference between walking a remote partition in seconds versus
    /// minutes. Best-effort by contract: failures surface later as ordinary
    /// per-blob `get` misses. `budget` is a soft deadline: implementations
    /// stop *starting* work past it (an individual transfer already in
    /// flight is always driven to completion — cancelling one mid-write
    /// can corrupt-mark the underlying store), so a wedged or slow store
    /// delays a caller by at most one bounded request, never forever.
    async fn prefetch(&self, _hashes: &[Hash], _budget: std::time::Duration) {}
}

/// Simple in-process `BlobStore` used by unit tests and as a lightweight
/// local fallback. Not durable — never used by a deployed service.
#[derive(Default)]
pub struct MemoryBlobStore {
    inner: RwLock<HashMap<Hash, Bytes>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(&self, bytes: Bytes) -> Result<Hash, BlobStoreError> {
        let hash = Hash::of(&bytes);
        self.inner.write().unwrap().insert(hash, bytes);
        Ok(hash)
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>, BlobStoreError> {
        Ok(self.inner.read().unwrap().get(hash).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_then_get_roundtrips() {
        let store = MemoryBlobStore::new();
        let hash = store.put(Bytes::from_static(b"hello")).await.unwrap();
        let back = store.get(&hash).await.unwrap();
        assert_eq!(back, Some(Bytes::from_static(b"hello")));
    }

    #[tokio::test]
    async fn missing_hash_returns_none() {
        let store = MemoryBlobStore::new();
        let missing = Hash::of(b"never stored");
        assert_eq!(store.get(&missing).await.unwrap(), None);
    }
}
