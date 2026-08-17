//! The root pointer document — docs/data-model.md and
//! docs/data-model.md, "Root pointer document (iroh-docs)". One
//! small `iroh-docs` document with one entry per top-level partition,
//! `partition/<id> -> current HAMT root hash`. The only mutable structure
//! in the system; everything else (HAMT nodes, record values) is immutable
//! content addressed by its own hash.
//!
//! Doc entry values in `iroh-docs` are themselves content-addressed (an
//! entry's "value" is a `(hash, size)` pointing at a blob, never inline
//! bytes) — so writing a partition root here stores the 32-byte hash as its
//! own tiny blob via the shared `iroh-blobs` store, and reading it back
//! requires one extra local blob fetch after resolving the doc entry.

use crate::blob_store::IrohBlobStore;
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::api::Doc;
use iroh_docs::store::Query;
use iroh_docs::{AuthorId, DocTicket};
use registry_core::{partition_key, BlobStore, Hash as CommonHash};

/// Pointer-document entry key under which the ISCC similarity index's
/// directory root is published — docs/similarity-search.md.
pub const ISCC_INDEX_ROOT_KEY: &str = "iscc-index/root";

pub struct PointerDoc {
    doc: Doc,
    blobs: IrohBlobStore,
    /// `Some` for the writer (`registryd`, holding write capability + an
    /// author identity to sign entries); `None` for every read-only holder
    /// (`storectl` and other reader nodes).
    writer_author: Option<AuthorId>,
}

impl PointerDoc {
    pub fn new(doc: Doc, blobs: IrohBlobStore, writer_author: Option<AuthorId>) -> Self {
        Self {
            doc,
            blobs,
            writer_author,
        }
    }

    pub async fn get_partition_root(
        &self,
        partition_id: u32,
    ) -> anyhow::Result<Option<CommonHash>> {
        self.get_named_root(&partition_key(partition_id)).await
    }

    /// Read any named root entry (a 32-byte hash stored as a tiny blob) from
    /// the pointer document. Besides `partition/<id>`, this carries the ISCC
    /// similarity index root under [`ISCC_INDEX_ROOT_KEY`]
    /// (docs/similarity-search.md).
    pub async fn get_named_root(&self, key: &str) -> anyhow::Result<Option<CommonHash>> {
        let Some(entry) = self
            .doc
            .get_one(Query::key_exact(key.as_bytes()).build())
            .await?
        else {
            return Ok(None);
        };
        let content_hash = CommonHash(*entry.content_hash().as_bytes());
        // The entry exists, so its 32-byte content MUST be readable: mapping
        // an unreadable content blob to `Ok(None)` ("no root") is how a
        // transient store hiccup silently became a from-empty partition
        // rebuild on the write side (observed live as shrinking partition
        // counts). Distinguish "no entry" (a real None, above) from "entry
        // present but content unreadable" (always an error).
        let Some(bytes) = self
            .blobs
            .get(&content_hash)
            .await
            .map_err(|e| anyhow::anyhow!("failed to fetch pointer doc entry blob: {e}"))?
        else {
            anyhow::bail!(
                "pointer doc entry for {key} exists but its content blob {content_hash} is unreadable; refusing to treat as missing"
            );
        };
        if bytes.len() != 32 {
            anyhow::bail!(
                "corrupt root pointer entry for {key}: expected 32 bytes, got {}",
                bytes.len()
            );
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(CommonHash(arr)))
    }

    /// Write-side only. Publishes a new HAMT root hash for `partition_id` —
    /// docs/data-model.md, "Publishing".
    pub async fn set_partition_root(
        &self,
        partition_id: u32,
        hash: CommonHash,
    ) -> anyhow::Result<()> {
        self.set_named_root(partition_key(partition_id), hash).await
    }

    /// Write-side only. Publishes any named root entry — see
    /// [`Self::get_named_root`].
    pub async fn set_named_root(&self, key: String, hash: CommonHash) -> anyhow::Result<()> {
        let author = self
            .writer_author
            .ok_or_else(|| anyhow::anyhow!("this PointerDoc handle holds no write capability"))?;
        self.doc
            .set_bytes(author, key.into_bytes(), hash.0.to_vec())
            .await?;
        Ok(())
    }

    /// Snapshot every `partition/<id>` root in one pass: a single streamed
    /// doc query plus concurrent reads of the tiny content blobs, instead
    /// of one serial `get_partition_root` round-trip per partition. A
    /// partition whose entry blob is unreadable right now is simply absent
    /// from the map — callers refresh continuously, so it is picked up on
    /// a later snapshot (readers only; the writer's read path keeps the
    /// strict unreadable-is-an-error semantics of [`Self::get_named_root`]).
    pub async fn partition_roots(
        &self,
    ) -> anyhow::Result<std::collections::HashMap<u32, CommonHash>> {
        use n0_future::StreamExt;
        let entries = self
            .doc
            .get_many(Query::key_prefix("partition/").build())
            .await?;
        let mut entries = std::pin::pin!(entries);
        let mut pending: Vec<(u32, CommonHash)> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            let key = String::from_utf8_lossy(entry.key()).into_owned();
            let Some(id) = key
                .strip_prefix("partition/")
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            pending.push((id, CommonHash(*entry.content_hash().as_bytes())));
        }
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(32));
        let mut tasks = tokio::task::JoinSet::new();
        for (id, content_hash) in pending {
            let blobs = self.blobs.clone();
            let semaphore = semaphore.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire().await;
                let bytes = blobs.get(&content_hash).await.ok().flatten()?;
                let arr: [u8; 32] = bytes.as_ref().try_into().ok()?;
                Some((id, CommonHash(arr)))
            });
        }
        let mut roots = std::collections::HashMap::new();
        while let Some(joined) = tasks.join_next().await {
            if let Ok(Some((id, root))) = joined {
                roots.insert(id, root);
            }
        }
        Ok(roots)
    }

    /// Generate a read-only ticket for this document — the artifact
    /// embedded in `storectl` release builds and handed to third parties.
    /// See docs/operator-guide.md, "Read ticket redistribution".
    /// Content hashes of every entry in the document — the doc's own blob
    /// footprint, needed by the writer's GC protect set (entry contents are
    /// blobs in the same store as everything else).
    pub async fn entry_content_hashes(&self) -> anyhow::Result<Vec<CommonHash>> {
        use n0_future::StreamExt;
        let entries = self.doc.get_many(Query::all().build()).await?;
        let mut entries = std::pin::pin!(entries);
        let mut hashes = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            hashes.push(CommonHash(*entry.content_hash().as_bytes()));
        }
        Ok(hashes)
    }

    pub async fn share_read_ticket(&self) -> anyhow::Result<DocTicket> {
        self.doc
            .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
            .await
    }

    pub fn doc(&self) -> &Doc {
        &self.doc
    }
}
