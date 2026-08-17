//! Bootstraps a single local iroh node with all three protocols this
//! project needs wired together: `iroh-blobs` (content storage/transfer),
//! `iroh-gossip` (propagation acceleration), and `iroh-docs` (the root
//! pointer document) — docs/data-model.md. Every binary in this workspace
//! that speaks iroh (`registryd`, `storectl`) goes through this one
//! constructor so the wiring is defined exactly once.

use crate::blob_store::IrohBlobStore;
use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use iroh_blobs::api::Store as BlobsStore;
use iroh_blobs::BlobsProtocol;
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;
use std::path::PathBuf;

pub struct RegistryNode {
    pub endpoint: Endpoint,
    pub blobs_store: BlobsStore,
    pub docs: Docs,
    router: Router,
}

impl RegistryNode {
    /// A persistent node: the blob store and the docs `redb` file live
    /// under independently-configurable paths — two subdirectories of the
    /// daemon's data directory in the default `registryd` layout. Local
    /// disk only; never point these at a network filesystem.
    pub async fn spawn_persistent(
        secret_key: SecretKey,
        blob_store_path: PathBuf,
        docs_store_path: PathBuf,
    ) -> anyhow::Result<Self> {
        Self::spawn_persistent_with_gc(secret_key, blob_store_path, docs_store_path, None).await
    }

    /// [`spawn_persistent`](Self::spawn_persistent) with blob-store garbage
    /// collection enabled. Path-copying writers (HAMT/ISCC-index rebuilds)
    /// orphan superseded nodes on every batch; without GC the store grows
    /// unboundedly and startup scans slow down with it (observed live:
    /// 35-minute opens). The caller's `GcConfig::add_protected` callback
    /// must enumerate every reachable hash (`hamt::collect_reachable` +
    /// `iscc_store::collect_reachable` + doc entry content) — the sweep
    /// removes everything else.
    pub async fn spawn_persistent_with_gc(
        secret_key: SecretKey,
        blob_store_path: PathBuf,
        docs_store_path: PathBuf,
        gc: Option<iroh_blobs::store::GcConfig>,
    ) -> anyhow::Result<Self> {
        // The docs redb store opens a file inside this directory and fails
        // if it doesn't exist; the blobs FsStore creates its own.
        tokio::fs::create_dir_all(&docs_store_path).await?;
        tokio::fs::create_dir_all(&blob_store_path).await?;
        let mut options = iroh_blobs::store::fs::options::Options::new(&blob_store_path);
        options.gc = gc;
        let blobs_store = iroh_blobs::store::fs::FsStore::load_with_opts(
            blob_store_path.join("blobs.db"),
            options,
        )
        .await?;
        Self::spawn_with_store(secret_key, blobs_store.into(), Some(docs_store_path)).await
    }

    /// An ephemeral node: in-memory blob cache, in-memory docs storage — a
    /// one-off `storectl get --ephemeral` invocation (docs/reader-guide.md).
    pub async fn spawn_ephemeral(secret_key: SecretKey) -> anyhow::Result<Self> {
        let blobs_store = iroh_blobs::store::mem::MemStore::new();
        Self::spawn_with_store(secret_key, blobs_store.into(), None).await
    }

    async fn spawn_with_store(
        secret_key: SecretKey,
        blobs_store: BlobsStore,
        docs_store_path: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(secret_key)
            .bind()
            .await?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let docs = match &docs_store_path {
            Some(dir) => {
                Docs::persistent(dir.clone())
                    .spawn(endpoint.clone(), blobs_store.clone(), gossip.clone())
                    .await?
            }
            None => {
                Docs::memory()
                    .spawn(endpoint.clone(), blobs_store.clone(), gossip.clone())
                    .await?
            }
        };

        let blobs_protocol = BlobsProtocol::new(&blobs_store, None);

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs_protocol)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        Ok(Self {
            endpoint,
            blobs_store,
            docs,
            router,
        })
    }

    pub fn blob_store(&self) -> IrohBlobStore {
        IrohBlobStore::local_only(self.blobs_store.clone())
    }

    pub fn blob_store_with_providers(&self, providers: Vec<iroh::EndpointAddr>) -> IrohBlobStore {
        IrohBlobStore::with_providers(self.blobs_store.clone(), self.endpoint.clone(), providers)
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        // Flush the blob store while it is still guaranteed open. A
        // persistent store killed without a clean close is left "unclean",
        // and the next open runs a full consistency scan — hours of silent
        // startup on a large store (observed live, repeatedly). Syncing
        // first means even an interrupted shutdown leaves the data durable.
        self.blobs_store.sync_db().await?;
        // Stops accepting connections and shuts down every protocol
        // handler — BlobsProtocol's handler performs the store's graceful
        // close as part of this.
        self.router.shutdown().await?;
        // If the store somehow outlived the router's protocol shutdown,
        // close it ourselves; "already shut down" is the expected case and
        // not an error.
        let _ = self.blobs_store.shutdown().await;
        Ok(())
    }
}
