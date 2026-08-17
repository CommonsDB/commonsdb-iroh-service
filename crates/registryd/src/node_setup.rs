//! Shared node bootstrap for `run` and `verify`: spawn the persistent
//! iroh node, import the (generated-once) author and namespace keys, and
//! wrap the root pointer document with write capability. Keys live under
//! `<data_dir>/secrets/` — docs/operator-guide.md, "Keys". Their stability
//! across restarts is what keeps every distributed read ticket valid.

use crate::config::Config;
use registry_node::{identity, IrohBlobStore, PointerDoc, RegistryNode};
use std::sync::Arc;

pub struct OpenedNode {
    pub node: RegistryNode,
    pub pointer_doc: Arc<PointerDoc>,
    pub blob_store: IrohBlobStore,
}

/// Open the node and pointer document. `gc` is `None` for one-shot
/// commands (verify) and the wired GC config for the daemon.
pub async fn open_node(
    cfg: &Config,
    gc: Option<iroh_blobs::store::GcConfig>,
) -> anyhow::Result<OpenedNode> {
    let secrets = cfg.secrets_dir();
    let node_key = identity::load_or_generate_secret_key(&secrets.join("node-secret-key")).await?;
    let namespace_bytes =
        identity::load_or_generate_bytes(&secrets.join("namespace-secret")).await?;
    let author_bytes = identity::load_or_generate_bytes(&secrets.join("author-secret")).await?;

    let node = RegistryNode::spawn_persistent_with_gc(
        node_key,
        cfg.blob_store_path(),
        cfg.docs_store_path(),
        gc,
    )
    .await?;

    let author = iroh_docs::Author::from_bytes(&author_bytes);
    let author_id = author.id();
    node.docs.author_import(author).await?;

    // The namespace secret is persisted and reused, so this resolves to
    // the SAME root pointer document on every start rather than minting a
    // new one — every distributed read ticket is pinned to it.
    let namespace = iroh_docs::NamespaceSecret::from_bytes(&namespace_bytes);
    let doc = node
        .docs
        .import_namespace(iroh_docs::Capability::Write(namespace))
        .await?;
    doc.start_sync(vec![]).await?;

    let blob_store = node.blob_store();
    let pointer_doc = Arc::new(PointerDoc::new(doc, blob_store.clone(), Some(author_id)));

    Ok(OpenedNode {
        node,
        pointer_doc,
        blob_store,
    })
}
