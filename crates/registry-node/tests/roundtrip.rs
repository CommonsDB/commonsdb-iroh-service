//! End-to-end validation of the write -> root pointer document -> gossip
//! sync -> read path over real local `iroh` networking between two
//! independent node identities — the same path `storectl get`/`watch`
//! exercise against a live node.
//!
//! Ignored by default (`#[ignore]`) since it opens real network sockets and
//! needs outbound connectivity to iroh's relay infrastructure for endpoint
//! discovery; run explicitly with `cargo test -p registry-node --test
//! roundtrip -- --ignored`.

use registry_core::{BlobStore, Hash as CommonHash};
use registry_node::identity::generate_secret_key;
use registry_node::pointer_doc::PointerDoc;
use registry_node::RegistryNode;
use std::time::Duration;

#[tokio::test]
#[ignore]
async fn write_then_read_partition_root_over_network() -> anyhow::Result<()> {
    let writer_node = RegistryNode::spawn_ephemeral(generate_secret_key()).await?;
    let author_id = writer_node.docs.author_create().await?;
    let doc = writer_node.docs.create().await?;
    let writer_pointer = PointerDoc::new(doc, writer_node.blob_store(), Some(author_id));

    let value = b"hello from the writer node".to_vec();
    let content_hash = writer_node
        .blob_store()
        .put(bytes::Bytes::from(value.clone()))
        .await?;
    writer_pointer.set_partition_root(7, content_hash).await?;

    let ticket = writer_pointer.share_read_ticket().await?;
    assert!(
        !ticket.nodes.is_empty(),
        "share() must embed bootstrap peer addresses"
    );

    let reader_node = RegistryNode::spawn_ephemeral(generate_secret_key()).await?;
    let reader_doc = reader_node.docs.import(ticket.clone()).await?;
    let reader_blob_store = reader_node.blob_store_with_providers(ticket.nodes.clone());
    let reader_pointer = PointerDoc::new(reader_doc, reader_blob_store.clone(), None);

    let resolved_hash =
        wait_for_partition_root(&reader_pointer, 7, Duration::from_secs(20)).await?;
    assert_eq!(resolved_hash, content_hash);

    let fetched = reader_blob_store
        .get(&content_hash)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .expect("content blob should be fetchable from the writer node over the network");
    assert_eq!(fetched.as_ref(), value.as_slice());

    Ok(())
}

async fn wait_for_partition_root(
    pointer: &PointerDoc,
    partition_id: u32,
    timeout: Duration,
) -> anyhow::Result<CommonHash> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(hash) = pointer.get_partition_root(partition_id).await? {
            return Ok(hash);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for partition {partition_id} to sync");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
