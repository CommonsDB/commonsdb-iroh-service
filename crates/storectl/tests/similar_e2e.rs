//! End-to-end validation of the ISCC similarity search path over real local
//! iroh networking — docs/similarity-search.md: a writer
//! node builds the banded index and publishes its root in the pointer
//! document; an independent reader node syncs the ticket, walks the index
//! buckets P2P, verifies Hamming distance client-side, and only then decides
//! what to fetch. Ignored by default (opens real sockets); run with
//! `cargo test -p storectl --test similar_e2e -- --ignored`.

use registry_core::iscc_store;
use registry_core::similarity::{BandParams, IsccMember};
use registry_node::{identity, PointerDoc, RegistryNode, ISCC_INDEX_ROOT_KEY};
use std::time::Duration;

#[tokio::test]
#[ignore]
async fn reader_finds_similar_records_over_network() -> anyhow::Result<()> {
    let bands = BandParams::new(8)?;

    // --- writer side: publish records + ISCC index ---
    let writer = RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?;
    let author_id = writer.docs.author_create().await?;
    let doc = writer.docs.create().await?;
    let writer_pointer = PointerDoc::new(doc, writer.blob_store(), Some(author_id));

    let base_code = 0xdead_beef_cafe_babeu64;
    let near_code = base_code ^ 0b101; // 2 bits flipped, same lowest band
    let far_code = !base_code; // distance 64

    let members = vec![
        IsccMember {
            key: "rec-base".into(),
            content_code: base_code,
        },
        IsccMember {
            key: "rec-near".into(),
            content_code: near_code,
        },
        IsccMember {
            key: "rec-far".into(),
            content_code: far_code,
        },
    ];
    let index_root = iscc_store::insert_batch(&writer.blob_store(), None, bands, &members).await?;
    writer_pointer
        .set_named_root(ISCC_INDEX_ROOT_KEY.to_string(), index_root)
        .await?;

    let ticket = writer_pointer.share_read_ticket().await?;

    // --- reader side: independent node, query over the network ---
    let reader = RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?;
    let reader_doc = reader.docs.import(ticket.clone()).await?;
    let reader_blobs = reader.blob_store_with_providers(ticket.nodes.clone());
    let reader_pointer = PointerDoc::new(reader_doc, reader_blobs.clone(), None);

    // wait for the index root to sync
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let synced_root = loop {
        if let Some(root) = reader_pointer.get_named_root(ISCC_INDEX_ROOT_KEY).await? {
            break root;
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for the ISCC index root to sync");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(synced_root, index_root);

    // Query near base_code with radius 4: base (0) and near (2) must be
    // found; far (64) must not.
    let matches =
        iscc_store::query(&reader_blobs, Some(synced_root), bands, base_code, 4, 0).await?;
    let keys: Vec<&str> = matches.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(
        keys,
        ["rec-base", "rec-near"],
        "unexpected match set: {matches:?}"
    );
    assert_eq!(matches[0].distance, 0);
    assert_eq!(matches[1].distance, 2);

    // The whole decision was made without fetching any record value — prove
    // the reader can now selectively fetch only what it wants: nothing here,
    // since no values were even published; the index alone answered.
    let _ = reader_blobs; // (values intentionally never fetched)

    Ok(())
}
