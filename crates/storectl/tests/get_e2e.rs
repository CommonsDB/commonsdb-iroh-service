//! Exercises the actual `storectl get` command function (not just the
//! underlying registry-node library) against a real, locally-spun-up
//! writer node — the same validation `registry-node`'s own roundtrip test
//! performs, but through the CLI's own code path this time. Ignored by
//! default; needs real network sockets. Run with:
//! `cargo test -p storectl --test get_e2e -- --ignored`.

use bytes::Bytes;
use registry_core::BlobStore;
use storectl::config::ResolvedConfig;

#[tokio::test]
#[ignore]
async fn storectl_get_resolves_a_value_written_by_a_separate_node() -> anyhow::Result<()> {
    let writer = registry_node::RegistryNode::spawn_ephemeral(
        registry_node::identity::generate_secret_key(),
    )
    .await?;
    let author_id = writer.docs.author_create().await?;
    let doc = writer.docs.create().await?;
    let pointer = registry_node::PointerDoc::new(doc, writer.blob_store(), Some(author_id));

    let key = "bafyreicelebrationcid0001";
    let value = br#"{"hello":"world"}"#.to_vec();
    let content_hash = writer
        .blob_store()
        .put(Bytes::from(value.clone()))
        .await
        .unwrap();

    let partition_id =
        registry_core::partition_id_for_key(key, registry_core::TOP_LEVEL_PARTITIONS_DEFAULT);
    let entry = registry_core::LeafEntry {
        key: key.to_string(),
        hash: content_hash,
        size: value.len() as u64,
        added_at: chrono::Utc::now(),
        content_code: None,
    };
    let root = registry_core::hamt::insert_one(
        &writer.blob_store(),
        None,
        entry,
        registry_core::LEAF_MAX_ENTRIES_DEFAULT,
    )
    .await
    .unwrap();
    pointer.set_partition_root(partition_id, root).await?;

    let ticket = pointer.share_read_ticket().await?;

    let tmp_dir = tempfile_dir();
    let out_file = tmp_dir.join("out.json");
    let cfg = ResolvedConfig {
        read_ticket: ticket.to_string(),
        read_ticket_source: "test",
        storage_dir: tmp_dir.join("storectl-data"),
        storage_dir_source: "test",
        top_level_partitions: registry_core::TOP_LEVEL_PARTITIONS_DEFAULT,
    };

    storectl::commands::get(cfg, key.to_string(), Some(out_file.clone()), true).await?;

    let written = std::fs::read(&out_file)?;
    assert_eq!(written, value);
    Ok(())
}

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("storectl-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
