use crate::config::ResolvedConfig;
use n0_future::StreamExt;
use registry_core::config::IsccConfig;
use registry_core::similarity::BandParams;
use registry_core::{hamt, iscc_store};
use registry_node::{identity, PointerDoc, RegistryNode, ISCC_INDEX_ROOT_KEY};
use std::path::PathBuf;
use std::time::Duration;

/// In-flight blob reads per partition walk — see
/// `hamt::walk_entries_parallel`.
const FETCH_CONCURRENCY: usize = 24;

fn parse_ticket(raw: &str) -> anyhow::Result<iroh_docs::DocTicket> {
    raw.parse()
        .map_err(|e| anyhow::anyhow!("invalid read ticket: {e}"))
}

async fn wait_for_partition_root(
    pointer: &PointerDoc,
    partition_id: u32,
    timeout: Duration,
) -> anyhow::Result<registry_core::Hash> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(hash) = pointer.get_partition_root(partition_id).await? {
            return Ok(hash);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for the root pointer document to sync partition {partition_id}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub async fn get(
    cfg: ResolvedConfig,
    cid: String,
    out: Option<PathBuf>,
    ephemeral: bool,
) -> anyhow::Result<()> {
    let ticket = parse_ticket(&cfg.read_ticket)?;

    let node = if ephemeral {
        RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?
    } else {
        let secret_key =
            identity::load_or_generate_secret_key(&cfg.storage_dir.join("identity")).await?;
        RegistryNode::spawn_persistent(
            secret_key,
            cfg.storage_dir.join("blobs"),
            cfg.storage_dir.join("docs"),
        )
        .await?
    };

    eprintln!("storectl: node identity {} ready", short_id(&node));
    eprintln!("storectl: syncing root pointer document...");

    let doc = node.docs.import(ticket.clone()).await?;
    let blob_store = node.blob_store_with_providers(ticket.nodes.clone());
    let pointer = PointerDoc::new(doc, blob_store.clone(), None);

    // The whole fallible flow runs inside this block so the node is ALWAYS
    // closed before exit — an early `?` used to drop the endpoint uncalled,
    // making iroh log a scary (but harmless) abort on every error path.
    let result: anyhow::Result<()> = async {
        let partition_id = registry_core::partition_id_for_key(&cid, cfg.top_level_partitions);
        let root = wait_for_partition_root(&pointer, partition_id, Duration::from_secs(20)).await?;
        eprintln!("storectl: resolved partition {partition_id}, walking index...");

        let entry = hamt::lookup(&blob_store, Some(root), &cid).await?;
        let Some(entry) = entry else {
            anyhow::bail!(
                "key '{cid}' is not in partition {partition_id}'s current index. If the \
network is mid-republish, the record may simply not be re-inserted yet — retry later. \
Otherwise the key was never declared (or is redacted)."
            );
        };

        eprintln!("storectl: fetching content...");
        let bytes = registry_core::BlobStore::get(&blob_store, &entry.hash)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| {
                anyhow::anyhow!("content blob for '{cid}' could not be fetched from any known peer")
            })?;

        match out {
            Some(path) => {
                tokio::fs::write(&path, &bytes).await?;
                eprintln!(
                    "storectl: wrote {} bytes to {}",
                    bytes.len(),
                    path.display()
                );
            }
            None => {
                use std::io::Write;
                std::io::stdout().write_all(&bytes)?;
                println!();
            }
        }
        Ok(())
    }
    .await;
    node.shutdown().await.ok();
    result
}

/// Parse the `similar` query argument: an ISCC string, or a raw 64-bit hex
/// Content-Code (`0x`-prefixed or 16 hex digits).
fn parse_similarity_query(raw: &str) -> anyhow::Result<u64> {
    let trimmed = raw.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        return u64::from_str_radix(hex, 16)
            .map_err(|e| anyhow::anyhow!("invalid hex content code '{trimmed}': {e}"));
    }
    if trimmed.len() == 16 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return u64::from_str_radix(trimmed, 16)
            .map_err(|e| anyhow::anyhow!("invalid hex content code '{trimmed}': {e}"));
    }
    registry_core::iscc::decode_content_code(trimmed).map_err(|e| {
        anyhow::anyhow!("could not decode '{trimmed}' as an ISCC or hex content code: {e}")
    })
}

/// Client-side P2P similarity search — docs/similarity-search.md.
/// Walks the banded ISCC index, verifies exact Hamming distance locally, and
/// only fetches full record values when `--fetch` is passed.
pub async fn similar(
    cfg: ResolvedConfig,
    query: String,
    radius: Option<u32>,
    probe: Option<u32>,
    fetch: bool,
) -> anyhow::Result<()> {
    let iscc_cfg = IsccConfig::from_env()?;
    let radius = radius.unwrap_or(iscc_cfg.search_max_radius);
    if radius > iscc_cfg.search_max_radius {
        anyhow::bail!(
            "requested radius {radius} exceeds ISCC_SEARCH_MAX_RADIUS ({})",
            iscc_cfg.search_max_radius
        );
    }
    let probe = probe.unwrap_or(iscc_cfg.band_probe_radius);
    let bands = BandParams::new(iscc_cfg.index_bands)?;
    let query_code = parse_similarity_query(&query)?;

    let ticket = parse_ticket(&cfg.read_ticket)?;
    let node = RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?;
    eprintln!("storectl: node identity {} ready", short_id(&node));
    eprintln!("storectl: syncing root pointer document...");

    let doc = node.docs.import(ticket.clone()).await?;
    let blob_store = node.blob_store_with_providers(ticket.nodes.clone());
    let pointer = PointerDoc::new(doc, blob_store.clone(), None);

    // Wait for the ISCC index root to sync, same pattern as partition roots.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let iscc_root = loop {
        if let Some(root) = pointer.get_named_root(ISCC_INDEX_ROOT_KEY).await? {
            break root;
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for the ISCC similarity index root to sync — \
                 the deployment may not have similarity indexing enabled yet"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    eprintln!(
        "storectl: querying ISCC index (code {query_code:#018x}, radius {radius}, probe {probe}, {} bands)...",
        bands.num_bands()
    );
    let matches = iscc_store::query(
        &blob_store,
        Some(iscc_root),
        bands,
        query_code,
        radius,
        probe,
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    if matches.is_empty() {
        eprintln!("storectl: no records within Hamming distance {radius}");
        node.shutdown().await.ok();
        return Ok(());
    }
    eprintln!("storectl: {} match(es):", matches.len());
    for m in &matches {
        println!("{}\t{}\t{:#018x}", m.distance, m.key, m.content_code);
    }

    if fetch {
        for m in &matches {
            let partition_id =
                registry_core::partition_id_for_key(&m.key, cfg.top_level_partitions);
            let root =
                wait_for_partition_root(&pointer, partition_id, Duration::from_secs(20)).await?;
            let Some(entry) = hamt::lookup(&blob_store, Some(root), &m.key).await? else {
                eprintln!(
                    "storectl: {} matched but is not (or no longer) in the primary index",
                    m.key
                );
                continue;
            };
            match registry_core::BlobStore::get(&blob_store, &entry.hash)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
            {
                Some(bytes) => {
                    use std::io::Write;
                    print!("{}\t", m.key);
                    std::io::stdout().write_all(&bytes)?;
                    println!();
                }
                None => eprintln!(
                    "storectl: content for {} unavailable from known peers",
                    m.key
                ),
            }
        }
    }
    node.shutdown().await.ok();
    Ok(())
}

/// Enumerate published records — key (cid) + ISCC Content-Code — straight
/// from the P2P index, for debugging and spot-checks. Streams partition by
/// partition; after `free_limit` rows it pages interactively (`page_size`
/// rows per Enter) unless `no_page` is set. With `values`, each row also
/// fetches the record's JSON and prints its full `iscc` field (slow — one
/// blob fetch per row; use --max or a single --partition with it). With
/// `summary`, prints per-partition counts and totals only, no rows.
///
/// A partition whose index nodes cannot be fetched (e.g. the pointer doc
/// still carries a root from before a worker store reset, superseded once
/// that partition republishes) is reported and skipped, never fatal: a
/// debugging tool must show the healthy 95% rather than die on the sick 5%.
#[allow(clippy::too_many_arguments)] // one call site, main's arg dispatch
pub async fn list(
    cfg: ResolvedConfig,
    partition: Option<u32>,
    page_size: usize,
    free_limit: usize,
    max: Option<usize>,
    no_page: bool,
    values: bool,
    summary: bool,
    ephemeral: bool,
    walk_timeout_secs: u64,
    settle_ms: u64,
) -> anyhow::Result<()> {
    let ticket = parse_ticket(&cfg.read_ticket)?;
    let node = if ephemeral {
        RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?
    } else {
        let secret_key =
            identity::load_or_generate_secret_key(&cfg.storage_dir.join("identity")).await?;
        RegistryNode::spawn_persistent(
            secret_key,
            cfg.storage_dir.join("blobs"),
            cfg.storage_dir.join("docs"),
        )
        .await?
    };
    eprintln!("storectl: node identity {} ready", short_id(&node));
    eprintln!("storectl: syncing root pointer document...");
    let doc = node.docs.import(ticket.clone()).await?;
    let blob_store = node.blob_store_with_providers(ticket.nodes.clone());
    let pointer = PointerDoc::new(doc, blob_store.clone(), None);

    let partitions: Vec<u32> = match partition {
        Some(id) => vec![id],
        None => (0..cfg.top_level_partitions).collect(),
    };

    // The doc import returns before entries have synced; without a settle
    // wait every root reads as absent (the same reason `get` uses
    // wait_for_partition_root). Anchor on the first requested partition —
    // its arrival implies the doc snapshot is flowing.
    // The publisher can be slow to serve the doc while a bulk republish is
    // running; be patient and say so rather than silently listing nothing.
    if let Some(first) = partitions.first() {
        let mut waited = 0u64;
        loop {
            match wait_for_partition_root(&pointer, *first, Duration::from_secs(15)).await {
                Ok(_) => break,
                Err(err) => {
                    waited += 15;
                    if waited >= 60 {
                        eprintln!("storectl: {err}; listing whatever has synced so far");
                        break;
                    }
                    eprintln!("storectl: pointer document still syncing ({waited}s)...");
                }
            }
        }
    }
    // A persistent store answers root reads instantly from its cached copy
    // of the doc — potentially entries superseded since the last run. Give
    // the sync a moment to merge newer entries before trusting the roots.
    if !ephemeral && settle_ms > 0 {
        eprintln!("storectl: letting the pointer document settle ({settle_ms} ms)...");
        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    }

    if summary && partition.is_none() {
        // Full-corpus summary: probe partitions in parallel. Sequential
        // probing pays the full walk (or timeout) per partition in turn —
        // hours against 256 partitions over WAN; eight in flight keeps the
        // sweep to minutes and each partition still gets its own timeout.
        const SUMMARY_CONCURRENCY: usize = 8;
        let pointer = std::sync::Arc::new(pointer);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(SUMMARY_CONCURRENCY));
        let mut tasks = tokio::task::JoinSet::new();
        for id in partitions {
            let pointer = pointer.clone();
            let blob_store = blob_store.clone();
            let semaphore = semaphore.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire().await;
                let root = match pointer.get_partition_root(id).await {
                    Ok(Some(root)) => root,
                    Ok(None) => return (id, Ok(None)),
                    Err(err) => return (id, Err(format!("{err}"))),
                };
                let outcome = hamt::walk_entries_parallel(
                    &blob_store,
                    root,
                    Duration::from_secs(walk_timeout_secs),
                    FETCH_CONCURRENCY,
                )
                .await;
                if outcome.entries.is_empty() && !outcome.complete {
                    (id, Err(format!("timed out after {walk_timeout_secs}s")))
                } else {
                    let with_code = outcome
                        .entries
                        .iter()
                        .filter(|e| e.content_code.is_some())
                        .count();
                    // A partial count still informs the summary; the caller
                    // reports incompleteness via the partial flag.
                    (
                        id,
                        Ok(Some((outcome.entries.len(), with_code, outcome.complete))),
                    )
                }
            });
        }
        type ProbeResult = (u32, Result<Option<(usize, usize, bool)>, String>);
        let mut results: Vec<ProbeResult> = Vec::new();
        let mut done = 0usize;
        while let Some(joined) = tasks.join_next().await {
            if let Ok(result) = joined {
                done += 1;
                if done.is_multiple_of(32) {
                    eprintln!(
                        "storectl: ...{done}/{} partitions probed",
                        cfg.top_level_partitions
                    );
                }
                results.push(result);
            }
        }
        results.sort_by_key(|(id, _)| *id);

        let mut total = 0usize;
        let mut total_code = 0usize;
        let mut readable = 0usize;
        let mut empty = 0usize;
        let mut unreadable: Vec<u32> = Vec::new();
        let mut partial: Vec<u32> = Vec::new();
        println!("partition\trecords");
        for (id, result) in &results {
            match result {
                Ok(Some((records, code, complete))) => {
                    if *complete {
                        println!("{id}\t{records}");
                    } else {
                        println!("{id}\t{records}+ (partial)");
                        partial.push(*id);
                    }
                    total += records;
                    total_code += code;
                    readable += 1;
                }
                Ok(None) => empty += 1,
                Err(_) => unreadable.push(*id),
            }
        }
        println!("storectl: ---- summary ----");
        println!(
            "storectl: records visible:           {total} (sum over the {readable}/{} partitions read — \
NOT the corpus size when partitions are unreadable; compare the corpus estimate between runs)",
            results.len()
        );
        println!(
            "storectl: with ISCC content code:    {total_code} ({:.1}%)",
            if total > 0 {
                total_code as f64 * 100.0 / total as f64
            } else {
                0.0
            }
        );
        println!(
            "storectl: partitions with records:   {readable} (of {})",
            results.len()
        );
        println!("storectl: partitions empty/unsynced: {empty}");
        if !partial.is_empty() {
            println!(
                "storectl: partitions partial:        {} {:?}{} — counts above are lower bounds; \
raise --walk-timeout or re-run after the republish settles",
                partial.len(),
                &partial[..partial.len().min(16)],
                if partial.len() > 16 { " …" } else { "" }
            );
        }
        if !unreadable.is_empty() {
            println!(
                "storectl: partitions unreadable:     {} {:?}{}",
                unreadable.len(),
                &unreadable[..unreadable.len().min(16)],
                if unreadable.len() > 16 { " …" } else { "" }
            );
        }
        if let Some(avg) = total.checked_div(readable) {
            println!(
                "storectl: records/partition (avg):   {avg} — corpus estimate ≈ {}",
                avg * cfg.top_level_partitions as usize
            );
        }
        node.shutdown().await.ok();
        return Ok(());
    }

    let mut printed = 0usize;
    let mut since_prompt = 0usize;
    let mut page_number = 1usize;
    let mut with_code = 0usize;
    let mut listed_partitions = 0usize;
    let mut empty_partitions = 0usize;
    let mut unreadable: Vec<u32> = Vec::new();
    let mut partial_partitions: Vec<u32> = Vec::new();
    let mut per_partition: Vec<(u32, usize)> = Vec::new();
    let stdin = std::io::stdin();

    if summary {
        eprintln!("storectl: counting records per partition...");
    } else if values {
        println!("partition\tkey\tcontent_code\tiscc");
    } else {
        println!("partition\tkey\tcontent_code");
    }

    'outer: for id in partitions {
        let Some(root) = pointer.get_partition_root(id).await? else {
            empty_partitions += 1;
            if partition.is_some() {
                eprintln!("storectl: partition {id} has no published records yet");
            }
            continue;
        };
        let outcome = hamt::walk_entries_parallel(
            &blob_store,
            root,
            Duration::from_secs(walk_timeout_secs),
            FETCH_CONCURRENCY,
        )
        .await;
        if outcome.entries.is_empty() && !outcome.complete {
            unreadable.push(id);
            eprintln!(
                "storectl: partition {id} yielded nothing within {walk_timeout_secs}s (no peer \
                 served its index blobs) — skipping; raise --walk-timeout to wait longer"
            );
            continue;
        }
        if !outcome.complete {
            partial_partitions.push(id);
            eprintln!(
                "storectl: partition {id} PARTIAL — {} records fetched before the {walk_timeout_secs}s \
                 deadline; re-run with a higher --walk-timeout (or after the republish settles) for the rest",
                outcome.entries.len()
            );
        }
        let entries = outcome.entries;
        if summary && id.is_multiple_of(16) {
            eprintln!(
                "storectl: ...partition {id}: {printed} records so far, {} partitions read",
                listed_partitions + 1
            );
        }
        listed_partitions += 1;
        per_partition.push((id, entries.len()));

        for entry in entries {
            if entry.content_code.is_some() {
                with_code += 1;
            }
            printed += 1;
            if !summary {
                let code = entry
                    .content_code
                    .map(|c| format!("{c:#018x}"))
                    .unwrap_or_else(|| "-".to_string());
                if values {
                    let full_iscc = match tokio::time::timeout(
                        Duration::from_secs(walk_timeout_secs),
                        registry_core::BlobStore::get(&blob_store, &entry.hash),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("value fetch timed out"))?
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    {
                        Some(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes)
                            .ok()
                            .and_then(|v| {
                                v.get("iscc")
                                    .or_else(|| v.get("ISCC"))
                                    .and_then(|s| s.as_str().map(String::from))
                            })
                            .unwrap_or_else(|| "-".to_string()),
                        None => "<value unavailable>".to_string(),
                    };
                    println!("{id}\t{}\t{code}\t{full_iscc}", entry.key);
                } else {
                    println!("{id}\t{}\t{code}", entry.key);
                }
            }
            if let Some(m) = max {
                if printed >= m {
                    eprintln!("storectl: stopped at --max {m}");
                    break 'outer;
                }
            }
            if !summary && !no_page && printed >= free_limit {
                since_prompt += 1;
                if since_prompt >= page_size {
                    since_prompt = 0;
                    page_number += 1;
                    eprint!(
                        "storectl: -- page {page_number} · {printed} shown · partition {id} -- \
                         Enter for next {page_size}, q to quit -- "
                    );
                    let mut line = String::new();
                    stdin.read_line(&mut line)?;
                    if line.trim().eq_ignore_ascii_case("q") {
                        break 'outer;
                    }
                }
            }
        }
    }

    if summary {
        println!("partition\trecords");
        for (id, count) in &per_partition {
            println!("{id}\t{count}");
        }
    }

    let total_pages = printed.div_ceil(page_size.max(1));
    println!("storectl: ---- summary ----");
    println!("storectl: records listed:            {printed}");
    println!(
        "storectl: with ISCC content code:    {with_code} ({:.1}%)",
        if printed > 0 {
            with_code as f64 * 100.0 / printed as f64
        } else {
            0.0
        }
    );
    println!(
        "storectl: partitions with records:   {listed_partitions} (of {} requested)",
        listed_partitions + empty_partitions + unreadable.len()
    );
    println!("storectl: partitions empty/unsynced: {empty_partitions}");
    if !partial_partitions.is_empty() {
        println!(
            "storectl: partitions partial:        {} {:?} — listed rows are a lower bound; \
raise --walk-timeout or re-run after the republish settles",
            partial_partitions.len(),
            partial_partitions
        );
    }
    if !unreadable.is_empty() {
        println!(
            "storectl: partitions unreadable:     {} {:?} (republish in progress — re-run later)",
            unreadable.len(),
            unreadable
        );
    }
    let walked_total: usize = per_partition.iter().map(|(_, n)| n).sum();
    if walked_total > printed {
        println!(
            "storectl: records in walked parts:  {walked_total} (listing stopped early at {printed})"
        );
    }
    if let Some((max_p, max_n)) = per_partition.iter().max_by_key(|(_, n)| *n) {
        let min = per_partition.iter().min_by_key(|(_, n)| *n);
        let avg = walked_total as f64 / per_partition.len().max(1) as f64;
        println!(
            "storectl: records/partition:         avg {avg:.0}, max {max_n} (partition {max_p}), min {} (partition {})",
            min.map(|(_, n)| *n).unwrap_or(0),
            min.map(|(p, _)| *p).unwrap_or(0)
        );
    }
    println!("storectl: pages at --page-size {page_size}: {total_pages}");
    node.shutdown().await.ok();
    Ok(())
}

pub async fn watch(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let ticket = parse_ticket(&cfg.read_ticket)?;
    let node = RegistryNode::spawn_ephemeral(identity::generate_secret_key()).await?;

    eprintln!("storectl: node identity {} ready", short_id(&node));
    eprintln!("storectl: watching root pointer document for updates (ctrl-c to stop)...");

    let (_doc, events) = node.docs.import_and_subscribe(ticket).await?;
    tokio::pin!(events);
    while let Some(event) = events.next().await {
        match event {
            Ok(e) => println!("{e:?}"),
            Err(err) => println!("storectl: watch stream error: {err}"),
        }
    }
    node.shutdown().await.ok();
    Ok(())
}

/// Print this node's public identity (EndpointId) without opening the blob
/// or doc stores — safe to run while a `seed` service holds their locks.
/// Seed hosts use it to register themselves as ticket providers
/// (docs/operator-guide.md).
pub async fn identity(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let secret_key =
        identity::load_or_generate_secret_key(&cfg.storage_dir.join("identity")).await?;
    println!("{}", secret_key.public());
    Ok(())
}

/// Warm one partition: fetch its index nodes (sequential — they are few
/// and shape the descent), then its value blobs with bounded concurrency.
/// Already-cached blobs are cheap local hits, so an interrupted warm
/// resumes where it left off on the next pass.
async fn warm_partition(
    blob_store: &registry_node::IrohBlobStore,
    root: registry_core::Hash,
) -> anyhow::Result<usize> {
    let mut entries = Vec::new();
    hamt::walk_entries(blob_store, root, &mut entries)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
    let mut fetches = tokio::task::JoinSet::new();
    let total = entries.len();
    for entry in entries {
        let blob_store = blob_store.clone();
        let semaphore = semaphore.clone();
        fetches.spawn(async move {
            let _permit = semaphore.acquire().await;
            if let Err(err) = registry_core::BlobStore::get(&blob_store, &entry.hash).await {
                tracing::debug!(key = %entry.key, error = %err, "value warm failed (retried next pass)");
            }
        });
    }
    while fetches.join_next().await.is_some() {}
    Ok(total)
}

pub async fn seed(cfg: ResolvedConfig, shards: Vec<String>) -> anyhow::Result<()> {
    let ticket = parse_ticket(&cfg.read_ticket)?;
    let secret_key =
        identity::load_or_generate_secret_key(&cfg.storage_dir.join("identity")).await?;
    let node = RegistryNode::spawn_persistent(
        secret_key,
        cfg.storage_dir.join("blobs"),
        cfg.storage_dir.join("docs"),
    )
    .await?;

    eprintln!("storectl: node identity {} ready", short_id(&node));

    let doc = node.docs.import(ticket.clone()).await?;
    let blob_store = node.blob_store_with_providers(ticket.nodes.clone());
    let pointer = PointerDoc::new(doc, blob_store.clone(), None);

    let selected: Option<Vec<u32>> =
        if shards.iter().any(|s| s.eq_ignore_ascii_case("all")) || shards.is_empty() {
            None
        } else {
            Some(
                shards
                    .iter()
                    .map(|s| {
                        s.parse::<u32>()
                            .map_err(|_| anyhow::anyhow!("invalid shard id '{s}'"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )
        };

    match &selected {
        None => eprintln!(
            "storectl: seeding all {} shards (ctrl-c to stop)",
            cfg.top_level_partitions
        ),
        Some(ids) => eprintln!("storectl: seeding shards {ids:?} (ctrl-c to stop)"),
    }

    // Incremental seeding: one full pass to warm everything, then a slow
    // safety-net rescan. Re-walking every partition every 30 s does not
    // survive contact with a large corpus — each pass would re-touch
    // millions of blobs; at the design scale it would never finish. The
    // pointer document is tiny and re-read cheaply, and `warm_cache` on an
    // already-warm root only re-reads index nodes until it sees no change,
    // so per-iteration cost tracks what actually changed.
    let mut last_roots: std::collections::HashMap<u32, registry_core::Hash> =
        std::collections::HashMap::new();
    let mut pass = 0u64;
    loop {
        let ids: Vec<u32> = selected
            .clone()
            .unwrap_or_else(|| (0..cfg.top_level_partitions).collect());
        let full_pass = pass.is_multiple_of(120); // safety net ~hourly at the 30 s cadence
        for id in ids {
            match pointer.get_partition_root(id).await {
                Ok(Some(root)) => {
                    if !full_pass && last_roots.get(&id) == Some(&root) {
                        continue; // unchanged since last warm
                    }
                    // Bounded per partition (a single hung download must
                    // not stall the loop forever — skip, retry next pass)
                    // and value fetches overlapped: warming sequentially
                    // costs one WAN round-trip per record and could not
                    // even finish a partition inside the timeout window.
                    match tokio::time::timeout(
                        Duration::from_secs(600),
                        warm_partition(&blob_store, root),
                    )
                    .await
                    {
                        Ok(Ok(count)) => {
                            last_roots.insert(id, root);
                            tracing::info!(partition = id, entries = count, "seeded partition")
                        }
                        Ok(Err(err)) => {
                            tracing::warn!(partition = id, error = %err, "seed pass failed for partition")
                        }
                        Err(_) => {
                            tracing::warn!(
                                partition = id,
                                "seed pass timed out after 600s; cached blobs are kept, will resume next pass"
                            )
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(partition = id, error = %err, "could not resolve partition root while seeding")
                }
            }
        }
        pass += 1;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

pub async fn status(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let ticket = parse_ticket(&cfg.read_ticket)?;
    let secret_key =
        identity::load_or_generate_secret_key(&cfg.storage_dir.join("identity")).await?;
    let node = RegistryNode::spawn_persistent(
        secret_key,
        cfg.storage_dir.join("blobs"),
        cfg.storage_dir.join("docs"),
    )
    .await?;
    let _doc = node.docs.import(ticket).await?;

    // Give direct/relay connectivity a brief moment to establish before
    // reporting it, matching docs/operator-guide.md's
    // `storectl status` behavior.
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("endpoint_id: {}", node.endpoint.id());
    println!("storage_dir: {}", cfg.storage_dir.display());
    let cache_bytes = dir_size(&cfg.storage_dir.join("blobs")).unwrap_or(0);
    println!("cache_size_bytes: {cache_bytes}");
    node.shutdown().await.ok();
    Ok(())
}

pub fn print_config(cfg: &ResolvedConfig) {
    println!("read_ticket_source: {}", cfg.read_ticket_source);
    println!(
        "read_ticket: {}...{}",
        &cfg.read_ticket[..cfg.read_ticket.len().min(24)],
        if cfg.read_ticket.len() > 24 {
            " (truncated)"
        } else {
            ""
        }
    );
    println!(
        "storage_dir: {} (source: {})",
        cfg.storage_dir.display(),
        cfg.storage_dir_source
    );
    println!("top_level_partitions: {}", cfg.top_level_partitions);
}

/// Print the resolved ticket with extra provider nodes appended (and
/// optionally the existing ones removed) — how a seed node's identity
/// gets into the ticket handed to third-party readers.
pub fn compose_ticket(cfg: ResolvedConfig, add: Vec<String>, strip: bool) -> anyhow::Result<()> {
    let mut ticket = parse_ticket(&cfg.read_ticket)?;
    if strip {
        ticket.nodes.clear();
    }
    let existing: std::collections::HashSet<_> = ticket.nodes.iter().map(|a| a.id).collect();
    for raw in add {
        let id: iroh::EndpointId = raw
            .trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("'{raw}' is not a valid EndpointId: {e}"))?;
        if !existing.contains(&id) {
            ticket.nodes.push(iroh::EndpointAddr::from(id));
        }
    }
    if ticket.nodes.is_empty() {
        anyhow::bail!("refusing to print a ticket with no provider nodes");
    }
    eprintln!(
        "storectl: ticket now lists {} provider(s)",
        ticket.nodes.len()
    );
    println!("{ticket}");
    Ok(())
}

fn short_id(node: &RegistryNode) -> String {
    let full = node.endpoint.id().to_string();
    format!("{}...", &full[..full.len().min(12)])
}

fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if !path.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}
