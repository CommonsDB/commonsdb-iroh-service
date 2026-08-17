//! registryd — a single-node content registry daemon: an authenticated
//! HTTP write API in front of an embedded queue, a publisher that builds
//! the partitioned HAMT structure, and an iroh node that serves the whole
//! dataset to any reader holding the read ticket. See the repository
//! README for the 5-minute quickstart.

mod api;
mod bulk_load;
mod config;
mod gc;
mod index;
mod node_setup;
mod publisher;
mod verify;

use clap::{Parser, Subcommand};
use config::Config;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "registryd", version, about)]
struct Cli {
    /// Path to config.toml (default: $REGISTRYD_CONFIG, then
    /// /etc/registryd/config.toml; missing default file = built-in
    /// defaults + environment variables).
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon (the default when no subcommand is given).
    Run,
    /// Audit published trees against the record index (stop the daemon
    /// first; see docs/operator-guide.md, "Verify"). Exits non-zero when
    /// gaps are found.
    Verify {
        /// Only this partition (default: all).
        #[arg(long)]
        partition: Option<u32>,
        /// Re-queue missing records for the next publish cycles.
        #[arg(long)]
        fix: bool,
    },
    /// Print the resolved configuration and data paths, then exit.
    Config,
    /// One resumable bulk-ingest chunk: stream dump lines [skip,
    /// skip+limit) — value blobs written once, index rows, spill files
    /// appended. Repeat with growing --skip until it reports
    /// lines_read=0, then run bulk-build. Stop the daemon first.
    BulkIngest {
        /// dump.ndjson or dump.ndjson.gz ({"key":...,"value":...} lines).
        dump: PathBuf,
        /// Scratch directory for per-partition spill files.
        #[arg(long, default_value = "/tmp/registryd-bulk-spill")]
        spill_dir: PathBuf,
        /// Lines to skip (resume position).
        #[arg(long, default_value_t = 0)]
        skip: u64,
        /// Maximum lines this run.
        #[arg(long, default_value_t = 100_000)]
        limit: u64,
    },
    /// Final bulk-load phase: build every partition's tree bottom-up from
    /// the accumulated spills (each node written exactly once) and
    /// publish the roots. Run `verify --fix` afterwards.
    BulkBuild {
        #[arg(long, default_value = "/tmp/registryd-bulk-spill")]
        spill_dir: PathBuf,
    },
    /// Re-stamp every record's created_at with the declaration timestamp
    /// embedded in its value (read from the bulk dump), then rebuild and
    /// republish all partition trees so published added_at reflects
    /// declaration time rather than import time. Stop the daemon first.
    Retimestamp {
        /// dump.ndjson or dump.ndjson.gz ({"key":...,"value":...} lines).
        dump: PathBuf,
        /// Scratch directory for the regenerated per-partition spills.
        #[arg(long, default_value = "/tmp/registryd-retimestamp-spill")]
        spill_dir: PathBuf,
    },
    /// Disaster recovery: overwrite one partition's root pointer with a
    /// known-good root hash (see docs/operator-guide.md, "Verify"). Stop
    /// the daemon first. Refuses hashes not present in the local store.
    SetRoot {
        #[arg(long)]
        partition: u32,
        #[arg(long)]
        hash: String,
    },
}

fn init_tracing() {
    // The iroh stack warns constantly about routine P2P churn — none of it
    // is actionable for an operator reading the daemon's log, so network
    // internals default to error-only. RUST_LOG/LOG_LEVEL override.
    let filter = registry_core::config::log_level_filter(
        "info,iroh=error,noq_proto=error,iroh_docs=error,iroh_blobs=error,\
         iroh_gossip=error,iroh_relay=error,iroh_util=error,netwatch=error,portmapper=error",
    );
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    registry_core::config::load_dotenv();
    init_tracing();

    let cli = Cli::parse();
    let cfg = Config::load(cli.config)?;

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => run(cfg).await,
        Command::Verify { partition, fix } => {
            let report = verify::run(cfg, partition, fix).await?;
            println!(
                "verify: {} partitions checked, {} expected records, {} in trees, \
                 {} missing, {} foreign, {} requeued",
                report.partitions_checked,
                report.expected_total,
                report.in_tree_total,
                report.missing_total,
                report.foreign_total,
                report.requeued_total,
            );
            if report.missing_total > 0 && report.requeued_total < report.missing_total {
                std::process::exit(1);
            }
            Ok(())
        }
        Command::BulkIngest {
            dump,
            spill_dir,
            skip,
            limit,
        } => bulk_load::ingest(cfg, dump, spill_dir, skip, limit)
            .await
            .map(|_| ()),
        Command::BulkBuild { spill_dir } => bulk_load::build_trees(cfg, spill_dir).await,
        Command::Retimestamp { dump, spill_dir } => {
            bulk_load::retimestamp(cfg, dump, spill_dir).await
        }
        Command::SetRoot { partition, hash } => verify::set_root(cfg, partition, &hash).await,
        Command::Config => {
            println!("bind_addr            = {}", cfg.bind_addr);
            println!("data_dir             = {}", cfg.data_dir.display());
            println!("api_tokens           = {} configured", cfg.api_tokens.len());
            println!("max_value_bytes      = {}", cfg.max_value_bytes);
            println!("batch_max_records    = {}", cfg.batch_max_records);
            println!("publish_max_pending  = {}", cfg.publish_max_pending);
            println!("publish_interval     = {}s", cfg.publish_interval_secs);
            println!("top_level_partitions = {}", cfg.top_level_partitions);
            println!("leaf_max_entries     = {}", cfg.leaf_max_entries);
            println!("gc_interval          = {}s", cfg.gc_interval_secs);
            println!(
                "denylist_path        = {}",
                cfg.denylist_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "(none)".to_string())
            );
            Ok(())
        }
    }
}

async fn run(cfg: Config) -> anyhow::Result<()> {
    if cfg.api_tokens.is_empty() {
        anyhow::bail!(
            "no API tokens configured — set api_tokens in the config file (or \
             REGISTRYD_API_TOKENS); there is no unauthenticated write mode"
        );
    }

    let index = Arc::new(index::RecordIndex::open(&cfg.index_path())?);
    let gc_state = Arc::new(gc::GcState::default());
    let opened =
        node_setup::open_node(&cfg, gc::config(gc_state.clone(), cfg.gc_interval_secs)).await?;
    if cfg.gc_interval_secs > 0 {
        tracing::info!(
            interval_seconds = cfg.gc_interval_secs,
            "blob-store garbage collection enabled"
        );
    }

    // Wire the GC protect callback now that the pointer doc exists.
    *gc_state.sources.write().expect("gc sources lock poisoned") = Some(gc::GcSources {
        pointer_doc: opened.pointer_doc.clone(),
        blob_store: opened.blob_store.clone(),
        index: index.clone(),
        top_level_partitions: cfg.top_level_partitions,
    });

    let ticket = opened.pointer_doc.share_read_ticket().await?.to_string();
    tokio::fs::write(cfg.ticket_path(), &ticket).await?;
    let endpoint_id = opened.node.endpoint.id().to_string();
    tracing::info!(
        endpoint_id,
        ticket_path = %cfg.ticket_path().display(),
        "read ticket exported (also served on GET /ticket)"
    );

    let publisher_status = Arc::new(publisher::PublisherStatus::default());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let publisher_task = tokio::spawn(
        publisher::Publisher {
            index: index.clone(),
            blob_store: opened.blob_store.clone(),
            pointer_doc: opened.pointer_doc.clone(),
            gc_state: gc_state.clone(),
            status: publisher_status.clone(),
            leaf_max_entries: cfg.leaf_max_entries,
            max_pending: cfg.publish_max_pending,
            max_interval: std::time::Duration::from_secs(cfg.publish_interval_secs),
            denylist_path: cfg.denylist_path.clone(),
            top_level_partitions: cfg.top_level_partitions,
        }
        .run(shutdown_rx),
    );

    let state = api::AppState {
        index,
        blob_store: opened.blob_store.clone(),
        gc_state,
        publisher_status,
        api_tokens: Arc::new(cfg.api_tokens.iter().cloned().collect::<HashSet<_>>()),
        read_ticket: Arc::new(ticket),
        endpoint_id: Arc::new(endpoint_id),
        max_value_bytes: cfg.max_value_bytes,
        batch_max_records: cfg.batch_max_records,
        top_level_partitions: cfg.top_level_partitions,
    };
    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!(addr = %cfg.bind_addr, "write API listening");

    // Serve until SIGTERM/SIGINT, then close the node deliberately: the
    // blob store MUST be flushed and shut down before the process exits,
    // or the next start pays a full-store consistency scan — hours of
    // silent unavailability on a large store. The systemd unit grants a
    // 300 s stop window; this is what spends it.
    let shutdown_signal = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        tracing::info!("shutdown signal received; draining publisher and closing node");
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    shutdown_tx.send(true).ok();
    publisher_task.await?;
    opened.node.shutdown().await?;
    tracing::info!("node closed cleanly; exiting");
    Ok(())
}
