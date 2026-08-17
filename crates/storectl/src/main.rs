use clap::{Parser, Subcommand};
use std::path::PathBuf;
use storectl::{commands, config};

/// storectl — a read-only console client for the iroh registry root
/// pointer document. See docs/reader-guide.md and
/// docs/operator-guide.md.
#[derive(Parser)]
#[command(name = "storectl", version, about)]
struct Cli {
    /// Overrides the compiled-in / configured read-only DocTicket.
    #[arg(long, global = true)]
    ticket: Option<String>,

    /// Overrides the local data directory (identity + cache).
    #[arg(long, global = true)]
    storage_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Resolve and print the JSON value for a single key.
    Get {
        cid: String,
        /// Write the value to a file instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Use a throwaway node identity and skip local persistence.
        #[arg(long)]
        ephemeral: bool,
    },
    /// Subscribe to the root pointer document's live updates.
    Watch,
    /// Find records whose ISCC Content-Code is within a Hamming radius of
    /// the query — approximate, verified exactly client-side
    /// (docs/similarity-search.md).
    Similar {
        /// The query: an ISCC string (e.g. `ISCC:EA...`) or a raw 16-digit
        /// hex Content-Code (e.g. `0xdeadbeefcafebabe`).
        query: String,
        /// Maximum Hamming distance (default/cap from ISCC_SEARCH_MAX_RADIUS).
        #[arg(long)]
        radius: Option<u32>,
        /// Per-band probe radius; raises recall at more lookup cost
        /// (default from ISCC_BAND_PROBE_RADIUS).
        #[arg(long)]
        probe: Option<u32>,
        /// Also fetch and print each matching record's JSON value, not just
        /// key + distance.
        #[arg(long)]
        fetch: bool,
    },
    /// List published records — key (cid) + ISCC Content-Code — from the
    /// P2P index, for debugging. Pages interactively past --free-limit.
    List {
        /// Restrict to one partition (0..TOP_LEVEL_PARTITIONS); default all.
        #[arg(long)]
        partition: Option<u32>,
        /// Rows per interactive page once past --free-limit.
        #[arg(long, default_value_t = 100)]
        page_size: usize,
        /// Rows printed freely before interactive paging kicks in.
        #[arg(long, default_value_t = 300)]
        free_limit: usize,
        /// Stop after this many rows.
        #[arg(long)]
        max: Option<usize>,
        /// Stream everything without prompting (for piping to files).
        #[arg(long)]
        no_page: bool,
        /// Also fetch each record's value and print its full `iscc` string
        /// (one blob fetch per row — slow; combine with --max/--partition).
        #[arg(long)]
        values: bool,
        /// Print per-partition counts and totals only, no record rows.
        #[arg(long)]
        summary: bool,
        /// Give up on a partition whose index blobs no peer serves within
        /// this many seconds (it is reported and skipped, not fatal).
        /// Sized for WAN reads: a populated partition takes 10-30 s of
        /// round-trips from outside the provider's network.
        #[arg(long, default_value_t = 25)]
        walk_timeout: u64,
        /// Extra wait (ms) after opening a persistent store so newer pointer
        /// document entries can sync in over the locally cached ones.
        #[arg(long, default_value_t = 2000)]
        settle_ms: u64,
        /// Use a throwaway node identity and skip local persistence.
        #[arg(long)]
        ephemeral: bool,
    },
    /// Run indefinitely, syncing and re-serving the given shards (or `all`).
    Seed {
        #[arg(long, value_delimiter = ',', default_value = "all")]
        shards: Vec<String>,
    },
    /// Print local node identity, sync state, and cache size.
    Status,
    /// Print a read ticket with extra provider nodes added — hand the
    /// result to readers so they bootstrap from seed nodes as well as the
    /// origin (e.g. `compose-ticket --add $(storectl identity)`).
    ComposeTicket {
        /// EndpointId(s) to add as providers (repeatable).
        #[arg(long = "add")]
        add: Vec<String>,
        /// Drop the ticket's existing providers first (testing isolation).
        #[arg(long)]
        strip: bool,
    },
    /// Print this node's public identity (EndpointId) without opening any
    /// stores — usable while a seed service is running.
    Identity,
    /// Print resolved configuration.
    Config,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The iroh stack warns constantly about routine P2P churn (handshake
    // aborts, multipath negotiation, address-detection variance) — none of
    // it is actionable for a CLI user and it buries the actual output, so
    // the network internals default to error-only. Unlike the services,
    // LOG_LEVEL is deliberately IGNORED here: it leaks into end-user
    // shells from service env files and turns every command into a log
    // dump. Only an explicit RUST_LOG (the debugging escape hatch)
    // overrides the quiet default.
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        "warn,iroh=error,noq_proto=error,iroh_docs=error,iroh_blobs=error,\
         iroh_gossip=error,iroh_relay=error,iroh_util=error,netwatch=error,portmapper=error"
            .to_string()
    });
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let cfg = config::resolve(cli.ticket, cli.storage_dir)?;

    match cli.command {
        Command::Get {
            cid,
            out,
            ephemeral,
        } => commands::get(cfg, cid, out, ephemeral).await,
        Command::Watch => commands::watch(cfg).await,
        Command::Identity => commands::identity(cfg).await,
        Command::List {
            partition,
            page_size,
            free_limit,
            max,
            no_page,
            values,
            summary,
            walk_timeout,
            settle_ms,
            ephemeral,
        } => {
            commands::list(
                cfg,
                partition,
                page_size,
                free_limit,
                max,
                no_page,
                values,
                summary,
                ephemeral,
                walk_timeout,
                settle_ms,
            )
            .await
        }
        Command::Similar {
            query,
            radius,
            probe,
            fetch,
        } => commands::similar(cfg, query, radius, probe, fetch).await,
        Command::Seed { shards } => commands::seed(cfg, shards).await,
        Command::Status => commands::status(cfg).await,
        Command::ComposeTicket { add, strip } => commands::compose_ticket(cfg, add, strip),
        Command::Config => {
            commands::print_config(&cfg);
            Ok(())
        }
    }
}
