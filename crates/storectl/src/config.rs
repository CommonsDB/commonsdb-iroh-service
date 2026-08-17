//! Configuration resolution, in the precedence order documented in
//! docs/reader-guide.md, "Configuration":
//! 1. CLI flags
//! 2. Environment variables (`STORECTL_READ_TICKET`, `STORECTL_STORAGE_DIR`, ...)
//! 3. Config file (`~/.config/storectl/config.toml`)
//! 4. Compiled-in default (the ticket baked into a release build)

use serde::Deserialize;
use std::path::PathBuf;

/// Baked in at compile time. Empty in this repository's own build; a
/// release pipeline for a real deployment may overwrite
/// `default_ticket.txt` with the actual read-only ticket before running
/// `cargo build --release` — see docs/reader-guide.md.
const DEFAULT_TICKET: &str = include_str!("../default_ticket.txt");

#[derive(Debug, Deserialize, Default)]
struct ConfigFile {
    read_ticket: Option<String>,
    storage_dir: Option<PathBuf>,
    top_level_partitions: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub read_ticket: String,
    pub read_ticket_source: &'static str,
    pub storage_dir: PathBuf,
    pub storage_dir_source: &'static str,
    pub top_level_partitions: u32,
}

fn config_file_path() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join("storectl").join("config.toml"))
}

fn read_config_file() -> ConfigFile {
    let Some(path) = config_file_path() else {
        return ConfigFile::default();
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => toml::from_str(&contents).unwrap_or_else(|e| {
            tracing::warn!(path = %path.display(), error = %e, "failed to parse storectl config file, ignoring it");
            ConfigFile::default()
        }),
        Err(_) => ConfigFile::default(),
    }
}

pub fn default_storage_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("storectl")
}

pub fn resolve(
    cli_ticket: Option<String>,
    cli_storage_dir: Option<PathBuf>,
) -> anyhow::Result<ResolvedConfig> {
    let file = read_config_file();

    let (read_ticket, read_ticket_source) = if let Some(t) = cli_ticket {
        (t, "--ticket flag")
    } else if let Ok(t) = std::env::var("STORECTL_READ_TICKET") {
        if t.is_empty() {
            (String::new(), "unset")
        } else {
            (t, "STORECTL_READ_TICKET")
        }
    } else if let Some(t) = file.read_ticket.filter(|s| !s.is_empty()) {
        (t, "config file")
    } else if !DEFAULT_TICKET.trim().is_empty() {
        (DEFAULT_TICKET.trim().to_string(), "compiled-in default")
    } else {
        (String::new(), "unset")
    };

    if read_ticket.is_empty() {
        // Name the platform's real config path: dirs::config_dir() is
        // ~/.config on Linux but ~/Library/Application Support on macOS,
        // and a hardcoded ~/.config in this message sent users to a file
        // the resolver never reads.
        let config_path = config_file_path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<no config dir on this platform>".to_string());
        anyhow::bail!(
            "no read ticket configured. Set one via --ticket, the STORECTL_READ_TICKET \
             environment variable, the read_ticket key in {config_path}, or build \
             a release with a compiled-in ticket (see docs/reader-guide.md)."
        );
    }

    let (storage_dir, storage_dir_source) = if let Some(d) = cli_storage_dir {
        (d, "--storage-dir flag")
    } else if let Ok(d) = std::env::var("STORECTL_STORAGE_DIR") {
        (PathBuf::from(d), "STORECTL_STORAGE_DIR")
    } else if let Some(d) = file.storage_dir {
        (d, "config file")
    } else {
        (default_storage_dir(), "platform default")
    };

    let top_level_partitions = std::env::var("STORECTL_TOP_LEVEL_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or(file.top_level_partitions)
        .unwrap_or(registry_core::TOP_LEVEL_PARTITIONS_DEFAULT);

    Ok(ResolvedConfig {
        read_ticket,
        read_ticket_source,
        storage_dir,
        storage_dir_source,
        top_level_partitions,
    })
}
