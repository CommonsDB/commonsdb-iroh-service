//! Daemon configuration: a single TOML file (default
//! `/etc/registryd/config.toml`, mode 600 — it holds the API bearer
//! tokens), overridable per-key by environment variables so container
//! deployments can avoid the file entirely. See docs/operator-guide.md,
//! "Configuration".

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/registryd/config.toml";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Where the HTTP write API listens. Bind to loopback and put a TLS
    /// proxy in front for public deployments (docs/operator-guide.md).
    pub bind_addr: SocketAddr,
    /// Everything persistent lives under here: the blob store, the docs
    /// store, the record index, node keys, and the exported read ticket.
    /// Local disk only — never a network filesystem.
    pub data_dir: PathBuf,
    /// Bearer tokens accepted by the write endpoints. The daemon refuses
    /// to start with an empty list — there is no unauthenticated write
    /// mode.
    pub api_tokens: Vec<String>,
    /// Maximum accepted `value` size in bytes, measured on the serialized
    /// JSON.
    pub max_value_bytes: usize,
    /// Maximum records per `POST /v1/records/batch` request.
    pub batch_max_records: usize,
    /// The publisher wakes when this many records are pending…
    pub publish_max_pending: usize,
    /// …or when this much time has passed since the last publish with
    /// anything pending, whichever comes first.
    pub publish_interval_secs: u64,
    /// Top-level partition count. Part of the wire format — readers must
    /// agree, so leave this at 256 unless you are building a private
    /// deployment from scratch.
    pub top_level_partitions: u32,
    /// HAMT leaf split threshold. Also wire-format-stable; leave default.
    pub leaf_max_entries: usize,
    /// Optional newline-separated file of record keys to exclude from
    /// published trees (`#` comments allowed). Reloaded every publish
    /// cycle; see docs/operator-guide.md, "Takedowns".
    pub denylist_path: Option<PathBuf>,
    /// Blob-store garbage collection interval. 0 disables GC.
    pub gc_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8080".parse().expect("valid default addr"),
            data_dir: PathBuf::from("/var/lib/registryd"),
            api_tokens: Vec::new(),
            max_value_bytes: 64 * 1024,
            batch_max_records: 500,
            publish_max_pending: 1000,
            publish_interval_secs: 30,
            top_level_partitions: registry_core::TOP_LEVEL_PARTITIONS_DEFAULT,
            leaf_max_entries: registry_core::LEAF_MAX_ENTRIES_DEFAULT,
            denylist_path: None,
            gc_interval_secs: 3600,
        }
    }
}

impl Config {
    /// Load from `path` (or the `REGISTRYD_CONFIG`/default path when
    /// `None`), then apply environment overrides. A missing file is only
    /// an error when it was named explicitly — defaults + env alone are a
    /// valid configuration.
    pub fn load(cli_path: Option<PathBuf>) -> anyhow::Result<Self> {
        let (path, explicit) = match cli_path {
            Some(p) => (p, true),
            None => match std::env::var("REGISTRYD_CONFIG") {
                Ok(p) => (PathBuf::from(p), true),
                Err(_) => (PathBuf::from(DEFAULT_CONFIG_PATH), false),
            },
        };

        let mut cfg = match std::fs::read_to_string(&path) {
            Ok(raw) => toml::from_str::<Config>(&raw)
                .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?,
            Err(err) if explicit => {
                anyhow::bail!("cannot read config file {}: {err}", path.display())
            }
            Err(_) => Config::default(),
        };

        cfg.apply_env()?;
        Ok(cfg)
    }

    fn apply_env(&mut self) -> anyhow::Result<()> {
        use registry_core::config::{env_optional, env_parse_or};

        if let Some(v) = env_optional("REGISTRYD_BIND_ADDR") {
            self.bind_addr = v
                .parse()
                .map_err(|e| anyhow::anyhow!("REGISTRYD_BIND_ADDR '{v}' is invalid: {e}"))?;
        }
        if let Some(v) = env_optional("REGISTRYD_DATA_DIR") {
            self.data_dir = PathBuf::from(v);
        }
        if let Some(v) = env_optional("REGISTRYD_API_TOKENS") {
            self.api_tokens = v
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(String::from)
                .collect();
        }
        if let Some(v) = env_optional("REGISTRYD_DENYLIST_PATH") {
            self.denylist_path = Some(PathBuf::from(v));
        }
        self.max_value_bytes = env_parse_or("REGISTRYD_MAX_VALUE_BYTES", self.max_value_bytes)?;
        self.batch_max_records =
            env_parse_or("REGISTRYD_BATCH_MAX_RECORDS", self.batch_max_records)?;
        self.publish_max_pending =
            env_parse_or("REGISTRYD_PUBLISH_MAX_PENDING", self.publish_max_pending)?;
        self.publish_interval_secs = env_parse_or(
            "REGISTRYD_PUBLISH_INTERVAL_SECS",
            self.publish_interval_secs,
        )?;
        self.top_level_partitions =
            env_parse_or("REGISTRYD_TOP_LEVEL_PARTITIONS", self.top_level_partitions)?;
        self.leaf_max_entries = env_parse_or("REGISTRYD_LEAF_MAX_ENTRIES", self.leaf_max_entries)?;
        self.gc_interval_secs = env_parse_or("REGISTRYD_GC_INTERVAL_SECS", self.gc_interval_secs)?;
        Ok(())
    }

    pub fn blob_store_path(&self) -> PathBuf {
        self.data_dir.join("blobs")
    }

    pub fn docs_store_path(&self) -> PathBuf {
        self.data_dir.join("docs")
    }

    pub fn secrets_dir(&self) -> PathBuf {
        self.data_dir.join("secrets")
    }

    pub fn index_path(&self) -> PathBuf {
        self.data_dir.join("index.redb")
    }

    pub fn ticket_path(&self) -> PathBuf {
        self.data_dir.join("read-ticket.txt")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_minimal_config_file() {
        let cfg: Config = toml::from_str(
            r#"
            bind_addr = "0.0.0.0:9000"
            data_dir = "/tmp/reg"
            api_tokens = ["secret-token"]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.bind_addr.port(), 9000);
        assert_eq!(cfg.api_tokens, vec!["secret-token".to_string()]);
        // Untouched keys keep their defaults.
        assert_eq!(cfg.publish_max_pending, 1000);
        assert_eq!(cfg.top_level_partitions, 256);
    }

    #[test]
    fn rejects_unknown_keys() {
        let err = toml::from_str::<Config>("no_such_key = 1").unwrap_err();
        assert!(err.to_string().contains("no_such_key"));
    }
}
