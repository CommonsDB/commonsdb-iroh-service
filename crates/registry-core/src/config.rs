//! Small, dependency-free environment variable helpers shared by the
//! binaries, so `.env` handling is consistent everywhere instead of each
//! binary reinventing `std::env::var` error handling.

use std::env;
use std::fmt::Display;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    Missing(String),
    #[error("environment variable {0} has invalid value '{1}': {2}")]
    Invalid(String, String, String),
}

pub fn env_required(key: &str) -> Result<String, ConfigError> {
    env::var(key).map_err(|_| ConfigError::Missing(key.to_string()))
}

pub fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

pub fn env_optional(key: &str) -> Option<String> {
    env::var(key).ok()
}

pub fn env_parse_or<T>(key: &str, default: T) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: Display,
{
    match env::var(key) {
        Ok(v) => v
            .parse::<T>()
            .map_err(|e| ConfigError::Invalid(key.to_string(), v, e.to_string())),
        Err(_) => Ok(default),
    }
}

pub fn env_parse_required<T>(key: &str) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: Display,
{
    let v = env_required(key)?;
    v.parse::<T>()
        .map_err(|e| ConfigError::Invalid(key.to_string(), v, e.to_string()))
}

/// Resolves the `tracing`/`EnvFilter` directive to use, bridging this
/// project's documented `LOG_LEVEL` variable (docs/operator-guide.md)
/// onto the `tracing` ecosystem's own `RUST_LOG` convention: `RUST_LOG`
/// wins if set (so the usual Rust tooling knob still works), otherwise
/// `LOG_LEVEL` is used, otherwise `default`.
pub fn log_level_filter(default: &str) -> String {
    env::var("RUST_LOG")
        .or_else(|_| env::var("LOG_LEVEL"))
        .unwrap_or_else(|_| default.to_string())
}

/// ISCC similarity-search parameters, shared by the services (index build)
/// and the client (query) so both agree on banding —
/// docs/similarity-search.md. The band count is a
/// deployment-wide constant (an index built with N bands must be queried
/// with N bands); radius and probe are per-query knobs with these defaults.
#[derive(Debug, Clone, Copy)]
pub struct IsccConfig {
    pub index_bands: u32,
    pub search_max_radius: u32,
    pub band_probe_radius: u32,
}

impl IsccConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            index_bands: env_parse_or("ISCC_INDEX_BANDS", 8u32)?,
            search_max_radius: env_parse_or("ISCC_SEARCH_MAX_RADIUS", 16u32)?,
            band_probe_radius: env_parse_or("ISCC_BAND_PROBE_RADIUS", 0u32)?,
        })
    }
}

impl Default for IsccConfig {
    fn default() -> Self {
        Self {
            index_bands: 8,
            search_max_radius: 16,
            band_probe_radius: 0,
        }
    }
}

/// Loads a `.env` file (if present) into the process environment without
/// overriding variables already set (so real environment variables in a
/// deployed container always win over a stray local `.env` file). No-op if
/// the file does not exist — `.env` is a local-development convenience,
/// not a requirement.
pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_or_falls_back_to_default() {
        // SAFETY (test-only): scoped to a key that no test/service uses,
        // exercised sequentially by the default single-threaded test runner
        // behavior of this crate's other tests not touching env vars.
        unsafe {
            std::env::remove_var("REGISTRY_COMMON_TEST_MISSING_KEY");
        }
        assert_eq!(
            env_or("REGISTRY_COMMON_TEST_MISSING_KEY", "fallback"),
            "fallback"
        );
    }
}
