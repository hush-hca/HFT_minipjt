use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

const REQUIRED_ADAPTERS: [&str; 4] = ["upbit_spot", "bithumb_spot", "binance_spot", "binance_usdm"];

#[derive(Debug, Clone, Deserialize)]
pub struct CollectorConfig {
    pub output_root: PathBuf,
    pub assets: Vec<String>,
    pub strict_symbols: bool,
    pub channel_capacity: usize,
    pub batch_rows: usize,
    pub flush_interval_ms: u64,
    pub enqueue_timeout_ms: u64,
    pub stats_interval_secs: u64,
    pub retry: RetryConfig,
    pub adapters: BTreeMap<String, AdapterConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub reset_after_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdapterConfig {
    pub enabled: bool,
    pub quote: String,
    pub rest_url: String,
    pub websocket_url: String,
    pub proactive_reconnect_secs: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse configuration at {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid configuration: {0}")]
    Validation(String),
}

impl CollectorConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.output_root.as_os_str().is_empty() {
            return invalid("output_root must not be empty");
        }
        if self.assets.is_empty() {
            return invalid("assets must not be empty");
        }

        let mut assets = BTreeSet::new();
        for asset in &self.assets {
            if !is_uppercase_identifier(asset) {
                return invalid(format!(
                    "asset {asset:?} must contain only uppercase ASCII letters and digits"
                ));
            }
            if !assets.insert(asset) {
                return invalid(format!("duplicate asset {asset:?}"));
            }
        }

        if self.channel_capacity == 0 {
            return invalid("channel_capacity must be greater than zero");
        }
        if self.batch_rows == 0 {
            return invalid("batch_rows must be greater than zero");
        }
        if self.flush_interval_ms == 0 {
            return invalid("flush_interval_ms must be greater than zero");
        }
        if self.enqueue_timeout_ms == 0 {
            return invalid("enqueue_timeout_ms must be greater than zero");
        }
        if self.stats_interval_secs == 0 {
            return invalid("stats_interval_secs must be greater than zero");
        }

        if self.retry.initial_ms == 0 {
            return invalid("retry.initial_ms must be greater than zero");
        }
        if self.retry.max_ms == 0 {
            return invalid("retry.max_ms must be greater than zero");
        }
        if self.retry.reset_after_secs == 0 {
            return invalid("retry.reset_after_secs must be greater than zero");
        }
        if self.retry.initial_ms > self.retry.max_ms {
            return invalid("retry.initial_ms must not exceed retry.max_ms");
        }

        for required in REQUIRED_ADAPTERS {
            if !self.adapters.contains_key(required) {
                return invalid(format!("missing required adapter {required:?}"));
            }
        }

        for (name, adapter) in &self.adapters {
            if !REQUIRED_ADAPTERS.contains(&name.as_str()) {
                return invalid(format!("unsupported adapter {name:?}"));
            }
            if !is_uppercase_identifier(&adapter.quote) {
                return invalid(format!(
                    "adapter {name:?} quote must contain only uppercase ASCII letters and digits"
                ));
            }
            if !allowed_endpoint(&adapter.rest_url, "https", "http") {
                return invalid(format!(
                    "adapter {name:?} rest_url must use HTTPS (HTTP is allowed only for loopback)"
                ));
            }
            if !allowed_endpoint(&adapter.websocket_url, "wss", "ws") {
                return invalid(format!(
                    "adapter {name:?} websocket_url must use secure WebSockets (WS is allowed only for loopback)"
                ));
            }
            if adapter.proactive_reconnect_secs == Some(0) {
                return invalid(format!(
                    "adapter {name:?} proactive_reconnect_secs must be greater than zero"
                ));
            }
        }

        Ok(())
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError::Validation(message.into()))
}

fn is_uppercase_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn allowed_endpoint(value: &str, secure_scheme: &str, loopback_scheme: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    if url.cannot_be_a_base() || url.host_str().is_none() {
        return false;
    }
    if url.scheme() == secure_scheme {
        return true;
    }
    url.scheme() == loopback_scheme
        && url.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> CollectorConfig {
        CollectorConfig {
            output_root: PathBuf::from("data"),
            assets: vec!["BTC".into(), "ETH".into()],
            strict_symbols: false,
            channel_capacity: 65_536,
            batch_rows: 8_192,
            flush_interval_ms: 1_000,
            enqueue_timeout_ms: 5_000,
            stats_interval_secs: 10,
            retry: RetryConfig {
                initial_ms: 1_000,
                max_ms: 30_000,
                reset_after_secs: 300,
            },
            adapters: REQUIRED_ADAPTERS
                .into_iter()
                .map(|name| {
                    (
                        name.to_owned(),
                        AdapterConfig {
                            enabled: true,
                            quote: if name.starts_with("binance") {
                                "USDT".into()
                            } else {
                                "KRW".into()
                            },
                            rest_url: "https://example.com/markets".into(),
                            websocket_url: "wss://example.com/stream".into(),
                            proactive_reconnect_secs: None,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn rejects_duplicate_and_non_uppercase_assets() {
        let mut duplicate = valid_config();
        duplicate.assets = vec!["BTC".into(), "BTC".into()];
        assert!(duplicate.validate().is_err());

        let mut lowercase = valid_config();
        lowercase.assets = vec!["btc".into()];
        assert!(lowercase.validate().is_err());
    }

    #[test]
    fn rejects_missing_adapter_and_insecure_urls() {
        let mut missing = valid_config();
        missing.adapters.remove("upbit_spot");
        assert!(missing.validate().is_err());

        let mut insecure = valid_config();
        insecure.adapters.get_mut("upbit_spot").unwrap().rest_url = "http://example.com".into();
        assert!(insecure.validate().is_err());
    }

    #[test]
    fn allows_insecure_protocols_only_on_loopback_for_deterministic_tests() {
        let mut config = valid_config();
        {
            let adapter = config.adapters.get_mut("upbit_spot").unwrap();
            adapter.rest_url = "http://127.0.0.1:8080/markets".into();
            adapter.websocket_url = "ws://localhost:8081/stream".into();
        }
        assert!(config.validate().is_ok());

        config.adapters.get_mut("upbit_spot").unwrap().rest_url =
            "http://example.com/markets".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_limits_and_inverted_retry_range() {
        let mut zero_capacity = valid_config();
        zero_capacity.channel_capacity = 0;
        assert!(zero_capacity.validate().is_err());

        let mut inverted_retry = valid_config();
        inverted_retry.retry.initial_ms = 30_001;
        assert!(inverted_retry.validate().is_err());
    }
}
