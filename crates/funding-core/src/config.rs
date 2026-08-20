use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;
use url::Url;

const REQUIRED_VENUES: [&str; 2] = ["binance_usdm", "bybit_linear"];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingConfig {
    pub output_root: PathBuf,
    pub assets: Vec<String>,
    pub quote_conversions: Vec<QuoteConversionConfig>,
    pub channel_capacity: usize,
    pub batch_rows: usize,
    pub flush_interval_ms: u64,
    pub poll: PollConfig,
    pub venues: BTreeMap<String, VenueConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollConfig {
    pub instrument_secs: u64,
    pub open_interest_secs: u64,
    pub trader_ratio_secs: u64,
    pub funding_metadata_secs: u64,
    pub reserved_order_weight: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueConfig {
    pub mainnet: EndpointSet,
    pub testnet: EndpointSet,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointSet {
    pub rest_url: String,
    pub public_websocket_url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteConversionConfig {
    pub base: String,
    pub quote: String,
    pub venues: Vec<String>,
}

#[derive(Debug, Error)]
pub enum FundingConfigError {
    #[error("failed to read funding configuration {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse funding configuration {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid funding configuration: {0}")]
    Invalid(String),
}

impl FundingConfig {
    pub fn load(path: &Path) -> Result<Self, FundingConfigError> {
        let text = fs::read_to_string(path).map_err(|source| FundingConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Self = toml::from_str(&text).map_err(|source| FundingConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), FundingConfigError> {
        if self.output_root.as_os_str().is_empty() {
            return invalid("output_root must not be empty");
        }
        if self.assets.is_empty() {
            return invalid("assets must not be empty");
        }
        let mut unique = HashSet::with_capacity(self.assets.len());
        for asset in &self.assets {
            if !is_symbol_component(asset) {
                return invalid(format!(
                    "asset {asset:?} must contain only uppercase ASCII letters or digits"
                ));
            }
            if !unique.insert(asset) {
                return invalid(format!("asset {asset:?} is duplicated"));
            }
        }

        require_positive("channel_capacity", self.channel_capacity)?;
        require_positive("batch_rows", self.batch_rows)?;
        require_positive("flush_interval_ms", self.flush_interval_ms)?;
        require_minimum("poll.instrument_secs", self.poll.instrument_secs, 900)?;
        require_minimum("poll.open_interest_secs", self.poll.open_interest_secs, 5)?;
        require_minimum("poll.trader_ratio_secs", self.poll.trader_ratio_secs, 300)?;
        require_minimum(
            "poll.funding_metadata_secs",
            self.poll.funding_metadata_secs,
            900,
        )?;
        require_positive(
            "poll.reserved_order_weight",
            self.poll.reserved_order_weight,
        )?;

        if self.venues.len() != REQUIRED_VENUES.len()
            || REQUIRED_VENUES
                .iter()
                .any(|required| !self.venues.contains_key(*required))
        {
            return invalid(format!(
                "venues must contain exactly {}",
                REQUIRED_VENUES.join(", ")
            ));
        }
        for (name, venue) in &self.venues {
            validate_endpoint_set(name, "mainnet", &venue.mainnet)?;
            validate_endpoint_set(name, "testnet", &venue.testnet)?;
        }

        if self.quote_conversions.is_empty() {
            return invalid("quote_conversions must not be empty");
        }
        let mut conversion_pairs = HashSet::with_capacity(self.quote_conversions.len());
        for conversion in &self.quote_conversions {
            if !is_symbol_component(&conversion.base) || !is_symbol_component(&conversion.quote) {
                return invalid("quote conversion symbols must be uppercase ASCII");
            }
            if conversion.base == conversion.quote {
                return invalid("quote conversion base and quote must differ");
            }
            if !conversion_pairs.insert((&conversion.base, &conversion.quote)) {
                return invalid(format!(
                    "quote conversion {}/{} is duplicated",
                    conversion.base, conversion.quote
                ));
            }
            if conversion.venues.is_empty() {
                return invalid("quote conversion venues must not be empty");
            }
            let mut venues = HashSet::with_capacity(conversion.venues.len());
            for venue in &conversion.venues {
                if !matches!(venue.as_str(), "upbit_spot" | "bithumb_spot") {
                    return invalid(format!("quote conversion venue {venue:?} is not supported"));
                }
                if !venues.insert(venue) {
                    return invalid(format!("quote conversion venue {venue:?} is duplicated"));
                }
            }
        }
        Ok(())
    }
}

fn validate_endpoint_set(
    venue: &str,
    environment: &str,
    endpoints: &EndpointSet,
) -> Result<(), FundingConfigError> {
    validate_url(
        &format!("venues.{venue}.{environment}.rest_url"),
        &endpoints.rest_url,
        "https",
        "http",
    )?;
    validate_url(
        &format!("venues.{venue}.{environment}.public_websocket_url"),
        &endpoints.public_websocket_url,
        "wss",
        "ws",
    )
}

fn validate_url(
    field: &str,
    value: &str,
    secure_scheme: &str,
    loopback_scheme: &str,
) -> Result<(), FundingConfigError> {
    let url = Url::parse(value).map_err(|error| {
        FundingConfigError::Invalid(format!("{field} is not a valid URL: {error}"))
    })?;
    let secure = url.scheme() == secure_scheme;
    let loopback = url.scheme() == loopback_scheme
        && url.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
    if (!secure && !loopback)
        || url.host_str().is_none()
        || url.cannot_be_a_base()
        || url.fragment().is_some()
        || url.query().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return invalid(format!(
            "{field} must use {secure_scheme}, except {loopback_scheme} is allowed for loopback"
        ));
    }
    Ok(())
}

fn require_minimum<T>(field: &str, value: T, minimum: T) -> Result<(), FundingConfigError>
where
    T: Copy + PartialOrd + std::fmt::Display,
{
    if value < minimum {
        invalid(format!("{field} must be at least {minimum}"))
    } else {
        Ok(())
    }
}

fn require_positive<T>(field: &str, value: T) -> Result<(), FundingConfigError>
where
    T: Copy + Default + PartialEq,
{
    if value == T::default() {
        invalid(format!("{field} must be positive"))
    } else {
        Ok(())
    }
}

fn is_symbol_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, FundingConfigError> {
    Err(FundingConfigError::Invalid(message.into()))
}
