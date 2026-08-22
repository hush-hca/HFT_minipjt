use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use url::Url;

use crate::opportunity::CapacitySource;

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
    pub cost: CostConfig,
    pub poll: PollConfig,
    pub venues: BTreeMap<String, VenueConfig>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct ExactDecimal(i128);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecimalRounding {
    TowardZero,
    Floor,
    Ceiling,
    HalfAwayFromZero,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum DecimalMathError {
    #[error("coefficient exceeds Decimal128(38,18) precision")]
    PrecisionOverflow,
    #[error("decimal addition overflowed")]
    AdditionOverflow,
    #[error("decimal subtraction overflowed")]
    SubtractionOverflow,
    #[error("division by zero")]
    DivisionByZero,
}

impl ExactDecimal {
    pub const SCALE: i128 = 1_000_000_000_000_000_000;
    pub const MAX_COEFFICIENT: i128 = 10_i128.pow(38) - 1;

    pub fn from_scaled(value: i128) -> Result<Self, DecimalMathError> {
        if value.unsigned_abs() > Self::MAX_COEFFICIENT as u128 {
            Err(DecimalMathError::PrecisionOverflow)
        } else {
            Ok(Self(value))
        }
    }

    pub const fn scaled(self) -> i128 {
        self.0
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self, DecimalMathError> {
        self.0
            .checked_add(rhs.0)
            .ok_or(DecimalMathError::AdditionOverflow)
            .and_then(Self::from_scaled)
    }

    pub fn checked_sub(self, rhs: Self) -> Result<Self, DecimalMathError> {
        self.0
            .checked_sub(rhs.0)
            .ok_or(DecimalMathError::SubtractionOverflow)
            .and_then(Self::from_scaled)
    }

    pub fn checked_mul(
        self,
        rhs: Self,
        rounding: DecimalRounding,
    ) -> Result<Self, DecimalMathError> {
        let numerator = BigInt::from(self.0) * BigInt::from(rhs.0);
        scaled_quotient(numerator, BigInt::from(Self::SCALE), rounding)
    }

    pub fn checked_div(
        self,
        rhs: Self,
        rounding: DecimalRounding,
    ) -> Result<Self, DecimalMathError> {
        if rhs.0 == 0 {
            return Err(DecimalMathError::DivisionByZero);
        }
        let numerator = BigInt::from(self.0) * BigInt::from(Self::SCALE);
        scaled_quotient(numerator, BigInt::from(rhs.0), rounding)
    }
}

fn scaled_quotient(
    mut numerator: BigInt,
    mut denominator: BigInt,
    rounding: DecimalRounding,
) -> Result<ExactDecimal, DecimalMathError> {
    if denominator.is_negative() {
        numerator = -numerator;
        denominator = -denominator;
    }
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    let rounded = if remainder.is_zero() {
        quotient
    } else {
        let direction = if numerator.is_negative() { -1 } else { 1 };
        match rounding {
            DecimalRounding::TowardZero => quotient,
            DecimalRounding::Floor if numerator.is_negative() => quotient - 1,
            DecimalRounding::Floor => quotient,
            DecimalRounding::Ceiling if numerator.is_positive() => quotient + 1,
            DecimalRounding::Ceiling => quotient,
            DecimalRounding::HalfAwayFromZero if remainder.abs() * 2 >= denominator.abs() => {
                quotient + direction
            }
            DecimalRounding::HalfAwayFromZero => quotient,
        }
    };
    rounded
        .to_i128()
        .ok_or(DecimalMathError::PrecisionOverflow)
        .and_then(ExactDecimal::from_scaled)
}

impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        md_core::decimal::parse_decimal_18(&value)
            .map_err(serde::de::Error::custom)
            .and_then(|value| Self::from_scaled(value).map_err(serde::de::Error::custom))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostConfig {
    pub binance_taker_rate: ExactDecimal,
    pub bybit_taker_rate: ExactDecimal,
    pub entry_slippage_bps: ExactDecimal,
    pub exit_slippage_bps: ExactDecimal,
    pub entry_book_impact_bps: ExactDecimal,
    pub exit_book_impact_bps: ExactDecimal,
    pub basis_risk_buffer_bps: ExactDecimal,
    pub funding_error_buffer_bps: ExactDecimal,
    pub leg_risk_buffer_bps: ExactDecimal,
    pub research_quote_per_leg: ExactDecimal,
    #[serde(skip, default = "configured_research_limit")]
    pub capacity_source: CapacitySource,
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
        self.cost.validate()?;
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

        if self.quote_conversions.len() != 1
            || self.quote_conversions[0].base != "USDT"
            || self.quote_conversions[0].quote != "KRW"
        {
            return invalid("quote_conversions must contain exactly the USDT/KRW reference");
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

impl CostConfig {
    fn validate(&self) -> Result<(), FundingConfigError> {
        require_exact_positive("cost.binance_taker_rate", self.binance_taker_rate)?;
        require_exact_positive("cost.bybit_taker_rate", self.bybit_taker_rate)?;
        require_exact_at_most_one("cost.binance_taker_rate", self.binance_taker_rate)?;
        require_exact_at_most_one("cost.bybit_taker_rate", self.bybit_taker_rate)?;
        require_exact_nonnegative("cost.entry_slippage_bps", self.entry_slippage_bps)?;
        require_exact_nonnegative("cost.exit_slippage_bps", self.exit_slippage_bps)?;
        require_exact_nonnegative("cost.entry_book_impact_bps", self.entry_book_impact_bps)?;
        require_exact_nonnegative("cost.exit_book_impact_bps", self.exit_book_impact_bps)?;
        require_exact_nonnegative("cost.basis_risk_buffer_bps", self.basis_risk_buffer_bps)?;
        require_exact_nonnegative(
            "cost.funding_error_buffer_bps",
            self.funding_error_buffer_bps,
        )?;
        require_exact_nonnegative("cost.leg_risk_buffer_bps", self.leg_risk_buffer_bps)?;
        require_exact_positive("cost.research_quote_per_leg", self.research_quote_per_leg)?;

        const MAX_RESEARCH_QUOTE_PER_LEG: i128 = 100_000_000_000_000_000_000;
        if self.research_quote_per_leg.scaled() > MAX_RESEARCH_QUOTE_PER_LEG {
            return invalid("cost.research_quote_per_leg must not exceed 100 USDT");
        }
        if self.capacity_source != CapacitySource::ConfiguredResearchLimit {
            return invalid("cost.capacity_source must be configured_research_limit");
        }
        Ok(())
    }
}

fn configured_research_limit() -> CapacitySource {
    CapacitySource::ConfiguredResearchLimit
}

fn require_exact_positive(field: &str, value: ExactDecimal) -> Result<(), FundingConfigError> {
    if value.scaled() <= 0 {
        invalid(format!("{field} must be positive"))
    } else {
        Ok(())
    }
}

fn require_exact_nonnegative(field: &str, value: ExactDecimal) -> Result<(), FundingConfigError> {
    if value.scaled() < 0 {
        invalid(format!("{field} must not be negative"))
    } else {
        Ok(())
    }
}

fn require_exact_at_most_one(field: &str, value: ExactDecimal) -> Result<(), FundingConfigError> {
    if value.scaled() > ExactDecimal::SCALE {
        invalid(format!("{field} must not exceed one"))
    } else {
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
