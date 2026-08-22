use funding_core::{
    instrument::InstrumentSpec,
    meta::DerivativeMeta,
    public::{
        DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
        FundingSettlement, MarkIndexSnapshot, OpenInterestSnapshot, OpenInterestUnit,
        TraderMetricKind, TraderRatioSnapshot,
    },
};
use md_core::{
    decimal::{DecimalError, parse_decimal_18},
    model::{AdapterId, CanonicalSymbol, TimestampError, TimestampPrecision, ms_to_us},
};
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_FUNDING_INTERVAL_SECS: u32 = 28_800;
const USDT: &str = "USDT";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FundingRules {
    pub interval_secs: u32,
    pub interval_provenance: FundingIntervalProvenance,
    pub rate_floor: Option<i128>,
    pub rate_cap: Option<i128>,
}

impl Default for FundingRules {
    fn default() -> Self {
        Self {
            interval_secs: DEFAULT_FUNDING_INTERVAL_SECS,
            interval_provenance: FundingIntervalProvenance::AssumedVenueDefault,
            rate_floor: None,
            rate_cap: None,
        }
    }
}

impl FundingRules {
    pub fn from_instrument(spec: &InstrumentSpec) -> Result<Self, DerivativeParseError> {
        // `/fapi/v1/fundingInfo` is an override feed: Binance omits ordinary
        // symbols entirely.  Missing bounds therefore means "venue default",
        // not an invalid instrument.  Keep rate validation unbounded until an
        // explicit override is discovered.
        let rules = Self {
            interval_secs: if spec.funding_interval_secs == 0 {
                DEFAULT_FUNDING_INTERVAL_SECS
            } else {
                spec.funding_interval_secs
            },
            interval_provenance: spec.funding_interval_provenance,
            rate_floor: spec.funding_rate_floor,
            rate_cap: spec.funding_rate_cap,
        };
        validate_rules(rules, false)?;
        Ok(rules)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LegacyRateTypePolicy {
    RejectMissing,
    AcceptMissing,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FundingHistoryRules {
    pub schedule: FundingSchedule,
    pub legacy_rate_type: LegacyRateTypePolicy,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct EffectiveFundingRule {
    pub effective_from_ts_us: i64,
    pub rules: FundingRules,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FundingSchedule {
    rules: Vec<EffectiveFundingRule>,
}

impl FundingSchedule {
    pub fn new(mut rules: Vec<EffectiveFundingRule>) -> Result<Self, DerivativeParseError> {
        if rules.is_empty() {
            return Err(DerivativeParseError::InvalidFundingSchedule);
        }
        rules.sort_by_key(|rule| rule.effective_from_ts_us);
        for (index, rule) in rules.iter().enumerate() {
            if rule.effective_from_ts_us < 0 {
                return Err(DerivativeParseError::InvalidFundingSchedule);
            }
            validate_rules(rule.rules, false)?;
            if index > 0 && rules[index - 1].effective_from_ts_us == rule.effective_from_ts_us {
                return Err(DerivativeParseError::InvalidFundingSchedule);
            }
        }
        Ok(Self { rules })
    }

    pub(crate) fn resolve(
        &self,
        settlement_ts_us: i64,
    ) -> Result<FundingRules, DerivativeParseError> {
        self.rules
            .partition_point(|rule| rule.effective_from_ts_us <= settlement_ts_us)
            .checked_sub(1)
            .map(|index| self.rules[index].rules)
            .ok_or(DerivativeParseError::FundingScheduleUnknown { settlement_ts_us })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PublicCapability {
    Available,
    UnavailableRequiresApiKey { code: &'static str },
}

pub const fn top_trader_public_capability() -> PublicCapability {
    PublicCapability::UnavailableRequiresApiKey {
        code: "BINANCE_TOP_TRADER_REQUIRES_API_KEY",
    }
}

#[derive(Debug, Error)]
pub enum DerivativeParseError {
    #[error("invalid {venue:?} derivative payload: {message}")]
    Decode { venue: AdapterId, message: String },
    #[error("invalid decimal field {field}: {source}")]
    Decimal {
        field: &'static str,
        #[source]
        source: DecimalError,
    },
    #[error("field {field} must be positive")]
    NonPositive { field: &'static str },
    #[error("field {field} must not be negative")]
    Negative { field: &'static str },
    #[error("invalid timestamp field {field}: {source}")]
    Timestamp {
        field: &'static str,
        #[source]
        source: TimestampError,
    },
    #[error("timestamp field {field} must be positive")]
    InvalidTimestamp { field: &'static str },
    #[error("series timestamp at index {index} decreased from {previous} to {current}")]
    TimestampOrder {
        index: usize,
        previous: i64,
        current: i64,
    },
    #[error("unsupported venue symbol {symbol:?}; expected a canonical USDT perpetual symbol")]
    UnsupportedSymbol { symbol: String },
    #[error("funding interval must be positive")]
    InvalidFundingInterval,
    #[error("funding rate floor exceeds cap")]
    InvalidRateBounds,
    #[error("production funding parsing requires both a rate floor and cap")]
    MissingRateBounds,
    #[error("funding schedule is empty, duplicated, or otherwise invalid")]
    InvalidFundingSchedule,
    #[error("no funding schedule rule covers settlement timestamp {settlement_ts_us}")]
    FundingScheduleUnknown { settlement_ts_us: i64 },
    #[error("timestamp {later_field} must be after {earlier_field}")]
    TimestampRelation {
        earlier_field: &'static str,
        later_field: &'static str,
    },
    #[error("funding rate {rate} is below floor {floor}")]
    RateBelowFloor { rate: i128, floor: i128 },
    #[error("funding rate {rate} is above cap {cap}")]
    RateAboveCap { rate: i128, cap: i128 },
    #[error("unsupported Binance funding rate type {rate_type:?}")]
    UnsupportedRateType { rate_type: String },
    #[error("Binance funding rate type is missing and legacy acceptance was not enabled")]
    MissingRateType,
    #[error("duplicate semantic series timestamp {timestamp_us}")]
    DuplicateTimestamp { timestamp_us: i64 },
    #[error("a stateful ticker snapshot is required before applying a delta")]
    SnapshotRequired,
    #[error("stateless ticker parsing accepts snapshots only")]
    StatefulParserRequired,
    #[error("ticker cross-sequence regressed from {previous} to {current}")]
    SequenceRegression { previous: u64, current: u64 },
    #[error("ticker timestamp regressed from {previous} to {current}")]
    TimestampRegression { previous: i64, current: i64 },
    #[error("ticker symbol {actual:?} does not match expected {expected:?}")]
    SymbolMismatch { expected: String, actual: String },
    #[error("ticker snapshot is missing required field {field}")]
    MissingSnapshotField { field: &'static str },
    #[error("invalid ratio: {message}")]
    InvalidRatio { message: &'static str },
}

pub fn parse_mark_funding(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_mark_funding_inner(payload, recv_us, FundingRules::default(), false)
}

pub fn parse_mark_funding_with_rules(
    payload: &mut [u8],
    recv_us: i64,
    rules: FundingRules,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    // Binance's fundingInfo endpoint only returns symbols whose venue-default
    // funding parameters have been adjusted. Ordinary symbols therefore have
    // a known interval but no explicit per-symbol bounds; validate a bound
    // whenever one is published without requiring both to exist.
    parse_mark_funding_inner(payload, recv_us, rules, false)
}

fn parse_mark_funding_inner(
    payload: &mut [u8],
    recv_us: i64,
    rules: FundingRules,
    require_bounds: bool,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_recv(recv_us)?;
    validate_rules(rules, require_bounds)?;
    let value: BinanceMarkFunding = decode(payload, AdapterId::BinanceUsdm)?;
    if value.event_type != "markPriceUpdate" {
        return Err(DerivativeParseError::Decode {
            venue: AdapterId::BinanceUsdm,
            message: format!("unexpected event type {:?}", value.event_type),
        });
    }
    let symbol = canonical_symbol(&value.symbol)?;
    let source_ts_us = timestamp(value.event_time, "event_time")?;
    let mark_price = positive_decimal("mark_price", &value.mark_price)?;
    let index_price = positive_decimal("index_price", &value.index_price)?;
    let rate = decimal("funding_rate", &value.funding_rate)?;
    validate_rate(rate, rules)?;
    let next_funding_ts_us = timestamp(value.next_funding_time, "next_funding_time")?;
    if next_funding_ts_us <= source_ts_us {
        return Err(DerivativeParseError::TimestampRelation {
            earlier_field: "event_time",
            later_field: "next_funding_time",
        });
    }
    let base_meta = meta(
        AdapterId::BinanceUsdm,
        symbol,
        value.symbol,
        source_ts_us,
        recv_us,
    );
    Ok(vec![
        DerivativeEvent::MarkIndex(MarkIndexSnapshot {
            meta: base_meta.clone(),
            mark_price,
            index_price,
        }),
        DerivativeEvent::FundingEstimate(FundingEstimate {
            meta: fresh_id(base_meta),
            rate,
            rate_kind: FundingRateKind::IndicativeNext,
            basis: FundingBasis::MarkNotional,
            interval_secs: rules.interval_secs,
            interval_provenance: rules.interval_provenance,
            next_funding_ts_us,
        }),
    ])
}

pub fn parse_funding_history(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_funding_history_inner(payload, recv_us, None, LegacyRateTypePolicy::RejectMissing)
}

pub fn parse_funding_history_with_rules(
    payload: &mut [u8],
    recv_us: i64,
    rules: FundingHistoryRules,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_funding_history_inner(
        payload,
        recv_us,
        Some(&rules.schedule),
        rules.legacy_rate_type,
    )
}

fn parse_funding_history_inner(
    payload: &mut [u8],
    recv_us: i64,
    schedule: Option<&FundingSchedule>,
    legacy_rate_type: LegacyRateTypePolicy,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_recv(recv_us)?;
    let rows: Vec<BinanceFundingHistory> = decode(payload, AdapterId::BinanceUsdm)?;
    let mut events = rows
        .into_iter()
        .map(|row| {
            let settlement_ts_us = timestamp(row.funding_time, "funding_time")?;
            validate_rate_type(row.rate_type.as_deref(), legacy_rate_type)?;
            let rules = schedule
                .map(|schedule| schedule.resolve(settlement_ts_us))
                .transpose()?
                .unwrap_or_default();
            let rate = decimal("funding_rate", &row.funding_rate)?;
            validate_rate(rate, rules)?;
            if let Some(mark_price) = row.mark_price.as_deref() {
                positive_decimal("mark_price", mark_price)?;
            }
            let symbol = canonical_symbol(&row.symbol)?;
            Ok(DerivativeEvent::FundingSettlement(FundingSettlement {
                meta: meta(
                    AdapterId::BinanceUsdm,
                    symbol,
                    row.symbol,
                    settlement_ts_us,
                    recv_us,
                ),
                rate,
                rate_kind: FundingRateKind::SettledActual,
                basis: FundingBasis::MarkNotional,
                interval_secs: rules.interval_secs,
                interval_provenance: rules.interval_provenance,
                settlement_ts_us,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sort_and_reject_duplicate_timestamps(&mut events)?;
    Ok(events)
}

pub fn parse_open_interest(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_recv(recv_us)?;
    let row: BinanceOpenInterest = decode(payload, AdapterId::BinanceUsdm)?;
    let source_ts_us = timestamp(row.time, "time")?;
    let symbol = canonical_symbol(&row.symbol)?;
    Ok(vec![DerivativeEvent::OpenInterest(OpenInterestSnapshot {
        meta: meta(
            AdapterId::BinanceUsdm,
            symbol,
            row.symbol,
            source_ts_us,
            recv_us,
        ),
        open_interest: positive_decimal("open_interest", &row.open_interest)?,
        unit: OpenInterestUnit::Contracts,
        quote_notional: None,
    })])
}

pub fn parse_top_account_ratio(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_top_ratio(payload, recv_us, TraderMetricKind::BinanceTopAccountRatio)
}

pub fn parse_top_position_ratio(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_top_ratio(payload, recv_us, TraderMetricKind::BinanceTopPositionRatio)
}

fn parse_top_ratio(
    payload: &mut [u8],
    recv_us: i64,
    metric_kind: TraderMetricKind,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_recv(recv_us)?;
    let rows: Vec<BinanceTopRatio> = decode(payload, AdapterId::BinanceUsdm)?;
    let mut events = rows
        .into_iter()
        .map(|row| {
            let source_ts_us = timestamp(row.timestamp, "timestamp")?;
            let long_ratio = nonnegative_decimal("long_ratio", &row.long_account)?;
            let short_ratio = positive_decimal("short_ratio", &row.short_account)?;
            let long_short_ratio = nonnegative_decimal("long_short_ratio", &row.long_short_ratio)?;
            let symbol = canonical_symbol(&row.symbol)?;
            Ok(DerivativeEvent::TraderRatio(TraderRatioSnapshot {
                meta: meta(
                    AdapterId::BinanceUsdm,
                    symbol,
                    row.symbol,
                    source_ts_us,
                    recv_us,
                ),
                metric_kind,
                long_ratio,
                short_ratio,
                long_short_ratio,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sort_and_reject_duplicate_timestamps(&mut events)?;
    Ok(events)
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(
    payload: &mut [u8],
    venue: AdapterId,
) -> Result<T, DerivativeParseError> {
    simd_json::serde::from_slice(payload).map_err(|error| DerivativeParseError::Decode {
        venue,
        message: error.to_string(),
    })
}

pub(crate) fn decimal(field: &'static str, value: &str) -> Result<i128, DerivativeParseError> {
    parse_decimal_18(value).map_err(|source| DerivativeParseError::Decimal { field, source })
}

pub(crate) fn positive_decimal(
    field: &'static str,
    value: &str,
) -> Result<i128, DerivativeParseError> {
    let value = decimal(field, value)?;
    if value <= 0 {
        Err(DerivativeParseError::NonPositive { field })
    } else {
        Ok(value)
    }
}

pub(crate) fn nonnegative_decimal(
    field: &'static str,
    value: &str,
) -> Result<i128, DerivativeParseError> {
    let value = decimal(field, value)?;
    if value < 0 {
        Err(DerivativeParseError::Negative { field })
    } else {
        Ok(value)
    }
}

pub(crate) fn timestamp(millis: i64, field: &'static str) -> Result<i64, DerivativeParseError> {
    if millis <= 0 {
        return Err(DerivativeParseError::InvalidTimestamp { field });
    }
    ms_to_us(millis).map_err(|source| DerivativeParseError::Timestamp { field, source })
}

pub(crate) fn timestamp_text(
    millis: &str,
    field: &'static str,
) -> Result<i64, DerivativeParseError> {
    let millis = millis
        .parse::<i64>()
        .map_err(|error| DerivativeParseError::Decode {
            venue: AdapterId::BybitLinear,
            message: format!("invalid {field}: {error}"),
        })?;
    timestamp(millis, field)
}

pub(crate) fn canonical_symbol(
    venue_symbol: &str,
) -> Result<CanonicalSymbol, DerivativeParseError> {
    let Some(base) = venue_symbol.strip_suffix(USDT) else {
        return Err(DerivativeParseError::UnsupportedSymbol {
            symbol: venue_symbol.to_owned(),
        });
    };
    if base.is_empty()
        || !base
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(DerivativeParseError::UnsupportedSymbol {
            symbol: venue_symbol.to_owned(),
        });
    }
    Ok(CanonicalSymbol::new(base, USDT))
}

pub(crate) fn meta(
    venue: AdapterId,
    symbol: CanonicalSymbol,
    venue_symbol: String,
    source_ts_us: i64,
    recv_us: i64,
) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol,
        venue_symbol,
        source_ts_us: Some(source_ts_us),
        source_ts_precision: TimestampPrecision::Millisecond,
        local_recv_ts_us: recv_us,
    }
}

pub(crate) fn validate_rules(
    rules: FundingRules,
    require_bounds: bool,
) -> Result<(), DerivativeParseError> {
    if rules.interval_secs == 0 {
        return Err(DerivativeParseError::InvalidFundingInterval);
    }
    if let (Some(floor), Some(cap)) = (rules.rate_floor, rules.rate_cap)
        && floor > cap
    {
        return Err(DerivativeParseError::InvalidRateBounds);
    }
    if require_bounds && (rules.rate_floor.is_none() || rules.rate_cap.is_none()) {
        return Err(DerivativeParseError::MissingRateBounds);
    }
    Ok(())
}

fn validate_rate_type(
    rate_type: Option<&str>,
    legacy: LegacyRateTypePolicy,
) -> Result<(), DerivativeParseError> {
    match rate_type {
        Some("Regular") => Ok(()),
        Some(other) => Err(DerivativeParseError::UnsupportedRateType {
            rate_type: other.to_owned(),
        }),
        None if legacy == LegacyRateTypePolicy::AcceptMissing => Ok(()),
        None => Err(DerivativeParseError::MissingRateType),
    }
}

pub(crate) fn sort_and_reject_duplicate_timestamps(
    events: &mut [DerivativeEvent],
) -> Result<(), DerivativeParseError> {
    events.sort_by(|left, right| {
        left.meta()
            .source_ts_us
            .cmp(&right.meta().source_ts_us)
            .then_with(|| left.meta().symbol.base.cmp(&right.meta().symbol.base))
            .then_with(|| left.meta().symbol.quote.cmp(&right.meta().symbol.quote))
            .then_with(|| left.meta().venue_symbol.cmp(&right.meta().venue_symbol))
    });
    for pair in events.windows(2) {
        if pair[0].meta().symbol == pair[1].meta().symbol
            && pair[0].meta().source_ts_us == pair[1].meta().source_ts_us
        {
            return Err(DerivativeParseError::DuplicateTimestamp {
                timestamp_us: pair[0].meta().source_ts_us.unwrap_or_default(),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_recv(recv_us: i64) -> Result<(), DerivativeParseError> {
    if recv_us <= 0 {
        Err(DerivativeParseError::InvalidTimestamp {
            field: "local_recv_ts_us",
        })
    } else {
        Ok(())
    }
}

pub(crate) fn validate_rate(rate: i128, rules: FundingRules) -> Result<(), DerivativeParseError> {
    if let Some(floor) = rules.rate_floor
        && rate < floor
    {
        return Err(DerivativeParseError::RateBelowFloor { rate, floor });
    }
    if let Some(cap) = rules.rate_cap
        && rate > cap
    {
        return Err(DerivativeParseError::RateAboveCap { rate, cap });
    }
    Ok(())
}

fn fresh_id(mut meta: DerivativeMeta) -> DerivativeMeta {
    meta.event_id = Uuid::now_v7();
    meta
}

#[derive(Debug, Deserialize)]
struct BinanceMarkFunding {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    mark_price: String,
    #[serde(rename = "i")]
    index_price: String,
    #[serde(rename = "r")]
    funding_rate: String,
    #[serde(rename = "T")]
    next_funding_time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFundingHistory {
    symbol: String,
    funding_rate: String,
    funding_time: i64,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    rate_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceOpenInterest {
    open_interest: String,
    symbol: String,
    time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceTopRatio {
    symbol: String,
    long_short_ratio: String,
    long_account: String,
    short_account: String,
    timestamp: i64,
}
