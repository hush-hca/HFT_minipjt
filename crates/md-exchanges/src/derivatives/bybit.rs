use funding_core::public::{
    DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
    FundingSettlement, MarkIndexSnapshot, OpenInterestSnapshot, OpenInterestUnit, TraderMetricKind,
    TraderRatioSnapshot,
};
use md_core::model::{AdapterId, CanonicalSymbol};
use serde::Deserialize;

pub use super::binance::DerivativeParseError;
use super::binance::{
    FundingRules, FundingSchedule, canonical_symbol, decimal, decode, meta, nonnegative_decimal,
    positive_decimal, sort_and_reject_duplicate_timestamps, timestamp, timestamp_text,
    validate_rate, validate_recv, validate_rules,
};

pub fn parse_ticker_funding(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    let mut copy = payload.to_vec();
    let envelope: BybitTickerResponse = decode(&mut copy, AdapterId::BybitLinear)?;
    if envelope.message_type != "snapshot" {
        return Err(DerivativeParseError::StatefulParserRequired);
    }
    let symbol = canonical_symbol(&envelope.data.symbol)?;
    BybitTickerParser::new(symbol).parse(payload, recv_us)
}

pub fn parse_ticker_funding_with_bounds(
    payload: &mut [u8],
    recv_us: i64,
    rate_floor: Option<i128>,
    rate_cap: Option<i128>,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    let events = parse_ticker_funding(payload, recv_us)?;
    let rules = FundingRules {
        interval_secs: funding(&events)?.interval_secs,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        rate_floor,
        rate_cap,
    };
    validate_rules(rules, true)?;
    validate_rate(funding(&events)?.rate, rules)?;
    Ok(events)
}

#[derive(Debug, Clone)]
pub struct BybitTickerParser {
    symbol: CanonicalSymbol,
    state: Option<TickerState>,
}

#[derive(Debug, Clone)]
struct TickerState {
    mark_price: i128,
    index_price: i128,
    funding_rate: i128,
    next_funding_ts_us: i64,
    interval_secs: u32,
    rate_floor: i128,
    rate_cap: i128,
    last_cs: u64,
    last_source_ts_us: i64,
}

impl BybitTickerParser {
    pub fn new(symbol: CanonicalSymbol) -> Self {
        Self {
            symbol,
            state: None,
        }
    }

    pub fn reset(&mut self) {
        self.state = None;
    }

    pub fn parse(
        &mut self,
        payload: &mut [u8],
        recv_us: i64,
    ) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
        validate_recv(recv_us)?;
        let response: BybitTickerResponse = decode(payload, AdapterId::BybitLinear)?;
        let expected = format!("{}{}", self.symbol.base, self.symbol.quote);
        if response.data.symbol != expected || response.topic != format!("tickers.{expected}") {
            return Err(DerivativeParseError::SymbolMismatch {
                expected,
                actual: response.data.symbol,
            });
        }
        let source_ts_us = timestamp(response.ts, "ts")?;
        if let Some(previous) = self.state.as_ref() {
            if response.cs <= previous.last_cs {
                return Err(DerivativeParseError::SequenceRegression {
                    previous: previous.last_cs,
                    current: response.cs,
                });
            }
            if source_ts_us < previous.last_source_ts_us {
                return Err(DerivativeParseError::TimestampRegression {
                    previous: previous.last_source_ts_us,
                    current: source_ts_us,
                });
            }
        }

        let next = match response.message_type.as_str() {
            "snapshot" => snapshot_state(&response, source_ts_us)?,
            "delta" => patch_state(
                self.state
                    .as_ref()
                    .ok_or(DerivativeParseError::SnapshotRequired)?,
                &response,
                source_ts_us,
            )?,
            other => {
                return Err(DerivativeParseError::Decode {
                    venue: AdapterId::BybitLinear,
                    message: format!("unexpected ticker message type {other:?}"),
                });
            }
        };
        let events = ticker_events(&self.symbol, &response.data.symbol, &next, recv_us);
        self.state = Some(next);
        Ok(events)
    }
}

fn snapshot_state(
    response: &BybitTickerResponse,
    source_ts_us: i64,
) -> Result<TickerState, DerivativeParseError> {
    let interval_hours = required(
        &response.data.funding_interval_hour,
        "funding_interval_hour",
    )?
    .parse::<u32>()
    .map_err(|error| DerivativeParseError::Decode {
        venue: AdapterId::BybitLinear,
        message: format!("invalid fundingIntervalHour: {error}"),
    })?;
    let interval_secs = interval_hours
        .checked_mul(3_600)
        .filter(|value| *value > 0)
        .ok_or(DerivativeParseError::InvalidFundingInterval)?;
    let cap = positive_decimal(
        "funding_cap",
        required(&response.data.funding_cap, "funding_cap")?,
    )?;
    let state = TickerState {
        mark_price: positive_decimal(
            "mark_price",
            required(&response.data.mark_price, "mark_price")?,
        )?,
        index_price: positive_decimal(
            "index_price",
            required(&response.data.index_price, "index_price")?,
        )?,
        funding_rate: decimal(
            "funding_rate",
            required(&response.data.funding_rate, "funding_rate")?,
        )?,
        next_funding_ts_us: timestamp_text(
            required(&response.data.next_funding_time, "next_funding_time")?,
            "next_funding_time",
        )?,
        interval_secs,
        rate_floor: -cap,
        rate_cap: cap,
        last_cs: response.cs,
        last_source_ts_us: source_ts_us,
    };
    validate_ticker_state(&state)?;
    Ok(state)
}

fn patch_state(
    previous: &TickerState,
    response: &BybitTickerResponse,
    source_ts_us: i64,
) -> Result<TickerState, DerivativeParseError> {
    let mut next = previous.clone();
    if let Some(value) = response.data.mark_price.as_deref() {
        next.mark_price = positive_decimal("mark_price", value)?;
    }
    if let Some(value) = response.data.index_price.as_deref() {
        next.index_price = positive_decimal("index_price", value)?;
    }
    if let Some(value) = response.data.funding_rate.as_deref() {
        next.funding_rate = decimal("funding_rate", value)?;
    }
    if let Some(value) = response.data.next_funding_time.as_deref() {
        next.next_funding_ts_us = timestamp_text(value, "next_funding_time")?;
    }
    if let Some(value) = response.data.funding_interval_hour.as_deref() {
        next.interval_secs = value
            .parse::<u32>()
            .ok()
            .and_then(|hours| hours.checked_mul(3_600))
            .filter(|seconds| *seconds > 0)
            .ok_or(DerivativeParseError::InvalidFundingInterval)?;
    }
    if let Some(value) = response.data.funding_cap.as_deref() {
        let cap = positive_decimal("funding_cap", value)?;
        next.rate_floor = -cap;
        next.rate_cap = cap;
    }
    next.last_cs = response.cs;
    next.last_source_ts_us = source_ts_us;
    validate_ticker_state(&next)?;
    Ok(next)
}

fn validate_ticker_state(state: &TickerState) -> Result<(), DerivativeParseError> {
    let rules = FundingRules {
        interval_secs: state.interval_secs,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        rate_floor: Some(state.rate_floor),
        rate_cap: Some(state.rate_cap),
    };
    validate_rules(rules, true)?;
    validate_rate(state.funding_rate, rules)?;
    if state.next_funding_ts_us <= state.last_source_ts_us {
        return Err(DerivativeParseError::TimestampRelation {
            earlier_field: "ts",
            later_field: "next_funding_time",
        });
    }
    Ok(())
}

fn ticker_events(
    symbol: &CanonicalSymbol,
    venue_symbol: &str,
    state: &TickerState,
    recv_us: i64,
) -> Vec<DerivativeEvent> {
    vec![
        DerivativeEvent::MarkIndex(MarkIndexSnapshot {
            meta: meta(
                AdapterId::BybitLinear,
                symbol.clone(),
                venue_symbol.to_owned(),
                state.last_source_ts_us,
                recv_us,
            ),
            mark_price: state.mark_price,
            index_price: state.index_price,
        }),
        DerivativeEvent::FundingEstimate(FundingEstimate {
            meta: meta(
                AdapterId::BybitLinear,
                symbol.clone(),
                venue_symbol.to_owned(),
                state.last_source_ts_us,
                recv_us,
            ),
            rate: state.funding_rate,
            rate_kind: FundingRateKind::IndicativeNext,
            basis: FundingBasis::MarkNotional,
            interval_secs: state.interval_secs,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            next_funding_ts_us: state.next_funding_ts_us,
        }),
    ]
}

fn required<'a>(
    value: &'a Option<String>,
    field: &'static str,
) -> Result<&'a str, DerivativeParseError> {
    value
        .as_deref()
        .ok_or(DerivativeParseError::MissingSnapshotField { field })
}

fn funding(events: &[DerivativeEvent]) -> Result<&FundingEstimate, DerivativeParseError> {
    match events.get(1) {
        Some(DerivativeEvent::FundingEstimate(value)) => Ok(value),
        _ => Err(DerivativeParseError::Decode {
            venue: AdapterId::BybitLinear,
            message: "ticker parser did not emit funding estimate".to_owned(),
        }),
    }
}

pub fn parse_funding_history(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_funding_history_inner(payload, recv_us, None)
}

pub fn parse_funding_history_with_rules(
    payload: &mut [u8],
    recv_us: i64,
    rules: FundingRules,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_rules(rules, true)?;
    let schedule = FundingSchedule::new(vec![super::binance::EffectiveFundingRule {
        effective_from_ts_us: 0,
        rules,
    }])?;
    parse_funding_history_inner(payload, recv_us, Some(&schedule))
}

pub fn parse_funding_history_with_schedule(
    payload: &mut [u8],
    recv_us: i64,
    schedule: FundingSchedule,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    parse_funding_history_inner(payload, recv_us, Some(&schedule))
}

fn parse_funding_history_inner(
    payload: &mut [u8],
    recv_us: i64,
    schedule: Option<&FundingSchedule>,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_recv(recv_us)?;
    let response: BybitFundingHistoryResponse = decode(payload, AdapterId::BybitLinear)?;
    validate_response(response.ret_code, &response.ret_msg)?;
    if response.result.category != "linear" {
        return Err(DerivativeParseError::Decode {
            venue: AdapterId::BybitLinear,
            message: format!("unexpected category {:?}", response.result.category),
        });
    }
    let mut events = response
        .result
        .list
        .into_iter()
        .map(|row| {
            let settlement_ts_us =
                timestamp_text(&row.funding_rate_timestamp, "funding_rate_timestamp")?;
            let rules = schedule
                .map(|schedule| schedule.resolve(settlement_ts_us))
                .transpose()?
                .unwrap_or_default();
            let rate = decimal("funding_rate", &row.funding_rate)?;
            validate_rate(rate, rules)?;
            let symbol = canonical_symbol(&row.symbol)?;
            Ok(DerivativeEvent::FundingSettlement(FundingSettlement {
                meta: meta(
                    AdapterId::BybitLinear,
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
    let response: BybitOpenInterestResponse = decode(payload, AdapterId::BybitLinear)?;
    validate_response(response.ret_code, &response.ret_msg)?;
    if response.result.category != "linear" {
        return Err(DerivativeParseError::Decode {
            venue: AdapterId::BybitLinear,
            message: format!("unexpected category {:?}", response.result.category),
        });
    }
    let symbol = canonical_symbol(&response.result.symbol)?;
    let venue_symbol = response.result.symbol;
    let mut events = response
        .result
        .list
        .into_iter()
        .map(|row| {
            let source_ts_us = timestamp_text(&row.timestamp, "timestamp")?;
            Ok(DerivativeEvent::OpenInterest(OpenInterestSnapshot {
                meta: meta(
                    AdapterId::BybitLinear,
                    symbol.clone(),
                    venue_symbol.clone(),
                    source_ts_us,
                    recv_us,
                ),
                open_interest: positive_decimal("open_interest", &row.open_interest)?,
                unit: OpenInterestUnit::BaseAsset,
                quote_notional: row
                    .open_interest_value
                    .as_deref()
                    .map(|value| positive_decimal("open_interest_value", value))
                    .transpose()?,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sort_and_reject_duplicate_timestamps(&mut events)?;
    Ok(events)
}

pub fn parse_long_short_ratio(
    payload: &mut [u8],
    recv_us: i64,
) -> Result<Vec<DerivativeEvent>, DerivativeParseError> {
    validate_recv(recv_us)?;
    let response: BybitLongShortResponse = decode(payload, AdapterId::BybitLinear)?;
    validate_response(response.ret_code, &response.ret_msg)?;
    let mut events = response
        .result
        .list
        .into_iter()
        .map(|row| {
            let source_ts_us = timestamp_text(&row.timestamp, "timestamp")?;
            let long_ratio = nonnegative_decimal("long_ratio", &row.buy_ratio)?;
            let short_ratio = positive_decimal("short_ratio", &row.sell_ratio)?;
            let long_short_ratio = scaled_ratio(long_ratio, short_ratio)?;
            let symbol = canonical_symbol(&row.symbol)?;
            Ok(DerivativeEvent::TraderRatio(TraderRatioSnapshot {
                meta: meta(
                    AdapterId::BybitLinear,
                    symbol,
                    row.symbol,
                    source_ts_us,
                    recv_us,
                ),
                metric_kind: TraderMetricKind::BybitLongShortRatio,
                long_ratio,
                short_ratio,
                long_short_ratio,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sort_and_reject_duplicate_timestamps(&mut events)?;
    Ok(events)
}

fn scaled_ratio(numerator: i128, denominator: i128) -> Result<i128, DerivativeParseError> {
    numerator
        .checked_mul(1_000_000_000_000_000_000)
        .and_then(|value| value.checked_div(denominator))
        .ok_or(DerivativeParseError::InvalidRatio {
            message: "long/short ratio overflows or has a zero denominator",
        })
}

fn validate_response(ret_code: i64, ret_msg: &str) -> Result<(), DerivativeParseError> {
    if ret_code == 0 {
        Ok(())
    } else {
        Err(DerivativeParseError::Decode {
            venue: AdapterId::BybitLinear,
            message: format!("retCode {ret_code}: {ret_msg}"),
        })
    }
}

#[derive(Debug, Deserialize)]
struct BybitTickerResponse {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    ts: i64,
    cs: u64,
    data: BybitTicker,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitTicker {
    symbol: String,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    next_funding_time: Option<String>,
    #[serde(default)]
    funding_interval_hour: Option<String>,
    #[serde(default)]
    funding_cap: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitFundingHistoryResponse {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    result: BybitFundingHistoryResult,
}

#[derive(Debug, Deserialize)]
struct BybitFundingHistoryResult {
    category: String,
    list: Vec<BybitFundingHistoryRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitFundingHistoryRow {
    symbol: String,
    funding_rate: String,
    funding_rate_timestamp: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitOpenInterestResponse {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    result: BybitOpenInterestResult,
}

#[derive(Debug, Deserialize)]
struct BybitOpenInterestResult {
    symbol: String,
    category: String,
    list: Vec<BybitOpenInterestRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitOpenInterestRow {
    open_interest: String,
    #[serde(default)]
    open_interest_value: Option<String>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLongShortResponse {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    result: BybitLongShortResult,
}

#[derive(Debug, Deserialize)]
struct BybitLongShortResult {
    list: Vec<BybitLongShortRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLongShortRow {
    symbol: String,
    buy_ratio: String,
    sell_ratio: String,
    timestamp: String,
}
