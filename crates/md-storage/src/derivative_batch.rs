use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array,
    UInt32Array,
};
use funding_core::instrument::{AccountMode, PositionMode};
use funding_core::public::{
    DerivativeEvent, FundingBasis, FundingIntervalProvenance, FundingRateKind, OpenInterestUnit,
    QuoteSide, TraderMetricKind,
};
use md_core::model::{DECIMAL_PRECISION, DECIMAL_SCALE, TimestampPrecision};

use crate::derivative_schema::{DerivativeEventFamily, DerivativeSchemaContext, derivative_schema};
use crate::{SCHEMA_VERSION, StorageError};

const MAX_DECIMAL_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

#[derive(Debug)]
pub struct DerivativeBatchBuilder {
    context: DerivativeSchemaContext,
    events: Vec<DerivativeEvent>,
}

impl DerivativeBatchBuilder {
    pub fn new(context: DerivativeSchemaContext) -> Self {
        Self {
            context,
            events: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn push(&mut self, event: DerivativeEvent) -> Result<(), StorageError> {
        validate_event(&self.context, &event)?;
        self.events.push(event);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch, StorageError> {
        let batch = self.build()?;
        self.commit();
        Ok(batch)
    }

    pub(crate) fn build(&self) -> Result<RecordBatch, StorageError> {
        let mut arrays = common_arrays(&self.context, &self.events)?;
        arrays.extend(specific_arrays(self.context.family, &self.events)?);
        Ok(RecordBatch::try_new(
            derivative_schema(&self.context),
            arrays,
        )?)
    }

    pub(crate) fn commit(&mut self) {
        self.events.clear();
    }
}

fn common_arrays(
    context: &DerivativeSchemaContext,
    events: &[DerivativeEvent],
) -> Result<Vec<ArrayRef>, StorageError> {
    let metas = events.iter().map(DerivativeEvent::meta).collect::<Vec<_>>();
    let mut ids = FixedSizeBinaryBuilder::with_capacity(events.len(), 16);
    for meta in &metas {
        ids.append_value(meta.event_id.as_bytes())?;
    }
    Ok(vec![
        Arc::new(UInt16Array::from(
            metas
                .iter()
                .map(|meta| meta.schema_version)
                .collect::<Vec<_>>(),
        )),
        Arc::new(ids.finish()),
        Arc::new(StringArray::from(vec![context.venue_name(); events.len()])),
        Arc::new(StringArray::from(vec![context.market_name(); events.len()])),
        Arc::new(StringArray::from(
            metas
                .iter()
                .map(|meta| meta.symbol.base.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            metas
                .iter()
                .map(|meta| meta.symbol.quote.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            metas
                .iter()
                .map(|meta| meta.venue_symbol.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metas
                .iter()
                .map(|meta| meta.source_ts_us)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metas
                .iter()
                .map(|meta| meta.local_recv_ts_us)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt8Array::from(
            metas
                .iter()
                .map(|meta| precision(meta.source_ts_precision))
                .collect::<Vec<_>>(),
        )),
    ])
}

fn specific_arrays(
    family: DerivativeEventFamily,
    events: &[DerivativeEvent],
) -> Result<Vec<ArrayRef>, StorageError> {
    macro_rules! collect_variant {
        ($variant:ident) => {{
            events
                .iter()
                .map(|event| match event {
                    DerivativeEvent::$variant(value) => value,
                    _ => unreachable!("validated family"),
                })
                .collect::<Vec<_>>()
        }};
    }
    Ok(match family {
        DerivativeEventFamily::Instrument => {
            let values = collect_variant!(Instrument);
            vec![
                strings(values.iter().map(|_| "perpetual")),
                strings(values.iter().map(|v| v.settlement_asset.as_str())),
                decimals(values.iter().map(|v| v.contract_multiplier))?,
                decimals(values.iter().map(|v| v.tick_size))?,
                decimals(values.iter().map(|v| v.quantity_step))?,
                decimals(values.iter().map(|v| v.min_quantity))?,
                optional_decimals(values.iter().map(|v| v.max_quantity))?,
                decimals(values.iter().map(|v| v.min_notional))?,
                Arc::new(UInt32Array::from(
                    values
                        .iter()
                        .map(|v| v.funding_interval_secs)
                        .collect::<Vec<_>>(),
                )),
                strings(values.iter().map(|v| interval_provenance(v.funding_interval_provenance))),
                optional_decimals(values.iter().map(|v| v.funding_rate_floor))?,
                optional_decimals(values.iter().map(|v| v.funding_rate_cap))?,
                strings(values.iter().map(|v| match v.funding_rate_bounds_provenance {
                    funding_core::instrument::FundingRateBoundsProvenance::VenueFundingInfo => "venue_funding_info",
                    funding_core::instrument::FundingRateBoundsProvenance::Unknown => "unknown",
                })),
                optional_decimals(values.iter().map(|v| v.price_lower_bound))?,
                optional_decimals(values.iter().map(|v| v.price_upper_bound))?,
                strings_owned(values.iter().map(|v| join_position_modes(&v.supported_position_modes))),
                strings_owned(values.iter().map(|v| join_account_modes(&v.supported_account_modes))),
            ]
        }
        DerivativeEventFamily::MarkIndex => {
            let values = collect_variant!(MarkIndex);
            vec![
                decimals(values.iter().map(|v| v.mark_price))?,
                decimals(values.iter().map(|v| v.index_price))?,
            ]
        }
        DerivativeEventFamily::FundingEstimate => {
            let values = collect_variant!(FundingEstimate);
            vec![
                decimals(values.iter().map(|v| v.rate))?,
                strings(values.iter().map(|v| funding_rate_kind(v.rate_kind))),
                strings(values.iter().map(|v| funding_basis(v.basis))),
                Arc::new(UInt32Array::from(
                    values.iter().map(|v| v.interval_secs).collect::<Vec<_>>(),
                )),
                strings(
                    values
                        .iter()
                        .map(|v| interval_provenance(v.interval_provenance)),
                ),
                Arc::new(Int64Array::from(
                    values
                        .iter()
                        .map(|v| v.next_funding_ts_us)
                        .collect::<Vec<_>>(),
                )),
            ]
        }
        DerivativeEventFamily::FundingSettlement => {
            let values = collect_variant!(FundingSettlement);
            vec![
                decimals(values.iter().map(|v| v.rate))?,
                strings(values.iter().map(|v| funding_rate_kind(v.rate_kind))),
                strings(values.iter().map(|v| funding_basis(v.basis))),
                Arc::new(UInt32Array::from(
                    values.iter().map(|v| v.interval_secs).collect::<Vec<_>>(),
                )),
                strings(
                    values
                        .iter()
                        .map(|v| interval_provenance(v.interval_provenance)),
                ),
                Arc::new(Int64Array::from(
                    values
                        .iter()
                        .map(|v| v.settlement_ts_us)
                        .collect::<Vec<_>>(),
                )),
            ]
        }
        DerivativeEventFamily::OpenInterest => {
            let values = collect_variant!(OpenInterest);
            vec![
                decimals(values.iter().map(|v| v.open_interest))?,
                strings(values.iter().map(|v| match v.unit {
                    OpenInterestUnit::Contracts => "contracts",
                    OpenInterestUnit::BaseAsset => "base_asset",
                })),
                optional_decimals(values.iter().map(|v| v.quote_notional))?,
            ]
        }
        DerivativeEventFamily::TraderRatio => {
            let values = collect_variant!(TraderRatio);
            vec![
                strings(values.iter().map(|v| metric_kind(v.metric_kind))),
                decimals(values.iter().map(|v| v.long_ratio))?,
                decimals(values.iter().map(|v| v.short_ratio))?,
                decimals(values.iter().map(|v| v.long_short_ratio))?,
            ]
        }
        DerivativeEventFamily::QuoteConversion => {
            let values = collect_variant!(QuoteConversion);
            vec![
                strings(values.iter().map(|v| match v.side {
                    QuoteSide::Bid => "bid",
                    QuoteSide::Ask => "ask",
                })),
                decimals(values.iter().map(|v| v.price))?,
                decimals(values.iter().map(|v| v.executable_quantity))?,
            ]
        }
    })
}

fn validate_event(
    context: &DerivativeSchemaContext,
    event: &DerivativeEvent,
) -> Result<(), StorageError> {
    let meta = event.meta();
    let family = family_of(event);
    if family != context.family {
        return invalid("event_family", "event does not match builder family");
    }
    if meta.schema_version != SCHEMA_VERSION {
        return Err(StorageError::SchemaVersion {
            expected: SCHEMA_VERSION,
            actual: meta.schema_version,
        });
    }
    if meta.venue != context.venue {
        return Err(StorageError::AdapterMismatch);
    }
    if meta.symbol != context.symbol {
        return Err(StorageError::SymbolMismatch);
    }
    if family != DerivativeEventFamily::QuoteConversion
        && !matches!(
            meta.venue,
            md_core::model::AdapterId::BinanceUsdm | md_core::model::AdapterId::BybitLinear
        )
    {
        return invalid("venue", "derivative event requires a derivatives venue");
    }
    if meta.local_recv_ts_us <= 0 {
        return invalid("local_recv_ts_us", "must be positive");
    }
    if meta.venue_symbol.is_empty() {
        return invalid("source_symbol", "must not be empty");
    }
    match (meta.source_ts_us, meta.source_ts_precision) {
        (None, TimestampPrecision::Unavailable)
        | (Some(_), TimestampPrecision::Millisecond | TimestampPrecision::Microsecond) => {}
        _ => {
            return invalid(
                "source_precision",
                "must agree with source timestamp presence",
            );
        }
    }
    if meta.source_ts_us.is_some_and(|ts| ts <= 0) {
        return invalid("exchange_event_ts_us", "must be positive when present");
    }
    match event {
        DerivativeEvent::Instrument(v) => {
            if !valid_asset(&v.settlement_asset) {
                return invalid("settlement_asset", "must be an uppercase asset code");
            }
            for (name, value) in [
                ("contract_multiplier", v.contract_multiplier),
                ("tick_size", v.tick_size),
                ("quantity_step", v.quantity_step),
                ("min_quantity", v.min_quantity),
                ("min_notional", v.min_notional),
            ] {
                positive_decimal(name, value)?;
            }
            optional_positive("max_quantity", v.max_quantity)?;
            optional_positive("price_lower_bound", v.price_lower_bound)?;
            optional_positive("price_upper_bound", v.price_upper_bound)?;
            decimal_opt("funding_rate_floor", v.funding_rate_floor)?;
            decimal_opt("funding_rate_cap", v.funding_rate_cap)?;
            if v.funding_interval_secs == 0 {
                return invalid("funding_interval_secs", "must be positive");
            }
            if v.max_quantity.is_some_and(|max| max < v.min_quantity) {
                return invalid("max_quantity", "must not be below min_quantity");
            }
            if v.price_lower_bound
                .zip(v.price_upper_bound)
                .is_some_and(|(low, high)| low >= high)
            {
                return invalid("price_bounds", "lower bound must be below upper bound");
            }
            if v.funding_rate_floor
                .zip(v.funding_rate_cap)
                .is_some_and(|(low, high)| low > high)
            {
                return invalid("funding_rate_bounds", "floor must not exceed cap");
            }
            match v.funding_rate_bounds_provenance {
                funding_core::instrument::FundingRateBoundsProvenance::VenueFundingInfo
                    if v.funding_rate_floor.is_none() || v.funding_rate_cap.is_none() =>
                {
                    return invalid(
                        "funding_rate_bounds_provenance",
                        "venue provenance requires both floor and cap",
                    );
                }
                funding_core::instrument::FundingRateBoundsProvenance::Unknown
                    if v.funding_rate_floor.is_some() || v.funding_rate_cap.is_some() =>
                {
                    return invalid(
                        "funding_rate_bounds_provenance",
                        "unknown provenance cannot carry venue bounds",
                    );
                }
                _ => {}
            }
            if v.supported_position_modes.is_empty()
                || v.supported_account_modes.is_empty()
                || has_duplicates(&v.supported_position_modes)
                || has_duplicates(&v.supported_account_modes)
            {
                return invalid("supported_modes", "must be nonempty and unique");
            }
        }
        DerivativeEvent::MarkIndex(v) => {
            positive_decimal("mark_price", v.mark_price)?;
            positive_decimal("index_price", v.index_price)?;
        }
        DerivativeEvent::FundingEstimate(v) => {
            decimal("rate", v.rate)?;
            if v.rate_kind != FundingRateKind::IndicativeNext {
                return invalid("rate_kind", "funding estimate must be indicative_next");
            }
            validate_funding(v.interval_secs, v.next_funding_ts_us)?;
        }
        DerivativeEvent::FundingSettlement(v) => {
            decimal("rate", v.rate)?;
            if v.rate_kind != FundingRateKind::SettledActual {
                return invalid("rate_kind", "funding settlement must be settled_actual");
            }
            validate_funding(v.interval_secs, v.settlement_ts_us)?;
        }
        DerivativeEvent::OpenInterest(v) => {
            positive_decimal("open_interest", v.open_interest)?;
            optional_positive("quote_notional", v.quote_notional)?;
        }
        DerivativeEvent::TraderRatio(v) => {
            nonnegative_decimal("long_ratio", v.long_ratio)?;
            positive_decimal("short_ratio", v.short_ratio)?;
            nonnegative_decimal("long_short_ratio", v.long_short_ratio)?;
        }
        DerivativeEvent::QuoteConversion(v) => {
            positive_decimal("price", v.price)?;
            positive_decimal("executable_quantity", v.executable_quantity)?;
        }
    }
    Ok(())
}

pub(crate) fn family_of(event: &DerivativeEvent) -> DerivativeEventFamily {
    match event {
        DerivativeEvent::Instrument(_) => DerivativeEventFamily::Instrument,
        DerivativeEvent::MarkIndex(_) => DerivativeEventFamily::MarkIndex,
        DerivativeEvent::FundingEstimate(_) => DerivativeEventFamily::FundingEstimate,
        DerivativeEvent::FundingSettlement(_) => DerivativeEventFamily::FundingSettlement,
        DerivativeEvent::OpenInterest(_) => DerivativeEventFamily::OpenInterest,
        DerivativeEvent::TraderRatio(_) => DerivativeEventFamily::TraderRatio,
        DerivativeEvent::QuoteConversion(_) => DerivativeEventFamily::QuoteConversion,
    }
}

fn validate_funding(interval: u32, timestamp: i64) -> Result<(), StorageError> {
    if interval == 0 {
        return invalid("interval_secs", "must be positive");
    }
    if timestamp <= 0 {
        return invalid("funding_timestamp", "must be positive");
    }
    Ok(())
}

fn invalid(field: &'static str, message: &str) -> Result<(), StorageError> {
    Err(StorageError::InvalidDerivative {
        field,
        message: message.into(),
    })
}

fn decimal(field: &'static str, value: i128) -> Result<(), StorageError> {
    if value.unsigned_abs() > MAX_DECIMAL_38 as u128 {
        return Err(StorageError::DecimalOutOfRange { field, value });
    }
    Ok(())
}

fn decimal_opt(field: &'static str, value: Option<i128>) -> Result<(), StorageError> {
    value.map_or(Ok(()), |value| decimal(field, value))
}

fn positive_decimal(field: &'static str, value: i128) -> Result<(), StorageError> {
    decimal(field, value)?;
    if value <= 0 {
        return invalid(field, "must be positive");
    }
    Ok(())
}

fn nonnegative_decimal(field: &'static str, value: i128) -> Result<(), StorageError> {
    decimal(field, value)?;
    if value < 0 {
        return invalid(field, "must be nonnegative");
    }
    Ok(())
}

fn optional_positive(field: &'static str, value: Option<i128>) -> Result<(), StorageError> {
    value.map_or(Ok(()), |value| positive_decimal(field, value))
}

fn precision(value: TimestampPrecision) -> u8 {
    match value {
        TimestampPrecision::Unavailable => 0,
        TimestampPrecision::Millisecond => 1,
        TimestampPrecision::Microsecond => 2,
    }
}

fn strings<'a>(values: impl IntoIterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from(values.into_iter().collect::<Vec<_>>()))
}

fn strings_owned(values: impl IntoIterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from(values.into_iter().collect::<Vec<_>>()))
}

fn decimals(values: impl IntoIterator<Item = i128>) -> Result<ArrayRef, arrow_schema::ArrowError> {
    Ok(Arc::new(
        Decimal128Array::from(values.into_iter().collect::<Vec<_>>())
            .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
    ))
}

fn optional_decimals(
    values: impl IntoIterator<Item = Option<i128>>,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    Ok(Arc::new(
        Decimal128Array::from(values.into_iter().collect::<Vec<_>>())
            .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
    ))
}

fn interval_provenance(value: FundingIntervalProvenance) -> &'static str {
    match value {
        FundingIntervalProvenance::VenuePayload => "venue_payload",
        FundingIntervalProvenance::InstrumentRule => "instrument_rule",
        FundingIntervalProvenance::AssumedVenueDefault => "assumed_venue_default",
    }
}

fn funding_rate_kind(value: FundingRateKind) -> &'static str {
    match value {
        FundingRateKind::IndicativeNext => "indicative_next",
        FundingRateKind::SettledActual => "settled_actual",
    }
}

fn funding_basis(value: FundingBasis) -> &'static str {
    match value {
        FundingBasis::MarkNotional => "mark_notional",
    }
}

fn metric_kind(value: TraderMetricKind) -> &'static str {
    match value {
        TraderMetricKind::BinanceTopAccountRatio => "binance_top_account_ratio",
        TraderMetricKind::BinanceTopPositionRatio => "binance_top_position_ratio",
        TraderMetricKind::BybitLongShortRatio => "bybit_long_short_ratio",
    }
}

fn join_position_modes(values: &[PositionMode]) -> String {
    values
        .iter()
        .map(|value| match value {
            PositionMode::OneWay => "one_way",
            PositionMode::Hedge => "hedge",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn join_account_modes(values: &[AccountMode]) -> String {
    values
        .iter()
        .map(|value| match value {
            AccountMode::Classic => "classic",
            AccountMode::Unified => "unified",
            AccountMode::Portfolio => "portfolio",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn valid_asset(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn has_duplicates<T: Eq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
}
