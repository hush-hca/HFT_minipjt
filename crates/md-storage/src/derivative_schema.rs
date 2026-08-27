use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use md_core::model::{AdapterId, CanonicalSymbol, DECIMAL_PRECISION, DECIMAL_SCALE};

use crate::{PROJECT_NAME, SCHEMA_VERSION};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum DerivativeEventFamily {
    Instrument,
    MarkIndex,
    FundingEstimate,
    FundingSettlement,
    OpenInterest,
    TraderRatio,
    QuoteConversion,
}

impl DerivativeEventFamily {
    pub const ALL: [Self; 7] = [
        Self::Instrument,
        Self::MarkIndex,
        Self::FundingEstimate,
        Self::FundingSettlement,
        Self::OpenInterest,
        Self::TraderRatio,
        Self::QuoteConversion,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Instrument => "instrument",
            Self::MarkIndex => "mark_index",
            Self::FundingEstimate => "funding_estimate",
            Self::FundingSettlement => "funding_settlement",
            Self::OpenInterest => "open_interest",
            Self::TraderRatio => "trader_ratio",
            Self::QuoteConversion => "quote_conversion",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|family| family.as_str() == value)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DerivativeSchemaContext {
    pub family: DerivativeEventFamily,
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub utc_hour: DateTime<Utc>,
}

impl DerivativeSchemaContext {
    pub fn venue_name(&self) -> &'static str {
        derivative_venue_path(self.venue).0
    }

    pub fn market_name(&self) -> &'static str {
        derivative_venue_path(self.venue).1
    }

    pub fn symbol_text(&self) -> String {
        format!("{}/{}", self.symbol.base, self.symbol.quote)
    }

    fn metadata(&self) -> HashMap<String, String> {
        let utc_hour = self
            .utc_hour
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .expect("valid UTC hour")
            .to_rfc3339_opts(SecondsFormat::Secs, true);
        HashMap::from([
            ("project".into(), PROJECT_NAME.into()),
            ("schema_version".into(), SCHEMA_VERSION.to_string()),
            ("timestamp_unit".into(), "microsecond".into()),
            ("decimal_scale".into(), DECIMAL_SCALE.to_string()),
            ("event_family".into(), self.family.as_str().into()),
            ("venue".into(), self.venue_name().into()),
            ("market".into(), self.market_name().into()),
            ("symbol".into(), self.symbol_text()),
            ("utc_hour".into(), utc_hour),
        ])
    }
}

pub fn derivative_schema(context: &DerivativeSchemaContext) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        derivative_fields(context.family),
        context.metadata(),
    ))
}

pub fn derivative_fields(family: DerivativeEventFamily) -> Vec<Field> {
    let mut fields = vec![
        Field::new("schema_version", DataType::UInt16, false),
        Field::new("event_id", DataType::FixedSizeBinary(16), false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("base", DataType::Utf8, false),
        Field::new("quote", DataType::Utf8, false),
        Field::new("source_symbol", DataType::Utf8, false),
        Field::new("exchange_event_ts_us", DataType::Int64, true),
        Field::new("local_recv_ts_us", DataType::Int64, false),
        Field::new("source_precision", DataType::UInt8, false),
    ];
    match family {
        DerivativeEventFamily::Instrument => fields.extend([
            text("contract_kind", false),
            text("settlement_asset", false),
            decimal("contract_multiplier", false),
            decimal("tick_size", false),
            decimal("quantity_step", false),
            decimal("min_quantity", false),
            decimal("max_quantity", true),
            decimal("min_notional", false),
            Field::new("funding_interval_secs", DataType::UInt32, false),
            text("funding_interval_provenance", false),
            decimal("funding_rate_floor", true),
            decimal("funding_rate_cap", true),
            text("funding_rate_bounds_provenance", false),
            decimal("price_lower_bound", true),
            decimal("price_upper_bound", true),
            text("supported_position_modes", false),
            text("supported_account_modes", false),
        ]),
        DerivativeEventFamily::MarkIndex => {
            fields.extend([decimal("mark_price", false), decimal("index_price", false)])
        }
        DerivativeEventFamily::FundingEstimate => fields.extend([
            decimal("rate", false),
            text("rate_kind", false),
            text("funding_basis", false),
            Field::new("interval_secs", DataType::UInt32, false),
            text("interval_provenance", false),
            Field::new("next_funding_ts_us", DataType::Int64, false),
        ]),
        DerivativeEventFamily::FundingSettlement => fields.extend([
            decimal("rate", false),
            text("rate_kind", false),
            text("funding_basis", false),
            Field::new("interval_secs", DataType::UInt32, false),
            text("interval_provenance", false),
            Field::new("settlement_ts_us", DataType::Int64, false),
        ]),
        DerivativeEventFamily::OpenInterest => fields.extend([
            decimal("open_interest", false),
            text("open_interest_unit", false),
            decimal("quote_notional", true),
        ]),
        DerivativeEventFamily::TraderRatio => fields.extend([
            text("metric_kind", false),
            decimal("long_ratio", false),
            decimal("short_ratio", false),
            decimal("long_short_ratio", false),
        ]),
        DerivativeEventFamily::QuoteConversion => fields.extend([
            text("side", false),
            decimal("price", false),
            decimal("executable_quantity", false),
        ]),
    }
    fields
}

fn text(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn decimal(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        nullable,
    )
}

pub(crate) fn derivative_venue_path(venue: AdapterId) -> (&'static str, &'static str) {
    match venue {
        AdapterId::BinanceUsdm => ("binance", "usdm_futures"),
        AdapterId::BybitLinear => ("bybit", "linear_futures"),
        AdapterId::UpbitSpot => ("upbit", "spot"),
        AdapterId::BithumbSpot => ("bithumb", "spot"),
        AdapterId::BinanceSpot => ("binance", "spot"),
    }
}
