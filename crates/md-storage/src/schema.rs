use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use md_core::model::{AdapterId, CanonicalSymbol, DECIMAL_PRECISION, DECIMAL_SCALE};

pub const PROJECT_NAME: &str = "hft-market-data-collector";
pub const SCHEMA_VERSION: u16 = 1;

pub const TIMESTAMP_PRECISION_UNAVAILABLE: u8 = 0;
pub const TIMESTAMP_PRECISION_MILLISECOND: u8 = 1;
pub const TIMESTAMP_PRECISION_MICROSECOND: u8 = 2;

pub const BOOK_SIDE_BID: u8 = 0;
pub const BOOK_SIDE_ASK: u8 = 1;

pub const TAKER_SIDE_UNKNOWN: u8 = 0;
pub const TAKER_SIDE_BUY: u8 = 1;
pub const TAKER_SIDE_SELL: u8 = 2;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SchemaContext {
    pub adapter: AdapterId,
    pub symbol: CanonicalSymbol,
    pub utc_hour: DateTime<Utc>,
}

impl SchemaContext {
    pub(crate) fn exchange(&self) -> &'static str {
        match self.adapter {
            AdapterId::UpbitSpot => "upbit",
            AdapterId::BithumbSpot => "bithumb",
            AdapterId::BinanceSpot | AdapterId::BinanceUsdm => "binance",
            AdapterId::BybitLinear => "bybit",
        }
    }

    pub(crate) fn market(&self) -> &'static str {
        match self.adapter {
            AdapterId::UpbitSpot | AdapterId::BithumbSpot | AdapterId::BinanceSpot => "spot",
            AdapterId::BinanceUsdm => "usdm_futures",
            AdapterId::BybitLinear => "linear_futures",
        }
    }

    pub(crate) fn symbol_text(&self) -> String {
        format!("{}/{}", self.symbol.base, self.symbol.quote)
    }

    fn metadata(&self) -> HashMap<String, String> {
        let utc_hour = self
            .utc_hour
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .expect("valid UTC hour components")
            .to_rfc3339_opts(SecondsFormat::Secs, true);

        HashMap::from([
            ("project".to_owned(), PROJECT_NAME.to_owned()),
            ("schema_version".to_owned(), SCHEMA_VERSION.to_string()),
            ("timestamp_unit".to_owned(), "microsecond".to_owned()),
            ("decimal_scale".to_owned(), DECIMAL_SCALE.to_string()),
            ("exchange".to_owned(), self.exchange().to_owned()),
            ("market".to_owned(), self.market().to_owned()),
            ("symbol".to_owned(), self.symbol_text()),
            ("utc_hour".to_owned(), utc_hour),
        ])
    }
}

pub fn book_schema(context: &SchemaContext) -> Arc<Schema> {
    let mut fields = common_fields();
    fields.extend([
        Field::new("side", DataType::UInt8, false),
        Field::new("level", DataType::UInt16, false),
        decimal_field("price"),
        decimal_field("quantity"),
    ]);
    Arc::new(Schema::new_with_metadata(fields, context.metadata()))
}

pub fn trade_schema(context: &SchemaContext) -> Arc<Schema> {
    let mut fields = common_fields();
    fields.extend([
        Field::new("trade_id", DataType::Utf8, false),
        decimal_field("price"),
        decimal_field("quantity"),
        Field::new("taker_side", DataType::UInt8, false),
    ]);
    Arc::new(Schema::new_with_metadata(fields, context.metadata()))
}

fn common_fields() -> Vec<Field> {
    vec![
        Field::new("schema_version", DataType::UInt16, false),
        Field::new("event_id", DataType::FixedSizeBinary(16), false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("source_symbol", DataType::Utf8, false),
        Field::new("source_stream", DataType::Utf8, false),
        Field::new("source_sequence", DataType::UInt64, true),
        Field::new("exchange_event_ts_us", DataType::Int64, true),
        Field::new("exchange_trade_ts_us", DataType::Int64, true),
        Field::new("local_recv_ts_us", DataType::Int64, false),
        Field::new("event_ts_precision", DataType::UInt8, false),
        Field::new("trade_ts_precision", DataType::UInt8, false),
        Field::new("raw_size_bytes", DataType::UInt32, false),
    ]
}

fn decimal_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        false,
    )
}
