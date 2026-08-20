use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const DECIMAL_PRECISION: u8 = 38;
pub const DECIMAL_SCALE: i8 = 18;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum AdapterId {
    UpbitSpot,
    BithumbSpot,
    BinanceSpot,
    BinanceUsdm,
    BybitLinear,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum TimestampPrecision {
    Microsecond,
    Millisecond,
    Unavailable,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CanonicalSymbol {
    pub base: String,
    pub quote: String,
}

impl CanonicalSymbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            quote: quote.into(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct EventMeta {
    pub schema_version: u16,
    pub event_id: Uuid,
    pub adapter: AdapterId,
    pub symbol: CanonicalSymbol,
    pub source_symbol: String,
    pub source_stream: String,
    pub source_sequence: Option<u64>,
    pub exchange_event_ts_us: Option<i64>,
    pub exchange_trade_ts_us: Option<i64>,
    pub event_ts_precision: TimestampPrecision,
    pub trade_ts_precision: TimestampPrecision,
    pub local_recv_ts_us: i64,
    pub raw_size_bytes: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PriceLevel {
    pub price: i128,
    pub quantity: i128,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BookSnapshot {
    pub meta: EventMeta,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub enum TakerSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TradeTick {
    pub meta: EventMeta,
    pub trade_id: String,
    pub price: i128,
    pub quantity: i128,
    pub taker_side: TakerSide,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NormalizedEvent {
    Book(BookSnapshot),
    Trade(TradeTick),
}

impl NormalizedEvent {
    pub fn meta(&self) -> &EventMeta {
        match self {
            Self::Book(value) => &value.meta,
            Self::Trade(value) => &value.meta,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum TimestampError {
    #[error("millisecond timestamp overflows microsecond storage: {value}")]
    Overflow { value: i64 },
}

pub fn ms_to_us(value: i64) -> Result<i64, TimestampError> {
    value
        .checked_mul(1_000)
        .ok_or(TimestampError::Overflow { value })
}
