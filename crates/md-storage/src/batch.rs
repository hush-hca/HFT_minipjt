use std::sync::Arc;
use std::{io, path::PathBuf};

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::ArrowError;
use md_core::model::{
    BookSnapshot, DECIMAL_PRECISION, DECIMAL_SCALE, EventMeta, NormalizedEvent, PriceLevel,
    TakerSide, TimestampPrecision, TradeTick,
};
use md_core::validation::{ValidationError, validate_event};
use thiserror::Error;

use crate::schema::{
    BOOK_SIDE_ASK, BOOK_SIDE_BID, SCHEMA_VERSION, SchemaContext, TAKER_SIDE_BUY, TAKER_SIDE_SELL,
    TAKER_SIDE_UNKNOWN, TIMESTAMP_PRECISION_MICROSECOND, TIMESTAMP_PRECISION_MILLISECOND,
    TIMESTAMP_PRECISION_UNAVAILABLE, book_schema, trade_schema,
};

const MAX_DECIMAL_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error("event schema version {actual} does not match storage schema version {expected}")]
    SchemaVersion { expected: u16, actual: u16 },
    #[error("event adapter does not match the batch schema context")]
    AdapterMismatch,
    #[error("event symbol does not match the batch schema context")]
    SymbolMismatch,
    #[error("book side has {levels} levels, exceeding the UInt16 level index")]
    TooManyLevels { levels: usize },
    #[error("{field} value {value} exceeds Decimal128 precision 38")]
    DecimalOutOfRange { field: &'static str, value: i128 },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Recovery(#[from] crate::recovery::RecoveryError),
    #[error("invalid partition component {component:?}")]
    InvalidPartitionComponent { component: String },
    #[error("timestamp {value} cannot be represented as UTC")]
    InvalidPartitionTimestamp { value: i64 },
    #[error("storage batch_rows must be greater than zero")]
    InvalidBatchRows,
    #[error("storage flush_interval must be greater than zero")]
    InvalidFlushInterval,
    #[error("existing finalized Arrow stream is unreadable: {path}: {message}")]
    UnreadableFinal { path: PathBuf, message: String },
    #[error("Arrow stream schema mismatch while merging {path}")]
    MergeSchemaMismatch { path: PathBuf },
    #[error("invalid derivative event field {field}: {message}")]
    InvalidDerivative {
        field: &'static str,
        message: String,
    },
    #[error("derivative writer is poisoned after a prior write or flush failure: {path}")]
    PoisonedDerivativeWriter { path: PathBuf },
}

#[derive(Debug, Default)]
struct CommonColumns {
    schema_version: Vec<u16>,
    event_id: Vec<[u8; 16]>,
    exchange: Vec<&'static str>,
    market: Vec<&'static str>,
    symbol: Vec<String>,
    source_symbol: Vec<String>,
    source_stream: Vec<String>,
    source_sequence: Vec<Option<u64>>,
    exchange_event_ts_us: Vec<Option<i64>>,
    exchange_trade_ts_us: Vec<Option<i64>>,
    local_recv_ts_us: Vec<i64>,
    event_ts_precision: Vec<u8>,
    trade_ts_precision: Vec<u8>,
    raw_size_bytes: Vec<u32>,
}

impl CommonColumns {
    fn len(&self) -> usize {
        self.schema_version.len()
    }

    fn push(&mut self, context: &SchemaContext, meta: &EventMeta) {
        self.schema_version.push(meta.schema_version);
        self.event_id.push(*meta.event_id.as_bytes());
        self.exchange.push(context.exchange());
        self.market.push(context.market());
        self.symbol.push(context.symbol_text());
        self.source_symbol.push(meta.source_symbol.clone());
        self.source_stream.push(meta.source_stream.clone());
        self.source_sequence.push(meta.source_sequence);
        self.exchange_event_ts_us.push(meta.exchange_event_ts_us);
        self.exchange_trade_ts_us.push(meta.exchange_trade_ts_us);
        self.local_recv_ts_us.push(meta.local_recv_ts_us);
        self.event_ts_precision
            .push(timestamp_precision(meta.event_ts_precision));
        self.trade_ts_precision
            .push(timestamp_precision(meta.trade_ts_precision));
        self.raw_size_bytes.push(meta.raw_size_bytes);
    }

    fn arrays(&self) -> Result<Vec<ArrayRef>, ArrowError> {
        let mut event_id = FixedSizeBinaryBuilder::with_capacity(self.len(), 16);
        for value in &self.event_id {
            event_id.append_value(value)?;
        }

        Ok(vec![
            Arc::new(UInt16Array::from(self.schema_version.clone())),
            Arc::new(event_id.finish()),
            Arc::new(StringArray::from(self.exchange.clone())),
            Arc::new(StringArray::from(self.market.clone())),
            Arc::new(StringArray::from(self.symbol.clone())),
            Arc::new(StringArray::from(self.source_symbol.clone())),
            Arc::new(StringArray::from(self.source_stream.clone())),
            Arc::new(UInt64Array::from(self.source_sequence.clone())),
            Arc::new(Int64Array::from(self.exchange_event_ts_us.clone())),
            Arc::new(Int64Array::from(self.exchange_trade_ts_us.clone())),
            Arc::new(Int64Array::from(self.local_recv_ts_us.clone())),
            Arc::new(UInt8Array::from(self.event_ts_precision.clone())),
            Arc::new(UInt8Array::from(self.trade_ts_precision.clone())),
            Arc::new(UInt32Array::from(self.raw_size_bytes.clone())),
        ])
    }

    fn clear(&mut self) {
        self.schema_version.clear();
        self.event_id.clear();
        self.exchange.clear();
        self.market.clear();
        self.symbol.clear();
        self.source_symbol.clear();
        self.source_stream.clear();
        self.source_sequence.clear();
        self.exchange_event_ts_us.clear();
        self.exchange_trade_ts_us.clear();
        self.local_recv_ts_us.clear();
        self.event_ts_precision.clear();
        self.trade_ts_precision.clear();
        self.raw_size_bytes.clear();
    }
}

#[derive(Debug)]
pub struct BookBatchBuilder {
    context: SchemaContext,
    common: CommonColumns,
    side: Vec<u8>,
    level: Vec<u16>,
    price: Vec<i128>,
    quantity: Vec<i128>,
}

impl BookBatchBuilder {
    pub fn new(context: SchemaContext) -> Self {
        Self {
            context,
            common: CommonColumns::default(),
            side: Vec::new(),
            level: Vec::new(),
            price: Vec::new(),
            quantity: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.common.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, book: &BookSnapshot) -> Result<(), StorageError> {
        validate_event(&NormalizedEvent::Book(book.clone()))?;
        validate_context(&self.context, &book.meta)?;
        validate_level_count(book.bids.len())?;
        validate_level_count(book.asks.len())?;
        validate_level_decimals(&book.bids)?;
        validate_level_decimals(&book.asks)?;

        self.push_side(&book.meta, &book.bids, BOOK_SIDE_BID);
        self.push_side(&book.meta, &book.asks, BOOK_SIDE_ASK);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch, StorageError> {
        let mut arrays = self.common.arrays()?;
        arrays.extend([
            Arc::new(UInt8Array::from(self.side.clone())) as ArrayRef,
            Arc::new(UInt16Array::from(self.level.clone())) as ArrayRef,
            decimal_array(self.price.clone())?,
            decimal_array(self.quantity.clone())?,
        ]);
        let batch = RecordBatch::try_new(book_schema(&self.context), arrays)?;
        self.clear();
        Ok(batch)
    }

    fn push_side(&mut self, meta: &EventMeta, levels: &[PriceLevel], side: u8) {
        for (level, value) in levels.iter().enumerate() {
            self.common.push(&self.context, meta);
            self.side.push(side);
            self.level.push(level as u16);
            self.price.push(value.price);
            self.quantity.push(value.quantity);
        }
    }

    fn clear(&mut self) {
        self.common.clear();
        self.side.clear();
        self.level.clear();
        self.price.clear();
        self.quantity.clear();
    }
}

#[derive(Debug)]
pub struct TradeBatchBuilder {
    context: SchemaContext,
    common: CommonColumns,
    trade_id: Vec<String>,
    price: Vec<i128>,
    quantity: Vec<i128>,
    taker_side: Vec<u8>,
}

impl TradeBatchBuilder {
    pub fn new(context: SchemaContext) -> Self {
        Self {
            context,
            common: CommonColumns::default(),
            trade_id: Vec::new(),
            price: Vec::new(),
            quantity: Vec::new(),
            taker_side: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.common.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, trade: &TradeTick) -> Result<(), StorageError> {
        validate_event(&NormalizedEvent::Trade(trade.clone()))?;
        validate_context(&self.context, &trade.meta)?;
        validate_decimal("price", trade.price)?;
        validate_decimal("quantity", trade.quantity)?;

        self.common.push(&self.context, &trade.meta);
        self.trade_id.push(trade.trade_id.clone());
        self.price.push(trade.price);
        self.quantity.push(trade.quantity);
        self.taker_side.push(match trade.taker_side {
            TakerSide::Unknown => TAKER_SIDE_UNKNOWN,
            TakerSide::Buy => TAKER_SIDE_BUY,
            TakerSide::Sell => TAKER_SIDE_SELL,
        });
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch, StorageError> {
        let mut arrays = self.common.arrays()?;
        arrays.extend([
            Arc::new(StringArray::from(self.trade_id.clone())) as ArrayRef,
            decimal_array(self.price.clone())?,
            decimal_array(self.quantity.clone())?,
            Arc::new(UInt8Array::from(self.taker_side.clone())) as ArrayRef,
        ]);
        let batch = RecordBatch::try_new(trade_schema(&self.context), arrays)?;
        self.clear();
        Ok(batch)
    }

    fn clear(&mut self) {
        self.common.clear();
        self.trade_id.clear();
        self.price.clear();
        self.quantity.clear();
        self.taker_side.clear();
    }
}

fn validate_context(context: &SchemaContext, meta: &EventMeta) -> Result<(), StorageError> {
    if meta.schema_version != SCHEMA_VERSION {
        return Err(StorageError::SchemaVersion {
            expected: SCHEMA_VERSION,
            actual: meta.schema_version,
        });
    }
    if meta.adapter != context.adapter {
        return Err(StorageError::AdapterMismatch);
    }
    if meta.symbol != context.symbol {
        return Err(StorageError::SymbolMismatch);
    }
    Ok(())
}

fn validate_level_count(levels: usize) -> Result<(), StorageError> {
    if levels > usize::from(u16::MAX) + 1 {
        return Err(StorageError::TooManyLevels { levels });
    }
    Ok(())
}

fn validate_level_decimals(levels: &[PriceLevel]) -> Result<(), StorageError> {
    for value in levels {
        validate_decimal("price", value.price)?;
        validate_decimal("quantity", value.quantity)?;
    }
    Ok(())
}

fn validate_decimal(field: &'static str, value: i128) -> Result<(), StorageError> {
    if value > MAX_DECIMAL_38 {
        return Err(StorageError::DecimalOutOfRange { field, value });
    }
    Ok(())
}

fn timestamp_precision(value: TimestampPrecision) -> u8 {
    match value {
        TimestampPrecision::Unavailable => TIMESTAMP_PRECISION_UNAVAILABLE,
        TimestampPrecision::Millisecond => TIMESTAMP_PRECISION_MILLISECOND,
        TimestampPrecision::Microsecond => TIMESTAMP_PRECISION_MICROSECOND,
    }
}

fn decimal_array(values: Vec<i128>) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(
        Decimal128Array::from(values).with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
    ))
}
