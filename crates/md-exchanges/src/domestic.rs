use md_core::{
    decimal::{DecimalError, parse_decimal_18},
    model::{
        AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
        TakerSide, TimestampError, TimestampPrecision, TradeTick, ms_to_us,
    },
    validation::{ValidationError, validate_event},
};
use simd_json::prelude::*;
use simd_json::{BorrowedValue, StaticNode};
use thiserror::Error;
use uuid::Uuid;

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid JSON frame: {0}")]
    Json(#[from] simd_json::Error),
    #[error("market data frame must be a JSON object")]
    InvalidRoot,
    #[error("missing required field `{field}`")]
    MissingField { field: &'static str },
    #[error("field `{field}` must be {expected}")]
    InvalidField {
        field: &'static str,
        expected: &'static str,
    },
    #[error("unknown event type `{event_type}`")]
    UnknownEventType { event_type: String },
    #[error("unsupported source stream `{stream}`")]
    UnsupportedStream { stream: String },
    #[error("invalid decimal in `{field}`: {source}")]
    Decimal {
        field: &'static str,
        #[source]
        source: DecimalError,
    },
    #[error("invalid timestamp in `{field}`: {source}")]
    Timestamp {
        field: &'static str,
        #[source]
        source: TimestampError,
    },
    #[error("frame exceeds the supported raw-size counter")]
    FrameTooLarge,
    #[error("normalized event failed validation: {0}")]
    Validation(#[from] ValidationError),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DomesticVenue {
    pub adapter: AdapterId,
    pub book_timestamp_precision: TimestampPrecision,
}

pub(crate) fn parse_frame(
    frame: &mut [u8],
    recv_us: i64,
    venue: DomesticVenue,
) -> Result<Vec<NormalizedEvent>, ParseError> {
    let raw_size_bytes = u32::try_from(frame.len()).map_err(|_| ParseError::FrameTooLarge)?;
    let root = simd_json::to_borrowed_value(frame)?;
    let BorrowedValue::Object(object) = root else {
        return Err(ParseError::InvalidRoot);
    };

    if optional_string(&object, &["status"])?.is_some_and(|status| status == "UP") {
        return Ok(Vec::new());
    }

    let event_type = required_string(&object, &["type", "ty"], "type")?;
    let event = match event_type {
        "orderbook" => parse_book(&object, recv_us, raw_size_bytes, venue)?,
        "trade" => parse_trade(&object, recv_us, raw_size_bytes, venue)?,
        value => {
            return Err(ParseError::UnknownEventType {
                event_type: value.to_owned(),
            });
        }
    };
    validate_event(&event)?;
    Ok(vec![event])
}

fn parse_book(
    object: &simd_json::value::borrowed::Object<'_>,
    recv_us: i64,
    raw_size_bytes: u32,
    venue: DomesticVenue,
) -> Result<NormalizedEvent, ParseError> {
    let source_symbol = source_symbol(object)?;
    let timestamp = required_i64(object, &["timestamp", "tms"], "timestamp")?;
    let exchange_event_ts_us = match venue.book_timestamp_precision {
        TimestampPrecision::Microsecond => timestamp,
        TimestampPrecision::Millisecond => timestamp_ms_to_us("timestamp", timestamp)?,
        TimestampPrecision::Unavailable => {
            return Err(ParseError::InvalidField {
                field: "timestamp",
                expected: "a timestamp with known source precision",
            });
        }
    };
    let units = required_value(object, &["orderbook_units", "obu"], "orderbook_units")?
        .as_array()
        .ok_or(ParseError::InvalidField {
            field: "orderbook_units",
            expected: "an array",
        })?;

    let mut bids = Vec::with_capacity(units.len());
    let mut asks = Vec::with_capacity(units.len());
    for unit in units {
        let unit = unit.as_object().ok_or(ParseError::InvalidField {
            field: "orderbook_units[]",
            expected: "an object",
        })?;
        asks.push(PriceLevel {
            price: required_decimal(unit, &["ask_price", "ap"], "ask_price")?,
            quantity: required_decimal(unit, &["ask_size", "as"], "ask_size")?,
        });
        bids.push(PriceLevel {
            price: required_decimal(unit, &["bid_price", "bp"], "bid_price")?,
            quantity: required_decimal(unit, &["bid_size", "bs"], "bid_size")?,
        });
    }

    Ok(NormalizedEvent::Book(BookSnapshot {
        meta: event_meta(
            venue.adapter,
            &source_symbol,
            "orderbook",
            None,
            Some(exchange_event_ts_us),
            None,
            venue.book_timestamp_precision,
            TimestampPrecision::Unavailable,
            recv_us,
            raw_size_bytes,
        )?,
        bids,
        asks,
    }))
}

fn parse_trade(
    object: &simd_json::value::borrowed::Object<'_>,
    recv_us: i64,
    raw_size_bytes: u32,
    venue: DomesticVenue,
) -> Result<NormalizedEvent, ParseError> {
    let source_symbol = source_symbol(object)?;
    let event_timestamp = required_i64(object, &["timestamp", "tms"], "timestamp")?;
    let trade_timestamp = required_i64(object, &["trade_timestamp", "ttms"], "trade_timestamp")?;
    let sequence = required_u64(object, &["sequential_id", "sid"], "sequential_id")?;
    let taker_side = match required_string(object, &["ask_bid", "ab"], "ask_bid")? {
        "BID" => TakerSide::Buy,
        "ASK" => TakerSide::Sell,
        _ => {
            return Err(ParseError::InvalidField {
                field: "ask_bid",
                expected: "`BID` or `ASK`",
            });
        }
    };

    Ok(NormalizedEvent::Trade(TradeTick {
        meta: event_meta(
            venue.adapter,
            &source_symbol,
            "trade",
            Some(sequence),
            Some(timestamp_ms_to_us("timestamp", event_timestamp)?),
            Some(timestamp_ms_to_us("trade_timestamp", trade_timestamp)?),
            TimestampPrecision::Millisecond,
            TimestampPrecision::Millisecond,
            recv_us,
            raw_size_bytes,
        )?,
        trade_id: sequence.to_string(),
        price: required_decimal(object, &["trade_price", "tp"], "trade_price")?,
        quantity: required_decimal(object, &["trade_volume", "tv"], "trade_volume")?,
        taker_side,
    }))
}

#[allow(clippy::too_many_arguments)]
fn event_meta(
    adapter: AdapterId,
    source_symbol: &str,
    source_stream: &str,
    source_sequence: Option<u64>,
    exchange_event_ts_us: Option<i64>,
    exchange_trade_ts_us: Option<i64>,
    event_ts_precision: TimestampPrecision,
    trade_ts_precision: TimestampPrecision,
    local_recv_ts_us: i64,
    raw_size_bytes: u32,
) -> Result<EventMeta, ParseError> {
    Ok(EventMeta {
        schema_version: SCHEMA_VERSION,
        event_id: Uuid::now_v7(),
        adapter,
        symbol: canonical_symbol(source_symbol)?,
        source_symbol: source_symbol.to_owned(),
        source_stream: source_stream.to_owned(),
        source_sequence,
        exchange_event_ts_us,
        exchange_trade_ts_us,
        event_ts_precision,
        trade_ts_precision,
        local_recv_ts_us,
        raw_size_bytes,
    })
}

fn canonical_symbol(source_symbol: &str) -> Result<CanonicalSymbol, ParseError> {
    let (quote, base) = source_symbol
        .split_once('-')
        .ok_or(ParseError::InvalidField {
            field: "code",
            expected: "a `QUOTE-BASE` market code",
        })?;
    if quote.is_empty()
        || base.is_empty()
        || quote.contains('-')
        || base.contains('-')
        || !quote.bytes().all(|byte| byte.is_ascii_uppercase())
        || !base
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(ParseError::InvalidField {
            field: "code",
            expected: "an uppercase `QUOTE-BASE` market code",
        });
    }
    Ok(CanonicalSymbol::new(base, quote))
}

fn source_symbol(object: &simd_json::value::borrowed::Object<'_>) -> Result<String, ParseError> {
    required_string(object, &["code", "cd", "market", "mk"], "code").map(str::to_owned)
}

fn required_decimal(
    object: &simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
    field: &'static str,
) -> Result<i128, ParseError> {
    decimal_from_value(required_value(object, aliases, field)?, field)
}

pub(crate) fn decimal_from_value(
    value: &BorrowedValue<'_>,
    field: &'static str,
) -> Result<i128, ParseError> {
    let result = match value {
        BorrowedValue::String(value) => parse_decimal_18(value),
        BorrowedValue::Static(StaticNode::I64(value)) => parse_decimal_18(&value.to_string()),
        BorrowedValue::Static(StaticNode::U64(value)) => parse_decimal_18(&value.to_string()),
        BorrowedValue::Static(StaticNode::F64(value)) if value.is_finite() => {
            let mut buffer = ryu::Buffer::new();
            let formatted = buffer.format_finite(*value);
            if formatted.contains(['e', 'E']) {
                expand_scientific(formatted).and_then(|plain| parse_decimal_18(&plain))
            } else {
                parse_decimal_18(formatted)
            }
        }
        _ => {
            return Err(ParseError::InvalidField {
                field,
                expected: "a decimal string or finite JSON number",
            });
        }
    };
    result.map_err(|source| ParseError::Decimal { field, source })
}

fn expand_scientific(text: &str) -> Result<String, DecimalError> {
    let exponent_at = text.find(['e', 'E']).ok_or(DecimalError::InvalidFormat)?;
    let (mantissa, exponent_with_marker) = text.split_at(exponent_at);
    let exponent: i32 = exponent_with_marker[1..]
        .parse()
        .map_err(|_| DecimalError::InvalidFormat)?;
    let (sign, unsigned) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |value| ("-", value));
    let decimal_at = unsigned.find('.').unwrap_or(unsigned.len());
    let digits: String = unsigned.chars().filter(|value| *value != '.').collect();
    if digits.is_empty() || !digits.bytes().all(|value| value.is_ascii_digit()) {
        return Err(DecimalError::InvalidFormat);
    }

    let shifted = i64::try_from(decimal_at)
        .ok()
        .and_then(|value| value.checked_add(i64::from(exponent)))
        .ok_or(DecimalError::InvalidFormat)?;
    let mut plain =
        String::with_capacity(sign.len() + digits.len() + exponent.unsigned_abs() as usize + 2);
    plain.push_str(sign);
    if shifted <= 0 {
        plain.push_str("0.");
        plain.extend(std::iter::repeat_n(
            '0',
            usize::try_from(-shifted).map_err(|_| DecimalError::InvalidFormat)?,
        ));
        plain.push_str(&digits);
    } else if usize::try_from(shifted).map_err(|_| DecimalError::InvalidFormat)? >= digits.len() {
        plain.push_str(&digits);
        plain.extend(std::iter::repeat_n(
            '0',
            usize::try_from(shifted).map_err(|_| DecimalError::InvalidFormat)? - digits.len(),
        ));
    } else {
        let split = usize::try_from(shifted).map_err(|_| DecimalError::InvalidFormat)?;
        plain.push_str(&digits[..split]);
        plain.push('.');
        plain.push_str(&digits[split..]);
    }
    Ok(plain)
}

fn timestamp_ms_to_us(field: &'static str, value: i64) -> Result<i64, ParseError> {
    ms_to_us(value).map_err(|source| ParseError::Timestamp { field, source })
}

fn required_i64(
    object: &simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
    field: &'static str,
) -> Result<i64, ParseError> {
    match required_value(object, aliases, field)? {
        BorrowedValue::Static(StaticNode::I64(value)) => Ok(*value),
        BorrowedValue::Static(StaticNode::U64(value)) => {
            i64::try_from(*value).map_err(|_| ParseError::InvalidField {
                field,
                expected: "a signed 64-bit integer",
            })
        }
        _ => Err(ParseError::InvalidField {
            field,
            expected: "an integer",
        }),
    }
}

fn required_u64(
    object: &simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
    field: &'static str,
) -> Result<u64, ParseError> {
    match required_value(object, aliases, field)? {
        BorrowedValue::Static(StaticNode::U64(value)) => Ok(*value),
        BorrowedValue::Static(StaticNode::I64(value)) => {
            u64::try_from(*value).map_err(|_| ParseError::InvalidField {
                field,
                expected: "a non-negative 64-bit integer",
            })
        }
        _ => Err(ParseError::InvalidField {
            field,
            expected: "a non-negative integer",
        }),
    }
}

fn required_string<'value>(
    object: &'value simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
    field: &'static str,
) -> Result<&'value str, ParseError> {
    required_value(object, aliases, field)?
        .as_str()
        .ok_or(ParseError::InvalidField {
            field,
            expected: "a string",
        })
}

fn optional_string<'value>(
    object: &'value simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
) -> Result<Option<&'value str>, ParseError> {
    let Some(value) = aliases.iter().find_map(|alias| object.get(*alias)) else {
        return Ok(None);
    };
    value.as_str().map(Some).ok_or(ParseError::InvalidField {
        field: "status",
        expected: "a string",
    })
}

fn required_value<'value>(
    object: &'value simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
    field: &'static str,
) -> Result<&'value BorrowedValue<'value>, ParseError> {
    aliases
        .iter()
        .find_map(|alias| object.get(*alias))
        .ok_or(ParseError::MissingField { field })
}
