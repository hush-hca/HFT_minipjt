use md_core::{
    model::{
        AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
        TakerSide, TimestampPrecision, TradeTick, ms_to_us,
    },
    validation::validate_event,
};
use simd_json::prelude::*;
use simd_json::{BorrowedValue, StaticNode};
use uuid::Uuid;

use crate::domestic::{ParseError, decimal_from_value};

const SCHEMA_VERSION: u16 = 1;
const QUOTE: &str = "USDT";

#[derive(Debug, Clone, Copy)]
pub(crate) struct BinanceVenue {
    pub adapter: AdapterId,
    pub require_book_symbol: bool,
    /// USD-M has been observed emitting marked raw-trade control frames with zero values.
    pub ignore_zero_trade_sentinel: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StreamKind {
    Trade,
    Depth20,
}

pub(crate) fn parse_frame(
    frame: &mut [u8],
    recv_us: i64,
    venue: BinanceVenue,
) -> Result<Vec<NormalizedEvent>, ParseError> {
    let raw_size_bytes = u32::try_from(frame.len()).map_err(|_| ParseError::FrameTooLarge)?;
    let root = simd_json::to_borrowed_value(frame)?;
    let root = root.as_object().ok_or(ParseError::InvalidRoot)?;
    let stream = required_string(root, "stream")?;
    let (stream_symbol, kind) = parse_stream(stream)?;
    let data = required_value(root, "data")?
        .as_object()
        .ok_or(ParseError::InvalidField {
            field: "data",
            expected: "an object",
        })?;

    let event = match kind {
        StreamKind::Trade => {
            let event = parse_trade(data, stream, stream_symbol, recv_us, raw_size_bytes, venue)?;
            if venue.ignore_zero_trade_sentinel && is_zero_trade_sentinel(data, &event)? {
                return Ok(Vec::new());
            }
            event
        }
        StreamKind::Depth20 => {
            parse_book(data, stream, stream_symbol, recv_us, raw_size_bytes, venue)?
        }
    };
    validate_event(&event)?;
    Ok(vec![event])
}

fn is_zero_trade_sentinel(
    data: &simd_json::value::borrowed::Object<'_>,
    event: &NormalizedEvent,
) -> Result<bool, ParseError> {
    let NormalizedEvent::Trade(trade) = event else {
        return Ok(false);
    };
    Ok(optional_string(data, "X")? == Some("NA")
        && optional_u64(data, "st")? == Some(1)
        && trade.price == 0
        && trade.quantity == 0)
}

fn parse_stream(stream: &str) -> Result<(&str, StreamKind), ParseError> {
    let (symbol, suffix) = stream.split_once('@').ok_or(ParseError::InvalidField {
        field: "stream",
        expected: "a Binance symbol and stream suffix",
    })?;
    if symbol.is_empty()
        || !symbol
            .bytes()
            .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit())
    {
        return Err(ParseError::InvalidField {
            field: "stream",
            expected: "a lowercase Binance symbol and stream suffix",
        });
    }

    let kind = match suffix {
        "trade" => StreamKind::Trade,
        "depth20" | "depth20@100ms" => StreamKind::Depth20,
        value if value.eq_ignore_ascii_case("aggtrade") => {
            return Err(ParseError::UnsupportedStream {
                stream: stream.to_owned(),
            });
        }
        _ => {
            return Err(ParseError::UnsupportedStream {
                stream: stream.to_owned(),
            });
        }
    };
    Ok((symbol, kind))
}

fn parse_trade(
    data: &simd_json::value::borrowed::Object<'_>,
    stream: &str,
    stream_symbol: &str,
    recv_us: i64,
    raw_size_bytes: u32,
    venue: BinanceVenue,
) -> Result<NormalizedEvent, ParseError> {
    require_event_type(data, "trade")?;
    let source_symbol = source_symbol(data, stream_symbol, true)?;
    let sequence = required_u64(data, "t")?;
    let event_timestamp = required_i64(data, "E")?;
    let trade_timestamp = required_i64(data, "T")?;
    let taker_side = if required_bool(data, "m")? {
        TakerSide::Sell
    } else {
        TakerSide::Buy
    };

    Ok(NormalizedEvent::Trade(TradeTick {
        meta: event_meta(
            venue.adapter,
            &source_symbol,
            stream,
            Some(sequence),
            Some(timestamp_ms_to_us("E", event_timestamp)?),
            Some(timestamp_ms_to_us("T", trade_timestamp)?),
            recv_us,
            raw_size_bytes,
        )?,
        trade_id: sequence.to_string(),
        price: required_decimal(data, "p")?,
        quantity: required_decimal(data, "q")?,
        taker_side,
    }))
}

fn parse_book(
    data: &simd_json::value::borrowed::Object<'_>,
    stream: &str,
    stream_symbol: &str,
    recv_us: i64,
    raw_size_bytes: u32,
    venue: BinanceVenue,
) -> Result<NormalizedEvent, ParseError> {
    if let Some(event_type) = optional_string(data, "e")?
        && event_type != "depthUpdate"
    {
        return Err(ParseError::UnknownEventType {
            event_type: event_type.to_owned(),
        });
    }

    let source_symbol = source_symbol(data, stream_symbol, venue.require_book_symbol)?;
    let source_sequence = optional_u64(data, "lastUpdateId")?
        .or(optional_u64(data, "u")?)
        .ok_or(ParseError::MissingField {
            field: "lastUpdateId/u",
        })?;
    let exchange_event_ts_us = optional_timestamp_ms(data, "E")?;
    let exchange_trade_ts_us = optional_timestamp_ms(data, "T")?;
    let bids = price_levels(data, &["bids", "b"], "bids/b")?;
    let asks = price_levels(data, &["asks", "a"], "asks/a")?;

    Ok(NormalizedEvent::Book(BookSnapshot {
        meta: event_meta(
            venue.adapter,
            &source_symbol,
            stream,
            Some(source_sequence),
            exchange_event_ts_us,
            exchange_trade_ts_us,
            recv_us,
            raw_size_bytes,
        )?,
        bids,
        asks,
    }))
}

fn price_levels(
    data: &simd_json::value::borrowed::Object<'_>,
    aliases: &[&str],
    field: &'static str,
) -> Result<Vec<PriceLevel>, ParseError> {
    let values = aliases
        .iter()
        .find_map(|alias| data.get(*alias))
        .ok_or(ParseError::MissingField { field })?
        .as_array()
        .ok_or(ParseError::InvalidField {
            field,
            expected: "an array of price/quantity pairs",
        })?;

    values
        .iter()
        .map(|value| {
            let level = value.as_array().ok_or(ParseError::InvalidField {
                field,
                expected: "an array of price/quantity pairs",
            })?;
            if level.len() != 2 {
                return Err(ParseError::InvalidField {
                    field,
                    expected: "two-element price/quantity pairs",
                });
            }
            Ok(PriceLevel {
                price: decimal_from_value(&level[0], field)?,
                quantity: decimal_from_value(&level[1], field)?,
            })
        })
        .collect()
}

fn source_symbol(
    data: &simd_json::value::borrowed::Object<'_>,
    stream_symbol: &str,
    required_in_payload: bool,
) -> Result<String, ParseError> {
    let stream_symbol = stream_symbol.to_ascii_uppercase();
    let payload_symbol = optional_string(data, "s")?;
    if required_in_payload && payload_symbol.is_none() {
        return Err(ParseError::MissingField { field: "s" });
    }
    if let Some(payload_symbol) = payload_symbol {
        if payload_symbol != stream_symbol {
            return Err(ParseError::InvalidField {
                field: "s",
                expected: "the symbol named by the combined stream",
            });
        }
        validate_source_symbol(payload_symbol)?;
        Ok(payload_symbol.to_owned())
    } else {
        validate_source_symbol(&stream_symbol)?;
        Ok(stream_symbol)
    }
}

fn validate_source_symbol(source_symbol: &str) -> Result<(), ParseError> {
    let Some(base) = source_symbol.strip_suffix(QUOTE) else {
        return Err(ParseError::InvalidField {
            field: "s",
            expected: "an uppercase USDT-quoted Binance symbol",
        });
    };
    if base.is_empty()
        || !source_symbol
            .bytes()
            .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit())
    {
        return Err(ParseError::InvalidField {
            field: "s",
            expected: "an uppercase USDT-quoted Binance symbol",
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn event_meta(
    adapter: AdapterId,
    source_symbol: &str,
    source_stream: &str,
    source_sequence: Option<u64>,
    exchange_event_ts_us: Option<i64>,
    exchange_trade_ts_us: Option<i64>,
    local_recv_ts_us: i64,
    raw_size_bytes: u32,
) -> Result<EventMeta, ParseError> {
    let base = source_symbol
        .strip_suffix(QUOTE)
        .ok_or(ParseError::InvalidField {
            field: "s",
            expected: "an uppercase USDT-quoted Binance symbol",
        })?;
    Ok(EventMeta {
        schema_version: SCHEMA_VERSION,
        event_id: Uuid::now_v7(),
        adapter,
        symbol: CanonicalSymbol::new(base, QUOTE),
        source_symbol: source_symbol.to_owned(),
        source_stream: source_stream.to_owned(),
        source_sequence,
        exchange_event_ts_us,
        exchange_trade_ts_us,
        event_ts_precision: timestamp_precision(exchange_event_ts_us),
        trade_ts_precision: timestamp_precision(exchange_trade_ts_us),
        local_recv_ts_us,
        raw_size_bytes,
    })
}

fn timestamp_precision(value: Option<i64>) -> TimestampPrecision {
    if value.is_some() {
        TimestampPrecision::Millisecond
    } else {
        TimestampPrecision::Unavailable
    }
}

fn require_event_type(
    data: &simd_json::value::borrowed::Object<'_>,
    expected: &'static str,
) -> Result<(), ParseError> {
    let event_type = required_string(data, "e")?;
    if event_type == expected {
        Ok(())
    } else {
        Err(ParseError::UnknownEventType {
            event_type: event_type.to_owned(),
        })
    }
}

fn required_decimal(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<i128, ParseError> {
    decimal_from_value(required_value(data, field)?, field)
}

fn optional_timestamp_ms(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<Option<i64>, ParseError> {
    optional_i64(data, field)?
        .map(|value| timestamp_ms_to_us(field, value))
        .transpose()
}

fn timestamp_ms_to_us(field: &'static str, value: i64) -> Result<i64, ParseError> {
    ms_to_us(value).map_err(|source| ParseError::Timestamp { field, source })
}

fn required_i64(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<i64, ParseError> {
    optional_i64(data, field)?.ok_or(ParseError::MissingField { field })
}

fn optional_i64(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<Option<i64>, ParseError> {
    let Some(value) = data.get(field) else {
        return Ok(None);
    };
    match value {
        BorrowedValue::Static(StaticNode::I64(value)) => Ok(Some(*value)),
        BorrowedValue::Static(StaticNode::U64(value)) => {
            i64::try_from(*value)
                .map(Some)
                .map_err(|_| ParseError::InvalidField {
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
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<u64, ParseError> {
    optional_u64(data, field)?.ok_or(ParseError::MissingField { field })
}

fn optional_u64(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<Option<u64>, ParseError> {
    let Some(value) = data.get(field) else {
        return Ok(None);
    };
    match value {
        BorrowedValue::Static(StaticNode::U64(value)) => Ok(Some(*value)),
        BorrowedValue::Static(StaticNode::I64(value)) => {
            u64::try_from(*value)
                .map(Some)
                .map_err(|_| ParseError::InvalidField {
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

fn required_bool(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<bool, ParseError> {
    required_value(data, field)?
        .as_bool()
        .ok_or(ParseError::InvalidField {
            field,
            expected: "a boolean",
        })
}

fn required_string<'value>(
    data: &'value simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<&'value str, ParseError> {
    required_value(data, field)?
        .as_str()
        .ok_or(ParseError::InvalidField {
            field,
            expected: "a string",
        })
}

fn optional_string<'value>(
    data: &'value simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<Option<&'value str>, ParseError> {
    let Some(value) = data.get(field) else {
        return Ok(None);
    };
    value.as_str().map(Some).ok_or(ParseError::InvalidField {
        field,
        expected: "a string",
    })
}

fn required_value<'value>(
    data: &'value simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<&'value BorrowedValue<'value>, ParseError> {
    data.get(field).ok_or(ParseError::MissingField { field })
}
