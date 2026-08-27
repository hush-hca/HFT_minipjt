use std::{collections::BTreeMap, sync::Mutex};

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

use crate::{FrameParser, ParseError, domestic::decimal_from_value};

const SCHEMA_VERSION: u16 = 1;
const TOP_LEVELS: usize = 20;

#[derive(Debug, Clone, Default)]
struct BybitBookState {
    bids: BTreeMap<i128, i128>,
    asks: BTreeMap<i128, i128>,
    last_update_id: Option<u64>,
    last_cross_sequence: Option<u64>,
    initialized: bool,
}

#[derive(Debug)]
pub struct BybitLinearParser {
    symbol: CanonicalSymbol,
    source_symbol: String,
    state: Mutex<BybitBookState>,
}

impl BybitLinearParser {
    pub fn new(symbol: CanonicalSymbol) -> Self {
        let source_symbol = format!("{}{}", symbol.base, symbol.quote);
        Self {
            symbol,
            source_symbol,
            state: Mutex::new(BybitBookState::default()),
        }
    }

    fn parse_frame(
        &self,
        frame: &mut [u8],
        recv_us: i64,
    ) -> Result<Vec<NormalizedEvent>, ParseError> {
        let raw_size_bytes = u32::try_from(frame.len()).map_err(|_| ParseError::FrameTooLarge)?;
        let root = simd_json::to_borrowed_value(frame)?;
        let root = root.as_object().ok_or(ParseError::InvalidRoot)?;
        if let Some(events) = parse_control(root)? {
            return Ok(events);
        }
        let topic = required_string(root, "topic")?;
        if topic == format!("orderbook.50.{}", self.source_symbol) {
            let result = self.parse_book(root, topic, recv_us, raw_size_bytes);
            // A book frame can mutate the reconstructed state before a later
            // validation rejects it (for example insufficient depth).  Never
            // let a following delta build on that partial state: force the
            // websocket supervisor to obtain a fresh authoritative snapshot.
            if result.is_err() {
                self.reset();
            }
            result
        } else if topic == format!("publicTrade.{}", self.source_symbol) {
            self.parse_trades(root, topic, recv_us, raw_size_bytes)
        } else {
            Err(ParseError::UnsupportedStream {
                stream: topic.to_owned(),
            })
        }
    }

    fn parse_book(
        &self,
        root: &simd_json::value::borrowed::Object<'_>,
        topic: &str,
        recv_us: i64,
        raw_size_bytes: u32,
    ) -> Result<Vec<NormalizedEvent>, ParseError> {
        let message_type = required_string(root, "type")?;
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if message_type == "snapshot" {
            // A snapshot is authoritative. Clear immediately so any malformed
            // replacement cannot leave an older book eligible for later deltas.
            *guard = BybitBookState::default();
        }
        let outer_ts = timestamp_ms(root, "ts")?;
        let data = required_object(root, "data")?;
        require_symbol(data, &self.source_symbol)?;
        let update_id = required_u64(data, "u")?;
        let replacement = message_type == "snapshot" || update_id == 1;
        if replacement {
            *guard = BybitBookState::default();
        }
        let cross_sequence = required_u64(data, "seq")?;
        let matching_ts = timestamp_ms(data, "cts")?;
        let bid_changes = price_levels(data, "b")?;
        let ask_changes = price_levels(data, "a")?;

        let mut next = guard.clone();
        match (message_type, replacement) {
            ("snapshot", _) | ("delta", true) => {
                next = BybitBookState::default();
                apply_changes(&mut next.bids, bid_changes, "bid")?;
                apply_changes(&mut next.asks, ask_changes, "ask")?;
                next.initialized = true;
            }
            ("delta", false) => {
                if !next.initialized {
                    return Err(ParseError::SnapshotRequired);
                }
                if let Err(error) = check_sequences(&next, update_id, cross_sequence) {
                    // Once continuity is lost, no later delta can safely repair the
                    // reconstruction. Require a new snapshot even for direct callers.
                    *guard = BybitBookState::default();
                    return Err(error);
                }
                apply_changes(&mut next.bids, bid_changes, "bid")?;
                apply_changes(&mut next.asks, ask_changes, "ask")?;
            }
            (value, _) => {
                return Err(ParseError::UnknownEventType {
                    event_type: value.to_owned(),
                });
            }
        }
        next.last_update_id = Some(update_id);
        next.last_cross_sequence = Some(cross_sequence);

        let bids = top_bids(&next.bids)?;
        let asks = top_asks(&next.asks)?;
        let event = NormalizedEvent::Book(BookSnapshot {
            meta: EventMeta {
                schema_version: SCHEMA_VERSION,
                event_id: Uuid::now_v7(),
                adapter: AdapterId::BybitLinear,
                symbol: self.symbol.clone(),
                source_symbol: self.source_symbol.clone(),
                source_stream: topic.to_owned(),
                source_sequence: Some(update_id),
                exchange_event_ts_us: Some(outer_ts),
                exchange_trade_ts_us: Some(matching_ts),
                event_ts_precision: TimestampPrecision::Millisecond,
                trade_ts_precision: TimestampPrecision::Millisecond,
                local_recv_ts_us: recv_us,
                raw_size_bytes,
            },
            bids,
            asks,
        });
        validate_event(&event)?;
        *guard = next;
        Ok(vec![event])
    }

    fn parse_trades(
        &self,
        root: &simd_json::value::borrowed::Object<'_>,
        topic: &str,
        recv_us: i64,
        raw_size_bytes: u32,
    ) -> Result<Vec<NormalizedEvent>, ParseError> {
        let event_type = required_string(root, "type")?;
        if event_type != "snapshot" {
            return Err(ParseError::UnknownEventType {
                event_type: event_type.to_owned(),
            });
        }
        let outer_ts = timestamp_ms(root, "ts")?;
        let data = required_value(root, "data")?
            .as_array()
            .ok_or(ParseError::InvalidField {
                field: "data",
                expected: "an array",
            })?;
        let mut events = Vec::with_capacity(data.len());
        for value in data {
            let trade = value.as_object().ok_or(ParseError::InvalidField {
                field: "data[]",
                expected: "an object",
            })?;
            require_symbol(trade, &self.source_symbol)?;
            let taker_side = match required_string(trade, "S")? {
                "Buy" => TakerSide::Buy,
                "Sell" => TakerSide::Sell,
                _ => {
                    return Err(ParseError::InvalidField {
                        field: "S",
                        expected: "`Buy` or `Sell`",
                    });
                }
            };
            let trade_timestamp = timestamp_ms(trade, "T")?;
            let event = NormalizedEvent::Trade(TradeTick {
                meta: EventMeta {
                    schema_version: SCHEMA_VERSION,
                    event_id: Uuid::now_v7(),
                    adapter: AdapterId::BybitLinear,
                    symbol: self.symbol.clone(),
                    source_symbol: self.source_symbol.clone(),
                    source_stream: topic.to_owned(),
                    source_sequence: Some(required_u64(trade, "seq")?),
                    exchange_event_ts_us: Some(outer_ts),
                    exchange_trade_ts_us: Some(trade_timestamp),
                    event_ts_precision: TimestampPrecision::Millisecond,
                    trade_ts_precision: TimestampPrecision::Millisecond,
                    local_recv_ts_us: recv_us,
                    raw_size_bytes,
                },
                trade_id: required_string(trade, "i")?.to_owned(),
                price: decimal_from_value(required_value(trade, "p")?, "p")?,
                quantity: decimal_from_value(required_value(trade, "v")?, "v")?,
                taker_side,
            });
            validate_event(&event)?;
            events.push(event);
        }
        Ok(events)
    }
}

impl FrameParser for BybitLinearParser {
    fn parse(&self, frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
        self.parse_frame(frame, recv_us)
    }

    fn reset(&self) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = BybitBookState::default();
    }
}

fn check_sequences(
    state: &BybitBookState,
    update_id: u64,
    cross_sequence: u64,
) -> Result<(), ParseError> {
    let previous = state.last_update_id.ok_or(ParseError::SnapshotRequired)?;
    if update_id <= previous {
        return Err(ParseError::SequenceRegression {
            field: "u",
            previous,
            received: update_id,
        });
    }
    if let Some(previous) = state.last_cross_sequence
        && cross_sequence < previous
    {
        return Err(ParseError::SequenceRegression {
            field: "seq",
            previous,
            received: cross_sequence,
        });
    }
    Ok(())
}

fn apply_changes(
    side: &mut BTreeMap<i128, i128>,
    changes: Vec<PriceLevel>,
    side_name: &'static str,
) -> Result<(), ParseError> {
    for level in &changes {
        if level.price <= 0 {
            return Err(ParseError::InvalidBookPrice {
                side: side_name,
                price: level.price,
            });
        }
        if level.quantity < 0 {
            return Err(ParseError::InvalidBookQuantity {
                side: side_name,
                price: level.price,
                quantity: level.quantity,
            });
        }
    }
    for level in changes {
        if level.quantity == 0 {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level.quantity);
        }
    }
    Ok(())
}

fn parse_control(
    root: &simd_json::value::borrowed::Object<'_>,
) -> Result<Option<Vec<NormalizedEvent>>, ParseError> {
    let success = optional_bool(root, "success")?;
    let operation = optional_string(root, "op")?;
    if success == Some(false) {
        return Err(ParseError::ControlFailure {
            op: operation.unwrap_or("unknown").to_owned(),
            detail: optional_string(root, "ret_msg")?
                .filter(|value| !value.is_empty())
                .unwrap_or("unspecified control failure")
                .to_owned(),
        });
    }
    if success == Some(true)
        || matches!(
            operation,
            Some("ping" | "pong" | "subscribe" | "unsubscribe" | "auth")
        )
    {
        return Ok(Some(Vec::new()));
    }
    Ok(None)
}

fn top_bids(side: &BTreeMap<i128, i128>) -> Result<Vec<PriceLevel>, ParseError> {
    require_depth(side, "bid")?;
    Ok(side
        .iter()
        .rev()
        .take(TOP_LEVELS)
        .map(|(&price, &quantity)| PriceLevel { price, quantity })
        .collect())
}

fn top_asks(side: &BTreeMap<i128, i128>) -> Result<Vec<PriceLevel>, ParseError> {
    require_depth(side, "ask")?;
    Ok(side
        .iter()
        .take(TOP_LEVELS)
        .map(|(&price, &quantity)| PriceLevel { price, quantity })
        .collect())
}

fn require_depth(side: &BTreeMap<i128, i128>, side_name: &'static str) -> Result<(), ParseError> {
    if side.len() < TOP_LEVELS {
        Err(ParseError::InsufficientBookDepth {
            side: side_name,
            actual: side.len(),
        })
    } else {
        Ok(())
    }
}

fn price_levels(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<Vec<PriceLevel>, ParseError> {
    required_value(data, field)?
        .as_array()
        .ok_or(ParseError::InvalidField {
            field,
            expected: "an array of price/quantity pairs",
        })?
        .iter()
        .map(|value| {
            let pair = value.as_array().ok_or(ParseError::InvalidField {
                field,
                expected: "an array of price/quantity pairs",
            })?;
            if pair.len() != 2 {
                return Err(ParseError::InvalidField {
                    field,
                    expected: "two-element price/quantity pairs",
                });
            }
            Ok(PriceLevel {
                price: decimal_from_value(&pair[0], field)?,
                quantity: decimal_from_value(&pair[1], field)?,
            })
        })
        .collect()
}

fn require_symbol(
    data: &simd_json::value::borrowed::Object<'_>,
    expected: &str,
) -> Result<(), ParseError> {
    if required_string(data, "s")? == expected {
        Ok(())
    } else {
        Err(ParseError::InvalidField {
            field: "s",
            expected: "the configured uppercase Bybit symbol",
        })
    }
}

fn timestamp_ms(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<i64, ParseError> {
    let value = required_i64(data, field)?;
    ms_to_us(value).map_err(|source| ParseError::Timestamp { field, source })
}

fn required_object<'value>(
    data: &'value simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<&'value simd_json::value::borrowed::Object<'value>, ParseError> {
    required_value(data, field)?
        .as_object()
        .ok_or(ParseError::InvalidField {
            field,
            expected: "an object",
        })
}

fn required_i64(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<i64, ParseError> {
    match required_value(data, field)? {
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
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<u64, ParseError> {
    match required_value(data, field)? {
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

fn optional_bool(
    data: &simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<Option<bool>, ParseError> {
    let Some(value) = data.get(field) else {
        return Ok(None);
    };
    value.as_bool().map(Some).ok_or(ParseError::InvalidField {
        field,
        expected: "a boolean",
    })
}

fn required_value<'value>(
    data: &'value simd_json::value::borrowed::Object<'_>,
    field: &'static str,
) -> Result<&'value BorrowedValue<'value>, ParseError> {
    data.get(field).ok_or(ParseError::MissingField { field })
}
