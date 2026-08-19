use thiserror::Error;

use crate::model::{BookSnapshot, EventMeta, NormalizedEvent, TradeTick};

const SEVEN_DAYS_US: i64 = 7 * 24 * 60 * 60 * 1_000_000;
const ONE_DAY_US: i64 = 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TimestampField {
    ExchangeEvent,
    ExchangeTrade,
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum ValidationError {
    #[error("local receive timestamp must be positive, got {value}")]
    NonPositiveLocalTimestamp { value: i64 },
    #[error("{field:?} timestamp {value} is outside [{minimum}, {maximum}]")]
    SourceTimestampOutOfRange {
        field: TimestampField,
        value: i64,
        minimum: i64,
        maximum: i64,
    },
    #[error("{side:?} side is empty")]
    EmptyBookSide { side: BookSide },
    #[error("{side:?} level {level} has non-positive price {value}")]
    NonPositiveBookPrice {
        side: BookSide,
        level: usize,
        value: i128,
    },
    #[error("{side:?} level {level} has non-positive quantity {value}")]
    NonPositiveBookQuantity {
        side: BookSide,
        level: usize,
        value: i128,
    },
    #[error("{side:?} levels {previous_level} and {level} are not strictly ordered")]
    UnsortedBook {
        side: BookSide,
        previous_level: usize,
        level: usize,
    },
    #[error("book is crossed or locked: best bid {best_bid} is not below best ask {best_ask}")]
    CrossedBook { best_bid: i128, best_ask: i128 },
    #[error("trade has non-positive price {value}")]
    NonPositiveTradePrice { value: i128 },
    #[error("trade has non-positive quantity {value}")]
    NonPositiveTradeQuantity { value: i128 },
}

pub fn validate_event(event: &NormalizedEvent) -> Result<(), ValidationError> {
    validate_meta(event.meta())?;
    match event {
        NormalizedEvent::Book(book) => validate_book(book),
        NormalizedEvent::Trade(trade) => validate_trade(trade),
    }
}

fn validate_meta(meta: &EventMeta) -> Result<(), ValidationError> {
    if meta.local_recv_ts_us <= 0 {
        return Err(ValidationError::NonPositiveLocalTimestamp {
            value: meta.local_recv_ts_us,
        });
    }

    let minimum = meta.local_recv_ts_us.saturating_sub(SEVEN_DAYS_US);
    let maximum = meta.local_recv_ts_us.saturating_add(ONE_DAY_US);
    validate_source_timestamp(
        TimestampField::ExchangeEvent,
        meta.exchange_event_ts_us,
        minimum,
        maximum,
    )?;
    validate_source_timestamp(
        TimestampField::ExchangeTrade,
        meta.exchange_trade_ts_us,
        minimum,
        maximum,
    )
}

fn validate_source_timestamp(
    field: TimestampField,
    value: Option<i64>,
    minimum: i64,
    maximum: i64,
) -> Result<(), ValidationError> {
    if let Some(value) = value
        && !(minimum..=maximum).contains(&value)
    {
        return Err(ValidationError::SourceTimestampOutOfRange {
            field,
            value,
            minimum,
            maximum,
        });
    }
    Ok(())
}

fn validate_book(book: &BookSnapshot) -> Result<(), ValidationError> {
    if book.bids.is_empty() {
        return Err(ValidationError::EmptyBookSide {
            side: BookSide::Bid,
        });
    }
    if book.asks.is_empty() {
        return Err(ValidationError::EmptyBookSide {
            side: BookSide::Ask,
        });
    }

    validate_levels(&book.bids, BookSide::Bid)?;
    validate_levels(&book.asks, BookSide::Ask)?;

    let best_bid = book.bids[0].price;
    let best_ask = book.asks[0].price;
    if best_bid >= best_ask {
        return Err(ValidationError::CrossedBook { best_bid, best_ask });
    }
    Ok(())
}

fn validate_levels(
    levels: &[crate::model::PriceLevel],
    side: BookSide,
) -> Result<(), ValidationError> {
    for (level, value) in levels.iter().enumerate() {
        if value.price <= 0 {
            return Err(ValidationError::NonPositiveBookPrice {
                side,
                level,
                value: value.price,
            });
        }
        if value.quantity <= 0 {
            return Err(ValidationError::NonPositiveBookQuantity {
                side,
                level,
                value: value.quantity,
            });
        }
    }

    for (previous_level, pair) in levels.windows(2).enumerate() {
        let ordered = match side {
            BookSide::Bid => pair[0].price > pair[1].price,
            BookSide::Ask => pair[0].price < pair[1].price,
        };
        if !ordered {
            return Err(ValidationError::UnsortedBook {
                side,
                previous_level,
                level: previous_level + 1,
            });
        }
    }
    Ok(())
}

fn validate_trade(trade: &TradeTick) -> Result<(), ValidationError> {
    if trade.price <= 0 {
        return Err(ValidationError::NonPositiveTradePrice { value: trade.price });
    }
    if trade.quantity <= 0 {
        return Err(ValidationError::NonPositiveTradeQuantity {
            value: trade.quantity,
        });
    }
    Ok(())
}
