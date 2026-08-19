use md_core::model::{AdapterId, NormalizedEvent, TimestampPrecision};
use std::collections::HashSet;
use uuid::Uuid;

use crate::{
    DiscoveryError, SubscriptionError,
    discovery::{build_domestic_subscription, parse_domestic_active_markets},
    domestic::{DomesticVenue, ParseError},
};

const VENUE: DomesticVenue = DomesticVenue {
    adapter: AdapterId::UpbitSpot,
    book_timestamp_precision: TimestampPrecision::Millisecond,
    omit_zero_size_book_levels: false,
};

/// Parses one Upbit DEFAULT or SIMPLE WebSocket market-data frame.
pub fn parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
    crate::domestic::parse_frame(frame, recv_us, VENUE)
}

pub fn parse_active_markets(
    payload: &mut [u8],
) -> Result<HashSet<md_core::model::CanonicalSymbol>, DiscoveryError> {
    parse_domestic_active_markets(AdapterId::UpbitSpot, payload)
}

pub fn build_subscription(
    pairs: &[md_core::model::CanonicalSymbol],
    ticket: Uuid,
) -> Result<String, SubscriptionError> {
    build_domestic_subscription(pairs, ticket, true)
}
