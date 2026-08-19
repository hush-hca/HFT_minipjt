use md_core::model::{AdapterId, NormalizedEvent, TimestampPrecision};
use std::collections::HashSet;
use uuid::Uuid;

use crate::{
    DiscoveryError, SubscriptionError,
    discovery::{build_domestic_subscription, parse_domestic_active_markets},
    domestic::{DomesticVenue, ParseError},
};

const VENUE: DomesticVenue = DomesticVenue {
    adapter: AdapterId::BithumbSpot,
    book_timestamp_precision: TimestampPrecision::Microsecond,
    omit_zero_size_book_levels: true,
};

/// Parses one Bithumb DEFAULT or SIMPLE WebSocket market-data frame.
pub fn parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
    crate::domestic::parse_frame(frame, recv_us, VENUE)
}

pub fn parse_active_markets(
    payload: &mut [u8],
) -> Result<HashSet<md_core::model::CanonicalSymbol>, DiscoveryError> {
    parse_domestic_active_markets(AdapterId::BithumbSpot, payload)
}

pub fn build_subscription(
    pairs: &[md_core::model::CanonicalSymbol],
    ticket: Uuid,
) -> Result<String, SubscriptionError> {
    build_domestic_subscription(pairs, ticket, false)
}
