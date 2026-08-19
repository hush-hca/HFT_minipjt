use std::collections::HashSet;

use md_core::model::{AdapterId, CanonicalSymbol, NormalizedEvent};

use crate::{
    DiscoveryError, SubscriptionError,
    binance::{BinanceVenue, parse_frame as parse_binance_frame},
    discovery::{build_binance_subscription_query, parse_binance_active_markets},
    domestic::ParseError,
};

const VENUE: BinanceVenue = BinanceVenue {
    adapter: AdapterId::BinanceUsdm,
    require_book_symbol: true,
};

/// Parses one Binance USDⓈ-M combined-stream trade or partial-depth frame.
pub fn parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
    parse_binance_frame(frame, recv_us, VENUE)
}

pub fn parse_active_markets(
    payload: &mut [u8],
) -> Result<HashSet<CanonicalSymbol>, DiscoveryError> {
    parse_binance_active_markets(AdapterId::BinanceUsdm, payload, true)
}

pub fn build_subscription(pairs: &[CanonicalSymbol]) -> Result<String, SubscriptionError> {
    build_binance_subscription_query(pairs)
}
