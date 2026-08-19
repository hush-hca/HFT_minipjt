use md_core::model::{AdapterId, NormalizedEvent};

use crate::{
    binance::{BinanceVenue, parse_frame as parse_binance_frame},
    domestic::ParseError,
};

const VENUE: BinanceVenue = BinanceVenue {
    adapter: AdapterId::BinanceSpot,
    require_book_symbol: false,
};

/// Parses one Binance Spot combined-stream trade or partial-depth frame.
pub fn parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
    parse_binance_frame(frame, recv_us, VENUE)
}
