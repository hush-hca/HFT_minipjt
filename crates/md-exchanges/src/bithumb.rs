use md_core::model::{AdapterId, NormalizedEvent, TimestampPrecision};

use crate::domestic::{DomesticVenue, ParseError};

const VENUE: DomesticVenue = DomesticVenue {
    adapter: AdapterId::BithumbSpot,
    book_timestamp_precision: TimestampPrecision::Microsecond,
};

/// Parses one Bithumb DEFAULT or SIMPLE WebSocket market-data frame.
pub fn parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
    crate::domestic::parse_frame(frame, recv_us, VENUE)
}
