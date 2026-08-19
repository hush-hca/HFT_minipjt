use md_core::decimal::parse_decimal_18;
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel, TakerSide,
    TimestampPrecision, TradeTick, ms_to_us,
};
use md_core::validation::validate_event;
use uuid::Uuid;

const ONE: i128 = 1_000_000_000_000_000_000;

fn meta() -> EventMeta {
    EventMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        adapter: AdapterId::BinanceSpot,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        source_symbol: "BTCUSDT".into(),
        source_stream: "btcusdt@depth20@100ms".into(),
        source_sequence: Some(42),
        exchange_event_ts_us: Some(1_725_929_934_373_000),
        exchange_trade_ts_us: None,
        event_ts_precision: TimestampPrecision::Millisecond,
        trade_ts_precision: TimestampPrecision::Unavailable,
        local_recv_ts_us: 1_725_929_934_374_000,
        raw_size_bytes: 512,
    }
}

fn fixture_book(bids: Vec<(i128, i128)>, asks: Vec<(i128, i128)>) -> BookSnapshot {
    BookSnapshot {
        meta: meta(),
        bids: bids
            .into_iter()
            .map(|(price, quantity)| PriceLevel { price, quantity })
            .collect(),
        asks: asks
            .into_iter()
            .map(|(price, quantity)| PriceLevel { price, quantity })
            .collect(),
    }
}

#[test]
fn decimal_preserves_scale_and_rejects_excess_precision() {
    assert_eq!(
        parse_decimal_18("123.45").unwrap(),
        123_450_000_000_000_000_000
    );
    assert_eq!(parse_decimal_18("0.00000001").unwrap(), 10_000_000_000);
    assert!(parse_decimal_18("1.0000000000000000001").is_err());
}

#[test]
fn decimal_accepts_precision_boundary_and_rejects_overflow() {
    assert_eq!(
        parse_decimal_18("99999999999999999999.999999999999999999").unwrap(),
        99_999_999_999_999_999_999_999_999_999_999_999_999
    );
    assert_eq!(
        parse_decimal_18("-99999999999999999999.999999999999999999").unwrap(),
        -99_999_999_999_999_999_999_999_999_999_999_999_999
    );
    assert!(parse_decimal_18("100000000000000000000").is_err());
    assert!(parse_decimal_18("1e3").is_err());
    assert!(parse_decimal_18("1.2.3").is_err());
}

#[test]
fn millisecond_timestamp_retains_declared_precision() {
    assert_eq!(ms_to_us(1_725_929_934_373).unwrap(), 1_725_929_934_373_000);
    assert!(ms_to_us(i64::MAX).is_err());
}

#[test]
fn valid_book_and_trade_are_accepted() {
    let book = fixture_book(
        vec![(100 * ONE, ONE), (99 * ONE, 2 * ONE)],
        vec![(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
    );
    validate_event(&NormalizedEvent::Book(book)).unwrap();

    let trade = TradeTick {
        meta: meta(),
        trade_id: "12345".into(),
        price: 100 * ONE,
        quantity: ONE,
        taker_side: TakerSide::Buy,
    };
    validate_event(&NormalizedEvent::Trade(trade)).unwrap();
}

#[test]
fn crossed_or_unsorted_book_is_rejected() {
    let unsorted = fixture_book(
        vec![(100 * ONE, ONE), (101 * ONE, ONE)],
        vec![(102 * ONE, ONE)],
    );
    assert!(validate_event(&NormalizedEvent::Book(unsorted)).is_err());

    let crossed = fixture_book(vec![(103 * ONE, ONE)], vec![(102 * ONE, ONE)]);
    assert!(validate_event(&NormalizedEvent::Book(crossed)).is_err());
}

#[test]
fn empty_side_or_zero_quantity_is_rejected() {
    let empty = fixture_book(vec![], vec![(102 * ONE, ONE)]);
    assert!(validate_event(&NormalizedEvent::Book(empty)).is_err());

    let zero_quantity = fixture_book(vec![(100 * ONE, 0)], vec![(102 * ONE, ONE)]);
    assert!(validate_event(&NormalizedEvent::Book(zero_quantity)).is_err());
}

#[test]
fn timestamp_outside_allowed_window_is_rejected() {
    let mut book = fixture_book(vec![(100 * ONE, ONE)], vec![(102 * ONE, ONE)]);
    book.meta.exchange_event_ts_us =
        Some(book.meta.local_recv_ts_us - (7 * 24 * 60 * 60 * 1_000_000) - 1);
    assert!(validate_event(&NormalizedEvent::Book(book)).is_err());
}
