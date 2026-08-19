use md_core::{
    decimal::parse_decimal_18,
    model::{AdapterId, NormalizedEvent, TakerSide, TimestampPrecision},
    validation::validate_event,
};
use md_exchanges::ParseError;

const RECV_US: i64 = 1_725_929_934_500_000;
type Parser = fn(&mut [u8], i64) -> Result<Vec<NormalizedEvent>, ParseError>;

#[test]
fn upbit_trade_maps_both_exchange_times() {
    let mut bytes = include_bytes!("fixtures/upbit_trade.json").to_vec();
    let events = md_exchanges::upbit::parse_frame(&mut bytes, RECV_US).unwrap();
    let NormalizedEvent::Trade(trade) = &events[0] else {
        panic!("expected trade")
    };

    assert_eq!(events.len(), 1);
    assert_eq!(trade.meta.adapter, AdapterId::UpbitSpot);
    assert_eq!(trade.meta.exchange_event_ts_us, Some(1_725_929_934_483_000));
    assert_eq!(trade.meta.exchange_trade_ts_us, Some(1_725_929_934_373_000));
    assert_eq!(
        trade.meta.event_ts_precision,
        TimestampPrecision::Millisecond
    );
    assert_eq!(
        trade.meta.trade_ts_precision,
        TimestampPrecision::Millisecond
    );
    assert_eq!(trade.meta.source_sequence, Some(17_259_299_343_730_000));
    assert_eq!(trade.trade_id, "17259299343730000");
    assert_eq!(trade.taker_side, TakerSide::Buy);
    assert_eq!(trade.price, parse_decimal_18("489700").unwrap());
    assert_eq!(trade.quantity, parse_decimal_18("1.4825").unwrap());
    validate_event(&events[0]).unwrap();
}

#[test]
fn upbit_book_maps_snapshot_metadata_and_levels() {
    let mut bytes = include_bytes!("fixtures/upbit_book.json").to_vec();
    let raw_len = bytes.len() as u32;
    let events = md_exchanges::upbit::parse_frame(&mut bytes, RECV_US).unwrap();
    let NormalizedEvent::Book(book) = &events[0] else {
        panic!("expected book")
    };

    assert_eq!(book.meta.adapter, AdapterId::UpbitSpot);
    assert_eq!(book.meta.symbol.base, "BTC");
    assert_eq!(book.meta.symbol.quote, "KRW");
    assert_eq!(book.meta.source_symbol, "KRW-BTC");
    assert_eq!(book.meta.source_stream, "orderbook");
    assert_eq!(book.meta.raw_size_bytes, raw_len);
    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 2);
    assert_eq!(book.bids[0].price, parse_decimal_18("489700").unwrap());
    assert_eq!(book.asks[1].quantity, parse_decimal_18("2.5").unwrap());
    validate_event(&events[0]).unwrap();
}

#[test]
fn bithumb_book_expands_all_available_levels() {
    let mut bytes = include_bytes!("fixtures/bithumb_book.json").to_vec();
    let events = md_exchanges::bithumb::parse_frame(&mut bytes, RECV_US).unwrap();
    let NormalizedEvent::Book(book) = &events[0] else {
        panic!("expected book")
    };

    assert_eq!(book.meta.adapter, AdapterId::BithumbSpot);
    assert_eq!(book.meta.exchange_event_ts_us, Some(1_725_929_934_483_000));
    assert_eq!(
        book.meta.event_ts_precision,
        TimestampPrecision::Microsecond
    );
    assert_eq!(
        book.meta.trade_ts_precision,
        TimestampPrecision::Unavailable
    );
    assert_eq!(book.bids.len(), 15);
    assert_eq!(book.asks.len(), 15);
    validate_event(&events[0]).unwrap();
}

#[test]
fn bithumb_trade_maps_side_sequence_and_times() {
    let mut bytes = include_bytes!("fixtures/bithumb_trade.json").to_vec();
    let events = md_exchanges::bithumb::parse_frame(&mut bytes, RECV_US).unwrap();
    let NormalizedEvent::Trade(trade) = &events[0] else {
        panic!("expected trade")
    };

    assert_eq!(trade.meta.adapter, AdapterId::BithumbSpot);
    assert_eq!(trade.meta.exchange_event_ts_us, Some(1_725_929_934_483_000));
    assert_eq!(trade.meta.exchange_trade_ts_us, Some(1_725_929_934_373_000));
    assert_eq!(trade.taker_side, TakerSide::Buy);
    assert_eq!(trade.trade_id, "17259299343730000");
    validate_event(&events[0]).unwrap();
}

#[test]
fn simple_aliases_parse_for_both_venues_and_event_kinds() {
    let cases: [(&str, Parser); 4] = [
        (
            r#"{"ty":"orderbook","cd":"KRW-BTC","tms":1725929934483,"obu":[{"ap":489800.0,"bp":489700.0,"as":"1.25","bs":1.5}],"st":"REALTIME"}"#,
            md_exchanges::upbit::parse_frame,
        ),
        (
            r#"{"ty":"trade","cd":"KRW-BTC","tms":1725929934483,"ttms":1725929934373,"tp":"489700.125","tv":0.00008428,"ab":"ASK","sid":17259299343730000,"st":"REALTIME"}"#,
            md_exchanges::upbit::parse_frame,
        ),
        (
            r#"{"ty":"orderbook","cd":"KRW-BTC","tms":1725929934483000,"obu":[{"ap":489800,"bp":489700,"as":1.25,"bs":"1.5"}],"lv":1,"st":"REALTIME"}"#,
            md_exchanges::bithumb::parse_frame,
        ),
        (
            r#"{"ty":"trade","cd":"KRW-BTC","tms":1725929934483,"ttms":1725929934373,"tp":489700,"tv":"1.4825","ab":"ASK","sid":17259299343730000,"st":"REALTIME"}"#,
            md_exchanges::bithumb::parse_frame,
        ),
    ];

    for (payload, parser) in cases {
        let mut bytes = payload.as_bytes().to_vec();
        let events = parser(&mut bytes, RECV_US).unwrap();
        assert_eq!(events.len(), 1);
        validate_event(&events[0]).unwrap();
    }
}

#[test]
fn floating_nodes_are_rounded_only_by_ryus_exact_f64_representation() {
    let mut bytes = br#"{"ty":"trade","cd":"KRW-BTC","tms":1725929934483,"ttms":1725929934373,"tp":4.89700125e5,"tv":8.428e-5,"ab":"ASK","sid":17259299343730000}"#.to_vec();
    let events = md_exchanges::upbit::parse_frame(&mut bytes, RECV_US).unwrap();
    let NormalizedEvent::Trade(trade) = &events[0] else {
        panic!("expected trade")
    };
    assert_eq!(trade.price, parse_decimal_18("489700.125").unwrap());
    assert_eq!(trade.quantity, parse_decimal_18("0.00008428").unwrap());
}

#[test]
fn scientific_float_output_is_expanded_before_decimal_parsing() {
    let mut bytes = br#"{"ty":"trade","cd":"KRW-BTC","tms":1725929934483,"ttms":1725929934373,"tp":489700,"tv":0.00000001,"ab":"ASK","sid":17259299343730000}"#.to_vec();
    let events = md_exchanges::upbit::parse_frame(&mut bytes, RECV_US).unwrap();
    let NormalizedEvent::Trade(trade) = &events[0] else {
        panic!("expected trade")
    };
    assert_eq!(trade.quantity, parse_decimal_18("0.00000001").unwrap());
}

#[test]
fn keepalive_returns_no_market_event() {
    for parser in [
        md_exchanges::upbit::parse_frame,
        md_exchanges::bithumb::parse_frame,
    ] {
        let mut bytes = br#"{"status":"UP"}"#.to_vec();
        assert!(parser(&mut bytes, RECV_US).unwrap().is_empty());
    }
}

#[test]
fn invalid_frames_return_typed_errors() {
    let invalid = [
        (
            r#"{"type":"trade","timestamp":1725929934483,"trade_timestamp":1725929934373,"trade_price":1,"trade_volume":1,"ask_bid":"BID","sequential_id":1}"#,
            "missing symbol",
        ),
        (
            r#"{"type":"trade","code":"KRW-BTC","trade_price":1,"trade_volume":1,"ask_bid":"BID","sequential_id":1}"#,
            "missing timestamp",
        ),
        (
            r#"{"type":"trade","code":"KRW-BTC","timestamp":1725929934483,"trade_timestamp":1725929934373,"trade_price":-1,"trade_volume":1,"ask_bid":"BID","sequential_id":1}"#,
            "negative value",
        ),
        (
            r#"{"type":"orderbook","code":"KRW-BTC","timestamp":1725929934483,"orderbook_units":[{"ask_price":100,"bid_price":101,"ask_size":1,"bid_size":1}]}"#,
            "crossed book",
        ),
        (
            r#"{"type":"ticker","code":"KRW-BTC","timestamp":1725929934483}"#,
            "unknown type",
        ),
        (
            r#"{"type":"trade","code":"KRW-BTC","timestamp":1,"trade_timestamp":1,"trade_price":1,"trade_volume":1,"ask_bid":"BID","sequential_id":1}"#,
            "timestamp bounds",
        ),
    ];

    for (payload, label) in invalid {
        let mut bytes = payload.as_bytes().to_vec();
        let error = md_exchanges::upbit::parse_frame(&mut bytes, RECV_US).expect_err(label);
        assert!(
            matches!(
                error,
                ParseError::MissingField { .. }
                    | ParseError::UnknownEventType { .. }
                    | ParseError::Validation(_)
            ),
            "{label}: {error:?}"
        );
    }
}

#[test]
fn malformed_json_and_bad_field_types_are_distinct() {
    let mut malformed = br#"{"type":"trade"#.to_vec();
    assert!(matches!(
        md_exchanges::upbit::parse_frame(&mut malformed, RECV_US),
        Err(ParseError::Json(_))
    ));

    let mut wrong_type = br#"{"type":"trade","code":7,"timestamp":1725929934483}"#.to_vec();
    assert!(matches!(
        md_exchanges::upbit::parse_frame(&mut wrong_type, RECV_US),
        Err(ParseError::InvalidField { .. })
    ));
}
