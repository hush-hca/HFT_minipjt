use md_core::model::{AdapterId, CanonicalSymbol, NormalizedEvent, TakerSide, TimestampPrecision};
use md_exchanges::{BybitLinearParser, FrameParser, ParseError};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("tests/fixtures/{name}")).unwrap()
}

fn parse_json(
    parser: &BybitLinearParser,
    json: String,
    recv_us: i64,
) -> Result<Vec<NormalizedEvent>, ParseError> {
    parser.parse(&mut json.into_bytes(), recv_us)
}

fn book(events: &[NormalizedEvent]) -> &md_core::model::BookSnapshot {
    let [NormalizedEvent::Book(book)] = events else {
        panic!("expected exactly one book event");
    };
    book
}

#[test]
fn snapshot_then_forward_delta_with_equal_cross_sequence_is_valid() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    let first = parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let first = book(&first);
    assert_eq!(first.bids.len(), 20);
    assert_eq!(first.asks.len(), 20);
    assert_eq!(first.meta.source_sequence, Some(100));
    assert_eq!(first.meta.exchange_event_ts_us, Some(1_700_000_000_000_000));
    assert_eq!(first.meta.exchange_trade_ts_us, Some(1_699_999_999_999_000));

    let second = parser
        .parse(&mut fixture("bybit_book_delta.json"), 1_700_000_000_101_000)
        .unwrap();
    let second = book(&second);
    assert_eq!(second.bids.len(), 20);
    assert_eq!(second.asks.len(), 20);
    assert_eq!(second.meta.source_sequence, Some(105));
    assert!(
        second
            .bids
            .windows(2)
            .all(|pair| pair[0].price > pair[1].price)
    );
    assert!(
        second
            .asks
            .windows(2)
            .all(|pair| pair[0].price < pair[1].price)
    );
    assert_eq!(second.bids[0].price, 99_500_000_000_000_000_000);
    assert!(
        !second
            .bids
            .iter()
            .any(|level| level.price == 100_000_000_000_000_000_000)
    );
    assert!(
        !second
            .asks
            .iter()
            .any(|level| level.price == 102_000_000_000_000_000_000)
    );
}

#[test]
fn rejected_negative_quantities_require_a_snapshot_before_replacement_delta() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let negative = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[["99","-1"]],"a":[],"u":101,"seq":1001,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&parser, negative.into(), 1_700_000_000_101_000),
        Err(ParseError::InvalidBookQuantity { .. })
    ));

    let replacement = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000200,"data":{"s":"BTCUSDT","b":[["99","7"],["99","8"]],"a":[],"u":101,"seq":1001,"cts":1700000000199}}"#;
    assert!(matches!(
        parse_json(&parser, replacement.to_owned(), 1_700_000_000_201_000),
        Err(ParseError::SnapshotRequired)
    ));
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let events = parse_json(&parser, replacement.into(), 1_700_000_000_201_000).unwrap();
    assert_eq!(
        book(&events)
            .bids
            .iter()
            .find(|level| level.price == 99_000_000_000_000_000_000)
            .unwrap()
            .quantity,
        8_000_000_000_000_000_000
    );
}

#[test]
fn regressions_and_reset_require_a_new_snapshot() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let regression = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[],"a":[],"u":100,"seq":1001,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&parser, regression.into(), 1_700_000_000_101_000),
        Err(ParseError::SequenceRegression { field: "u", .. })
    ));

    let delta = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[],"a":[],"u":101,"seq":1001,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&parser, delta.into(), 1_700_000_000_101_000),
        Err(ParseError::SnapshotRequired)
    ));

    parser.reset();
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let regression = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[],"a":[],"u":105,"seq":999,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&parser, regression.into(), 1_700_000_000_101_000),
        Err(ParseError::SequenceRegression { field: "seq", .. })
    ));

    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let update_regression = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[],"a":[],"u":100,"seq":1001,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&parser, update_regression.into(), 1_700_000_000_101_000),
        Err(ParseError::SequenceRegression { field: "u", .. })
    ));
}

#[test]
fn service_restart_snapshot_with_update_id_one_replaces_state() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let mut replacement = String::from_utf8(fixture("bybit_book_snapshot.json")).unwrap();
    replacement = replacement.replacen("\"u\": 100", "\"u\": 1", 1);
    let events = parse_json(&parser, replacement, 1_700_000_000_001_000).unwrap();
    assert_eq!(book(&events).meta.source_sequence, Some(1));
}

#[test]
fn control_frames_are_ignored_on_success_and_failures_require_reconnect() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    for json in [
        r#"{"success":true,"ret_msg":"","op":"subscribe","conn_id":"conn"}"#,
        r#"{"success":true,"ret_msg":"pong","op":"ping","conn_id":"conn"}"#,
        r#"{"args":["1700000000000"],"op":"pong"}"#,
    ] {
        assert!(
            parse_json(&parser, json.into(), 1_700_000_000_001_000)
                .unwrap()
                .is_empty()
        );
    }
    for operation in ["subscribe", "auth", "ping"] {
        let error = parse_json(
            &parser,
            format!(
                r#"{{"success":false,"ret_msg":"rejected","op":"{operation}","conn_id":"conn"}}"#
            ),
            1_700_000_000_001_000,
        )
        .unwrap_err();
        assert!(matches!(error, ParseError::ControlFailure { .. }));
        assert!(error.requires_reconnect());
    }
}

#[test]
fn hidden_non_positive_prices_invalidate_delta_state() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let invalid_delta = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[["-1","2"]],"a":[],"u":105,"seq":1000,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&parser, invalid_delta.into(), 1_700_000_000_101_000),
        Err(ParseError::InvalidBookPrice { .. })
    ));
    assert!(matches!(
        parser.parse(&mut fixture("bybit_book_delta.json"), 1_700_000_000_101_000),
        Err(ParseError::SnapshotRequired)
    ));
    parser
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    let valid = parser
        .parse(&mut fixture("bybit_book_delta.json"), 1_700_000_000_101_000)
        .unwrap();
    assert_eq!(book(&valid).meta.source_sequence, Some(105));

    let snapshot = String::from_utf8(fixture("bybit_book_snapshot.json")).unwrap();
    let hidden_zero = snapshot.replacen("[\"80\", \"21\"]", "[\"0\", \"21\"]", 1);
    assert!(matches!(
        parse_json(&parser, hidden_zero, 1_700_000_000_001_000),
        Err(ParseError::InvalidBookPrice { .. })
    ));
    assert!(matches!(
        parser.parse(&mut fixture("bybit_book_delta.json"), 1_700_000_000_101_000),
        Err(ParseError::SnapshotRequired)
    ));
}

#[test]
fn malformed_crossed_and_insufficient_books_are_rejected() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    let empty = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1700000000000,"data":{"s":"BTCUSDT","b":[],"a":[],"u":1,"seq":1,"cts":1700000000000}}"#;
    assert!(matches!(
        parse_json(&parser, empty.into(), 1_700_000_000_001_000),
        Err(ParseError::InsufficientBookDepth { .. })
    ));
    let bids = (0..20)
        .map(|offset| format!(r#"["{}","1"]"#, 101 - offset))
        .collect::<Vec<_>>()
        .join(",");
    let asks = (0..20)
        .map(|offset| format!(r#"["{}","1"]"#, 100 + offset))
        .collect::<Vec<_>>()
        .join(",");
    let crossed = format!(
        r#"{{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1700000000000,"data":{{"s":"BTCUSDT","b":[{bids}],"a":[{asks}],"u":1,"seq":1,"cts":1700000000000}}}}"#
    );
    assert!(matches!(
        parse_json(&parser, crossed, 1_700_000_000_001_000),
        Err(ParseError::Validation(_))
    ));
    let malformed = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":"bad","data":{}}"#;
    assert!(parse_json(&parser, malformed.into(), 1_700_000_000_001_000).is_err());

    let seeded = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    seeded
        .parse(
            &mut fixture("bybit_book_snapshot.json"),
            1_700_000_000_001_000,
        )
        .unwrap();
    assert!(parse_json(&seeded, malformed.into(), 1_700_000_000_001_000).is_err());
    let delta = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000100,"data":{"s":"BTCUSDT","b":[],"a":[],"u":101,"seq":1001,"cts":1700000000099}}"#;
    assert!(matches!(
        parse_json(&seeded, delta.into(), 1_700_000_000_101_000),
        Err(ParseError::SnapshotRequired)
    ));
    // Any rejected book frame invalidates the reconstruction; a later delta
    // must not be accepted until a new snapshot arrives.
    let valid_delta = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000101,"data":{"s":"BTCUSDT","b":[],"a":[],"u":102,"seq":1002,"cts":1700000000100}}"#;
    assert!(matches!(
        parse_json(&seeded, valid_delta.into(), 1_700_000_000_102_000),
        Err(ParseError::SnapshotRequired)
    ));
}

#[test]
fn public_trade_array_emits_every_individual_execution_with_exact_fields() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    let recv_us = 1_700_000_000_201_234;
    let events = parser
        .parse(&mut fixture("bybit_trade.json"), recv_us)
        .unwrap();
    assert_eq!(events.len(), 2);
    let trades: Vec<_> = events
        .into_iter()
        .map(|event| match event {
            NormalizedEvent::Trade(trade) => trade,
            _ => panic!("expected trade"),
        })
        .collect();
    assert_eq!(trades[0].trade_id, "trade-a");
    assert_eq!(trades[0].price, 100_250_000_000_000_000_000);
    assert_eq!(trades[0].quantity, 125_000_000_000_000_000);
    assert_eq!(trades[0].taker_side, TakerSide::Buy);
    assert_eq!(trades[1].taker_side, TakerSide::Sell);
    assert_eq!(trades[0].meta.source_sequence, Some(1_783_284_617));
    assert_eq!(trades[1].meta.source_sequence, Some(1_783_284_617));
    assert!(
        trades
            .iter()
            .all(|trade| trade.meta.adapter == AdapterId::BybitLinear)
    );
    assert!(
        trades
            .iter()
            .all(|trade| trade.meta.local_recv_ts_us == recv_us)
    );
    assert_eq!(
        trades[0].meta.exchange_event_ts_us,
        Some(1_700_000_000_200_000)
    );
    assert_eq!(
        trades[0].meta.exchange_trade_ts_us,
        Some(1_700_000_000_198_000)
    );
    assert_eq!(
        trades[0].meta.event_ts_precision,
        TimestampPrecision::Millisecond
    );
}
