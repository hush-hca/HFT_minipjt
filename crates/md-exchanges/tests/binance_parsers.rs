use md_core::{
    decimal::parse_decimal_18,
    model::{AdapterId, CanonicalSymbol, NormalizedEvent, TakerSide, TimestampPrecision},
    validation::{ValidationError, validate_event},
};
use md_exchanges::ParseError;

const RECV_US: i64 = 1_672_515_782_200_000;

#[test]
fn spot_trade_maps_buyer_maker_to_sell_aggressor() {
    let mut bytes = include_bytes!("fixtures/binance_spot_trade.json").to_vec();
    let event = md_exchanges::binance_spot::parse_frame(&mut bytes, RECV_US)
        .unwrap()
        .remove(0);
    let NormalizedEvent::Trade(trade) = event else {
        panic!("expected trade")
    };

    assert_eq!(trade.meta.adapter, AdapterId::BinanceSpot);
    assert_eq!(trade.meta.symbol, CanonicalSymbol::new("BTC", "USDT"));
    assert_eq!(trade.meta.source_symbol, "BTCUSDT");
    assert_eq!(trade.meta.source_stream, "btcusdt@trade");
    assert_eq!(trade.meta.source_sequence, Some(12_345));
    assert_eq!(trade.trade_id, "12345");
    assert_eq!(trade.taker_side, TakerSide::Sell);
    assert_eq!(trade.meta.exchange_event_ts_us, Some(1_672_515_782_136_000));
    assert_eq!(trade.meta.exchange_trade_ts_us, Some(1_672_515_782_136_000));
    assert_eq!(
        trade.meta.event_ts_precision,
        TimestampPrecision::Millisecond
    );
    assert_eq!(
        trade.meta.trade_ts_precision,
        TimestampPrecision::Millisecond
    );
    assert_eq!(trade.price, parse_decimal_18("16550.12500000").unwrap());
    assert_eq!(trade.quantity, parse_decimal_18("0.00125000").unwrap());
}

#[test]
fn spot_partial_book_derives_symbol_and_allows_unavailable_event_time() {
    let mut bytes = include_bytes!("fixtures/binance_spot_book.json").to_vec();
    let event = md_exchanges::binance_spot::parse_frame(&mut bytes, RECV_US)
        .unwrap()
        .remove(0);
    let NormalizedEvent::Book(book) = &event else {
        panic!("expected book")
    };

    assert_eq!(book.meta.adapter, AdapterId::BinanceSpot);
    assert_eq!(book.meta.symbol, CanonicalSymbol::new("BTC", "USDT"));
    assert_eq!(book.meta.source_symbol, "BTCUSDT");
    assert_eq!(book.meta.source_stream, "btcusdt@depth20@100ms");
    assert_eq!(book.meta.source_sequence, Some(160));
    assert_eq!(book.meta.exchange_event_ts_us, None);
    assert_eq!(book.meta.exchange_trade_ts_us, None);
    assert_eq!(
        book.meta.event_ts_precision,
        TimestampPrecision::Unavailable
    );
    assert_eq!(
        book.meta.trade_ts_precision,
        TimestampPrecision::Unavailable
    );
    assert_eq!(book.bids.len(), 20);
    assert_eq!(book.asks.len(), 20);
    validate_event(&event).unwrap();
}

#[test]
fn usdm_trade_preserves_event_and_trade_times_and_maps_aggressor() {
    let mut bytes = include_bytes!("fixtures/binance_usdm_trade.json").to_vec();
    let event = md_exchanges::binance_usdm::parse_frame(&mut bytes, RECV_US)
        .unwrap()
        .remove(0);
    let NormalizedEvent::Trade(trade) = event else {
        panic!("expected trade")
    };

    assert_eq!(trade.meta.adapter, AdapterId::BinanceUsdm);
    assert_eq!(trade.meta.symbol, CanonicalSymbol::new("ETH", "USDT"));
    assert_eq!(trade.meta.source_sequence, Some(98_765));
    assert_eq!(trade.trade_id, "98765");
    assert_eq!(trade.taker_side, TakerSide::Buy);
    assert_eq!(trade.meta.exchange_event_ts_us, Some(1_672_515_782_140_000));
    assert_eq!(trade.meta.exchange_trade_ts_us, Some(1_672_515_782_139_000));
}

#[test]
fn usdm_partial_book_preserves_event_transaction_times_and_twenty_levels() {
    let mut bytes = include_bytes!("fixtures/binance_usdm_book.json").to_vec();
    let event = md_exchanges::binance_usdm::parse_frame(&mut bytes, RECV_US)
        .unwrap()
        .remove(0);
    let NormalizedEvent::Book(book) = &event else {
        panic!("expected book")
    };

    assert_eq!(book.meta.adapter, AdapterId::BinanceUsdm);
    assert_eq!(book.meta.symbol, CanonicalSymbol::new("ETH", "USDT"));
    assert_eq!(book.meta.source_sequence, Some(390_497_878));
    assert_eq!(book.meta.exchange_event_ts_us, Some(1_672_515_782_142_000));
    assert_eq!(book.meta.exchange_trade_ts_us, Some(1_672_515_782_141_000));
    assert_eq!(book.bids.len(), 20);
    assert_eq!(book.asks.len(), 20);
    validate_event(&event).unwrap();
}

#[test]
fn usdm_ignores_zero_trade_sentinel() {
    let mut bytes = include_bytes!("fixtures/binance_usdm_trade_sentinel.json").to_vec();

    let events = md_exchanges::binance_usdm::parse_frame(&mut bytes, RECV_US).unwrap();

    assert!(events.is_empty());
}

#[test]
fn spot_does_not_ignore_usdm_zero_trade_sentinel() {
    let mut bytes = include_bytes!("fixtures/binance_usdm_trade_sentinel.json").to_vec();

    let error = md_exchanges::binance_spot::parse_frame(&mut bytes, RECV_US).unwrap_err();

    assert!(matches!(
        error,
        ParseError::Validation(ValidationError::NonPositiveTradePrice { value: 0 })
    ));
}

#[test]
fn usdm_rejects_unmarked_zero_trade() {
    let mut bytes = br#"{
        "stream":"ethusdt@trade",
        "data":{"e":"trade","E":1672515782140,"T":1672515782140,
        "s":"ETHUSDT","t":98765,"p":"0","q":"0","m":false}
    }"#
    .to_vec();

    let error = md_exchanges::binance_usdm::parse_frame(&mut bytes, RECV_US).unwrap_err();

    assert!(matches!(
        error,
        ParseError::Validation(ValidationError::NonPositiveTradePrice { value: 0 })
    ));
}

#[test]
fn aggregate_trade_stream_is_rejected_explicitly() {
    let mut bytes = br#"{
        "stream":"btcusdt@aggTrade",
        "data":{"e":"aggTrade","E":1672515782136,"s":"BTCUSDT","a":12345,
        "p":"16550.125","q":"0.00125","f":100,"l":105,"T":1672515782136,
        "m":true,"M":true}
    }"#
    .to_vec();

    let error = md_exchanges::binance_spot::parse_frame(&mut bytes, RECV_US).unwrap_err();
    assert!(error.to_string().contains("aggTrade"));
}

#[test]
fn binance_levels_reuse_exact_decimal_conversion() {
    let mut bytes = include_bytes!("fixtures/binance_spot_book.json").to_vec();
    let event = md_exchanges::binance_spot::parse_frame(&mut bytes, RECV_US)
        .unwrap()
        .remove(0);
    let NormalizedEvent::Book(book) = event else {
        panic!("expected book")
    };

    assert_eq!(book.bids[0].price, parse_decimal_18("16550.00").unwrap());
    assert_eq!(
        book.bids[0].quantity,
        parse_decimal_18("0.10000000").unwrap()
    );
}
