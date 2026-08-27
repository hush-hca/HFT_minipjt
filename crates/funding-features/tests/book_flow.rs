use funding_core::{
    config::{DecimalRounding, ExactDecimal},
    feature::{
        BookInvalidReason, ExecutableQuoteSide, FeatureInvalidReason, FeatureSource,
        FeatureValidity, FlowInputState, QuoteInvalidReason, QuoteValidity, StructuralBookValidity,
    },
};
use funding_features::{
    book::compute_book_features,
    flow::{TradePushOutcome, TradeWindow},
};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, PriceLevel, TakerSide, TimestampPrecision,
    TradeTick,
};
use uuid::Uuid;

fn decimal(value: &str) -> ExactDecimal {
    ExactDecimal::from_scaled(md_core::decimal::parse_decimal_18(value).unwrap()).unwrap()
}

fn event_meta(
    event_id: u128,
    adapter: AdapterId,
    base: &str,
    sequence: Option<u64>,
    exchange_ts_us: i64,
    local_recv_ts_us: i64,
) -> EventMeta {
    EventMeta {
        schema_version: 1,
        event_id: Uuid::from_u128(event_id),
        adapter,
        symbol: CanonicalSymbol::new(base, "USDT"),
        source_symbol: format!("{base}USDT"),
        source_stream: "test".to_owned(),
        source_sequence: sequence,
        exchange_event_ts_us: Some(exchange_ts_us),
        exchange_trade_ts_us: None,
        event_ts_precision: TimestampPrecision::Microsecond,
        trade_ts_precision: TimestampPrecision::Unavailable,
        local_recv_ts_us,
        raw_size_bytes: 1,
    }
}

fn book_with(
    event_id: u128,
    adapter: AdapterId,
    base: &str,
    sequence: Option<u64>,
    ts_us: i64,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
) -> BookSnapshot {
    BookSnapshot {
        meta: event_meta(event_id, adapter, base, sequence, ts_us, ts_us + 10),
        bids: bids
            .iter()
            .map(|(price, quantity)| PriceLevel {
                price: decimal(price).scaled(),
                quantity: decimal(quantity).scaled(),
            })
            .collect(),
        asks: asks
            .iter()
            .map(|(price, quantity)| PriceLevel {
                price: decimal(price).scaled(),
                quantity: decimal(quantity).scaled(),
            })
            .collect(),
    }
}

fn book(event_id: u128, ts_us: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> BookSnapshot {
    book_with(
        event_id,
        AdapterId::BinanceUsdm,
        "BTC",
        Some(event_id as u64),
        ts_us,
        bids,
        asks,
    )
}

#[allow(clippy::too_many_arguments)]
fn trade_with(
    event_id: u128,
    adapter: AdapterId,
    base: &str,
    trade_id: &str,
    ts_us: i64,
    local_recv_ts_us: i64,
    price: &str,
    quantity: &str,
    side: TakerSide,
) -> TradeTick {
    let mut meta = event_meta(
        event_id,
        adapter,
        base,
        Some(event_id as u64),
        ts_us,
        local_recv_ts_us,
    );
    meta.exchange_trade_ts_us = Some(ts_us);
    meta.trade_ts_precision = TimestampPrecision::Microsecond;
    TradeTick {
        meta,
        trade_id: trade_id.to_owned(),
        price: decimal(price).scaled(),
        quantity: decimal(quantity).scaled(),
        taker_side: side,
    }
}

fn trade(
    event_id: u128,
    trade_id: &str,
    ts_us: i64,
    price: &str,
    quantity: &str,
    side: TakerSide,
) -> TradeTick {
    trade_with(
        event_id,
        AdapterId::BinanceUsdm,
        "BTC",
        trade_id,
        ts_us,
        ts_us + 10,
        price,
        quantity,
        side,
    )
}

#[test]
fn exact_book_math_ofi_and_insufficient_quote_are_independent() {
    let previous = book(1, 10_000_000, &[("99", "4")], &[("101", "6")]);
    let current = book(2, 10_000_100, &[("99", "6")], &[("101", "2")]);

    let features = compute_book_features(
        Some(&previous),
        &current,
        decimal("5"),
        10_000_200,
        1_000_000,
    );

    assert_eq!(features.validity, FeatureValidity::Valid);
    assert_eq!(features.mid, Some(decimal("100")));
    assert_eq!(features.microprice, Some(decimal("100.5")));
    assert_eq!(features.imbalance_1, Some(decimal("0.5")));
    assert_eq!(features.snapshot_ofi, Some(decimal("6")));
    assert_eq!(features.depth_delta_bid, Some(decimal("2")));
    assert_eq!(features.depth_delta_ask, Some(decimal("-4")));
    assert_eq!(features.sell_into_bids.average_price, Some(decimal("99")));
    assert!(matches!(
        features.buy_from_asks.validity,
        QuoteValidity::Invalid(QuoteInvalidReason::InsufficientDepth {
            requested_base,
            available_base,
        }) if requested_base == decimal("5") && available_base == decimal("2")
    ));
}

#[test]
fn executable_quotes_use_side_specific_rounding_and_partial_levels() {
    let current = book(
        3,
        20_000_000,
        &[("100", "1"), ("99", "2")],
        &[("101", "1"), ("102", "2")],
    );

    let features = compute_book_features(None, &current, decimal("1.5"), 20_000_100, 1_000_000);

    assert_eq!(
        features.sell_into_bids.side,
        ExecutableQuoteSide::SellIntoBids
    );
    assert_eq!(features.sell_into_bids.available_base, decimal("3"));
    assert_eq!(
        features.sell_into_bids.quote_notional,
        Some(decimal("149.5"))
    );
    assert_eq!(
        features.sell_into_bids.average_price,
        Some(decimal("99.666666666666666666"))
    );
    assert_eq!(features.sell_into_bids.levels_consumed, 2);
    assert_eq!(
        features.buy_from_asks.side,
        ExecutableQuoteSide::BuyFromAsks
    );
    assert_eq!(features.buy_from_asks.quote_notional, Some(decimal("152")));
    assert_eq!(
        features.buy_from_asks.average_price,
        Some(decimal("101.333333333333333334"))
    );
}

#[test]
fn depth_delta_uses_a_deterministic_union_and_keeps_zero_add_remove_levels() {
    let previous = book(
        4,
        30_000_000,
        &[("100", "2"), ("99", "3"), ("98", "4")],
        &[("101", "2"), ("102", "3"), ("103", "4")],
    );
    let current = book(
        5,
        30_000_100,
        &[("100.5", "1"), ("100", "2"), ("98", "1")],
        &[("100.75", "1"), ("101", "2"), ("103", "5")],
    );

    let features = compute_book_features(
        Some(&previous),
        &current,
        decimal("1"),
        30_000_200,
        1_000_000,
    );

    let bid_prices: Vec<_> = features
        .depth_delta_bids
        .iter()
        .map(|level| level.price)
        .collect();
    assert_eq!(
        bid_prices,
        [
            decimal("100.5"),
            decimal("100"),
            decimal("99"),
            decimal("98")
        ]
    );
    assert_eq!(features.depth_delta_bids[0].previous_base, decimal("0"));
    assert_eq!(features.depth_delta_bids[0].delta_base, decimal("1"));
    assert_eq!(features.depth_delta_bids[1].delta_base, decimal("0"));
    assert_eq!(features.depth_delta_bids[2].current_base, decimal("0"));
    assert_eq!(features.depth_delta_bids[2].delta_base, decimal("-3"));

    let ask_prices: Vec<_> = features
        .depth_delta_asks
        .iter()
        .map(|level| level.price)
        .collect();
    assert_eq!(
        ask_prices,
        [
            decimal("100.75"),
            decimal("101"),
            decimal("102"),
            decimal("103")
        ]
    );
    assert_eq!(features.depth_delta_asks[1].delta_base, decimal("0"));
    assert_eq!(features.depth_delta_asks[2].delta_base, decimal("-3"));
}

#[test]
fn duplicate_snapshot_has_zero_ofi_and_zero_depth_delta() {
    let previous = book(
        6,
        40_000_000,
        &[("100", "2"), ("99", "1")],
        &[("101", "3"), ("102", "4")],
    );
    let mut current = previous.clone();
    current.meta.event_id = Uuid::from_u128(7);
    current.meta.source_sequence = Some(7);
    current.meta.exchange_event_ts_us = Some(40_000_100);
    current.meta.local_recv_ts_us = 40_000_110;

    let features = compute_book_features(
        Some(&previous),
        &current,
        decimal("1"),
        40_000_200,
        1_000_000,
    );
    assert_eq!(features.snapshot_ofi, Some(decimal("0")));
    assert_eq!(features.depth_delta_bid, Some(decimal("0")));
    assert_eq!(features.depth_delta_ask, Some(decimal("0")));
    assert!(
        features
            .depth_delta_bids
            .iter()
            .all(|level| level.delta_base == decimal("0"))
    );
    assert!(
        features
            .depth_delta_asks
            .iter()
            .all(|level| level.delta_base == decimal("0"))
    );
}

#[test]
fn best_level_ofi_covers_price_improvement_and_retreat_branches() {
    let cases = [
        (
            book(8, 50_000_000, &[("99", "4")], &[("102", "6")]),
            book(9, 50_000_100, &[("100", "5")], &[("101", "2")]),
            "3",
        ),
        (
            book(10, 50_001_000, &[("100", "4")], &[("101", "6")]),
            book(11, 50_001_100, &[("99", "5")], &[("102", "2")]),
            "2",
        ),
    ];
    for (previous, current, expected) in cases {
        let features = compute_book_features(
            Some(&previous),
            &current,
            decimal("1"),
            current.meta.local_recv_ts_us + 100,
            1_000_000,
        );
        assert_eq!(features.snapshot_ofi, Some(decimal(expected)));
    }
}

#[test]
fn price_migration_can_have_zero_total_depth_delta_but_nonzero_per_level_evidence() {
    let previous = book(12, 60_000_000, &[("99", "2")], &[("102", "2")]);
    let current = book(13, 60_000_100, &[("100", "2")], &[("101", "2")]);
    let features = compute_book_features(
        Some(&previous),
        &current,
        decimal("1"),
        60_000_200,
        1_000_000,
    );
    assert_eq!(features.depth_delta_bid, Some(decimal("0")));
    assert_eq!(features.depth_delta_ask, Some(decimal("0")));
    assert!(
        features
            .depth_delta_bids
            .iter()
            .any(|level| level.delta_base != decimal("0"))
    );
}

#[test]
fn invalid_books_never_emit_analytics_and_freshness_boundary_is_inclusive() {
    let valid = book(14, 70_000_000, &[("100", "1")], &[("101", "1")]);
    let boundary = compute_book_features(None, &valid, decimal("1"), 71_000_010, 1_000_000);
    assert_eq!(boundary.validity, FeatureValidity::Valid);

    let stale = compute_book_features(None, &valid, decimal("1"), 71_000_011, 1_000_000);
    assert!(matches!(
        stale.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::Stale {
            age_us: 1_000_001,
            limit_us: 1_000_000,
        })
    ));
    assert_book_has_no_analytics(&stale);
    assert!(matches!(
        stale.sell_into_bids.validity,
        QuoteValidity::Invalid(QuoteInvalidReason::Stale { .. })
    ));

    let future = book(15, 72_000_001, &[("100", "1")], &[("101", "1")]);
    let future_features = compute_book_features(None, &future, decimal("1"), 72_000_000, 1_000_000);
    assert!(matches!(
        future_features.structural_validity,
        StructuralBookValidity::Invalid(BookInvalidReason::FutureTimestamp { .. })
    ));
    assert_book_has_no_analytics(&future_features);

    for invalid in [
        book(16, 73_000_000, &[], &[("101", "1")]),
        book(17, 73_000_000, &[("101", "1")], &[("101", "1")]),
        book(18, 73_000_000, &[("102", "1")], &[("101", "1")]),
        book(
            19,
            73_000_000,
            &[("99", "1"), ("100", "1")],
            &[("101", "1")],
        ),
        book(
            20,
            73_000_000,
            &[("100", "1"), ("100", "2")],
            &[("101", "1")],
        ),
    ] {
        let features = compute_book_features(None, &invalid, decimal("1"), 73_000_100, 1_000_000);
        assert_book_has_no_analytics(&features);
        assert!(matches!(
            features.structural_validity,
            StructuralBookValidity::Invalid(_)
        ));
    }
}

fn assert_book_has_no_analytics(features: &funding_core::feature::BookFeatures) {
    assert!(features.mid.is_none());
    assert!(features.microprice.is_none());
    assert!(features.imbalance_1.is_none());
    assert!(features.snapshot_ofi.is_none());
    assert!(features.depth_delta_bids.is_empty());
    assert!(features.depth_delta_asks.is_empty());
}

#[test]
fn previous_identity_mismatch_is_explicit_and_comparison_metrics_are_suppressed() {
    let previous = book_with(
        21,
        AdapterId::BybitLinear,
        "ETH",
        Some(1),
        80_000_000,
        &[("99", "1")],
        &[("101", "1")],
    );
    let current = book(22, 80_000_100, &[("99", "2")], &[("101", "2")]);
    let features = compute_book_features(
        Some(&previous),
        &current,
        decimal("1"),
        80_000_200,
        1_000_000,
    );
    assert_eq!(features.validity, FeatureValidity::Valid);
    assert!(features.mid.is_some());
    assert!(features.snapshot_ofi.is_none());
    assert!(features.depth_delta_bids.is_empty());
    assert!(matches!(
        features.comparison_validity,
        funding_core::feature::BookComparisonValidity::Invalid(
            FeatureInvalidReason::PreviousBookIdentityMismatch { .. }
        )
    ));
}

#[test]
fn comparison_overflow_is_reported_without_invalidating_current_analytics() {
    let previous = book(23, 90_000_000, &[("2", "1"), ("1", "1")], &[("3", "1")]);
    let mut current = book(24, 90_000_100, &[("2", "1"), ("1", "1")], &[("3", "1")]);
    current.bids[0].quantity = ExactDecimal::MAX_COEFFICIENT;
    current.bids[1].quantity = ExactDecimal::MAX_COEFFICIENT;
    let features = compute_book_features(
        Some(&previous),
        &current,
        decimal("1"),
        90_000_200,
        1_000_000,
    );
    assert_eq!(features.validity, FeatureValidity::Valid);
    assert!(features.mid.is_some());
    assert!(matches!(
        features.comparison_validity,
        funding_core::feature::BookComparisonValidity::Invalid(
            FeatureInvalidReason::ArithmeticOverflow
        )
    ));
    assert!(matches!(
        features.sell_into_bids.validity,
        QuoteValidity::Invalid(QuoteInvalidReason::ArithmeticOverflow)
    ));
}

#[test]
fn structural_comparison_and_configuration_failures_keep_precise_evidence() {
    let locked = book(25, 91_000_000, &[("100", "1")], &[("100", "1")]);
    let locked_features = compute_book_features(None, &locked, decimal("1"), 91_000_010, 1_000_000);
    assert!(matches!(
        locked_features.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::StructuralBookInvalid {
            reason: BookInvalidReason::LockedBook,
        })
    ));

    let valid = book(26, 92_000_000, &[("100", "1")], &[("101", "1")]);
    let invalid_limit = compute_book_features(None, &valid, decimal("1"), 92_000_010, -1);
    assert!(matches!(
        invalid_limit.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::InvalidFreshnessLimit { limit_us: -1 })
    ));

    let old_previous = book(27, 92_000_100, &[("100", "1")], &[("101", "1")]);
    let current = book(28, 94_000_100, &[("100", "2")], &[("101", "2")]);
    let stale_comparison = compute_book_features(
        Some(&old_previous),
        &current,
        decimal("1"),
        94_000_110,
        1_000_000,
    );
    assert_eq!(stale_comparison.validity, FeatureValidity::Valid);
    assert!(matches!(
        stale_comparison.comparison_validity,
        funding_core::feature::BookComparisonValidity::Invalid(FeatureInvalidReason::Stale { .. })
    ));
    assert!(stale_comparison.snapshot_ofi.is_none());
}

#[test]
fn equal_sequence_conflict_and_receive_order_regression_are_explicit() {
    let previous = book_with(
        29,
        AdapterId::BinanceUsdm,
        "BTC",
        Some(77),
        95_000_000,
        &[("100", "1")],
        &[("101", "1")],
    );
    let conflict = book_with(
        30,
        AdapterId::BinanceUsdm,
        "BTC",
        Some(77),
        95_000_100,
        &[("100", "2")],
        &[("101", "1")],
    );
    let features = compute_book_features(
        Some(&previous),
        &conflict,
        decimal("1"),
        95_000_110,
        1_000_000,
    );
    assert!(matches!(
        features.comparison_validity,
        funding_core::feature::BookComparisonValidity::Invalid(
            FeatureInvalidReason::SourceSequenceConflict { sequence: 77 }
        )
    ));

    let mut receive_regression = book(31, 95_000_200, &[("100", "1")], &[("101", "1")]);
    receive_regression.meta.source_sequence = Some(78);
    receive_regression.meta.local_recv_ts_us = previous.meta.local_recv_ts_us - 1;
    let features = compute_book_features(
        Some(&previous),
        &receive_regression,
        decimal("1"),
        95_000_210,
        1_000_000,
    );
    assert!(matches!(
        features.comparison_validity,
        funding_core::feature::BookComparisonValidity::Invalid(
            FeatureInvalidReason::PreviousBookReceiveOrderInvalid { .. }
        )
    ));
}

#[test]
fn flow_distinguishes_no_input_zero_activity_and_activity() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    let no_input = window.snapshot(10_000_000);
    assert_eq!(no_input.input_state, FlowInputState::NoInput);
    assert!(matches!(
        no_input.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::NoInput)
    ));

    let source = FeatureSource {
        event_id: Uuid::from_u128(100),
        adapter: AdapterId::BinanceUsdm,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        source_sequence: Some(100),
        exchange_event_ts_us: Some(10_000_000),
        exchange_trade_ts_us: None,
        local_recv_ts_us: 10_000_010,
        effective_ts_us: 10_000_000,
        effective_ts_source: funding_core::feature::EffectiveTimestampSource::ExchangeEvent,
    };
    window.observe_watermark(source.clone()).unwrap();
    let zero = window.snapshot(10_000_010);
    assert_eq!(zero.input_state, FlowInputState::ZeroActivity);
    assert_eq!(zero.source_watermark, Some(source));
    assert_eq!(zero.burst_trade_rate_per_second, Some(decimal("0")));

    let mut window = TradeWindow::new(5_000_000).unwrap();
    window
        .push(&trade(101, "a", 18_000_000, "100", "2", TakerSide::Buy))
        .unwrap();
    window
        .push(&trade(102, "b", 19_000_010, "101", "1", TakerSide::Sell))
        .unwrap();
    window
        .push(&trade(103, "c", 20_000_000, "102", "3", TakerSide::Unknown))
        .unwrap();
    let flow = window.snapshot(20_000_010);
    assert_eq!(flow.input_state, FlowInputState::Activity);
    assert_eq!(flow.buy_base_volume, decimal("2"));
    assert_eq!(flow.sell_base_volume, decimal("1"));
    assert_eq!(flow.unknown_base_volume, decimal("3"));
    assert_eq!(flow.buy_quote_notional, decimal("200"));
    assert_eq!(flow.sell_quote_notional, decimal("101"));
    assert_eq!(flow.unknown_quote_notional, decimal("306"));
    assert_eq!(flow.buy_trade_count, 1);
    assert_eq!(flow.sell_trade_count, 1);
    assert_eq!(flow.unknown_trade_count, 1);
    assert_eq!(flow.mean_trade_size, Some(decimal("2")));
    assert_eq!(
        flow.signed_volume_imbalance,
        Some(decimal("0.333333333333333333"))
    );
    assert_eq!(flow.cumulative_volume_delta, decimal("1"));
    assert_eq!(flow.mean_inter_trade_us, Some(1_000_000));
    assert_eq!(flow.burst_count, 2);
    assert_eq!(flow.burst_trade_rate_per_second, Some(decimal("2")));
}

#[test]
fn flow_dedupes_before_ordering_and_rejected_inputs_do_not_move_watermark() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    let first = trade(110, "venue-1", 20_000_000, "100", "1", TakerSide::Buy);
    assert_eq!(window.push(&first).unwrap(), TradePushOutcome::Accepted);
    let accepted_watermark = window.snapshot(20_000_010).source_watermark.unwrap();

    let duplicate_event = trade_with(
        110,
        AdapterId::BinanceUsdm,
        "BTC",
        "different-id",
        20_000_100,
        20_000_110,
        "100",
        "5",
        TakerSide::Sell,
    );
    assert_eq!(
        window.push(&duplicate_event).unwrap(),
        TradePushOutcome::Duplicate
    );
    let duplicate_venue_id = trade(111, "venue-1", 20_000_200, "100", "5", TakerSide::Sell);
    assert_eq!(
        window.push(&duplicate_venue_id).unwrap(),
        TradePushOutcome::Duplicate
    );
    let out_of_order = trade_with(
        112,
        AdapterId::BinanceUsdm,
        "BTC",
        "venue-2",
        19_999_999,
        20_000_300,
        "100",
        "5",
        TakerSide::Sell,
    );
    assert_eq!(
        window.push(&out_of_order).unwrap(),
        TradePushOutcome::RejectedOutOfOrder {
            previous_ts_us: 20_000_000,
            current_ts_us: 19_999_999,
        }
    );
    let repeated_rejection = window.push(&out_of_order).unwrap();
    assert_eq!(repeated_rejection, TradePushOutcome::Duplicate);

    let flow = window.snapshot(20_000_500);
    assert_eq!(flow.buy_base_volume, decimal("1"));
    assert_eq!(flow.sell_base_volume, decimal("0"));
    assert_eq!(flow.duplicate_trade_count, 3);
    assert_eq!(flow.out_of_order_trade_count, 1);
    assert_eq!(flow.source_watermark, Some(accepted_watermark));
}

#[test]
fn equal_trade_timestamps_are_accepted_and_unknown_only_flow_is_unsigned() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    window
        .push(&trade(
            120,
            "one",
            30_000_000,
            "100",
            "1",
            TakerSide::Unknown,
        ))
        .unwrap();
    assert_eq!(
        window
            .push(&trade(
                121,
                "two",
                30_000_000,
                "100",
                "2",
                TakerSide::Unknown
            ))
            .unwrap(),
        TradePushOutcome::Accepted
    );
    let flow = window.snapshot(30_000_010);
    assert_eq!(flow.unknown_trade_count, 2);
    assert_eq!(flow.unknown_base_volume, decimal("3"));
    assert_eq!(flow.cumulative_volume_delta, decimal("0"));
    assert_eq!(flow.signed_volume_imbalance, None);
    assert_eq!(flow.mean_inter_trade_us, Some(0));
}

#[test]
fn trade_windows_are_market_bound_and_trade_ids_are_scoped_per_market() {
    let mut binance_btc = TradeWindow::new(5_000_000).unwrap();
    binance_btc
        .push(&trade(130, "same", 40_000_000, "100", "1", TakerSide::Buy))
        .unwrap();
    let bybit = trade_with(
        131,
        AdapterId::BybitLinear,
        "BTC",
        "same",
        40_000_001,
        40_000_011,
        "100",
        "1",
        TakerSide::Buy,
    );
    let eth = trade_with(
        132,
        AdapterId::BinanceUsdm,
        "ETH",
        "same",
        40_000_002,
        40_000_012,
        "100",
        "1",
        TakerSide::Buy,
    );
    assert!(matches!(
        binance_btc.push(&bybit),
        Err(FeatureInvalidReason::FlowIdentityMismatch { .. })
    ));
    assert!(matches!(
        binance_btc.push(&eth),
        Err(FeatureInvalidReason::FlowIdentityMismatch { .. })
    ));

    let mut bybit_btc = TradeWindow::new(5_000_000).unwrap();
    let mut binance_eth = TradeWindow::new(5_000_000).unwrap();
    assert_eq!(bybit_btc.push(&bybit).unwrap(), TradePushOutcome::Accepted);
    assert_eq!(binance_eth.push(&eth).unwrap(), TradePushOutcome::Accepted);
    assert_eq!(binance_btc.snapshot(40_000_012).buy_trade_count, 1);
    assert_eq!(bybit_btc.snapshot(40_000_012).buy_trade_count, 1);
    assert_eq!(binance_eth.snapshot(40_000_012).buy_trade_count, 1);
}

#[test]
fn flow_window_is_inclusive_and_evicts_only_before_its_left_boundary() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    window
        .push(&trade(140, "old", 50_000_009, "100", "1", TakerSide::Buy))
        .unwrap();
    window
        .push(&trade(141, "edge", 50_000_010, "100", "2", TakerSide::Buy))
        .unwrap();
    window
        .push(&trade(142, "end", 55_000_000, "100", "3", TakerSide::Sell))
        .unwrap();
    let flow = window.snapshot(55_000_010);
    assert_eq!(flow.buy_trade_count, 1);
    assert_eq!(flow.buy_base_volume, decimal("2"));
    assert_eq!(flow.sell_trade_count, 1);
    assert_eq!(flow.first_trade_ts_us, Some(50_000_010));
}

#[test]
fn flow_becomes_zero_activity_after_eviction_and_rejected_input_adds_no_trade() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    window
        .push(&trade(
            143,
            "accepted",
            56_000_000,
            "100",
            "1",
            TakerSide::Buy,
        ))
        .unwrap();
    let zero = window.snapshot(61_000_011);
    assert_eq!(zero.input_state, FlowInputState::ZeroActivity);
    assert_eq!(zero.buy_trade_count, 0);

    let invalid = trade(144, "invalid", 61_000_020, "100", "0", TakerSide::Buy);
    assert_eq!(
        window.push(&invalid),
        Err(FeatureInvalidReason::InvalidQuantity)
    );
    let still_zero = window.snapshot(61_000_030);
    assert_eq!(still_zero.input_state, FlowInputState::ZeroActivity);
    assert_eq!(still_zero.buy_trade_count, 0);
}

#[test]
fn flow_source_preserves_original_clocks_and_labels_local_fallback() {
    let mut fallback = trade(145, "fallback", 62_000_000, "100", "1", TakerSide::Buy);
    fallback.meta.exchange_event_ts_us = None;
    fallback.meta.exchange_trade_ts_us = None;
    fallback.meta.event_ts_precision = TimestampPrecision::Unavailable;
    fallback.meta.trade_ts_precision = TimestampPrecision::Unavailable;

    let mut window = TradeWindow::new(5_000_000).unwrap();
    window.push(&fallback).unwrap();
    let source = window.snapshot(62_000_010).source_watermark.unwrap();
    assert_eq!(source.exchange_event_ts_us, None);
    assert_eq!(source.exchange_trade_ts_us, None);
    assert_eq!(source.effective_ts_us, 62_000_010);
    assert_eq!(
        source.effective_ts_source,
        funding_core::feature::EffectiveTimestampSource::LocalReceive
    );
}

#[test]
fn invalid_trade_quantity_future_snapshot_and_window_regression_are_explicit() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    let invalid = trade(150, "bad", 60_000_000, "100", "0", TakerSide::Buy);
    assert_eq!(
        window.push(&invalid),
        Err(FeatureInvalidReason::InvalidQuantity)
    );

    window
        .push(&trade(
            151,
            "future",
            61_000_001,
            "100",
            "1",
            TakerSide::Buy,
        ))
        .unwrap();
    let future = window.snapshot(61_000_000);
    assert!(matches!(
        future.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::FutureTimestamp {
            source_ts_us: 61_000_011,
            decision_ts_us: 61_000_000,
        })
    ));
    let regressing = window.snapshot(60_999_999);
    assert!(matches!(
        regressing.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::RegressingTimestamp {
            previous_ts_us: 61_000_000,
            current_ts_us: 60_999_999,
        })
    ));
}

#[test]
fn flow_aggregate_overflow_is_invalid_not_wrapped() {
    let mut window = TradeWindow::new(5_000_000).unwrap();
    let mut first = trade(160, "max-1", 70_000_000, "1", "1", TakerSide::Buy);
    first.quantity = ExactDecimal::MAX_COEFFICIENT;
    let mut second = trade(161, "max-2", 70_000_000, "1", "1", TakerSide::Buy);
    second.quantity = ExactDecimal::MAX_COEFFICIENT;
    window.push(&first).unwrap();
    window.push(&second).unwrap();
    let flow = window.snapshot(70_000_010);
    assert_eq!(
        flow.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::ArithmeticOverflow)
    );
}

#[test]
fn decimal_division_rounding_reference_values_are_stable() {
    assert_eq!(
        decimal("1")
            .checked_div(decimal("3"), DecimalRounding::TowardZero)
            .unwrap(),
        decimal("0.333333333333333333")
    );
}
