use funding_core::{
    config::ExactDecimal,
    meta::DerivativeMeta,
    opportunity::{CostModel, FeeAssumption, FeeLiquidity, FeeSource, VenueCostModel},
    public::{
        DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
        MarkIndexSnapshot, OpenInterestSnapshot, OpenInterestUnit, TraderMetricKind,
        TraderRatioSnapshot,
    },
    replay::{DecisionEvent, ReplayConfig, ReplayDecisionOutcome, ReplayRejectionReason},
};
use funding_features::replay::{
    CANONICAL_ENCODING_VERSION, ReplayEvent, SAME_RECEIVE_POLICY, run_replay,
};
use md_core::{
    decimal::parse_decimal_18,
    model::{
        AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
        TakerSide, TimestampPrecision, TradeTick,
    },
};
use uuid::Uuid;

fn d(value: &str) -> ExactDecimal {
    ExactDecimal::from_scaled(parse_decimal_18(value).unwrap()).unwrap()
}
fn symbol() -> CanonicalSymbol {
    CanonicalSymbol::new("BTC", "USDT")
}
fn market_meta(id: u128, venue: AdapterId, source: i64, recv: i64) -> EventMeta {
    EventMeta {
        schema_version: 1,
        event_id: Uuid::from_u128(id),
        adapter: venue,
        symbol: symbol(),
        source_symbol: "BTCUSDT".into(),
        source_stream: "fixture".into(),
        source_sequence: Some(id as u64),
        exchange_event_ts_us: Some(source),
        exchange_trade_ts_us: None,
        event_ts_precision: TimestampPrecision::Microsecond,
        trade_ts_precision: TimestampPrecision::Unavailable,
        local_recv_ts_us: recv,
        raw_size_bytes: 1,
    }
}
fn book(id: u128, venue: AdapterId, bid: &str, ask: &str, source: i64, recv: i64) -> ReplayEvent {
    ReplayEvent::Market(NormalizedEvent::Book(BookSnapshot {
        meta: market_meta(id, venue, source, recv),
        bids: vec![PriceLevel {
            price: d(bid).scaled(),
            quantity: d("2").scaled(),
        }],
        asks: vec![PriceLevel {
            price: d(ask).scaled(),
            quantity: d("2").scaled(),
        }],
    }))
}
fn trade(id: u128, venue: AdapterId, source: i64, recv: i64) -> ReplayEvent {
    let mut meta = market_meta(id, venue, source, recv);
    meta.exchange_trade_ts_us = Some(source);
    meta.trade_ts_precision = TimestampPrecision::Microsecond;
    ReplayEvent::Market(NormalizedEvent::Trade(TradeTick {
        meta,
        trade_id: format!("t{id}"),
        price: d("100").scaled(),
        quantity: d("0.1").scaled(),
        taker_side: TakerSide::Buy,
    }))
}
fn derivative_meta(id: u128, venue: AdapterId, source: i64, recv: i64) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::from_u128(id),
        venue,
        symbol: symbol(),
        venue_symbol: "BTCUSDT".into(),
        source_ts_us: Some(source),
        source_ts_precision: TimestampPrecision::Microsecond,
        local_recv_ts_us: recv,
    }
}
fn mark(id: u128, venue: AdapterId, price: &str, source: i64, recv: i64) -> ReplayEvent {
    ReplayEvent::Derivative(DerivativeEvent::MarkIndex(MarkIndexSnapshot {
        meta: derivative_meta(id, venue, source, recv),
        mark_price: d(price).scaled(),
        index_price: d(price).scaled(),
    }))
}
fn funding(
    id: u128,
    venue: AdapterId,
    rate: &str,
    source: i64,
    recv: i64,
    next: i64,
) -> ReplayEvent {
    ReplayEvent::Derivative(DerivativeEvent::FundingEstimate(FundingEstimate {
        meta: derivative_meta(id, venue, source, recv),
        rate: d(rate).scaled(),
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        next_funding_ts_us: next,
    }))
}
fn oi(id: u128, venue: AdapterId, source: i64, recv: i64) -> ReplayEvent {
    ReplayEvent::Derivative(DerivativeEvent::OpenInterest(OpenInterestSnapshot {
        meta: derivative_meta(id, venue, source, recv),
        open_interest: d("10").scaled(),
        unit: if venue == AdapterId::BinanceUsdm {
            OpenInterestUnit::Contracts
        } else {
            OpenInterestUnit::BaseAsset
        },
        quote_notional: Some(d("1000").scaled()),
    }))
}
fn ratio(
    id: u128,
    venue: AdapterId,
    kind: TraderMetricKind,
    source: i64,
    recv: i64,
) -> ReplayEvent {
    ReplayEvent::Derivative(DerivativeEvent::TraderRatio(TraderRatioSnapshot {
        meta: derivative_meta(id, venue, source, recv),
        metric_kind: kind,
        long_ratio: d("0.6").scaled(),
        short_ratio: d("0.4").scaled(),
        long_short_ratio: d("1.5").scaled(),
    }))
}
fn fee() -> FeeAssumption {
    FeeAssumption::new(d("0.0001"), FeeSource::ExplicitConfig, FeeLiquidity::Taker).unwrap()
}
fn costs() -> CostModel {
    let venue = VenueCostModel {
        entry_fee: fee(),
        exit_fee: fee(),
        entry_slippage_bps: d("0"),
        exit_slippage_bps: d("0"),
        entry_book_impact_bps: d("0"),
        exit_book_impact_bps: d("0"),
    };
    CostModel {
        binance: venue.clone(),
        bybit: venue,
        basis_risk_buffer_bps: d("0"),
        funding_error_buffer_bps: d("0"),
        leg_risk_buffer_bps: d("0"),
    }
}
fn decision(id: u128, recv: i64, holding_end: i64) -> ReplayEvent {
    ReplayEvent::Decision(DecisionEvent {
        event_id: Uuid::from_u128(id),
        local_recv_ts_us: recv,
        symbol: symbol(),
        long_venue: AdapterId::BybitLinear,
        short_venue: AdapterId::BinanceUsdm,
        requested_base: d("1"),
        holding_end_ts_us: holding_end,
        cost_model: costs(),
        minimum_net_bps: d("0"),
        capacity_evidence: vec![],
    })
}
fn config() -> ReplayConfig {
    ReplayConfig {
        book_freshness_us: 20,
        metadata_freshness_us: 20,
        mark_freshness_us: 20,
        flow_window_us: 100,
        dedupe_capacity: 100,
        seed: 7,
    }
}
fn valid_events(decision_recv: i64) -> Vec<ReplayEvent> {
    let recv = 200;
    vec![
        book(1, AdapterId::BybitLinear, "99", "100", 199, recv),
        book(2, AdapterId::BinanceUsdm, "101", "102", 199, recv),
        trade(3, AdapterId::BybitLinear, 199, recv),
        mark(4, AdapterId::BybitLinear, "100", 199, recv),
        mark(5, AdapterId::BinanceUsdm, "101", 199, recv),
        funding(6, AdapterId::BybitLinear, "-0.01", 199, recv, 300),
        funding(7, AdapterId::BinanceUsdm, "0.01", 199, recv, 300),
        oi(8, AdapterId::BybitLinear, 199, recv),
        oi(9, AdapterId::BinanceUsdm, 199, recv),
        ratio(
            10,
            AdapterId::BybitLinear,
            TraderMetricKind::BybitLongShortRatio,
            199,
            recv,
        ),
        ratio(
            11,
            AdapterId::BinanceUsdm,
            TraderMetricKind::BinanceTopAccountRatio,
            199,
            recv,
        ),
        ratio(
            12,
            AdapterId::BinanceUsdm,
            TraderMetricKind::BinanceTopPositionRatio,
            199,
            recv,
        ),
        decision(20, decision_recv, 300),
    ]
}

#[test]
fn shuffled_input_is_repeatable_and_equal_receive_evidence_is_inclusive() {
    assert_eq!(CANONICAL_ENCODING_VERSION, 1);
    assert_eq!(
        SAME_RECEIVE_POLICY,
        "evidence_before_decision_at_equal_local_receive_timestamp"
    );
    let a = run_replay(config(), valid_events(200)).unwrap();
    assert_eq!(
        a.event_digest_hex,
        "93c70b43cc2729ff181f1fe008db17b1197a03ed2ecd4b5bfa48e26324b9507d"
    );
    let mut reversed = valid_events(200);
    reversed.reverse();
    let b = run_replay(config(), reversed).unwrap();
    assert_eq!(a, b);
    assert!(matches!(
        a.decisions[0].outcome,
        ReplayDecisionOutcome::Evaluated(_)
    ));
    assert_eq!(a.decisions[0].book_features.len(), 2);
    assert_eq!(a.decisions[0].flow_features.len(), 2);
    assert_eq!(
        a.decisions[0]
            .open_interest
            .iter()
            .filter(|v| v.feature.is_some())
            .count(),
        2
    );
    assert_eq!(
        a.decisions[0]
            .trader_ratios
            .iter()
            .filter(|v| v.feature.is_some())
            .count(),
        3
    );
    assert!(
        a.decisions[0]
            .evidence_event_ids
            .contains(&Uuid::from_u128(3))
    );
    assert!(a.reconciliation.input_identity_holds());
    assert!(a.reconciliation.candidate_identity_holds());
    assert!(!a.simulation_enabled && a.paper_validation_only);
}

#[test]
fn nonzero_quote_age_evaluates_at_freshness_boundary() {
    let report = run_replay(config(), valid_events(220)).unwrap();
    assert!(matches!(
        report.decisions[0].outcome,
        ReplayDecisionOutcome::Evaluated(_)
    ));
    assert!(
        report.decisions[0]
            .book_features
            .iter()
            .all(|v| v.source.local_recv_ts_us == 200)
    );
}

#[test]
fn duplicate_conflict_and_changed_option_have_distinct_input_digests() {
    let original = book(1, AdapterId::BybitLinear, "99", "100", 199, 200);
    let identical = original.clone();
    let conflicting = book(1, AdapterId::BybitLinear, "98", "100", 199, 200);
    let duplicate = run_replay(config(), vec![original.clone(), identical]).unwrap();
    let conflict = run_replay(config(), vec![original.clone(), conflicting]).unwrap();
    let reversed_conflict = run_replay(
        config(),
        vec![
            book(1, AdapterId::BybitLinear, "98", "100", 199, 200),
            original.clone(),
        ],
    )
    .unwrap();
    let mut none_sequence = original.clone();
    if let ReplayEvent::Market(NormalizedEvent::Book(value)) = &mut none_sequence {
        value.meta.source_sequence = None;
    }
    let changed = run_replay(config(), vec![none_sequence]).unwrap();
    let single = run_replay(config(), vec![original]).unwrap();
    assert_eq!(duplicate.reconciliation.duplicate_events, 1);
    assert!(matches!(
        conflict.rejections[0].reason,
        ReplayRejectionReason::DuplicateEventIdConflict { .. }
    ));
    assert_eq!(conflict, reversed_conflict);
    assert_eq!(conflict.reconciliation.applied_events, 0);
    assert_eq!(conflict.reconciliation.rejected_events, 2);
    assert_eq!(conflict.first_clock_us, Some(200));
    assert_eq!(conflict.last_clock_us, Some(200));
    assert_ne!(duplicate.event_digest_hex, conflict.event_digest_hex);
    assert_ne!(changed.event_digest_hex, single.event_digest_hex);
    assert_ne!(duplicate.event_digest_hex, single.event_digest_hex);
}

#[test]
fn future_secondary_clock_is_rejected_without_replacing_prior_book() {
    let mut future = match book(30, AdapterId::BybitLinear, "98", "100", 199, 200) {
        ReplayEvent::Market(NormalizedEvent::Book(v)) => v,
        _ => unreachable!(),
    };
    future.meta.exchange_trade_ts_us = Some(201);
    let report = run_replay(
        config(),
        vec![
            book(1, AdapterId::BybitLinear, "99", "100", 198, 199),
            ReplayEvent::Market(NormalizedEvent::Book(future)),
            decision(31, 200, 300),
        ],
    )
    .unwrap();
    assert!(matches!(
        report.rejections[0].reason,
        ReplayRejectionReason::SourceAfterAvailability { .. }
    ));
    assert_eq!(
        report.decisions[0].book_features[0].source.event_id,
        Uuid::from_u128(1)
    );
}

#[test]
fn late_book_after_newer_trade_is_rejected_atomically() {
    let report = run_replay(
        config(),
        vec![
            book(1, AdapterId::BybitLinear, "99", "100", 195, 196),
            trade(2, AdapterId::BybitLinear, 190, 196),
            book(3, AdapterId::BybitLinear, "98", "101", 192, 197),
            decision(4, 200, 300),
        ],
    )
    .unwrap();
    assert!(matches!(
        report.rejections[0].reason,
        ReplayRejectionReason::RegressingInput { .. }
    ));
    assert_eq!(
        report.decisions[0].book_features[0].source.event_id,
        Uuid::from_u128(1)
    );
}

#[test]
fn opportunity_rejection_count_uses_a_stable_payload_free_code() {
    let mut events = valid_events(200);
    if let ReplayEvent::Decision(value) = events.last_mut().unwrap() {
        value.minimum_net_bps = d("10000");
    }
    let report = run_replay(config(), events).unwrap();
    assert_eq!(
        report
            .rejection_counts
            .get("OPPORTUNITY_NET_EDGE_BELOW_MINIMUM"),
        Some(&1)
    );
    assert!(report.rejection_counts.keys().all(|key| !key.contains('{')));
}

#[test]
fn invalid_decision_is_typed_before_lookup() {
    let mut value = match decision(40, 200, 300) {
        ReplayEvent::Decision(v) => v,
        _ => unreachable!(),
    };
    value.short_venue = value.long_venue;
    let report = run_replay(config(), vec![ReplayEvent::Decision(value)]).unwrap();
    assert!(
        matches!(report.rejections[0].reason, ReplayRejectionReason::InvalidDecision { ref field } if field == "venues")
    );
    assert!(report.decisions.is_empty());
}

#[test]
fn unavailable_decision_is_retained_and_not_a_candidate() {
    let report = run_replay(config(), vec![decision(50, 200, 300)]).unwrap();
    assert!(matches!(
        report.decisions[0].outcome,
        ReplayDecisionOutcome::Unavailable(ReplayRejectionReason::MissingBook { .. })
    ));
    assert_eq!(report.reconciliation.decisions_recorded, 1);
    assert_eq!(report.reconciliation.candidate_evaluations, 0);
}
