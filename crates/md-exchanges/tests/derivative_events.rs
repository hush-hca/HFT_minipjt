use funding_core::public::{
    DerivativeEvent, FundingBasis, FundingIntervalProvenance, FundingRateKind, OpenInterestUnit,
    TraderMetricKind,
};
use md_core::model::{AdapterId, TimestampPrecision};
use md_exchanges::derivatives::{binance, bybit};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("tests/fixtures/{name}")).unwrap()
}

#[test]
fn binance_current_funding_and_prices_keep_exact_semantics() {
    let events = binance::parse_mark_funding(
        &mut fixture("binance_mark_funding.json"),
        1_720_000_000_124_000,
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    let DerivativeEvent::MarkIndex(mark) = &events[0] else {
        panic!()
    };
    assert_eq!(mark.mark_price, 60_000_123_456_789_012_345_678);
    assert_eq!(mark.index_price, 59_999_987_654_321_098_765_432);
    assert_eq!(mark.meta.venue, AdapterId::BinanceUsdm);
    assert_eq!(mark.meta.source_ts_us, Some(1_720_000_000_123_000));
    assert_eq!(
        mark.meta.source_ts_precision,
        TimestampPrecision::Millisecond
    );
    assert_ne!(mark.meta.event_id, events[1].meta().event_id);

    let DerivativeEvent::FundingEstimate(funding) = &events[1] else {
        panic!()
    };
    assert_eq!(funding.rate, 100_000_000_000_001);
    assert_eq!(funding.rate_kind, FundingRateKind::IndicativeNext);
    assert_eq!(funding.basis, FundingBasis::MarkNotional);
    assert_eq!(funding.interval_secs, 28_800);
    assert_eq!(
        funding.interval_provenance,
        FundingIntervalProvenance::AssumedVenueDefault
    );
    assert_eq!(funding.next_funding_ts_us, 1_720_000_800_000_000);
}

#[test]
fn settled_history_is_actual_and_time_ordered() {
    let binance_events = binance::parse_funding_history(
        &mut fixture("binance_funding_history.json"),
        1_720_022_400_101_000,
    )
    .unwrap();
    let bybit_events = bybit::parse_funding_history(
        &mut fixture("bybit_funding_history.json"),
        1_720_022_400_101_000,
    )
    .unwrap();
    for events in [&binance_events, &bybit_events] {
        assert_eq!(events.len(), 2);
        let DerivativeEvent::FundingSettlement(first) = &events[0] else {
            panic!()
        };
        let DerivativeEvent::FundingSettlement(second) = &events[1] else {
            panic!()
        };
        assert_eq!(first.rate_kind, FundingRateKind::SettledActual);
        assert_eq!(first.basis, FundingBasis::MarkNotional);
        assert_eq!(first.interval_secs, 28_800);
        assert!(first.settlement_ts_us <= second.settlement_ts_us);
    }
}

#[test]
fn oi_and_each_ratio_metric_are_not_conflated() {
    let oi = binance::parse_open_interest(
        &mut fixture("binance_open_interest.json"),
        1_720_000_000_124_000,
    )
    .unwrap();
    let DerivativeEvent::OpenInterest(oi) = &oi[0] else {
        panic!()
    };
    assert_eq!(oi.open_interest, 10_659_509_000_000_000_000_001);
    assert_eq!(oi.unit, OpenInterestUnit::Contracts);
    assert_eq!(oi.quote_notional, None);

    let account = binance::parse_top_account_ratio(
        &mut fixture("binance_top_ratio.json"),
        1_720_000_300_001_000,
    )
    .unwrap();
    let position = binance::parse_top_position_ratio(
        &mut fixture("binance_top_ratio.json"),
        1_720_000_300_001_000,
    )
    .unwrap();
    let bybit_ratio =
        bybit::parse_long_short_ratio(&mut fixture("bybit_long_short.json"), 1_720_000_300_101_000)
            .unwrap();
    assert_eq!(
        ratio_kind(&account[0]),
        TraderMetricKind::BinanceTopAccountRatio
    );
    assert_eq!(
        ratio_kind(&position[0]),
        TraderMetricKind::BinanceTopPositionRatio
    );
    assert_eq!(
        ratio_kind(&bybit_ratio[0]),
        TraderMetricKind::BybitLongShortRatio
    );
}

#[test]
fn bybit_ticker_and_series_preserve_interval_and_order() {
    let ticker = bybit::parse_ticker_funding(
        &mut fixture("bybit_ticker_funding.json"),
        1_720_000_000_124_000,
    )
    .unwrap();
    let DerivativeEvent::FundingEstimate(funding) = &ticker[1] else {
        panic!()
    };
    assert_eq!(funding.interval_secs, 28_800);
    assert_eq!(
        funding.interval_provenance,
        FundingIntervalProvenance::VenuePayload
    );
    assert_eq!(funding.rate_kind, FundingRateKind::IndicativeNext);

    let oi = bybit::parse_open_interest(
        &mut fixture("bybit_open_interest.json"),
        1_720_000_300_101_000,
    )
    .unwrap();
    assert_eq!(oi.len(), 2);
    assert!(oi[0].meta().source_ts_us <= oi[1].meta().source_ts_us);
    let DerivativeEvent::OpenInterest(bybit_oi) = &oi[0] else {
        panic!()
    };
    assert_eq!(bybit_oi.unit, OpenInterestUnit::BaseAsset);
    assert_eq!(bybit_oi.quote_notional, None);
}

#[test]
fn malformed_economics_and_invalid_series_timestamps_are_typed_errors() {
    let mut zero_price = fixture("binance_mark_funding.json");
    let replacement = String::from_utf8(zero_price)
        .unwrap()
        .replace("60000.123456789012345678", "0");
    zero_price = replacement.into_bytes();
    assert!(matches!(
        binance::parse_mark_funding(&mut zero_price, 1_720_000_000_124_000),
        Err(binance::DerivativeParseError::NonPositive {
            field: "mark_price"
        })
    ));

    let mut invalid_timestamp = fixture("bybit_open_interest.json");
    let replacement = String::from_utf8(invalid_timestamp)
        .unwrap()
        .replace("1720000300000\"}", "0\"}");
    invalid_timestamp = replacement.into_bytes();
    assert!(matches!(
        bybit::parse_open_interest(&mut invalid_timestamp, 1_720_000_300_101_000),
        Err(bybit::DerivativeParseError::InvalidTimestamp { field: "timestamp" })
    ));
}

#[test]
fn configured_funding_bounds_are_enforced_without_rounding() {
    let unbounded = binance::FundingRules {
        interval_secs: 14_400,
        interval_provenance: FundingIntervalProvenance::InstrumentRule,
        rate_floor: None,
        rate_cap: None,
    };
    assert!(matches!(
        binance::parse_mark_funding_with_rules(
            &mut fixture("binance_mark_funding.json"),
            1_720_000_000_124_000,
            unbounded,
        ),
        Err(binance::DerivativeParseError::MissingRateBounds)
    ));

    let rules = binance::FundingRules {
        interval_secs: 14_400,
        interval_provenance: FundingIntervalProvenance::InstrumentRule,
        rate_floor: Some(-100_000_000_000_000),
        rate_cap: Some(100_000_000_000_000),
    };
    assert!(matches!(
        binance::parse_mark_funding_with_rules(
            &mut fixture("binance_mark_funding.json"),
            1_720_000_000_124_000,
            rules,
        ),
        Err(binance::DerivativeParseError::RateAboveCap {
            rate: 100_000_000_000_001,
            cap: 100_000_000_000_000,
        })
    ));
}

#[test]
fn sparse_bybit_ticker_delta_patches_a_coherent_seeded_pair() {
    let mut parser =
        bybit::BybitTickerParser::new(md_core::model::CanonicalSymbol::new("BTC", "USDT"));
    parser
        .parse(
            &mut fixture("bybit_ticker_funding.json"),
            1_720_000_000_124_000,
        )
        .unwrap();
    let delta = parser
        .parse(
            &mut fixture("bybit_ticker_delta.json"),
            1_720_000_000_224_000,
        )
        .unwrap();
    let DerivativeEvent::MarkIndex(mark) = &delta[0] else {
        panic!()
    };
    let DerivativeEvent::FundingEstimate(funding) = &delta[1] else {
        panic!()
    };
    assert_eq!(mark.mark_price, 60_001_123_456_789_012_345_678);
    assert_eq!(mark.index_price, 59_999_987_654_321_098_765_432);
    assert_eq!(funding.rate, 110_000_000_000_001);
    assert_eq!(delta[1].meta().source_ts_us, Some(1_720_000_000_223_000));
}

#[test]
fn bybit_ticker_requires_snapshot_and_rejects_sequence_regressions_atomically() {
    let mut parser =
        bybit::BybitTickerParser::new(md_core::model::CanonicalSymbol::new("BTC", "USDT"));
    assert!(matches!(
        parser.parse(
            &mut fixture("bybit_ticker_delta.json"),
            1_720_000_000_224_000,
        ),
        Err(bybit::DerivativeParseError::SnapshotRequired)
    ));
    parser
        .parse(
            &mut fixture("bybit_ticker_funding.json"),
            1_720_000_000_124_000,
        )
        .unwrap();
    assert!(matches!(
        parser.parse(
            &mut fixture("bybit_ticker_delta_regression.json"),
            1_720_000_000_224_000,
        ),
        Err(bybit::DerivativeParseError::SequenceRegression { .. })
    ));
    let mut time_regression = String::from_utf8(fixture("bybit_ticker_delta_regression.json"))
        .unwrap()
        .replace("\"cs\": 99", "\"cs\": 102")
        .into_bytes();
    assert!(matches!(
        parser.parse(&mut time_regression, 1_720_000_000_224_000),
        Err(bybit::DerivativeParseError::TimestampRegression { .. })
    ));
    let mut wrong_symbol = String::from_utf8(fixture("bybit_ticker_delta.json"))
        .unwrap()
        .replace("BTCUSDT", "ETHUSDT")
        .into_bytes();
    assert!(matches!(
        parser.parse(&mut wrong_symbol, 1_720_000_000_224_000),
        Err(bybit::DerivativeParseError::SymbolMismatch { .. })
    ));
    let after = parser
        .parse(
            &mut fixture("bybit_ticker_delta.json"),
            1_720_000_000_224_000,
        )
        .unwrap();
    let DerivativeEvent::FundingEstimate(funding) = &after[1] else {
        panic!()
    };
    assert_eq!(funding.rate, 110_000_000_000_001);
}

#[test]
fn special_binance_funding_and_public_top_trader_use_typed_capabilities() {
    let mut special = String::from_utf8(fixture("binance_funding_history.json"))
        .unwrap()
        .replace("Regular", "Special")
        .into_bytes();
    assert!(matches!(
        binance::parse_funding_history(&mut special, 1_720_022_400_101_000),
        Err(binance::DerivativeParseError::UnsupportedRateType { .. })
    ));
    assert!(matches!(
        binance::top_trader_public_capability(),
        binance::PublicCapability::UnavailableRequiresApiKey {
            code: "BINANCE_TOP_TRADER_REQUIRES_API_KEY"
        }
    ));
}

#[test]
fn binance_legacy_rate_type_and_history_rules_are_explicit() {
    let mut missing = String::from_utf8(fixture("binance_funding_history.json"))
        .unwrap()
        .replace(",\n    \"rateType\": \"Regular\"", "")
        .into_bytes();
    assert!(matches!(
        binance::parse_funding_history(&mut missing.clone(), 1_720_022_400_101_000),
        Err(binance::DerivativeParseError::MissingRateType)
    ));

    let rules = binance::FundingHistoryRules {
        schedule: binance::FundingSchedule::new(vec![binance::EffectiveFundingRule {
            effective_from_ts_us: 0,
            rules: bounded_rules(28_800),
        }])
        .unwrap(),
        legacy_rate_type: binance::LegacyRateTypePolicy::AcceptMissing,
    };
    let accepted =
        binance::parse_funding_history_with_rules(&mut missing, 1_720_022_400_101_000, rules)
            .unwrap();
    let DerivativeEvent::FundingSettlement(value) = &accepted[0] else {
        panic!()
    };
    assert_eq!(
        value.interval_provenance,
        FundingIntervalProvenance::InstrumentRule
    );
}

#[test]
fn funding_history_resolves_effective_dated_interval_transitions() {
    let transition_ts = 1_720_022_400_000_000;
    let schedule = binance::FundingSchedule::new(vec![
        binance::EffectiveFundingRule {
            effective_from_ts_us: 0,
            rules: bounded_rules(3_600),
        },
        binance::EffectiveFundingRule {
            effective_from_ts_us: transition_ts,
            rules: bounded_rules(28_800),
        },
    ])
    .unwrap();
    let binance_events = binance::parse_funding_history_with_rules(
        &mut fixture("binance_funding_history.json"),
        1_720_022_400_101_000,
        binance::FundingHistoryRules {
            schedule: schedule.clone(),
            legacy_rate_type: binance::LegacyRateTypePolicy::RejectMissing,
        },
    )
    .unwrap();
    let bybit_events = bybit::parse_funding_history_with_schedule(
        &mut fixture("bybit_funding_history.json"),
        1_720_022_400_101_000,
        schedule,
    )
    .unwrap();
    for events in [&binance_events, &bybit_events] {
        let DerivativeEvent::FundingSettlement(first) = &events[0] else {
            panic!()
        };
        let DerivativeEvent::FundingSettlement(second) = &events[1] else {
            panic!()
        };
        assert_eq!(first.interval_secs, 3_600);
        assert_eq!(second.interval_secs, 28_800);
    }
}

#[test]
fn production_history_rejects_uncovered_settlements() {
    let schedule = binance::FundingSchedule::new(vec![binance::EffectiveFundingRule {
        effective_from_ts_us: 1_720_000_000_000_000,
        rules: bounded_rules(28_800),
    }])
    .unwrap();
    assert!(matches!(
        binance::parse_funding_history_with_rules(
            &mut fixture("binance_funding_history.json"),
            1_720_022_400_101_000,
            binance::FundingHistoryRules {
                schedule: schedule.clone(),
                legacy_rate_type: binance::LegacyRateTypePolicy::RejectMissing,
            },
        ),
        Err(binance::DerivativeParseError::FundingScheduleUnknown { .. })
    ));
    assert!(matches!(
        bybit::parse_funding_history_with_schedule(
            &mut fixture("bybit_funding_history.json"),
            1_720_022_400_101_000,
            schedule,
        ),
        Err(bybit::DerivativeParseError::FundingScheduleUnknown { .. })
    ));
}

#[test]
fn series_are_sorted_and_semantic_duplicate_timestamps_are_rejected() {
    let mut descending = String::from_utf8(fixture("binance_top_ratio.json"))
        .unwrap()
        .replace("1720000000000", "1720000600000")
        .replacen("BTCUSDT", "ETHUSDT", 1)
        .into_bytes();
    let sorted = binance::parse_top_account_ratio(&mut descending, 1_720_000_700_001_000).unwrap();
    assert!(sorted[0].meta().source_ts_us < sorted[1].meta().source_ts_us);
    assert_eq!(sorted[0].meta().symbol.base, "BTC");
    assert_eq!(sorted[1].meta().symbol.base, "ETH");

    let mut duplicate = String::from_utf8(fixture("bybit_long_short.json"))
        .unwrap()
        .replace("1720000300000", "1720000000000")
        .into_bytes();
    assert!(matches!(
        bybit::parse_long_short_ratio(&mut duplicate, 1_720_000_300_101_000),
        Err(bybit::DerivativeParseError::DuplicateTimestamp { .. })
    ));
}

#[test]
fn bybit_open_interest_value_is_preserved_only_when_supplied() {
    let mut payload = String::from_utf8(fixture("bybit_open_interest.json"))
        .unwrap()
        .replace(
            "\"openInterest\": \"101.000000000000000001\"",
            "\"openInterest\": \"101.000000000000000001\", \"openInterestValue\": \"6060000.000000000000000001\"",
        )
        .into_bytes();
    let events = bybit::parse_open_interest(&mut payload, 1_720_000_300_101_000).unwrap();
    let value = events
        .iter()
        .find_map(|event| match event {
            DerivativeEvent::OpenInterest(value) if value.quote_notional.is_some() => Some(value),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        value.quote_notional,
        Some(6_060_000_000_000_000_000_000_001)
    );
}

fn ratio_kind(event: &DerivativeEvent) -> TraderMetricKind {
    let DerivativeEvent::TraderRatio(value) = event else {
        panic!()
    };
    value.metric_kind
}

fn bounded_rules(interval_secs: u32) -> binance::FundingRules {
    binance::FundingRules {
        interval_secs,
        interval_provenance: FundingIntervalProvenance::InstrumentRule,
        rate_floor: Some(-5_000_000_000_000_000),
        rate_cap: Some(5_000_000_000_000_000),
    }
}
