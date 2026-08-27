use funding_core::{
    config::{DecimalMathError, ExactDecimal},
    meta::DerivativeMeta,
    metadata::{
        FundingGapConvention, FundingRateSignConvention, MetadataInvalidReason, ObservationOutcome,
    },
    metadata::{OpenInterestNormalization, QuoteNotionalProvenance},
    public::{
        FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
        OpenInterestSnapshot, OpenInterestUnit, TraderMetricKind, TraderRatioSnapshot,
    },
};
use funding_features::metadata::{MetadataAligner, funding_gap};
use md_core::model::{AdapterId, CanonicalSymbol, TimestampPrecision};
use uuid::Uuid;

const ONE: i128 = ExactDecimal::SCALE;
const DECISION_US: i64 = 1_800_000_100_000_000;

fn meta(venue: AdapterId, symbol: CanonicalSymbol, source_ts_us: i64) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        venue_symbol: format!("{}{}", symbol.base, symbol.quote),
        symbol,
        source_ts_us: Some(source_ts_us),
        source_ts_precision: TimestampPrecision::Millisecond,
        local_recv_ts_us: source_ts_us + 50,
    }
}

fn funding(
    venue: AdapterId,
    symbol: CanonicalSymbol,
    rate: i128,
    interval_secs: u32,
    source_ts_us: i64,
    next_funding_ts_us: i64,
) -> FundingEstimate {
    FundingEstimate {
        meta: meta(venue, symbol, source_ts_us),
        rate,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        next_funding_ts_us,
    }
}

#[test]
fn unequal_intervals_are_preserved_and_linearly_normalized() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let mut aligner = MetadataAligner::new();
    aligner
        .observe_funding(funding(
            AdapterId::BinanceUsdm,
            symbol.clone(),
            8 * ONE / 10_000,
            28_800,
            DECISION_US - 100,
            DECISION_US + 1_000_000,
        ))
        .unwrap();
    aligner
        .observe_funding(funding(
            AdapterId::BybitLinear,
            symbol.clone(),
            -ONE / 10_000,
            14_400,
            DECISION_US - 90,
            DECISION_US + 2_000_000,
        ))
        .unwrap();

    let short = aligner
        .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000)
        .unwrap();
    let long = aligner
        .funding_feature(AdapterId::BybitLinear, &symbol, DECISION_US, 1_000)
        .unwrap();
    let gap = funding_gap(short, long, DECISION_US).unwrap();

    assert_eq!(
        gap.convention,
        FundingGapConvention::ShortHourlyRateMinusLongHourlyRate
    );
    assert_eq!(gap.short.raw_rate.scaled(), 8 * ONE / 10_000);
    assert_eq!(
        gap.short.sign_convention,
        FundingRateSignConvention::PositiveLongsPayShorts
    );
    assert_eq!(gap.short.interval_secs, 28_800);
    assert_eq!(gap.short.hourly_linear_rate.scaled(), ONE / 10_000);
    assert_eq!(gap.long.raw_rate.scaled(), -ONE / 10_000);
    assert_eq!(gap.long.interval_secs, 14_400);
    assert_eq!(gap.long.hourly_linear_rate.scaled(), -ONE / 40_000);
    assert_eq!(gap.signed_hourly_gap.scaled(), 5 * ONE / 40_000);
    assert_ne!(
        gap.short.next_settlement_ts_us,
        gap.long.next_settlement_ts_us
    );
    assert!(gap.short.initial && gap.long.initial);
}

#[test]
fn gap_rejects_non_indicative_or_tampered_schedule_evidence() {
    let symbol = CanonicalSymbol::new("ATOM", "USDT");
    let mut aligner = MetadataAligner::new();
    for venue in [AdapterId::BinanceUsdm, AdapterId::BybitLinear] {
        aligner
            .observe_funding(funding(
                venue,
                symbol.clone(),
                ONE / 10_000,
                28_800,
                DECISION_US - 100,
                DECISION_US + 100,
            ))
            .unwrap();
    }
    let mut short = aligner
        .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000)
        .unwrap();
    let long = aligner
        .funding_feature(AdapterId::BybitLinear, &symbol, DECISION_US, 1_000)
        .unwrap();
    short.rate_kind = FundingRateKind::SettledActual;
    assert!(matches!(
        funding_gap(short, long.clone(), DECISION_US),
        Err(MetadataInvalidReason::FundingRateKindMismatch)
    ));

    let mut short = aligner
        .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000)
        .unwrap();
    short.hourly_linear_rate = ExactDecimal::from_scaled(0).unwrap();
    assert!(matches!(
        funding_gap(short, long, DECISION_US),
        Err(MetadataInvalidReason::FundingEvidenceMismatch)
    ));
}

#[test]
fn gap_revalidates_causal_freshness_evidence_for_each_leg() {
    let symbol = CanonicalSymbol::new("LINK", "USDT");
    let mut aligner = MetadataAligner::new();
    for venue in [AdapterId::BinanceUsdm, AdapterId::BybitLinear] {
        aligner
            .observe_funding(funding(
                venue,
                symbol.clone(),
                ONE / 10_000,
                28_800,
                DECISION_US - 1_050,
                DECISION_US + 100,
            ))
            .unwrap();
    }
    let valid_short = aligner
        .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000)
        .unwrap();
    let valid_long = aligner
        .funding_feature(AdapterId::BybitLinear, &symbol, DECISION_US, 1_000)
        .unwrap();

    assert!(funding_gap(valid_short.clone(), valid_long.clone(), DECISION_US).is_ok());

    let mut short = valid_short.clone();
    short.decision_ts_us -= 1;
    assert_eq!(
        funding_gap(short, valid_long.clone(), DECISION_US),
        Err(MetadataInvalidReason::DecisionTimestampMismatch)
    );

    let mut short = valid_short.clone();
    short.freshness_limit_us = -1;
    assert_eq!(
        funding_gap(short, valid_long.clone(), DECISION_US),
        Err(MetadataInvalidReason::InvalidFreshnessLimit { limit_us: -1 })
    );

    let mut short = valid_short.clone();
    short.age_us -= 1;
    assert_eq!(
        funding_gap(short, valid_long.clone(), DECISION_US),
        Err(MetadataInvalidReason::FundingEvidenceMismatch)
    );

    let mut short = valid_short.clone();
    short.freshness_limit_us = 999;
    assert_eq!(
        funding_gap(short, valid_long.clone(), DECISION_US),
        Err(MetadataInvalidReason::Stale {
            age_us: 1_000,
            limit_us: 999,
        })
    );

    let mut short = valid_short;
    short.source.local_recv_ts_us = DECISION_US + 1;
    short.age_us = -1;
    assert_eq!(
        funding_gap(short, valid_long, DECISION_US),
        Err(MetadataInvalidReason::FutureTimestamp {
            source_ts_us: DECISION_US + 1,
            decision_ts_us: DECISION_US,
        })
    );
}

#[test]
fn freshness_boundary_is_inclusive_and_stale_and_future_are_explicit() {
    let symbol = CanonicalSymbol::new("ETH", "USDT");
    let mut aligner = MetadataAligner::new();
    aligner
        .observe_funding(funding(
            AdapterId::BinanceUsdm,
            symbol.clone(),
            ONE / 10_000,
            28_800,
            DECISION_US - 1_050,
            DECISION_US + 1,
        ))
        .unwrap();
    assert!(
        aligner
            .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000)
            .is_ok()
    );
    assert!(matches!(
        aligner.funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 999),
        Err(MetadataInvalidReason::Stale {
            age_us: 1_000,
            limit_us: 999
        })
    ));

    let future_symbol = CanonicalSymbol::new("SOL", "USDT");
    aligner
        .observe_funding(funding(
            AdapterId::BinanceUsdm,
            future_symbol.clone(),
            ONE / 10_000,
            28_800,
            DECISION_US + 1,
            DECISION_US + 2,
        ))
        .unwrap();
    assert!(matches!(
        aligner.funding_feature(AdapterId::BinanceUsdm, &future_symbol, DECISION_US, 1_000,),
        Err(MetadataInvalidReason::FutureTimestamp { .. })
    ));
}

#[test]
fn missing_identity_and_missing_settlement_evidence_are_rejected() {
    let btc = CanonicalSymbol::new("BTC", "USDT");
    let eth = CanonicalSymbol::new("ETH", "USDT");
    let aligner = MetadataAligner::new();
    assert!(matches!(
        aligner.funding_feature(AdapterId::BinanceUsdm, &btc, DECISION_US, 10),
        Err(MetadataInvalidReason::MissingFunding { .. })
    ));

    let mut aligner = MetadataAligner::new();
    let mut estimate = funding(
        AdapterId::BinanceUsdm,
        btc.clone(),
        ONE / 10_000,
        28_800,
        DECISION_US - 1,
        DECISION_US + 1,
    );
    estimate.meta.symbol = eth.clone();
    assert!(matches!(
        aligner.observe_funding_for(AdapterId::BinanceUsdm, &btc, estimate),
        Err(MetadataInvalidReason::IdentityMismatch { actual_symbol, .. }) if actual_symbol == eth
    ));

    let mut invalid_settlement = funding(
        AdapterId::BinanceUsdm,
        btc,
        ONE / 10_000,
        28_800,
        DECISION_US - 1,
        0,
    );
    invalid_settlement.meta.event_id = Uuid::now_v7();
    assert!(matches!(
        aligner.observe_funding(invalid_settlement),
        Err(MetadataInvalidReason::MissingNextSettlement)
    ));
}

#[test]
fn duplicate_regressing_and_conflicting_updates_do_not_replace_latest() {
    let symbol = CanonicalSymbol::new("XRP", "USDT");
    let mut aligner = MetadataAligner::new();
    let first = funding(
        AdapterId::BinanceUsdm,
        symbol.clone(),
        ONE / 10_000,
        28_800,
        DECISION_US - 100,
        DECISION_US + 10,
    );
    assert_eq!(
        aligner.observe_funding(first.clone()).unwrap(),
        ObservationOutcome::Accepted
    );
    assert_eq!(
        aligner.observe_funding(first.clone()).unwrap(),
        ObservationOutcome::IgnoredDuplicate
    );
    let mut event_id_collision = first.clone();
    event_id_collision.rate = 9 * ONE / 10_000;
    assert!(matches!(
        aligner.observe_funding(event_id_collision),
        Err(MetadataInvalidReason::EventIdConflict { .. })
    ));

    let regressing = funding(
        AdapterId::BinanceUsdm,
        symbol.clone(),
        2 * ONE / 10_000,
        28_800,
        DECISION_US - 101,
        DECISION_US + 10,
    );
    assert!(matches!(
        aligner.observe_funding(regressing),
        Err(MetadataInvalidReason::RegressingUpdate { .. })
    ));
    let conflicting = funding(
        AdapterId::BinanceUsdm,
        symbol.clone(),
        3 * ONE / 10_000,
        28_800,
        DECISION_US - 100,
        DECISION_US + 10,
    );
    assert!(matches!(
        aligner.observe_funding(conflicting),
        Err(MetadataInvalidReason::TimestampConflict { .. })
    ));
    assert_eq!(
        aligner
            .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 100)
            .unwrap()
            .raw_rate
            .scaled(),
        ONE / 10_000
    );
}

#[test]
fn causal_alignment_checks_receive_time_and_accepts_newer_estimate_revision() {
    let symbol = CanonicalSymbol::new("AVAX", "USDT");
    let mut aligner = MetadataAligner::new();
    let mut unavailable = funding(
        AdapterId::BinanceUsdm,
        symbol.clone(),
        ONE / 10_000,
        28_800,
        DECISION_US - 10,
        DECISION_US + 10,
    );
    unavailable.meta.local_recv_ts_us = DECISION_US + 1;
    aligner.observe_funding(unavailable).unwrap();
    assert!(matches!(
        aligner.funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 100),
        Err(MetadataInvalidReason::FutureTimestamp { source_ts_us, .. })
            if source_ts_us == DECISION_US + 1
    ));

    let revision_symbol = CanonicalSymbol::new("LINK", "USDT");
    let next_settlement = DECISION_US + 1_000;
    aligner
        .observe_funding(funding(
            AdapterId::BinanceUsdm,
            revision_symbol.clone(),
            ONE / 10_000,
            28_800,
            DECISION_US - 200,
            next_settlement,
        ))
        .unwrap();
    aligner
        .observe_funding(funding(
            AdapterId::BinanceUsdm,
            revision_symbol.clone(),
            2 * ONE / 10_000,
            28_800,
            DECISION_US - 100,
            next_settlement,
        ))
        .unwrap();
    assert_eq!(
        aligner
            .funding_feature(AdapterId::BinanceUsdm, &revision_symbol, DECISION_US, 100,)
            .unwrap()
            .raw_rate
            .scaled(),
        2 * ONE / 10_000
    );
}

#[test]
fn oi_and_trader_features_keep_units_ratio_kind_and_provenance() {
    let symbol = CanonicalSymbol::new("DOGE", "USDT");
    let mut aligner = MetadataAligner::new();
    let oi_meta = meta(AdapterId::BinanceUsdm, symbol.clone(), DECISION_US - 100);
    let oi_event_id = oi_meta.event_id;
    aligner
        .observe_open_interest(OpenInterestSnapshot {
            meta: oi_meta,
            open_interest: 1_234 * ONE,
            unit: OpenInterestUnit::Contracts,
            quote_notional: Some(20_000 * ONE),
        })
        .unwrap();
    aligner
        .observe_trader_ratio(TraderRatioSnapshot {
            meta: meta(AdapterId::BinanceUsdm, symbol.clone(), DECISION_US - 90),
            metric_kind: TraderMetricKind::BinanceTopPositionRatio,
            long_ratio: 55 * ONE / 100,
            short_ratio: 45 * ONE / 100,
            long_short_ratio: 11 * ONE / 9,
        })
        .unwrap();

    let oi = aligner
        .open_interest_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 100)
        .unwrap();
    assert_eq!(oi.unit, OpenInterestUnit::Contracts);
    assert_eq!(oi.quote_notional.unwrap().scaled(), 20_000 * ONE);
    assert_eq!(
        oi.quote_notional_provenance,
        Some(QuoteNotionalProvenance::VenueReported)
    );
    assert_eq!(
        oi.normalization,
        OpenInterestNormalization::RawVenueUnitNonComparable
    );
    assert_eq!(oi.source.event_id, oi_event_id);

    let ratio = aligner
        .trader_ratio_feature(
            AdapterId::BinanceUsdm,
            &symbol,
            TraderMetricKind::BinanceTopPositionRatio,
            DECISION_US,
            100,
        )
        .unwrap();
    assert_eq!(ratio.metric_kind, TraderMetricKind::BinanceTopPositionRatio);
    assert_eq!(ratio.long_ratio.scaled(), 55 * ONE / 100);
    assert!(matches!(
        aligner.trader_ratio_feature(
            AdapterId::BinanceUsdm,
            &symbol,
            TraderMetricKind::BinanceTopAccountRatio,
            DECISION_US,
            10,
        ),
        Err(MetadataInvalidReason::MissingTraderRatio { .. })
    ));
}

#[test]
fn invalid_intervals_values_and_decimal_overflow_are_explicit() {
    let symbol = CanonicalSymbol::new("ADA", "USDT");
    let mut aligner = MetadataAligner::new();
    assert!(matches!(
        aligner.observe_funding(funding(
            AdapterId::BinanceUsdm,
            symbol.clone(),
            ONE,
            0,
            DECISION_US - 1,
            DECISION_US + 1,
        )),
        Err(MetadataInvalidReason::InvalidFundingInterval)
    ));
    assert!(matches!(
        aligner.observe_open_interest(OpenInterestSnapshot {
            meta: meta(AdapterId::BinanceUsdm, symbol.clone(), DECISION_US - 1),
            open_interest: -1,
            unit: OpenInterestUnit::Contracts,
            quote_notional: None,
        }),
        Err(MetadataInvalidReason::NegativeOpenInterest)
    ));
    assert!(matches!(
        aligner.observe_funding(funding(
            AdapterId::BinanceUsdm,
            symbol,
            i128::MAX,
            28_800,
            DECISION_US - 1,
            DECISION_US + 1,
        )),
        Err(MetadataInvalidReason::ArithmeticOverflow)
    ));

    assert!(matches!(
        ExactDecimal::from_scaled(i128::MAX),
        Err(DecimalMathError::PrecisionOverflow)
    ));
}

#[test]
fn trader_ratio_semantics_are_venue_specific_and_consistent() {
    let symbol = CanonicalSymbol::new("SUI", "USDT");
    let mut aligner = MetadataAligner::new();
    assert!(matches!(
        aligner.observe_trader_ratio(TraderRatioSnapshot {
            meta: meta(AdapterId::BybitLinear, symbol.clone(), DECISION_US - 1),
            metric_kind: TraderMetricKind::BinanceTopAccountRatio,
            long_ratio: ONE / 2,
            short_ratio: ONE / 2,
            long_short_ratio: ONE,
        }),
        Err(MetadataInvalidReason::TraderMetricVenueMismatch { .. })
    ));
    assert!(matches!(
        aligner.observe_trader_ratio(TraderRatioSnapshot {
            meta: meta(AdapterId::BinanceUsdm, symbol, DECISION_US - 1),
            metric_kind: TraderMetricKind::BinanceTopAccountRatio,
            long_ratio: 7 * ONE / 10,
            short_ratio: 4 * ONE / 10,
            long_short_ratio: 7 * ONE / 4,
        }),
        Err(MetadataInvalidReason::InvalidTraderRatio)
    ));
}

#[test]
fn invalid_clock_or_oi_unit_cannot_replace_valid_latest_state() {
    let symbol = CanonicalSymbol::new("NEAR", "USDT");
    let mut aligner = MetadataAligner::new();
    let valid = funding(
        AdapterId::BinanceUsdm,
        symbol.clone(),
        ONE / 10_000,
        28_800,
        DECISION_US - 200,
        DECISION_US + 100,
    );
    aligner.observe_funding(valid).unwrap();
    let mut invalid_clock = funding(
        AdapterId::BinanceUsdm,
        symbol.clone(),
        9 * ONE / 10_000,
        28_800,
        DECISION_US - 100,
        DECISION_US + 100,
    );
    invalid_clock.meta.local_recv_ts_us = invalid_clock.meta.source_ts_us.unwrap() - 1;
    assert!(matches!(
        aligner.observe_funding(invalid_clock),
        Err(MetadataInvalidReason::SourceAfterLocalReceive { .. })
    ));
    assert_eq!(
        aligner
            .funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000)
            .unwrap()
            .raw_rate
            .scaled(),
        ONE / 10_000
    );

    assert!(matches!(
        aligner.observe_open_interest(OpenInterestSnapshot {
            meta: meta(AdapterId::BybitLinear, symbol, DECISION_US - 100),
            open_interest: ONE,
            unit: OpenInterestUnit::Contracts,
            quote_notional: None,
        }),
        Err(MetadataInvalidReason::OpenInterestVenueUnitMismatch { .. })
    ));
}

#[test]
fn dedupe_memory_is_bounded_and_expired_settlement_is_not_exposed() {
    let mut aligner = MetadataAligner::with_dedupe_capacity(2).unwrap();
    for (index, base) in ["BTC", "ETH", "SOL"].into_iter().enumerate() {
        aligner
            .observe_funding(funding(
                AdapterId::BinanceUsdm,
                CanonicalSymbol::new(base, "USDT"),
                ONE / 10_000,
                28_800,
                DECISION_US - 300 + index as i64,
                DECISION_US + 100,
            ))
            .unwrap();
    }
    assert_eq!(aligner.dedupe_len(), 2);
    assert!(matches!(
        MetadataAligner::with_dedupe_capacity(0),
        Err(MetadataInvalidReason::InvalidDedupeCapacity)
    ));

    let symbol = CanonicalSymbol::new("OP", "USDT");
    let mut expired = MetadataAligner::new();
    expired
        .observe_funding(funding(
            AdapterId::BinanceUsdm,
            symbol.clone(),
            ONE / 10_000,
            28_800,
            DECISION_US - 100,
            DECISION_US,
        ))
        .unwrap();
    assert!(matches!(
        expired.funding_feature(AdapterId::BinanceUsdm, &symbol, DECISION_US, 1_000),
        Err(MetadataInvalidReason::NextSettlementNotFuture { .. })
    ));
}
