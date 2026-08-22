use funding_core::{
    calendar::{FundingCalendar, FundingSlot, FundingSlotEvidence, FundingTimestampSource},
    config::{DecimalMathError, DecimalRounding, ExactDecimal, FundingConfig},
    feature::{
        BookFeatures, BookIdentity, BookInvalidReason, EffectiveTimestampSource, ExecutableQuote,
        ExecutableQuoteSide, FeatureInvalidReason, FeatureSource, FeatureValidity, FlowFeatures,
        FlowInputState, FlowPolicy, OutOfOrderPolicy, QuoteInvalidReason, QuoteValidity,
        StructuralBookValidity, TradeDedupePolicy,
    },
    opportunity::{
        CapacityAssessment, CapacityEvidence, CapacityEvidenceKey, CapacityEvidenceValidity,
        CapacityLeg, CapacitySource, CostModel, FeeAssumption, FeeLiquidity, FeeSource,
        OpportunityExclusion, PnlBreakdown, VenueCostModel,
    },
    public::FundingIntervalProvenance,
};
use md_core::model::{AdapterId, CanonicalSymbol};
use uuid::Uuid;

const ONE: i128 = 1_000_000_000_000_000_000;

fn exact(value: i128) -> ExactDecimal {
    ExactDecimal::from_scaled(value).unwrap()
}

fn source(ts_us: i64) -> FeatureSource {
    FeatureSource {
        event_id: Uuid::now_v7(),
        adapter: AdapterId::BinanceUsdm,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        source_sequence: Some(1),
        exchange_event_ts_us: Some(ts_us),
        exchange_trade_ts_us: None,
        local_recv_ts_us: ts_us + 10,
        effective_ts_us: ts_us,
        effective_ts_source: EffectiveTimestampSource::ExchangeEvent,
    }
}

#[test]
fn exact_decimal_enforces_precision_and_uses_checked_scaled_math() {
    assert!(ExactDecimal::from_scaled(ExactDecimal::MAX_COEFFICIENT).is_ok());
    assert!(matches!(
        ExactDecimal::from_scaled(i128::MAX),
        Err(DecimalMathError::PrecisionOverflow)
    ));
    let product = exact(15 * ONE / 10)
        .checked_mul(exact(2 * ONE), DecimalRounding::HalfAwayFromZero)
        .unwrap();
    assert_eq!(product.scaled(), 3 * ONE);
    assert!(matches!(
        exact(ExactDecimal::MAX_COEFFICIENT)
            .checked_mul(exact(2 * ONE), DecimalRounding::TowardZero),
        Err(DecimalMathError::PrecisionOverflow)
    ));
}

#[test]
fn executable_quotes_are_side_specific_and_separate_from_book_structure() {
    let current = source(1_800_000_000_000_000);
    let previous = BookIdentity {
        event_id: Uuid::now_v7(),
        adapter: AdapterId::BinanceUsdm,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        source_sequence: Some(0),
        exchange_event_ts_us: Some(1_799_999_999_999_000),
        local_recv_ts_us: 1_799_999_999_999_010,
    };
    let sell = ExecutableQuote {
        side: ExecutableQuoteSide::SellIntoBids,
        requested_base: exact(ONE / 4),
        available_base: exact(ONE / 10),
        average_price: None,
        quote_notional: None,
        levels_consumed: 1,
        validity: QuoteValidity::Invalid(QuoteInvalidReason::InsufficientDepth {
            requested_base: exact(ONE / 4),
            available_base: exact(ONE / 10),
        }),
    };
    let buy = ExecutableQuote::invalid(
        ExecutableQuoteSide::BuyFromAsks,
        exact(0),
        exact(0),
        QuoteInvalidReason::InvalidQuantity,
    );
    let features = BookFeatures::invalid(
        current.clone(),
        Some(previous.clone()),
        StructuralBookValidity::Valid,
        sell,
        buy,
        FeatureInvalidReason::ArithmeticOverflow,
    );
    assert_eq!(features.source, current);
    assert_eq!(features.previous_book, Some(previous));
    assert_eq!(features.structural_validity, StructuralBookValidity::Valid);
    assert!(matches!(
        features.sell_into_bids.validity,
        QuoteValidity::Invalid(QuoteInvalidReason::InsufficientDepth { .. })
    ));
    assert!(matches!(
        features.buy_from_asks.validity,
        QuoteValidity::Invalid(QuoteInvalidReason::InvalidQuantity)
    ));

    let crossed = StructuralBookValidity::Invalid(BookInvalidReason::CrossedBook);
    assert_ne!(crossed, features.structural_validity);
}

#[test]
fn feature_invalidity_has_time_overflow_quantity_and_depth_evidence() {
    assert!(matches!(
        FeatureInvalidReason::FutureTimestamp {
            source_ts_us: 20,
            decision_ts_us: 10
        },
        FeatureInvalidReason::FutureTimestamp { .. }
    ));
    assert!(matches!(
        FeatureInvalidReason::RegressingTimestamp {
            previous_ts_us: 20,
            current_ts_us: 10
        },
        FeatureInvalidReason::RegressingTimestamp { .. }
    ));
    assert!(matches!(
        FeatureInvalidReason::InsufficientDepth {
            requested_base: exact(ONE),
            available_base: exact(ONE / 2)
        },
        FeatureInvalidReason::InsufficientDepth { .. }
    ));
}

#[test]
fn flow_contract_distinguishes_no_input_zero_activity_and_trade_sides() {
    let policy = FlowPolicy {
        dedupe: TradeDedupePolicy::EventIdAndVenueTradeId,
        out_of_order: OutOfOrderPolicy::RejectRegressingExchangeTime,
    };
    let no_input = FlowFeatures::no_input(5_000_000, 10_000_000, policy);
    assert_eq!(no_input.input_state, FlowInputState::NoInput);
    assert!(matches!(
        no_input.validity,
        FeatureValidity::Invalid(FeatureInvalidReason::NoInput)
    ));

    let watermark = source(9_999_999);
    let zero = FlowFeatures::zero_activity(5_000_000, 10_000_000, watermark.clone(), policy);
    assert_eq!(zero.input_state, FlowInputState::ZeroActivity);
    assert_eq!(zero.buy_trade_count, 0);
    assert_eq!(zero.sell_trade_count, 0);
    assert_eq!(zero.unknown_trade_count, 0);
    assert_eq!(zero.first_trade_ts_us, None);
    assert_eq!(zero.last_trade_ts_us, None);
    assert_eq!(zero.source_watermark, Some(watermark));
    assert_eq!(zero.duplicate_trade_count, 0);
    assert_eq!(zero.out_of_order_trade_count, 0);
}

#[test]
fn funding_slots_preserve_interval_provenance_initial_and_timestamp_source() {
    let calendar = FundingCalendar::new(vec![FundingSlot::estimated(
        AdapterId::BinanceUsdm,
        CanonicalSymbol::new("BTC", "USDT"),
        1_800_000_000_000_000,
        100_000_000_000_000,
        FundingSlotEvidence {
            interval_secs: 14_400,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            initial: true,
            timestamp_source: FundingTimestampSource::VenueAnnounced,
        },
    )])
    .unwrap();
    let slot = &calendar.slots()[0];
    assert_eq!(slot.interval_secs, 14_400);
    assert_eq!(
        slot.interval_provenance,
        FundingIntervalProvenance::VenuePayload
    );
    assert!(slot.initial);
    assert_eq!(
        slot.timestamp_source,
        FundingTimestampSource::VenueAnnounced
    );
}

#[test]
fn cost_pnl_and_capacity_keep_every_component_and_limit_evidence() {
    let fee = FeeAssumption::new(
        exact(400_000_000_000_000),
        FeeSource::ExplicitConfig,
        FeeLiquidity::Taker,
    )
    .unwrap();
    assert!(FeeAssumption::new(exact(0), FeeSource::ExplicitConfig, FeeLiquidity::Taker).is_err());
    let venue = VenueCostModel {
        entry_fee: fee,
        exit_fee: fee,
        entry_slippage_bps: exact(2 * ONE),
        exit_slippage_bps: exact(10 * ONE),
        entry_book_impact_bps: exact(2 * ONE),
        exit_book_impact_bps: exact(3 * ONE),
    };
    let costs = CostModel {
        binance: venue.clone(),
        bybit: venue,
        basis_risk_buffer_bps: exact(5 * ONE),
        funding_error_buffer_bps: exact(3 * ONE),
        leg_risk_buffer_bps: exact(2 * ONE),
    };
    assert_eq!(costs.binance.exit_slippage_bps.scaled(), 10 * ONE);

    let pnl = PnlBreakdown::zero();
    assert_eq!(pnl.total().unwrap(), 0);
    assert_eq!(pnl.entry_fees, 0);
    assert_eq!(pnl.exit_fees, 0);
    assert_eq!(pnl.entry_slippage, 0);
    assert_eq!(pnl.exit_slippage, 0);
    assert_eq!(pnl.entry_book_impact, 0);
    assert_eq!(pnl.exit_book_impact, 0);
    assert_eq!(pnl.basis_risk_reserve, 0);
    assert_eq!(pnl.funding_error_reserve, 0);
    assert_eq!(pnl.leg_risk_reserve, 0);

    let evidence = vec![
        CapacityEvidence {
            source: CapacitySource::BookDepth,
            venue: Some(AdapterId::BinanceUsdm),
            leg: CapacityLeg::Short,
            symbol: Some(CanonicalSymbol::new("BTC", "USDT")),
            capacity_base: Some(exact(ONE)),
            capacity_quote: Some(exact(60_000 * ONE)),
            source_event_id: Some(Uuid::now_v7()),
            source_ts_us: Some(1_800_000_000_000_000),
            validity: CapacityEvidenceValidity::Available,
        },
        CapacityEvidence {
            source: CapacitySource::ConfiguredResearchLimit,
            venue: None,
            leg: CapacityLeg::Pair,
            symbol: Some(CanonicalSymbol::new("BTC", "USDT")),
            capacity_base: None,
            capacity_quote: Some(exact(100 * ONE)),
            source_event_id: None,
            source_ts_us: None,
            validity: CapacityEvidenceValidity::Available,
        },
    ];
    let capacity = CapacityAssessment::new(
        exact(ONE / 600),
        exact(100 * ONE),
        vec![CapacityEvidenceKey {
            source: CapacitySource::ConfiguredResearchLimit,
            venue: None,
            leg: CapacityLeg::Pair,
        }],
        evidence,
    )
    .unwrap();
    assert_eq!(capacity.evidence.len(), 2);
    assert_eq!(capacity.binding_sources.len(), 1);
    assert_eq!(
        OpportunityExclusion::MissingFeeSource.code(),
        "MISSING_FEE_SOURCE"
    );
}

#[test]
fn cost_config_uses_exact_string_decimals_and_enforces_research_limits() {
    let config = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    assert_eq!(config.cost.binance_taker_rate.scaled(), 400_000_000_000_000);
    assert_eq!(config.cost.bybit_taker_rate.scaled(), 550_000_000_000_000);
    assert_eq!(config.cost.research_quote_per_leg.scaled(), 100 * ONE);
    assert_eq!(
        config.cost.capacity_source,
        CapacitySource::ConfiguredResearchLimit
    );

    let source = std::fs::read_to_string("../../config/funding.toml").unwrap();
    let float_fee = source.replace(
        "binance_taker_rate = \"0.0004\"",
        "binance_taker_rate = 0.0004",
    );
    assert!(toml::from_str::<FundingConfig>(&float_fee).is_err());

    let excessive_precision = source.replace(
        "research_quote_per_leg = \"100\"",
        "research_quote_per_leg = \"999999999999999999999.999999999999999999\"",
    );
    assert!(toml::from_str::<FundingConfig>(&excessive_precision).is_err());
}

#[test]
fn calendar_rejects_invalid_interval_duplicate_and_nonpositive_slots() {
    assert!(FundingCalendar::new(Vec::new()).is_err());
    let slot = FundingSlot::estimated(
        AdapterId::BinanceUsdm,
        CanonicalSymbol::new("BTC", "USDT"),
        1_800_000_000_000_000,
        100_000_000_000_000,
        FundingSlotEvidence {
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            initial: true,
            timestamp_source: FundingTimestampSource::VenueAnnounced,
        },
    );
    assert!(FundingCalendar::new(vec![slot.clone(), slot]).is_err());
    let invalid = FundingSlot::estimated(
        AdapterId::BybitLinear,
        CanonicalSymbol::new("BTC", "USDT"),
        0,
        100_000_000_000_000,
        FundingSlotEvidence {
            interval_secs: 0,
            interval_provenance: FundingIntervalProvenance::InstrumentRule,
            initial: false,
            timestamp_source: FundingTimestampSource::IntervalDerived,
        },
    );
    assert!(FundingCalendar::new(vec![invalid]).is_err());
}

#[test]
fn calendar_allows_estimate_and_reported_settlement_at_the_same_slot() {
    use funding_core::calendar::FundingSlotKind;

    let venue = AdapterId::BinanceUsdm;
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let timestamp = 1_800_000_000_000_000;
    let estimate = FundingSlot::estimated(
        venue,
        symbol.clone(),
        timestamp,
        100_000_000_000_000,
        FundingSlotEvidence {
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            initial: true,
            timestamp_source: FundingTimestampSource::VenueAnnounced,
        },
    );
    let settled = FundingSlot::settled(
        venue,
        symbol,
        timestamp,
        90_000_000_000_000,
        FundingSlotEvidence {
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            initial: true,
            timestamp_source: FundingTimestampSource::VenueReportedSettlement,
        },
    );
    let calendar = FundingCalendar::new(vec![estimate, settled]).unwrap();
    assert_eq!(calendar.slots().len(), 2);
    assert!(
        calendar
            .slots()
            .iter()
            .any(|slot| slot.kind == FundingSlotKind::Settled)
    );
}
