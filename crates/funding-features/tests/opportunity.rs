use funding_core::{
    calendar::FundingTimestampSource,
    config::{DecimalRounding, ExactDecimal},
    feature::{
        BasisFeature, BasisKind, FeatureSource, InstrumentKind, NamedPrice, NbboQuote, NbboSide,
        PriceKind,
    },
    metadata::{FundingMetadataFeature, FundingRateSignConvention},
    opportunity::{
        CandidateEvaluation, CapacityEvidence, CapacityEvidenceValidity, CapacityLeg,
        CapacitySource, CostModel, FeeAssumption, FeeLiquidity, FeeSource,
        OpportunityRejectionReason, SettlementInclusion, VenueCostModel,
    },
    public::{FundingBasis, FundingIntervalProvenance, FundingRateKind},
};
use funding_features::opportunity::{
    CandidateInput, MarkPriceInput, evaluate_candidate, rank_eligible,
};
use md_core::model::{AdapterId, CanonicalSymbol};
use uuid::Uuid;

const ONE: i128 = ExactDecimal::SCALE;
const DECISION: i64 = 1_800_000_000_000_000;

fn exact(value: i128) -> ExactDecimal {
    ExactDecimal::from_scaled(value).unwrap()
}

fn source(venue: AdapterId, symbol: &CanonicalSymbol, recv: i64) -> FeatureSource {
    FeatureSource {
        event_id: Uuid::now_v7(),
        adapter: venue,
        symbol: symbol.clone(),
        source_sequence: None,
        exchange_event_ts_us: Some(recv - 1),
        exchange_trade_ts_us: None,
        local_recv_ts_us: recv,
        effective_ts_us: recv - 1,
        effective_ts_source: funding_core::feature::EffectiveTimestampSource::ExchangeEvent,
    }
}

fn named(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    kind: PriceKind,
    integer_price: i128,
) -> NamedPrice {
    NamedPrice {
        venue,
        instrument_kind: InstrumentKind::Perpetual,
        kind,
        value: exact(integer_price * ONE),
        source: source(venue, symbol, DECISION - 100),
    }
}

fn quote(
    side: NbboSide,
    price: &NamedPrice,
    requested: ExactDecimal,
    available: ExactDecimal,
) -> NbboQuote {
    NbboQuote {
        venue: price.venue,
        instrument_kind: InstrumentKind::Perpetual,
        symbol: price.source.symbol.clone(),
        side,
        price: price.value,
        requested_base: requested,
        available_base: available,
        quote_notional: Some(
            requested
                .checked_mul(price.value, DecimalRounding::HalfAwayFromZero)
                .unwrap(),
        ),
        levels_consumed: 1,
        age_us: 100,
        source: price.source.clone(),
    }
}

fn funding(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    rate: i128,
    next_settlement_ts_us: i64,
) -> FundingMetadataFeature {
    FundingMetadataFeature {
        source: source(venue, symbol, DECISION - 100),
        raw_rate: exact(rate),
        sign_convention: FundingRateSignConvention::PositiveLongsPayShorts,
        hourly_linear_rate: exact(rate / 8),
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        next_settlement_ts_us,
        settlement_timestamp_source: FundingTimestampSource::VenueAnnounced,
        initial: true,
        decision_ts_us: DECISION,
        freshness_limit_us: 1_000,
        age_us: 100,
    }
}

fn fee(rate: i128) -> FeeAssumption {
    FeeAssumption::new(exact(rate), FeeSource::ExplicitConfig, FeeLiquidity::Taker).unwrap()
}

fn costs() -> CostModel {
    let binance = VenueCostModel {
        entry_fee: fee(4 * ONE / 10_000),
        exit_fee: fee(4 * ONE / 10_000),
        entry_slippage_bps: exact(2 * ONE),
        exit_slippage_bps: exact(10 * ONE),
        entry_book_impact_bps: exact(2 * ONE),
        exit_book_impact_bps: exact(3 * ONE),
    };
    let bybit = VenueCostModel {
        entry_fee: fee(55 * ONE / 100_000),
        exit_fee: fee(55 * ONE / 100_000),
        ..binance.clone()
    };
    CostModel {
        binance,
        bybit,
        basis_risk_buffer_bps: exact(5 * ONE),
        funding_error_buffer_bps: exact(3 * ONE),
        leg_risk_buffer_bps: exact(2 * ONE),
    }
}

struct Fixture {
    basis: BasisFeature,
    long_quote: NbboQuote,
    short_quote: NbboQuote,
    long_funding: FundingMetadataFeature,
    short_funding: FundingMetadataFeature,
    long_mark: NamedPrice,
    short_mark: NamedPrice,
    cost: CostModel,
    caps: Vec<CapacityEvidence>,
}

impl Fixture {
    fn new(long_rate: i128, short_rate: i128, long_next: i64, short_next: i64) -> Self {
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let requested = exact(ONE);
        let available = exact(2 * ONE);
        let long_price = named(
            AdapterId::BybitLinear,
            &symbol,
            PriceKind::PerpetualBuyFromAsks,
            100,
        );
        let short_price = named(
            AdapterId::BinanceUsdm,
            &symbol,
            PriceKind::PerpetualSellIntoBids,
            101,
        );
        let long_quote = quote(NbboSide::Ask, &long_price, requested, available);
        let short_quote = quote(NbboSide::Bid, &short_price, requested, available);
        let basis =
            funding_features::basis::basis_bps(long_price, short_price, DECISION, 1_000).unwrap();
        Self {
            basis,
            long_quote,
            short_quote,
            long_funding: funding(AdapterId::BybitLinear, &symbol, long_rate, long_next),
            short_funding: funding(AdapterId::BinanceUsdm, &symbol, short_rate, short_next),
            long_mark: named(AdapterId::BybitLinear, &symbol, PriceKind::Mark, 100),
            short_mark: named(AdapterId::BinanceUsdm, &symbol, PriceKind::Mark, 101),
            cost: costs(),
            caps: Vec::new(),
        }
    }

    fn evaluate(&self, holding_end_ts_us: i64) -> CandidateEvaluation {
        self.evaluate_with_minimum(holding_end_ts_us, exact(0))
    }

    fn evaluate_with_minimum(
        &self,
        holding_end_ts_us: i64,
        minimum_net_bps: ExactDecimal,
    ) -> CandidateEvaluation {
        evaluate_candidate(CandidateInput {
            entry_basis: &self.basis,
            long_quote: &self.long_quote,
            short_quote: &self.short_quote,
            long_funding: &self.long_funding,
            short_funding: &self.short_funding,
            long_mark: MarkPriceInput {
                price: &self.long_mark,
                freshness_limit_us: 1_000,
            },
            short_mark: MarkPriceInput {
                price: &self.short_mark,
                freshness_limit_us: 1_000,
            },
            cost_model: &self.cost,
            minimum_net_bps,
            holding_end_ts_us,
            caps: &self.caps,
        })
    }
}

fn eligible(result: CandidateEvaluation) -> funding_core::opportunity::Opportunity {
    match result {
        CandidateEvaluation::Eligible(value) => *value,
        CandidateEvaluation::Rejected(value) => panic!("unexpected rejection: {:?}", value.reason),
    }
}

#[test]
fn includes_only_announced_settlements_inside_horizon_and_preserves_unequal_times() {
    let fixture = Fixture::new(-ONE / 100, ONE / 100, DECISION + 20, DECISION + 10);
    let both = eligible(fixture.evaluate(DECISION + 20));
    assert_eq!(both.settlements.len(), 2);
    assert!(
        both.settlements
            .iter()
            .all(|item| item.inclusion == SettlementInclusion::Included)
    );
    assert_ne!(
        both.settlements[0].settlement_ts_us,
        both.settlements[1].settlement_ts_us
    );
    assert!(both.expected_pnl.funding_income > 0);

    let one = eligible(fixture.evaluate(DECISION + 10));
    assert_eq!(
        one.settlements
            .iter()
            .filter(|item| item.inclusion == SettlementInclusion::Included)
            .count(),
        1
    );
    assert_eq!(
        one.settlements
            .iter()
            .filter(|item| item.inclusion == SettlementInclusion::OutsideHoldingWindow)
            .count(),
        1
    );

    let none = fixture.evaluate(DECISION + 5);
    assert!(
        matches!(none, CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::NoAnnouncedSettlementInWindow)
    );
}

#[test]
fn funding_signs_and_entry_basis_are_explicit_and_costs_are_deductions_once() {
    let fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    let opportunity = eligible(fixture.evaluate(DECISION + 10));
    assert_eq!(opportunity.entry_basis.kind, BasisKind::ExecutableEntry);
    assert_eq!(
        opportunity.entry_basis.signed_price_difference.scaled(),
        ONE
    );
    assert_eq!(opportunity.entry_basis.basis_bps.scaled(), 100 * ONE);
    assert!(
        opportunity
            .settlements
            .iter()
            .all(|item| item.cashflow_quote.scaled() > 0)
    );
    assert!(opportunity.expected_pnl.entry_fees > 0);
    assert!(opportunity.expected_pnl.exit_fees > 0);
    assert!(opportunity.expected_pnl.entry_slippage > 0);
    assert!(opportunity.expected_pnl.exit_slippage > 0);
    assert!(opportunity.expected_pnl.entry_book_impact > 0);
    assert!(opportunity.expected_pnl.exit_book_impact > 0);
    assert!(opportunity.expected_pnl.basis_risk_reserve > 0);
    assert!(opportunity.expected_pnl.funding_error_reserve > 0);
    assert!(opportunity.expected_pnl.leg_risk_reserve > 0);
    assert_eq!(opportunity.cost_model, fixture.cost);
    assert_eq!(opportunity.expected_pnl.execution_pnl, 0);
    assert_eq!(opportunity.expected_pnl.basis_pnl, 0);
    assert_eq!(opportunity.expected_pnl.residual_mark_to_market, 0);
    assert_eq!(
        opportunity.expected_net_pnl,
        opportunity.expected_pnl.total().unwrap()
    );
    assert!(opportunity.gross_edge_quote.scaled() > opportunity.net_edge_quote.scaled());

    let positive_long = eligible(
        Fixture::new(ONE / 1_000, ONE / 100, DECISION + 10, DECISION + 10).evaluate(DECISION + 10),
    );
    assert!(
        positive_long
            .settlements
            .iter()
            .find(|item| item.leg == funding_core::opportunity::SettlementLeg::Long)
            .unwrap()
            .cashflow_quote
            .scaled()
            < 0
    );

    let negative_short = eligible(
        Fixture::new(-ONE / 100, -ONE / 1_000, DECISION + 10, DECISION + 10)
            .evaluate(DECISION + 10),
    );
    assert!(
        negative_short
            .settlements
            .iter()
            .find(|item| item.leg == funding_core::opportunity::SettlementLeg::Short)
            .unwrap()
            .cashflow_quote
            .scaled()
            < 0
    );
}

#[test]
fn capacity_is_minimum_of_both_depths_and_explicit_caps_with_all_evidence() {
    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.caps.push(CapacityEvidence {
        source: CapacitySource::RiskLimit,
        venue: Some(AdapterId::BybitLinear),
        leg: CapacityLeg::Long,
        symbol: Some(CanonicalSymbol::new("BTC", "USDT")),
        capacity_base: Some(exact(ONE / 2)),
        capacity_quote: Some(exact(50 * ONE)),
        source_event_id: None,
        source_ts_us: Some(DECISION),
        validity: CapacityEvidenceValidity::Available,
    });
    let result = fixture.evaluate(DECISION + 10);
    assert!(matches!(result, CandidateEvaluation::Rejected(value)
        if matches!(value.reason, OpportunityRejectionReason::InsufficientCapacity { .. })
        && value.capacity_evidence.len() == 3));

    fixture.caps[0].capacity_base = Some(exact(ONE));
    fixture.caps[0].capacity_quote = Some(exact(100 * ONE));
    let value = eligible(fixture.evaluate(DECISION + 10));
    assert_eq!(value.capacity.capacity_base.scaled(), ONE);
    assert_eq!(value.capacity.evidence.len(), 3);
    assert_eq!(value.capacity.binding_evidence.len(), 1);
    assert_eq!(
        value.capacity.binding_evidence[0].source,
        CapacitySource::RiskLimit
    );
}

#[test]
fn rejects_zero_overflow_and_causal_or_identity_tampering() {
    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.long_quote.requested_base = exact(0);
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::NonPositiveRequestedBase)
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.long_mark.source.local_recv_ts_us = DECISION + 1;
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if matches!(value.reason, OpportunityRejectionReason::FutureEvidence { .. }))
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.long_mark.source.local_recv_ts_us = DECISION - 1_001;
    fixture.long_mark.source.effective_ts_us = DECISION - 1_001;
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if matches!(value.reason, OpportunityRejectionReason::StaleEvidence { age_us: 1_001, limit_us: 1_000 }))
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.short_funding.age_us = 99;
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::FundingEvidenceMismatch)
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.short_mark.value = exact(ExactDecimal::MAX_COEFFICIENT);
    let requested = exact(2 * ONE);
    fixture.long_quote.requested_base = requested;
    fixture.short_quote.requested_base = requested;
    fixture.long_quote.quote_notional = Some(
        requested
            .checked_mul(fixture.long_quote.price, DecimalRounding::HalfAwayFromZero)
            .unwrap(),
    );
    fixture.short_quote.quote_notional = Some(
        requested
            .checked_mul(fixture.short_quote.price, DecimalRounding::HalfAwayFromZero)
            .unwrap(),
    );
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::ArithmeticOverflow)
    );
}

#[test]
fn nonpositive_net_is_typed_and_ranking_ties_are_deterministic() {
    let weak = Fixture::new(0, 0, DECISION + 10, DECISION + 10);
    assert!(
        matches!(weak.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::NetEdgeNotPositive)
    );

    let a = eligible(
        Fixture::new(
            -5 * ONE / 1_000,
            5 * ONE / 1_000,
            DECISION + 10,
            DECISION + 10,
        )
        .evaluate(DECISION + 10),
    );
    let mut b = a.clone();
    b.symbol = CanonicalSymbol::new("ETH", "USDT");
    let mut ranked = vec![b, a];
    rank_eligible(&mut ranked);
    assert_eq!(ranked[0].symbol, CanonicalSymbol::new("BTC", "USDT"));
}

#[test]
fn rejects_maker_fee_fabricated_basis_future_cap_and_edge_below_explicit_minimum() {
    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.cost.binance.entry_fee.liquidity = FeeLiquidity::Maker;
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::InvalidCostModel)
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.basis.signed_price_difference = exact(2 * ONE);
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::InvalidEntryBasis)
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.caps.push(CapacityEvidence {
        source: CapacitySource::RiskLimit,
        venue: Some(AdapterId::BybitLinear),
        leg: CapacityLeg::Long,
        symbol: Some(CanonicalSymbol::new("BTC", "USDT")),
        capacity_base: Some(exact(2 * ONE)),
        capacity_quote: Some(exact(200 * ONE)),
        source_event_id: Some(Uuid::now_v7()),
        source_ts_us: Some(DECISION + 1),
        validity: CapacityEvidenceValidity::Available,
    });
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::IdentityMismatch)
    );

    let fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    assert!(
        matches!(fixture.evaluate_with_minimum(DECISION + 10, exact(1_000 * ONE)), CandidateEvaluation::Rejected(value) if matches!(value.reason, OpportunityRejectionReason::NetEdgeBelowMinimum { .. }))
    );

    let mut fixture = Fixture::new(
        -5 * ONE / 1_000,
        5 * ONE / 1_000,
        DECISION + 10,
        DECISION + 10,
    );
    fixture.caps.push(CapacityEvidence {
        source: CapacitySource::BookDepth,
        venue: None,
        leg: CapacityLeg::Pair,
        symbol: Some(CanonicalSymbol::new("BTC", "USDT")),
        capacity_base: Some(exact(2 * ONE)),
        capacity_quote: Some(exact(200 * ONE)),
        source_event_id: Some(Uuid::now_v7()),
        source_ts_us: Some(DECISION),
        validity: CapacityEvidenceValidity::Available,
    });
    assert!(
        matches!(fixture.evaluate(DECISION + 10), CandidateEvaluation::Rejected(value) if value.reason == OpportunityRejectionReason::IdentityMismatch)
    );
}
