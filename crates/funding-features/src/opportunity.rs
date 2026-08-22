//! Pure, read-only evaluation of one cross-perpetual funding candidate.

use std::collections::HashSet;

pub use funding_core::opportunity::*;
use funding_core::{
    config::{DecimalRounding, ExactDecimal},
    feature::{
        BasisFeature, BasisKind, FeatureSource, FeatureValidity, InstrumentKind, NamedPrice,
        NbboQuote, NbboSide, PriceKind,
    },
    metadata::{FundingMetadataFeature, FundingRateSignConvention, MetadataInvalidReason},
    public::{FundingBasis, FundingRateKind},
};
use md_core::model::{AdapterId, CanonicalSymbol};

use crate::metadata::funding_gap;

const BPS_DENOMINATOR: i128 = 10_000;
const HOURS_PER_YEAR: i128 = 24 * 365;

#[derive(Debug, Clone, Copy)]
pub struct MarkPriceInput<'a> {
    pub price: &'a NamedPrice,
    pub freshness_limit_us: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct CandidateInput<'a> {
    pub entry_basis: &'a BasisFeature,
    pub long_quote: &'a NbboQuote,
    pub short_quote: &'a NbboQuote,
    pub long_funding: &'a FundingMetadataFeature,
    pub short_funding: &'a FundingMetadataFeature,
    pub long_mark: MarkPriceInput<'a>,
    pub short_mark: MarkPriceInput<'a>,
    pub cost_model: &'a CostModel,
    pub minimum_net_bps: ExactDecimal,
    pub holding_end_ts_us: i64,
    pub caps: &'a [CapacityEvidence],
}

pub fn evaluate_candidate(input: CandidateInput<'_>) -> CandidateEvaluation {
    match evaluate(input) {
        Ok(value) => CandidateEvaluation::Eligible(Box::new(value)),
        Err(value) => CandidateEvaluation::Rejected(value),
    }
}

/// Sorts eligible candidates by net quote edge, then capacity, symbol, and
/// stable venue rank. No allocation or portfolio optimization is performed.
pub fn rank_eligible(values: &mut [Opportunity]) {
    values.sort_by(|left, right| {
        right
            .expected_net_bps
            .cmp(&left.expected_net_bps)
            .then_with(|| left.symbol.base.cmp(&right.symbol.base))
            .then_with(|| left.symbol.quote.cmp(&right.symbol.quote))
            .then_with(|| venue_rank(left.short_venue).cmp(&venue_rank(right.short_venue)))
            .then_with(|| venue_rank(left.long_venue).cmp(&venue_rank(right.long_venue)))
    });
}

fn evaluate(input: CandidateInput<'_>) -> Result<Opportunity, Box<OpportunityRejection>> {
    let decision_ts_us = input.entry_basis.decision_ts_us;
    let symbol = Some(input.entry_basis.symbol.clone());
    let reject = |reason, capacity_evidence| {
        Box::new(OpportunityRejection {
            symbol: symbol.clone(),
            decision_ts_us,
            reason,
            capacity_evidence,
        })
    };

    if input.holding_end_ts_us <= decision_ts_us {
        return Err(reject(
            OpportunityRejectionReason::InvalidHoldingWindow,
            Vec::new(),
        ));
    }
    validate_entry(input).map_err(|reason| reject(reason, Vec::new()))?;
    validate_mark(
        input.long_mark,
        input.long_quote.venue,
        &input.entry_basis.symbol,
        decision_ts_us,
    )
    .map_err(|reason| reject(reason, Vec::new()))?;
    validate_mark(
        input.short_mark,
        input.short_quote.venue,
        &input.entry_basis.symbol,
        decision_ts_us,
    )
    .map_err(|reason| reject(reason, Vec::new()))?;

    let gap = funding_gap(
        input.short_funding.clone(),
        input.long_funding.clone(),
        decision_ts_us,
    )
    .map_err(|reason| reject(map_funding_error(reason), Vec::new()))?;
    validate_funding_identity(input).map_err(|reason| reject(reason, Vec::new()))?;
    validate_cost_model(input.cost_model).map_err(|reason| reject(reason, Vec::new()))?;
    if input.minimum_net_bps.scaled() < 0 {
        return Err(reject(
            OpportunityRejectionReason::InvalidCostModel,
            Vec::new(),
        ));
    }

    let (capacity, capacity_evidence) =
        capacity(input).map_err(|(reason, evidence)| reject(reason, evidence))?;
    let requested_base = input.long_quote.requested_base;
    if requested_base > capacity.capacity_base {
        return Err(reject(
            OpportunityRejectionReason::InsufficientCapacity {
                requested_base,
                capacity_base: capacity.capacity_base,
            },
            capacity_evidence,
        ));
    }

    let long_funding_notional = mul(requested_base, input.long_mark.price.value)
        .map_err(|reason| reject(reason, capacity_evidence.clone()))?;
    let short_funding_notional = mul(requested_base, input.short_mark.price.value)
        .map_err(|reason| reject(reason, capacity_evidence.clone()))?;
    let long_settlement = settlement(
        input.long_funding,
        SettlementLeg::Long,
        input.long_mark.price.value,
        long_funding_notional,
        input.holding_end_ts_us,
    )
    .map_err(|reason| reject(reason, capacity_evidence.clone()))?;
    let short_settlement = settlement(
        input.short_funding,
        SettlementLeg::Short,
        input.short_mark.price.value,
        short_funding_notional,
        input.holding_end_ts_us,
    )
    .map_err(|reason| reject(reason, capacity_evidence.clone()))?;
    let settlements = vec![short_settlement, long_settlement];
    if !settlements
        .iter()
        .any(|item| item.inclusion == SettlementInclusion::Included)
    {
        return Err(reject(
            OpportunityRejectionReason::NoAnnouncedSettlementInWindow,
            capacity_evidence,
        ));
    }

    let long_entry_notional = input.long_quote.quote_notional.ok_or_else(|| {
        reject(
            OpportunityRejectionReason::MissingNotionalEvidence,
            capacity_evidence.clone(),
        )
    })?;
    let short_entry_notional = input.short_quote.quote_notional.ok_or_else(|| {
        reject(
            OpportunityRejectionReason::MissingNotionalEvidence,
            capacity_evidence.clone(),
        )
    })?;
    let risk_notional_quote = long_entry_notional
        .checked_add(short_entry_notional)
        .map_err(|_| {
            reject(
                OpportunityRejectionReason::ArithmeticOverflow,
                capacity_evidence.clone(),
            )
        })?;
    let pnl = pnl(
        &settlements,
        input.cost_model,
        input.long_quote.venue,
        long_entry_notional,
        input.short_quote.venue,
        short_entry_notional,
        risk_notional_quote,
    )
    .map_err(|reason| reject(reason, capacity_evidence.clone()))?;
    let expected_net_pnl = pnl.total().map_err(|_| {
        reject(
            OpportunityRejectionReason::ArithmeticOverflow,
            capacity_evidence.clone(),
        )
    })?;
    if expected_net_pnl <= 0 {
        return Err(reject(
            OpportunityRejectionReason::NetEdgeNotPositive,
            capacity_evidence,
        ));
    }
    let net_edge_quote = ExactDecimal::from_scaled(expected_net_pnl).map_err(|_| {
        reject(
            OpportunityRejectionReason::ArithmeticOverflow,
            capacity.evidence.clone(),
        )
    })?;
    let gross_edge_quote = ExactDecimal::from_scaled(pnl.funding_income).map_err(|_| {
        reject(
            OpportunityRejectionReason::ArithmeticOverflow,
            capacity.evidence.clone(),
        )
    })?;
    let expected_net_bps = ratio_bps(net_edge_quote, risk_notional_quote)
        .map_err(|reason| reject(reason, capacity.evidence.clone()))?;
    if expected_net_bps < input.minimum_net_bps {
        return Err(reject(
            OpportunityRejectionReason::NetEdgeBelowMinimum {
                net_bps: expected_net_bps,
                minimum_bps: input.minimum_net_bps,
            },
            capacity.evidence.clone(),
        ));
    }
    let raw_gap = input
        .short_funding
        .raw_rate
        .checked_sub(input.long_funding.raw_rate)
        .map_err(|_| {
            reject(
                OpportunityRejectionReason::ArithmeticOverflow,
                capacity.evidence.clone(),
            )
        })?;
    // Display-only normalization retained by the existing Opportunity contract;
    // neither value participates in eligibility or ranking.
    let hours_per_year = exact_integer(HOURS_PER_YEAR)
        .map_err(|reason| reject(reason, capacity.evidence.clone()))?;
    let indicative_apr = gap
        .signed_hourly_gap
        .checked_mul(hours_per_year, DecimalRounding::HalfAwayFromZero)
        .map_err(|_| {
            reject(
                OpportunityRejectionReason::ArithmeticOverflow,
                capacity.evidence.clone(),
            )
        })?;

    Ok(Opportunity {
        symbol: input.entry_basis.symbol.clone(),
        short_venue: input.short_quote.venue,
        long_venue: input.long_quote.venue,
        capacity,
        requested_base,
        long_entry: input.long_quote.clone(),
        short_entry: input.short_quote.clone(),
        long_mark: input.long_mark.price.clone(),
        short_mark: input.short_mark.price.clone(),
        long_funding: input.long_funding.clone(),
        short_funding: input.short_funding.clone(),
        entry_basis: input.entry_basis.clone(),
        settlements,
        holding_end_ts_us: input.holding_end_ts_us,
        risk_notional_quote,
        risk_notional_convention: RiskNotionalConvention::TotalEntryGrossQuote,
        cost_model: input.cost_model.clone(),
        quote_asset: input.entry_basis.symbol.quote.clone(),
        decimal_scale: 18,
        raw_gap,
        hourly_gap: gap.signed_hourly_gap,
        indicative_apr,
        conservative_funding_cashflows: pnl.funding_income,
        expected_pnl: pnl,
        expected_net_pnl,
        expected_net_bps,
        minimum_net_bps: input.minimum_net_bps,
        gross_edge_quote,
        net_edge_quote,
        decision_ts_us,
    })
}

fn validate_entry(input: CandidateInput<'_>) -> Result<(), OpportunityRejectionReason> {
    let basis = input.entry_basis;
    if basis.kind != BasisKind::ExecutableEntry || basis.validity != FeatureValidity::Valid {
        return Err(OpportunityRejectionReason::InvalidEntryBasis);
    }
    if basis.reference.kind != PriceKind::PerpetualBuyFromAsks
        || basis.compared.kind != PriceKind::PerpetualSellIntoBids
        || basis.reference.instrument_kind != InstrumentKind::Perpetual
        || basis.compared.instrument_kind != InstrumentKind::Perpetual
        || input.long_quote.side != NbboSide::Ask
        || input.short_quote.side != NbboSide::Bid
    {
        return Err(OpportunityRejectionReason::InvalidEntryBasis);
    }
    if input.long_quote.requested_base.scaled() <= 0 {
        return Err(OpportunityRejectionReason::NonPositiveRequestedBase);
    }
    if input.long_quote.requested_base != input.short_quote.requested_base {
        return Err(OpportunityRejectionReason::RequestedQuantityMismatch);
    }
    validate_quote(input.long_quote, &basis.reference, basis)?;
    validate_quote(input.short_quote, &basis.compared, basis)?;
    let recomputed = crate::basis::basis_bps(
        basis.reference.clone(),
        basis.compared.clone(),
        basis.decision_ts_us,
        basis.freshness_limit_us,
    )
    .map_err(|_| OpportunityRejectionReason::InvalidEntryBasis)?;
    if recomputed.signed_price_difference != basis.signed_price_difference
        || recomputed.basis_bps != basis.basis_bps
    {
        return Err(OpportunityRejectionReason::InvalidEntryBasis);
    }
    if input.long_quote.venue == input.short_quote.venue {
        return Err(OpportunityRejectionReason::IdentityMismatch);
    }
    Ok(())
}

fn validate_quote(
    quote: &NbboQuote,
    price: &NamedPrice,
    basis: &BasisFeature,
) -> Result<(), OpportunityRejectionReason> {
    if quote.venue != price.venue
        || quote.instrument_kind != InstrumentKind::Perpetual
        || quote.symbol != basis.symbol
        || quote.source != price.source
        || quote.price != price.value
        || quote.source.adapter != quote.venue
        || quote.source.symbol != quote.symbol
    {
        return Err(OpportunityRejectionReason::IdentityMismatch);
    }
    validate_source(
        &quote.source,
        basis.decision_ts_us,
        basis.freshness_limit_us,
    )?;
    let age = basis.decision_ts_us - quote.source.local_recv_ts_us;
    if quote.age_us != age {
        return Err(OpportunityRejectionReason::IdentityMismatch);
    }
    if quote.available_base.scaled() <= 0 {
        return Err(OpportunityRejectionReason::InsufficientCapacity {
            requested_base: quote.requested_base,
            capacity_base: quote.available_base,
        });
    }
    let expected_notional = mul(quote.requested_base, quote.price)?;
    if quote.quote_notional != Some(expected_notional) || expected_notional.scaled() <= 0 {
        return Err(OpportunityRejectionReason::MissingNotionalEvidence);
    }
    Ok(())
}

fn validate_mark(
    mark: MarkPriceInput<'_>,
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    decision_ts_us: i64,
) -> Result<(), OpportunityRejectionReason> {
    if mark.freshness_limit_us < 0 {
        return Err(OpportunityRejectionReason::StaleEvidence {
            age_us: 0,
            limit_us: mark.freshness_limit_us,
        });
    }
    if mark.price.venue != venue
        || mark.price.source.adapter != venue
        || mark.price.source.symbol != *symbol
        || mark.price.instrument_kind != InstrumentKind::Perpetual
        || mark.price.kind != PriceKind::Mark
        || mark.price.value.scaled() <= 0
    {
        return Err(OpportunityRejectionReason::IdentityMismatch);
    }
    validate_source(&mark.price.source, decision_ts_us, mark.freshness_limit_us)
}

fn validate_source(
    source: &FeatureSource,
    decision_ts_us: i64,
    freshness_limit_us: i64,
) -> Result<(), OpportunityRejectionReason> {
    let source_ts_us = source.local_recv_ts_us.max(source.effective_ts_us);
    if source_ts_us > decision_ts_us {
        return Err(OpportunityRejectionReason::FutureEvidence {
            source_ts_us,
            decision_ts_us,
        });
    }
    let age_us = decision_ts_us
        .checked_sub(source.local_recv_ts_us)
        .ok_or(OpportunityRejectionReason::ArithmeticOverflow)?;
    if age_us > freshness_limit_us {
        return Err(OpportunityRejectionReason::StaleEvidence {
            age_us,
            limit_us: freshness_limit_us,
        });
    }
    Ok(())
}

fn validate_funding_identity(input: CandidateInput<'_>) -> Result<(), OpportunityRejectionReason> {
    if input.long_funding.source.adapter != input.long_quote.venue
        || input.short_funding.source.adapter != input.short_quote.venue
        || input.long_funding.source.symbol != input.entry_basis.symbol
        || input.short_funding.source.symbol != input.entry_basis.symbol
        || input.long_funding.sign_convention != FundingRateSignConvention::PositiveLongsPayShorts
        || input.short_funding.sign_convention != FundingRateSignConvention::PositiveLongsPayShorts
        || input.long_funding.rate_kind != FundingRateKind::IndicativeNext
        || input.short_funding.rate_kind != FundingRateKind::IndicativeNext
        || input.long_funding.basis != FundingBasis::MarkNotional
        || input.short_funding.basis != FundingBasis::MarkNotional
    {
        return Err(OpportunityRejectionReason::FundingEvidenceMismatch);
    }
    Ok(())
}

fn validate_cost_model(cost: &CostModel) -> Result<(), OpportunityRejectionReason> {
    validate_venue_cost(&cost.binance)?;
    validate_venue_cost(&cost.bybit)?;
    for bps in [
        cost.basis_risk_buffer_bps,
        cost.funding_error_buffer_bps,
        cost.leg_risk_buffer_bps,
    ] {
        if bps.scaled() < 0 {
            return Err(OpportunityRejectionReason::InvalidCostModel);
        }
    }
    Ok(())
}

fn validate_venue_cost(cost: &VenueCostModel) -> Result<(), OpportunityRejectionReason> {
    for fee in [cost.entry_fee, cost.exit_fee] {
        if fee.rate.scaled() <= 0
            || fee.rate.scaled() > ExactDecimal::SCALE
            || fee.liquidity != FeeLiquidity::Taker
        {
            return Err(OpportunityRejectionReason::InvalidCostModel);
        }
    }
    for bps in [
        cost.entry_slippage_bps,
        cost.exit_slippage_bps,
        cost.entry_book_impact_bps,
        cost.exit_book_impact_bps,
    ] {
        if bps.scaled() < 0 {
            return Err(OpportunityRejectionReason::InvalidCostModel);
        }
    }
    Ok(())
}

fn capacity(
    input: CandidateInput<'_>,
) -> Result<
    (CapacityAssessment, Vec<CapacityEvidence>),
    (OpportunityRejectionReason, Vec<CapacityEvidence>),
> {
    let long_depth = depth_evidence(input.long_quote, CapacityLeg::Long)
        .map_err(|reason| (reason, Vec::new()))?;
    let short_depth = depth_evidence(input.short_quote, CapacityLeg::Short)
        .map_err(|reason| (reason, vec![long_depth.clone()]))?;
    let mut evidence = vec![long_depth, short_depth];
    if input.caps.iter().any(|item| {
        !matches!(
            item.source,
            CapacitySource::ConfiguredResearchLimit
                | CapacitySource::RiskLimit
                | CapacitySource::AuthenticatedMargin
        )
    }) {
        return Err((OpportunityRejectionReason::IdentityMismatch, evidence));
    }
    evidence.extend_from_slice(input.caps);
    let mut keys = HashSet::new();
    let mut minimum: Option<ExactDecimal> = None;
    for item in &evidence {
        if !keys.insert((item.source, item.venue, item.leg))
            || !matches!(item.validity, CapacityEvidenceValidity::Available)
        {
            return Err((OpportunityRejectionReason::IdentityMismatch, evidence));
        }
        if item.symbol.as_ref() != Some(&input.entry_basis.symbol) {
            return Err((OpportunityRejectionReason::IdentityMismatch, evidence));
        }
        if item.source != CapacitySource::BookDepth {
            let identity_valid = match item.leg {
                CapacityLeg::Long => item.venue == Some(input.long_quote.venue),
                CapacityLeg::Short => item.venue == Some(input.short_quote.venue),
                CapacityLeg::Pair => item.venue.is_none(),
            };
            let causal = item
                .source_ts_us
                .is_some_and(|timestamp| timestamp <= input.entry_basis.decision_ts_us);
            if !identity_valid || !causal {
                return Err((OpportunityRejectionReason::IdentityMismatch, evidence));
            }
        }
        let price = match item.leg {
            CapacityLeg::Long => input.long_quote.price,
            CapacityLeg::Short => input.short_quote.price,
            CapacityLeg::Pair => input.long_quote.price.max(input.short_quote.price),
        };
        let quote_base = match item.capacity_quote {
            Some(value) if value.scaled() > 0 => Some(
                value
                    .checked_div(price, DecimalRounding::Floor)
                    .map_err(|_| {
                        (
                            OpportunityRejectionReason::ArithmeticOverflow,
                            evidence.clone(),
                        )
                    })?,
            ),
            Some(_) => Some(exact_zero()),
            None => None,
        };
        let item_base = match (item.capacity_base, quote_base) {
            (Some(base), Some(from_quote)) => base.min(from_quote),
            (Some(base), None) => base,
            (None, Some(from_quote)) => from_quote,
            (None, None) => {
                return Err((OpportunityRejectionReason::IdentityMismatch, evidence));
            }
        };
        minimum = Some(minimum.map_or(item_base, |current| current.min(item_base)));
    }
    let capacity_base = minimum.unwrap_or_else(exact_zero);
    if capacity_base.scaled() <= 0 {
        return Err((
            OpportunityRejectionReason::InsufficientCapacity {
                requested_base: input.long_quote.requested_base,
                capacity_base,
            },
            evidence,
        ));
    }
    let conservative_price = input.long_quote.price.max(input.short_quote.price);
    let capacity_quote =
        mul(capacity_base, conservative_price).map_err(|reason| (reason, evidence.clone()))?;
    let mut binding_evidence = Vec::new();
    for item in &evidence {
        let price = match item.leg {
            CapacityLeg::Long => input.long_quote.price,
            CapacityLeg::Short => input.short_quote.price,
            CapacityLeg::Pair => conservative_price,
        };
        let quote_base = item
            .capacity_quote
            .and_then(|value| value.checked_div(price, DecimalRounding::Floor).ok());
        let item_base = match (item.capacity_base, quote_base) {
            (Some(base), Some(from_quote)) => base.min(from_quote),
            (Some(base), None) => base,
            (None, Some(from_quote)) => from_quote,
            (None, None) => continue,
        };
        if item_base == capacity_base {
            binding_evidence.push(CapacityEvidenceKey {
                source: item.source,
                venue: item.venue,
                leg: item.leg,
            });
        }
    }
    let assessment = CapacityAssessment::new(
        capacity_base,
        capacity_quote,
        binding_evidence,
        evidence.clone(),
    )
    .map_err(|_| {
        (
            OpportunityRejectionReason::IdentityMismatch,
            evidence.clone(),
        )
    })?;
    Ok((assessment, evidence))
}

fn depth_evidence(
    quote: &NbboQuote,
    leg: CapacityLeg,
) -> Result<CapacityEvidence, OpportunityRejectionReason> {
    let capacity_quote = mul(quote.available_base, quote.price)?;
    Ok(CapacityEvidence {
        source: CapacitySource::BookDepth,
        venue: Some(quote.venue),
        leg,
        symbol: Some(quote.symbol.clone()),
        capacity_base: Some(quote.available_base),
        capacity_quote: Some(capacity_quote),
        source_event_id: Some(quote.source.event_id),
        source_ts_us: Some(quote.source.local_recv_ts_us),
        validity: CapacityEvidenceValidity::Available,
    })
}

fn settlement(
    funding: &FundingMetadataFeature,
    leg: SettlementLeg,
    mark_price: ExactDecimal,
    funding_notional_quote: ExactDecimal,
    holding_end_ts_us: i64,
) -> Result<SettlementCashflowEvidence, OpportunityRejectionReason> {
    let inclusion = if funding.next_settlement_ts_us <= holding_end_ts_us {
        SettlementInclusion::Included
    } else {
        SettlementInclusion::OutsideHoldingWindow
    };
    let cashflow_quote = if inclusion == SettlementInclusion::Included {
        let signed = mul(funding_notional_quote, funding.raw_rate)?;
        match leg {
            SettlementLeg::Short => signed,
            SettlementLeg::Long => ExactDecimal::from_scaled(-signed.scaled())
                .map_err(|_| OpportunityRejectionReason::ArithmeticOverflow)?,
        }
    } else {
        exact_zero()
    };
    Ok(SettlementCashflowEvidence {
        venue: funding.source.adapter,
        leg,
        settlement_ts_us: funding.next_settlement_ts_us,
        inclusion,
        announced_rate: funding.raw_rate,
        mark_price,
        funding_notional_quote,
        cashflow_quote,
    })
}

fn pnl(
    settlements: &[SettlementCashflowEvidence],
    cost: &CostModel,
    long_venue: AdapterId,
    long_notional: ExactDecimal,
    short_venue: AdapterId,
    short_notional: ExactDecimal,
    risk_notional: ExactDecimal,
) -> Result<PnlBreakdown, OpportunityRejectionReason> {
    let long_cost = venue_cost(cost, long_venue)?;
    let short_cost = venue_cost(cost, short_venue)?;
    let funding_income = settlements.iter().try_fold(0_i128, |total, item| {
        total
            .checked_add(item.cashflow_quote.scaled())
            .ok_or(OpportunityRejectionReason::ArithmeticOverflow)
    })?;
    Ok(PnlBreakdown {
        funding_income,
        execution_pnl: 0,
        basis_pnl: 0,
        entry_fees: sum_fraction_cost(
            long_notional,
            long_cost.entry_fee.rate,
            short_notional,
            short_cost.entry_fee.rate,
        )?,
        exit_fees: sum_fraction_cost(
            long_notional,
            long_cost.exit_fee.rate,
            short_notional,
            short_cost.exit_fee.rate,
        )?,
        entry_slippage: sum_bps_cost(
            long_notional,
            long_cost.entry_slippage_bps,
            short_notional,
            short_cost.entry_slippage_bps,
        )?,
        exit_slippage: sum_bps_cost(
            long_notional,
            long_cost.exit_slippage_bps,
            short_notional,
            short_cost.exit_slippage_bps,
        )?,
        entry_book_impact: sum_bps_cost(
            long_notional,
            long_cost.entry_book_impact_bps,
            short_notional,
            short_cost.entry_book_impact_bps,
        )?,
        exit_book_impact: sum_bps_cost(
            long_notional,
            long_cost.exit_book_impact_bps,
            short_notional,
            short_cost.exit_book_impact_bps,
        )?,
        basis_risk_reserve: bps_cost(risk_notional, cost.basis_risk_buffer_bps)?.scaled(),
        funding_error_reserve: bps_cost(risk_notional, cost.funding_error_buffer_bps)?.scaled(),
        leg_risk_reserve: bps_cost(risk_notional, cost.leg_risk_buffer_bps)?.scaled(),
        residual_mark_to_market: 0,
    })
}

fn venue_cost(
    cost: &CostModel,
    venue: AdapterId,
) -> Result<&VenueCostModel, OpportunityRejectionReason> {
    match venue {
        AdapterId::BinanceUsdm => Ok(&cost.binance),
        AdapterId::BybitLinear => Ok(&cost.bybit),
        _ => Err(OpportunityRejectionReason::InvalidCostModel),
    }
}

fn sum_fraction_cost(
    left_notional: ExactDecimal,
    left_rate: ExactDecimal,
    right_notional: ExactDecimal,
    right_rate: ExactDecimal,
) -> Result<i128, OpportunityRejectionReason> {
    let left = mul(left_notional, left_rate)?;
    let right = mul(right_notional, right_rate)?;
    left.scaled()
        .checked_add(right.scaled())
        .ok_or(OpportunityRejectionReason::ArithmeticOverflow)
}

fn sum_bps_cost(
    left_notional: ExactDecimal,
    left_bps: ExactDecimal,
    right_notional: ExactDecimal,
    right_bps: ExactDecimal,
) -> Result<i128, OpportunityRejectionReason> {
    let left = bps_cost(left_notional, left_bps)?;
    let right = bps_cost(right_notional, right_bps)?;
    left.scaled()
        .checked_add(right.scaled())
        .ok_or(OpportunityRejectionReason::ArithmeticOverflow)
}

/// Fee rates are fractions. Slippage, impact, and pair reserves are bps and
/// are divided by 10,000 exactly once.
fn bps_cost(
    notional: ExactDecimal,
    bps: ExactDecimal,
) -> Result<ExactDecimal, OpportunityRejectionReason> {
    let fraction = bps
        .checked_div(
            exact_integer(BPS_DENOMINATOR)?,
            DecimalRounding::HalfAwayFromZero,
        )
        .map_err(|_| OpportunityRejectionReason::ArithmeticOverflow)?;
    mul(notional, fraction)
}

fn ratio_bps(
    value: ExactDecimal,
    notional: ExactDecimal,
) -> Result<ExactDecimal, OpportunityRejectionReason> {
    value
        .checked_div(notional, DecimalRounding::HalfAwayFromZero)
        .and_then(|ratio| {
            ratio.checked_mul(
                ExactDecimal::from_scaled(BPS_DENOMINATOR * ExactDecimal::SCALE)?,
                DecimalRounding::HalfAwayFromZero,
            )
        })
        .map_err(|_| OpportunityRejectionReason::ArithmeticOverflow)
}

fn mul(
    left: ExactDecimal,
    right: ExactDecimal,
) -> Result<ExactDecimal, OpportunityRejectionReason> {
    left.checked_mul(right, DecimalRounding::HalfAwayFromZero)
        .map_err(|_| OpportunityRejectionReason::ArithmeticOverflow)
}

fn exact_integer(value: i128) -> Result<ExactDecimal, OpportunityRejectionReason> {
    let scaled = value
        .checked_mul(ExactDecimal::SCALE)
        .ok_or(OpportunityRejectionReason::ArithmeticOverflow)?;
    ExactDecimal::from_scaled(scaled).map_err(|_| OpportunityRejectionReason::ArithmeticOverflow)
}

fn exact_zero() -> ExactDecimal {
    ExactDecimal::from_scaled(0).expect("zero is representable")
}

fn map_funding_error(reason: MetadataInvalidReason) -> OpportunityRejectionReason {
    match reason {
        MetadataInvalidReason::FutureTimestamp {
            source_ts_us,
            decision_ts_us,
        } => OpportunityRejectionReason::FutureEvidence {
            source_ts_us,
            decision_ts_us,
        },
        MetadataInvalidReason::Stale { age_us, limit_us } => {
            OpportunityRejectionReason::StaleEvidence { age_us, limit_us }
        }
        MetadataInvalidReason::ArithmeticOverflow => OpportunityRejectionReason::ArithmeticOverflow,
        _ => OpportunityRejectionReason::FundingEvidenceMismatch,
    }
}

const fn venue_rank(venue: AdapterId) -> u8 {
    match venue {
        AdapterId::BinanceUsdm => 0,
        AdapterId::BybitLinear => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::UpbitSpot => 3,
        AdapterId::BithumbSpot => 4,
    }
}
