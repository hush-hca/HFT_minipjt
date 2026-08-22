use std::collections::HashSet;

use md_core::model::{AdapterId, CanonicalSymbol};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::config::ExactDecimal;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    AuthenticatedCommission,
    ExplicitConfig,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeeAssumption {
    pub rate: ExactDecimal,
    pub source: FeeSource,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum FeeAssumptionError {
    #[error("fee rate must be positive")]
    NonPositiveRate,
    #[error("fee rate must not exceed one")]
    RateAboveOne,
}

impl FeeAssumption {
    pub fn new(rate: ExactDecimal, source: FeeSource) -> Result<Self, FeeAssumptionError> {
        if rate.scaled() <= 0 {
            return Err(FeeAssumptionError::NonPositiveRate);
        }
        if rate.scaled() > ExactDecimal::SCALE {
            return Err(FeeAssumptionError::RateAboveOne);
        }
        Ok(Self { rate, source })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacitySource {
    #[default]
    ConfiguredResearchLimit,
    InstrumentRule,
    BookDepth,
    RiskLimit,
    AuthenticatedMargin,
}

impl CapacitySource {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ConfiguredResearchLimit => "configured_research_limit",
            Self::InstrumentRule => "instrument_rule",
            Self::BookDepth => "book_depth",
            Self::RiskLimit => "risk_limit",
            Self::AuthenticatedMargin => "authenticated_margin",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityEvidenceValidity {
    Available,
    Unavailable { reason: String },
    Stale { age_us: i64, limit_us: i64 },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapacityEvidence {
    pub source: CapacitySource,
    pub capacity_base: Option<ExactDecimal>,
    pub capacity_quote: Option<ExactDecimal>,
    pub source_event_id: Option<Uuid>,
    pub source_ts_us: Option<i64>,
    pub validity: CapacityEvidenceValidity,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapacityAssessment {
    pub capacity_base: ExactDecimal,
    pub capacity_quote: ExactDecimal,
    pub binding_sources: Vec<CapacitySource>,
    pub evidence: Vec<CapacityEvidence>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum CapacityAssessmentError {
    #[error("capacity must be positive")]
    NonPositiveCapacity,
    #[error("capacity evidence must not be empty")]
    MissingEvidence,
    #[error("capacity evidence contains a duplicate source")]
    DuplicateEvidence,
    #[error("binding capacity source is missing available evidence")]
    MissingBindingEvidence,
}

impl CapacityAssessment {
    pub fn new(
        capacity_base: ExactDecimal,
        capacity_quote: ExactDecimal,
        binding_sources: Vec<CapacitySource>,
        evidence: Vec<CapacityEvidence>,
    ) -> Result<Self, CapacityAssessmentError> {
        if capacity_base.scaled() <= 0 || capacity_quote.scaled() <= 0 {
            return Err(CapacityAssessmentError::NonPositiveCapacity);
        }
        if evidence.is_empty() {
            return Err(CapacityAssessmentError::MissingEvidence);
        }
        let sources = evidence
            .iter()
            .map(|item| item.source)
            .collect::<HashSet<_>>();
        if sources.len() != evidence.len() {
            return Err(CapacityAssessmentError::DuplicateEvidence);
        }
        if binding_sources.is_empty()
            || binding_sources.iter().any(|source| {
                !evidence.iter().any(|item| {
                    item.source == *source
                        && matches!(item.validity, CapacityEvidenceValidity::Available)
                })
            })
        {
            return Err(CapacityAssessmentError::MissingBindingEvidence);
        }
        Ok(Self {
            capacity_base,
            capacity_quote,
            binding_sources,
            evidence,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VenueCostModel {
    pub entry_fee: FeeAssumption,
    pub exit_fee: FeeAssumption,
    pub entry_slippage_bps: ExactDecimal,
    pub exit_slippage_bps: ExactDecimal,
    pub entry_book_impact_bps: ExactDecimal,
    pub exit_book_impact_bps: ExactDecimal,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CostModel {
    pub binance: VenueCostModel,
    pub bybit: VenueCostModel,
    pub basis_risk_buffer_bps: ExactDecimal,
    pub funding_error_buffer_bps: ExactDecimal,
    pub leg_risk_buffer_bps: ExactDecimal,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PnlBreakdown {
    pub funding_income: i128,
    pub execution_pnl: i128,
    pub basis_pnl: i128,
    pub entry_fees: i128,
    pub exit_fees: i128,
    pub entry_slippage: i128,
    pub exit_slippage: i128,
    pub entry_book_impact: i128,
    pub exit_book_impact: i128,
    pub basis_risk_reserve: i128,
    pub funding_error_reserve: i128,
    pub leg_risk_reserve: i128,
    pub residual_mark_to_market: i128,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum PnlError {
    #[error("PnL component sum overflowed Decimal128 storage")]
    Overflow,
}

impl PnlBreakdown {
    pub const fn zero() -> Self {
        Self {
            funding_income: 0,
            execution_pnl: 0,
            basis_pnl: 0,
            entry_fees: 0,
            exit_fees: 0,
            entry_slippage: 0,
            exit_slippage: 0,
            entry_book_impact: 0,
            exit_book_impact: 0,
            basis_risk_reserve: 0,
            funding_error_reserve: 0,
            leg_risk_reserve: 0,
            residual_mark_to_market: 0,
        }
    }

    pub fn total(&self) -> Result<i128, PnlError> {
        let income = [
            self.funding_income,
            self.execution_pnl,
            self.basis_pnl,
            self.residual_mark_to_market,
        ]
        .into_iter()
        .try_fold(0_i128, i128::checked_add)
        .ok_or(PnlError::Overflow)?;
        [
            self.entry_fees,
            self.exit_fees,
            self.entry_slippage,
            self.exit_slippage,
            self.entry_book_impact,
            self.exit_book_impact,
            self.basis_risk_reserve,
            self.funding_error_reserve,
            self.leg_risk_reserve,
        ]
        .into_iter()
        .try_fold(income, i128::checked_sub)
        .ok_or(PnlError::Overflow)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Opportunity {
    pub symbol: CanonicalSymbol,
    pub short_venue: AdapterId,
    pub long_venue: AdapterId,
    pub capacity: CapacityAssessment,
    pub raw_gap: ExactDecimal,
    pub hourly_gap: ExactDecimal,
    pub indicative_apr: ExactDecimal,
    pub conservative_funding_cashflows: i128,
    pub expected_pnl: PnlBreakdown,
    pub expected_net_pnl: i128,
    pub expected_net_bps: ExactDecimal,
    pub decision_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityExclusion {
    MissingBook,
    StaleBook,
    MissingInstrumentRule,
    MissingFundingCalendar,
    MissingFeeSource,
    InsufficientDepth,
    NetPnlBelowMinimum,
}

impl OpportunityExclusion {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingBook => "MISSING_BOOK",
            Self::StaleBook => "STALE_BOOK",
            Self::MissingInstrumentRule => "MISSING_INSTRUMENT_RULE",
            Self::MissingFundingCalendar => "MISSING_FUNDING_CALENDAR",
            Self::MissingFeeSource => "MISSING_FEE_SOURCE",
            Self::InsufficientDepth => "INSUFFICIENT_DEPTH",
            Self::NetPnlBelowMinimum => "NET_PNL_BELOW_MINIMUM",
        }
    }
}
