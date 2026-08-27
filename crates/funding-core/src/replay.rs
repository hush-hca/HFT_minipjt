use std::collections::BTreeMap;

use md_core::model::{AdapterId, CanonicalSymbol};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    config::ExactDecimal,
    feature::{BookFeatures, FlowFeatures},
    metadata::{MetadataInvalidReason, OpenInterestFeature, TraderRatioFeature},
    opportunity::{CandidateEvaluation, CapacityEvidence, CostModel},
    public::TraderMetricKind,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecisionEvent {
    pub event_id: Uuid,
    pub local_recv_ts_us: i64,
    pub symbol: CanonicalSymbol,
    pub long_venue: AdapterId,
    pub short_venue: AdapterId,
    pub requested_base: ExactDecimal,
    pub holding_end_ts_us: i64,
    pub cost_model: CostModel,
    pub minimum_net_bps: ExactDecimal,
    pub capacity_evidence: Vec<CapacityEvidence>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayConfig {
    pub book_freshness_us: i64,
    pub metadata_freshness_us: i64,
    pub mark_freshness_us: i64,
    pub flow_window_us: i64,
    pub dedupe_capacity: usize,
    /// An input label retained in reports. The evaluator has no randomness.
    pub seed: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayEventFamily {
    Instrument,
    Book,
    Trade,
    MarkIndex,
    FundingEstimate,
    FundingSettlement,
    OpenInterest,
    TraderRatio,
    QuoteConversion,
    Decision,
}

impl ReplayEventFamily {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Instrument => "instrument",
            Self::Book => "book",
            Self::Trade => "trade",
            Self::MarkIndex => "mark_index",
            Self::FundingEstimate => "funding_estimate",
            Self::FundingSettlement => "funding_settlement",
            Self::OpenInterest => "open_interest",
            Self::TraderRatio => "trader_ratio",
            Self::QuoteConversion => "quote_conversion",
            Self::Decision => "decision",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayRejectionReason {
    InvalidConfig {
        field: String,
    },
    InvalidAvailabilityTimestamp {
        timestamp_us: i64,
    },
    SourceAfterAvailability {
        source_ts_us: i64,
        local_recv_ts_us: i64,
    },
    DuplicateEventIdConflict {
        event_id: Uuid,
    },
    RegressingInput {
        previous_ts_us: i64,
        current_ts_us: i64,
    },
    TimestampConflict {
        timestamp_us: i64,
    },
    InvalidInput {
        detail: String,
    },
    InvalidDecision {
        field: String,
    },
    ReconciliationFailure,
    MissingBook {
        venue: AdapterId,
    },
    MissingMark {
        venue: AdapterId,
    },
    FeatureUnavailable {
        detail: String,
    },
    MetadataUnavailable {
        reason: MetadataInvalidReason,
    },
}

impl ReplayRejectionReason {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig { .. } => "INVALID_CONFIG",
            Self::InvalidAvailabilityTimestamp { .. } => "INVALID_AVAILABILITY_TIMESTAMP",
            Self::SourceAfterAvailability { .. } => "SOURCE_AFTER_AVAILABILITY",
            Self::DuplicateEventIdConflict { .. } => "DUPLICATE_EVENT_ID_CONFLICT",
            Self::RegressingInput { .. } => "REGRESSING_INPUT",
            Self::TimestampConflict { .. } => "TIMESTAMP_CONFLICT",
            Self::InvalidInput { .. } => "INVALID_INPUT",
            Self::InvalidDecision { .. } => "INVALID_DECISION",
            Self::ReconciliationFailure => "RECONCILIATION_FAILURE",
            Self::MissingBook { .. } => "MISSING_BOOK",
            Self::MissingMark { .. } => "MISSING_MARK",
            Self::FeatureUnavailable { .. } => "FEATURE_UNAVAILABLE",
            Self::MetadataUnavailable { .. } => "METADATA_UNAVAILABLE",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayRejection {
    pub event_id: Uuid,
    pub family: ReplayEventFamily,
    pub local_recv_ts_us: i64,
    pub reason: ReplayRejectionReason,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AlignedOpenInterest {
    pub venue: AdapterId,
    pub feature: Option<OpenInterestFeature>,
    pub rejection: Option<MetadataInvalidReason>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AlignedTraderRatio {
    pub venue: AdapterId,
    pub metric_kind: TraderMetricKind,
    pub feature: Option<TraderRatioFeature>,
    pub rejection: Option<MetadataInvalidReason>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDecisionOutcome {
    Evaluated(CandidateEvaluation),
    Unavailable(ReplayRejectionReason),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayDecisionRecord {
    pub decision: DecisionEvent,
    pub book_features: Vec<BookFeatures>,
    pub flow_features: Vec<FlowFeatures>,
    pub open_interest: Vec<AlignedOpenInterest>,
    pub trader_ratios: Vec<AlignedTraderRatio>,
    pub evidence_event_ids: Vec<Uuid>,
    pub outcome: ReplayDecisionOutcome,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayReconciliation {
    pub input_events: u64,
    pub applied_events: u64,
    pub duplicate_events: u64,
    pub rejected_events: u64,
    pub decisions_recorded: u64,
    pub candidate_evaluations: u64,
    pub eligible_candidates: u64,
    pub rejected_candidates: u64,
}

impl ReplayReconciliation {
    pub const fn input_identity_holds(&self) -> bool {
        self.input_events == self.applied_events + self.duplicate_events + self.rejected_events
    }

    pub const fn candidate_identity_holds(&self) -> bool {
        self.candidate_evaluations == self.eligible_candidates + self.rejected_candidates
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub canonical_encoding_version: u16,
    pub digest_algorithm: String,
    pub event_digest_hex: String,
    pub config: ReplayConfig,
    pub simulation_enabled: bool,
    pub paper_validation_only: bool,
    pub first_clock_us: Option<i64>,
    pub last_clock_us: Option<i64>,
    pub event_counts: BTreeMap<String, u64>,
    pub rejection_counts: BTreeMap<String, u64>,
    pub causality_violations: u64,
    pub decisions: Vec<ReplayDecisionRecord>,
    pub rejections: Vec<ReplayRejection>,
    pub reconciliation: ReplayReconciliation,
}
