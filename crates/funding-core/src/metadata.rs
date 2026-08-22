use md_core::model::{AdapterId, CanonicalSymbol};
use serde::{Deserialize, Serialize};

use crate::{
    calendar::FundingTimestampSource,
    config::ExactDecimal,
    feature::FeatureSource,
    public::{
        FundingBasis, FundingIntervalProvenance, FundingRateKind, OpenInterestUnit,
        TraderMetricKind,
    },
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationOutcome {
    Accepted,
    IgnoredDuplicate,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenInterestNormalization {
    /// Raw venue-unit observation. It is not cross-venue comparable without
    /// an as-of contract multiplier supplied by a later, explicit stage.
    RawVenueUnitNonComparable,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteNotionalProvenance {
    VenueReported,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingGapConvention {
    /// Linear time normalization only: `short_rate * 3600 / short_interval`
    /// minus `long_rate * 3600 / long_interval`. This is not APR, APY, or
    /// compounded return.
    ShortHourlyRateMinusLongHourlyRate,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingRateSignConvention {
    PositiveLongsPayShorts,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataInvalidReason {
    InvalidFreshnessLimit {
        limit_us: i64,
    },
    InvalidDedupeCapacity,
    MissingFunding {
        venue: AdapterId,
        symbol: CanonicalSymbol,
    },
    MissingOpenInterest {
        venue: AdapterId,
        symbol: CanonicalSymbol,
    },
    MissingTraderRatio {
        venue: AdapterId,
        symbol: CanonicalSymbol,
        metric_kind: TraderMetricKind,
    },
    IdentityMismatch {
        expected_venue: AdapterId,
        expected_symbol: CanonicalSymbol,
        actual_venue: AdapterId,
        actual_symbol: CanonicalSymbol,
    },
    FutureTimestamp {
        source_ts_us: i64,
        decision_ts_us: i64,
    },
    Stale {
        age_us: i64,
        limit_us: i64,
    },
    RegressingUpdate {
        previous_ts_us: i64,
        current_ts_us: i64,
    },
    TimestampConflict {
        timestamp_us: i64,
    },
    EventIdConflict {
        event_id: uuid::Uuid,
    },
    InvalidFundingInterval,
    MissingNextSettlement,
    NextSettlementNotFuture {
        next_settlement_ts_us: i64,
        decision_ts_us: i64,
    },
    NegativeOpenInterest,
    InvalidTraderRatio,
    TraderMetricVenueMismatch {
        venue: AdapterId,
        metric_kind: TraderMetricKind,
    },
    UnsupportedFundingVenue {
        venue: AdapterId,
    },
    OpenInterestVenueUnitMismatch {
        venue: AdapterId,
        unit: OpenInterestUnit,
    },
    InvalidLocalReceiveTimestamp,
    InvalidSourceTimestamp,
    SourceAfterLocalReceive {
        source_ts_us: i64,
        local_recv_ts_us: i64,
    },
    ArithmeticOverflow,
    FundingSymbolMismatch {
        short_symbol: CanonicalSymbol,
        long_symbol: CanonicalSymbol,
    },
    FundingVenueCollision {
        venue: AdapterId,
    },
    FundingBasisMismatch,
    FundingRateKindMismatch,
    FundingEvidenceMismatch,
    DecisionTimestampMismatch,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundingMetadataFeature {
    pub source: FeatureSource,
    pub raw_rate: ExactDecimal,
    pub sign_convention: FundingRateSignConvention,
    pub hourly_linear_rate: ExactDecimal,
    pub rate_kind: FundingRateKind,
    pub basis: FundingBasis,
    pub interval_secs: u32,
    pub interval_provenance: FundingIntervalProvenance,
    pub next_settlement_ts_us: i64,
    pub settlement_timestamp_source: FundingTimestampSource,
    pub initial: bool,
    pub decision_ts_us: i64,
    pub freshness_limit_us: i64,
    pub age_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenInterestFeature {
    pub source: FeatureSource,
    pub open_interest: ExactDecimal,
    pub unit: OpenInterestUnit,
    pub quote_notional: Option<ExactDecimal>,
    pub quote_notional_provenance: Option<QuoteNotionalProvenance>,
    pub normalization: OpenInterestNormalization,
    pub decision_ts_us: i64,
    pub freshness_limit_us: i64,
    pub age_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraderRatioFeature {
    pub source: FeatureSource,
    pub metric_kind: TraderMetricKind,
    pub long_ratio: ExactDecimal,
    pub short_ratio: ExactDecimal,
    pub long_short_ratio: ExactDecimal,
    pub decision_ts_us: i64,
    pub freshness_limit_us: i64,
    pub age_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundingGapFeature {
    pub symbol: CanonicalSymbol,
    pub short: FundingMetadataFeature,
    pub long: FundingMetadataFeature,
    pub signed_hourly_gap: ExactDecimal,
    pub convention: FundingGapConvention,
    pub decision_ts_us: i64,
}
