use serde::{Deserialize, Serialize};

use crate::{instrument::InstrumentSpec, meta::DerivativeMeta};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingRateKind {
    IndicativeNext,
    SettledActual,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingBasis {
    MarkNotional,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingIntervalProvenance {
    VenuePayload,
    InstrumentRule,
    AssumedVenueDefault,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenInterestUnit {
    Contracts,
    BaseAsset,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraderMetricKind {
    BinanceTopAccountRatio,
    BinanceTopPositionRatio,
    BybitLongShortRatio,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MarkIndexSnapshot {
    pub meta: DerivativeMeta,
    pub mark_price: i128,
    pub index_price: i128,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FundingEstimate {
    pub meta: DerivativeMeta,
    pub rate: i128,
    pub rate_kind: FundingRateKind,
    pub basis: FundingBasis,
    pub interval_secs: u32,
    pub interval_provenance: FundingIntervalProvenance,
    pub next_funding_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FundingSettlement {
    pub meta: DerivativeMeta,
    pub rate: i128,
    pub rate_kind: FundingRateKind,
    pub basis: FundingBasis,
    pub interval_secs: u32,
    pub interval_provenance: FundingIntervalProvenance,
    pub settlement_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OpenInterestSnapshot {
    pub meta: DerivativeMeta,
    pub open_interest: i128,
    pub unit: OpenInterestUnit,
    pub quote_notional: Option<i128>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TraderRatioSnapshot {
    pub meta: DerivativeMeta,
    pub metric_kind: TraderMetricKind,
    pub long_ratio: i128,
    pub short_ratio: i128,
    pub long_short_ratio: i128,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct QuoteConversionSnapshot {
    pub meta: DerivativeMeta,
    pub side: QuoteSide,
    pub price: i128,
    pub executable_quantity: i128,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DerivativeEvent {
    Instrument(Box<InstrumentSpec>),
    MarkIndex(MarkIndexSnapshot),
    FundingEstimate(FundingEstimate),
    FundingSettlement(FundingSettlement),
    OpenInterest(OpenInterestSnapshot),
    TraderRatio(TraderRatioSnapshot),
    QuoteConversion(QuoteConversionSnapshot),
}

impl DerivativeEvent {
    pub fn meta(&self) -> &DerivativeMeta {
        match self {
            Self::Instrument(value) => &value.meta,
            Self::MarkIndex(value) => &value.meta,
            Self::FundingEstimate(value) => &value.meta,
            Self::FundingSettlement(value) => &value.meta,
            Self::OpenInterest(value) => &value.meta,
            Self::TraderRatio(value) => &value.meta,
            Self::QuoteConversion(value) => &value.meta,
        }
    }

    pub fn partition_ts_us(&self) -> i64 {
        let meta = self.meta();
        meta.source_ts_us
            .filter(|timestamp| *timestamp > 0)
            .unwrap_or(meta.local_recv_ts_us)
    }
}
