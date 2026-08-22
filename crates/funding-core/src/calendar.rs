use std::collections::HashSet;

use md_core::model::{AdapterId, CanonicalSymbol};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::public::FundingIntervalProvenance;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingSlotKind {
    Estimated,
    Settled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingTimestampSource {
    VenueAnnounced,
    IntervalDerived,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundingSlotEvidence {
    pub interval_secs: u32,
    pub interval_provenance: FundingIntervalProvenance,
    pub initial: bool,
    pub timestamp_source: FundingTimestampSource,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundingSlot {
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub settlement_ts_us: i64,
    pub rate: i128,
    pub kind: FundingSlotKind,
    pub interval_secs: u32,
    pub interval_provenance: FundingIntervalProvenance,
    pub initial: bool,
    pub timestamp_source: FundingTimestampSource,
}

impl FundingSlot {
    pub fn estimated(
        venue: AdapterId,
        symbol: CanonicalSymbol,
        settlement_ts_us: i64,
        rate: i128,
        evidence: FundingSlotEvidence,
    ) -> Self {
        Self {
            venue,
            symbol,
            settlement_ts_us,
            rate,
            kind: FundingSlotKind::Estimated,
            interval_secs: evidence.interval_secs,
            interval_provenance: evidence.interval_provenance,
            initial: evidence.initial,
            timestamp_source: evidence.timestamp_source,
        }
    }

    pub fn settled(
        venue: AdapterId,
        symbol: CanonicalSymbol,
        settlement_ts_us: i64,
        rate: i128,
        evidence: FundingSlotEvidence,
    ) -> Self {
        Self {
            venue,
            symbol,
            settlement_ts_us,
            rate,
            kind: FundingSlotKind::Settled,
            interval_secs: evidence.interval_secs,
            interval_provenance: evidence.interval_provenance,
            initial: evidence.initial,
            timestamp_source: evidence.timestamp_source,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundingCalendar {
    slots: Vec<FundingSlot>,
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum FundingCalendarError {
    #[error("funding calendar must contain at least one slot")]
    Empty,
    #[error("funding settlement timestamp must be positive")]
    NonPositiveTimestamp,
    #[error("funding interval must be positive")]
    NonPositiveInterval,
    #[error("funding calendar contains a duplicate venue, symbol, and timestamp")]
    DuplicateSlot,
}

impl FundingCalendar {
    pub fn new(mut slots: Vec<FundingSlot>) -> Result<Self, FundingCalendarError> {
        if slots.is_empty() {
            return Err(FundingCalendarError::Empty);
        }
        if slots.iter().any(|slot| slot.settlement_ts_us <= 0) {
            return Err(FundingCalendarError::NonPositiveTimestamp);
        }
        if slots.iter().any(|slot| slot.interval_secs == 0) {
            return Err(FundingCalendarError::NonPositiveInterval);
        }

        let mut identities = HashSet::with_capacity(slots.len());
        for slot in &slots {
            if !identities.insert((slot.venue, slot.symbol.clone(), slot.settlement_ts_us)) {
                return Err(FundingCalendarError::DuplicateSlot);
            }
        }
        slots.sort_by(|left, right| {
            left.settlement_ts_us
                .cmp(&right.settlement_ts_us)
                .then_with(|| adapter_order(left.venue).cmp(&adapter_order(right.venue)))
                .then_with(|| left.symbol.base.cmp(&right.symbol.base))
                .then_with(|| left.symbol.quote.cmp(&right.symbol.quote))
        });
        Ok(Self { slots })
    }

    pub fn slots(&self) -> &[FundingSlot] {
        &self.slots
    }
}

fn adapter_order(adapter: AdapterId) -> u8 {
    match adapter {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}
