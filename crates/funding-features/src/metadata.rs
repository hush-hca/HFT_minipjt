use std::collections::{HashMap, VecDeque};

use funding_core::{
    calendar::FundingTimestampSource,
    config::ExactDecimal,
    feature::{EffectiveTimestampSource, FeatureSource},
    meta::DerivativeMeta,
    metadata::{
        FundingGapConvention, FundingGapFeature, FundingMetadataFeature, FundingRateSignConvention,
        MetadataInvalidReason, ObservationOutcome, OpenInterestFeature, OpenInterestNormalization,
        QuoteNotionalProvenance, TraderRatioFeature,
    },
    public::{FundingEstimate, OpenInterestSnapshot, TraderMetricKind, TraderRatioSnapshot},
};
use md_core::model::{AdapterId, CanonicalSymbol};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use uuid::Uuid;

type MarketKey = (AdapterId, CanonicalSymbol);
type RatioKey = (AdapterId, CanonicalSymbol, TraderMetricKind);

#[derive(Debug, Clone, Eq, PartialEq)]
enum SeenObservation {
    Funding(FundingEstimate),
    OpenInterest(OpenInterestSnapshot),
    TraderRatio(TraderRatioSnapshot),
}

const DEFAULT_DEDUPE_CAPACITY: usize = 4_096;

#[derive(Debug)]
pub struct MetadataAligner {
    funding: HashMap<MarketKey, FundingEstimate>,
    open_interest: HashMap<MarketKey, OpenInterestSnapshot>,
    trader_ratio: HashMap<RatioKey, TraderRatioSnapshot>,
    seen_events: HashMap<Uuid, SeenObservation>,
    seen_order: VecDeque<Uuid>,
    dedupe_capacity: usize,
}

impl Default for MetadataAligner {
    fn default() -> Self {
        Self {
            funding: HashMap::new(),
            open_interest: HashMap::new(),
            trader_ratio: HashMap::new(),
            seen_events: HashMap::new(),
            seen_order: VecDeque::new(),
            dedupe_capacity: DEFAULT_DEDUPE_CAPACITY,
        }
    }
}

impl MetadataAligner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_dedupe_capacity(capacity: usize) -> Result<Self, MetadataInvalidReason> {
        if capacity == 0 {
            return Err(MetadataInvalidReason::InvalidDedupeCapacity);
        }
        Ok(Self {
            dedupe_capacity: capacity,
            ..Self::default()
        })
    }

    pub fn dedupe_len(&self) -> usize {
        self.seen_events.len()
    }

    pub fn observe_funding(
        &mut self,
        estimate: FundingEstimate,
    ) -> Result<ObservationOutcome, MetadataInvalidReason> {
        validate_funding(&estimate)?;
        if let Some(outcome) = duplicate_outcome(
            self.seen_events.get(&estimate.meta.event_id),
            &SeenObservation::Funding(estimate.clone()),
            estimate.meta.event_id,
        )? {
            return Ok(outcome);
        }
        let key = market_key(&estimate.meta);
        validate_update(
            self.funding.get(&key).map(|value| &value.meta),
            &estimate.meta,
        )?;
        self.remember(
            estimate.meta.event_id,
            SeenObservation::Funding(estimate.clone()),
        );
        self.funding.insert(key, estimate);
        Ok(ObservationOutcome::Accepted)
    }

    pub fn observe_funding_for(
        &mut self,
        expected_venue: AdapterId,
        expected_symbol: &CanonicalSymbol,
        estimate: FundingEstimate,
    ) -> Result<ObservationOutcome, MetadataInvalidReason> {
        validate_identity(expected_venue, expected_symbol, &estimate.meta)?;
        self.observe_funding(estimate)
    }

    pub fn observe_open_interest(
        &mut self,
        snapshot: OpenInterestSnapshot,
    ) -> Result<ObservationOutcome, MetadataInvalidReason> {
        validate_open_interest(&snapshot)?;
        if let Some(outcome) = duplicate_outcome(
            self.seen_events.get(&snapshot.meta.event_id),
            &SeenObservation::OpenInterest(snapshot.clone()),
            snapshot.meta.event_id,
        )? {
            return Ok(outcome);
        }
        let key = market_key(&snapshot.meta);
        validate_update(
            self.open_interest.get(&key).map(|value| &value.meta),
            &snapshot.meta,
        )?;
        self.remember(
            snapshot.meta.event_id,
            SeenObservation::OpenInterest(snapshot.clone()),
        );
        self.open_interest.insert(key, snapshot);
        Ok(ObservationOutcome::Accepted)
    }

    pub fn observe_trader_ratio(
        &mut self,
        snapshot: TraderRatioSnapshot,
    ) -> Result<ObservationOutcome, MetadataInvalidReason> {
        validate_trader_ratio(&snapshot)?;
        if let Some(outcome) = duplicate_outcome(
            self.seen_events.get(&snapshot.meta.event_id),
            &SeenObservation::TraderRatio(snapshot.clone()),
            snapshot.meta.event_id,
        )? {
            return Ok(outcome);
        }
        let key = (
            snapshot.meta.venue,
            snapshot.meta.symbol.clone(),
            snapshot.metric_kind,
        );
        validate_update(
            self.trader_ratio.get(&key).map(|value| &value.meta),
            &snapshot.meta,
        )?;
        self.remember(
            snapshot.meta.event_id,
            SeenObservation::TraderRatio(snapshot.clone()),
        );
        self.trader_ratio.insert(key, snapshot);
        Ok(ObservationOutcome::Accepted)
    }

    pub fn funding_feature(
        &self,
        venue: AdapterId,
        symbol: &CanonicalSymbol,
        decision_ts_us: i64,
        freshness_limit_us: i64,
    ) -> Result<FundingMetadataFeature, MetadataInvalidReason> {
        let estimate = self.funding.get(&(venue, symbol.clone())).ok_or_else(|| {
            MetadataInvalidReason::MissingFunding {
                venue,
                symbol: symbol.clone(),
            }
        })?;
        validate_identity(venue, symbol, &estimate.meta)?;
        let (source, age_us) = aligned_source(&estimate.meta, decision_ts_us, freshness_limit_us)?;
        if estimate.next_funding_ts_us <= decision_ts_us {
            return Err(MetadataInvalidReason::NextSettlementNotFuture {
                next_settlement_ts_us: estimate.next_funding_ts_us,
                decision_ts_us,
            });
        }
        let raw_rate = exact(estimate.rate)?;
        let hourly_linear_rate = hourly_linear_rate(raw_rate, estimate.interval_secs)?;
        Ok(FundingMetadataFeature {
            source,
            raw_rate,
            sign_convention: FundingRateSignConvention::PositiveLongsPayShorts,
            hourly_linear_rate,
            rate_kind: estimate.rate_kind,
            basis: estimate.basis,
            interval_secs: estimate.interval_secs,
            interval_provenance: estimate.interval_provenance,
            next_settlement_ts_us: estimate.next_funding_ts_us,
            settlement_timestamp_source: FundingTimestampSource::VenueAnnounced,
            initial: true,
            decision_ts_us,
            freshness_limit_us,
            age_us,
        })
    }

    pub fn open_interest_feature(
        &self,
        venue: AdapterId,
        symbol: &CanonicalSymbol,
        decision_ts_us: i64,
        freshness_limit_us: i64,
    ) -> Result<OpenInterestFeature, MetadataInvalidReason> {
        let snapshot = self
            .open_interest
            .get(&(venue, symbol.clone()))
            .ok_or_else(|| MetadataInvalidReason::MissingOpenInterest {
                venue,
                symbol: symbol.clone(),
            })?;
        validate_identity(venue, symbol, &snapshot.meta)?;
        let (source, age_us) = aligned_source(&snapshot.meta, decision_ts_us, freshness_limit_us)?;
        Ok(OpenInterestFeature {
            source,
            open_interest: exact(snapshot.open_interest)?,
            unit: snapshot.unit,
            quote_notional: snapshot.quote_notional.map(exact).transpose()?,
            quote_notional_provenance: snapshot
                .quote_notional
                .map(|_| QuoteNotionalProvenance::VenueReported),
            normalization: OpenInterestNormalization::RawVenueUnitNonComparable,
            decision_ts_us,
            freshness_limit_us,
            age_us,
        })
    }

    pub fn trader_ratio_feature(
        &self,
        venue: AdapterId,
        symbol: &CanonicalSymbol,
        metric_kind: TraderMetricKind,
        decision_ts_us: i64,
        freshness_limit_us: i64,
    ) -> Result<TraderRatioFeature, MetadataInvalidReason> {
        let snapshot = self
            .trader_ratio
            .get(&(venue, symbol.clone(), metric_kind))
            .ok_or_else(|| MetadataInvalidReason::MissingTraderRatio {
                venue,
                symbol: symbol.clone(),
                metric_kind,
            })?;
        validate_identity(venue, symbol, &snapshot.meta)?;
        let (source, age_us) = aligned_source(&snapshot.meta, decision_ts_us, freshness_limit_us)?;
        Ok(TraderRatioFeature {
            source,
            metric_kind: snapshot.metric_kind,
            long_ratio: exact(snapshot.long_ratio)?,
            short_ratio: exact(snapshot.short_ratio)?,
            long_short_ratio: exact(snapshot.long_short_ratio)?,
            decision_ts_us,
            freshness_limit_us,
            age_us,
        })
    }

    fn remember(&mut self, event_id: Uuid, observation: SeenObservation) {
        self.seen_events.insert(event_id, observation);
        self.seen_order.push_back(event_id);
        while self.seen_order.len() > self.dedupe_capacity {
            if let Some(expired) = self.seen_order.pop_front() {
                self.seen_events.remove(&expired);
            }
        }
    }
}

pub fn funding_gap(
    short: FundingMetadataFeature,
    long: FundingMetadataFeature,
    decision_ts_us: i64,
) -> Result<FundingGapFeature, MetadataInvalidReason> {
    validate_gap_leg(&short, decision_ts_us)?;
    validate_gap_leg(&long, decision_ts_us)?;
    if short.source.symbol != long.source.symbol {
        return Err(MetadataInvalidReason::FundingSymbolMismatch {
            short_symbol: short.source.symbol.clone(),
            long_symbol: long.source.symbol.clone(),
        });
    }
    if short.source.adapter == long.source.adapter {
        return Err(MetadataInvalidReason::FundingVenueCollision {
            venue: short.source.adapter,
        });
    }
    if short.basis != funding_core::public::FundingBasis::MarkNotional
        || long.basis != funding_core::public::FundingBasis::MarkNotional
    {
        return Err(MetadataInvalidReason::FundingBasisMismatch);
    }
    if short.rate_kind != funding_core::public::FundingRateKind::IndicativeNext
        || long.rate_kind != funding_core::public::FundingRateKind::IndicativeNext
    {
        return Err(MetadataInvalidReason::FundingRateKindMismatch);
    }
    if short.decision_ts_us != decision_ts_us || long.decision_ts_us != decision_ts_us {
        return Err(MetadataInvalidReason::DecisionTimestampMismatch);
    }
    if short.next_settlement_ts_us <= decision_ts_us {
        return Err(MetadataInvalidReason::NextSettlementNotFuture {
            next_settlement_ts_us: short.next_settlement_ts_us,
            decision_ts_us,
        });
    }
    if long.next_settlement_ts_us <= decision_ts_us {
        return Err(MetadataInvalidReason::NextSettlementNotFuture {
            next_settlement_ts_us: long.next_settlement_ts_us,
            decision_ts_us,
        });
    }
    let signed_hourly_gap = short
        .hourly_linear_rate
        .checked_sub(long.hourly_linear_rate)
        .map_err(|_| MetadataInvalidReason::ArithmeticOverflow)?;
    Ok(FundingGapFeature {
        symbol: short.source.symbol.clone(),
        short,
        long,
        signed_hourly_gap,
        convention: FundingGapConvention::ShortHourlyRateMinusLongHourlyRate,
        decision_ts_us,
    })
}

fn validate_gap_leg(
    leg: &FundingMetadataFeature,
    decision_ts_us: i64,
) -> Result<(), MetadataInvalidReason> {
    if !matches!(
        leg.source.adapter,
        AdapterId::BinanceUsdm | AdapterId::BybitLinear
    ) {
        return Err(MetadataInvalidReason::UnsupportedFundingVenue {
            venue: leg.source.adapter,
        });
    }
    if leg.decision_ts_us != decision_ts_us {
        return Err(MetadataInvalidReason::DecisionTimestampMismatch);
    }
    if leg.freshness_limit_us < 0 {
        return Err(MetadataInvalidReason::InvalidFreshnessLimit {
            limit_us: leg.freshness_limit_us,
        });
    }
    if leg.interval_secs == 0 {
        return Err(MetadataInvalidReason::InvalidFundingInterval);
    }
    if !leg.initial
        || leg.settlement_timestamp_source != FundingTimestampSource::VenueAnnounced
        || leg.hourly_linear_rate != hourly_linear_rate(leg.raw_rate, leg.interval_secs)?
    {
        return Err(MetadataInvalidReason::FundingEvidenceMismatch);
    }
    if leg.source.local_recv_ts_us > decision_ts_us || leg.source.effective_ts_us > decision_ts_us {
        return Err(MetadataInvalidReason::FutureTimestamp {
            source_ts_us: leg.source.local_recv_ts_us.max(leg.source.effective_ts_us),
            decision_ts_us,
        });
    }
    let age_us = decision_ts_us
        .checked_sub(leg.source.local_recv_ts_us)
        .ok_or(MetadataInvalidReason::ArithmeticOverflow)?;
    if leg.age_us != age_us {
        return Err(MetadataInvalidReason::FundingEvidenceMismatch);
    }
    if age_us > leg.freshness_limit_us {
        return Err(MetadataInvalidReason::Stale {
            age_us,
            limit_us: leg.freshness_limit_us,
        });
    }
    Ok(())
}

fn market_key(meta: &DerivativeMeta) -> MarketKey {
    (meta.venue, meta.symbol.clone())
}

fn duplicate_outcome(
    previous: Option<&SeenObservation>,
    current: &SeenObservation,
    event_id: Uuid,
) -> Result<Option<ObservationOutcome>, MetadataInvalidReason> {
    match previous {
        None => Ok(None),
        Some(previous) if previous == current => Ok(Some(ObservationOutcome::IgnoredDuplicate)),
        Some(_) => Err(MetadataInvalidReason::EventIdConflict { event_id }),
    }
}

fn validate_identity(
    expected_venue: AdapterId,
    expected_symbol: &CanonicalSymbol,
    meta: &DerivativeMeta,
) -> Result<(), MetadataInvalidReason> {
    if meta.venue != expected_venue || &meta.symbol != expected_symbol {
        return Err(MetadataInvalidReason::IdentityMismatch {
            expected_venue,
            expected_symbol: expected_symbol.clone(),
            actual_venue: meta.venue,
            actual_symbol: meta.symbol.clone(),
        });
    }
    Ok(())
}

fn validate_update(
    previous: Option<&DerivativeMeta>,
    current: &DerivativeMeta,
) -> Result<(), MetadataInvalidReason> {
    let Some(previous) = previous else {
        return Ok(());
    };
    let previous_ts_us = effective_ts_us(previous);
    let current_ts_us = effective_ts_us(current);
    if current_ts_us < previous_ts_us {
        return Err(MetadataInvalidReason::RegressingUpdate {
            previous_ts_us,
            current_ts_us,
        });
    }
    if current_ts_us == previous_ts_us {
        return Err(MetadataInvalidReason::TimestampConflict {
            timestamp_us: current_ts_us,
        });
    }
    Ok(())
}

fn validate_funding(estimate: &FundingEstimate) -> Result<(), MetadataInvalidReason> {
    validate_meta_clock(&estimate.meta)?;
    if !matches!(
        estimate.meta.venue,
        AdapterId::BinanceUsdm | AdapterId::BybitLinear
    ) {
        return Err(MetadataInvalidReason::UnsupportedFundingVenue {
            venue: estimate.meta.venue,
        });
    }
    if estimate.rate_kind != funding_core::public::FundingRateKind::IndicativeNext {
        return Err(MetadataInvalidReason::FundingRateKindMismatch);
    }
    exact(estimate.rate)?;
    if estimate.interval_secs == 0 {
        return Err(MetadataInvalidReason::InvalidFundingInterval);
    }
    if estimate.next_funding_ts_us <= 0 {
        return Err(MetadataInvalidReason::MissingNextSettlement);
    }
    if estimate.next_funding_ts_us <= effective_ts_us(&estimate.meta) {
        return Err(MetadataInvalidReason::MissingNextSettlement);
    }
    Ok(())
}

fn validate_open_interest(snapshot: &OpenInterestSnapshot) -> Result<(), MetadataInvalidReason> {
    validate_meta_clock(&snapshot.meta)?;
    let unit_matches = matches!(
        (snapshot.meta.venue, snapshot.unit),
        (
            AdapterId::BinanceUsdm,
            funding_core::public::OpenInterestUnit::Contracts
        ) | (
            AdapterId::BybitLinear,
            funding_core::public::OpenInterestUnit::BaseAsset
        )
    );
    if !unit_matches {
        return Err(MetadataInvalidReason::OpenInterestVenueUnitMismatch {
            venue: snapshot.meta.venue,
            unit: snapshot.unit,
        });
    }
    if snapshot.open_interest < 0 || snapshot.quote_notional.is_some_and(|value| value < 0) {
        return Err(MetadataInvalidReason::NegativeOpenInterest);
    }
    exact(snapshot.open_interest)?;
    snapshot.quote_notional.map(exact).transpose()?;
    Ok(())
}

fn validate_trader_ratio(snapshot: &TraderRatioSnapshot) -> Result<(), MetadataInvalidReason> {
    validate_meta_clock(&snapshot.meta)?;
    let venue_matches = matches!(
        (snapshot.meta.venue, snapshot.metric_kind),
        (
            AdapterId::BinanceUsdm,
            TraderMetricKind::BinanceTopAccountRatio | TraderMetricKind::BinanceTopPositionRatio
        ) | (
            AdapterId::BybitLinear,
            TraderMetricKind::BybitLongShortRatio
        )
    );
    if !venue_matches {
        return Err(MetadataInvalidReason::TraderMetricVenueMismatch {
            venue: snapshot.meta.venue,
            metric_kind: snapshot.metric_kind,
        });
    }
    if snapshot.long_ratio < 0 || snapshot.short_ratio < 0 || snapshot.long_short_ratio < 0 {
        return Err(MetadataInvalidReason::InvalidTraderRatio);
    }
    let long = exact(snapshot.long_ratio)?;
    let short = exact(snapshot.short_ratio)?;
    let reported_ratio = exact(snapshot.long_short_ratio)?;
    if short.scaled() == 0 {
        return Err(MetadataInvalidReason::InvalidTraderRatio);
    }
    // Venue ratios are decimal strings with finite precision. Require share
    // normalization and reported L/S consistency within one part per million.
    const TOLERANCE: i128 = ExactDecimal::SCALE / 1_000_000;
    let one = exact(ExactDecimal::SCALE)?;
    let share_error = long
        .checked_add(short)
        .and_then(|sum| sum.checked_sub(one))
        .map_err(|_| MetadataInvalidReason::ArithmeticOverflow)?
        .scaled()
        .unsigned_abs();
    let computed_ratio = exact_ratio_once(
        BigInt::from(long.scaled()) * BigInt::from(ExactDecimal::SCALE),
        BigInt::from(short.scaled()),
    )?;
    let ratio_error = computed_ratio
        .checked_sub(reported_ratio)
        .map_err(|_| MetadataInvalidReason::ArithmeticOverflow)?
        .scaled()
        .unsigned_abs();
    if share_error > TOLERANCE as u128 || ratio_error > TOLERANCE as u128 {
        return Err(MetadataInvalidReason::InvalidTraderRatio);
    }
    Ok(())
}

fn validate_meta_clock(meta: &DerivativeMeta) -> Result<(), MetadataInvalidReason> {
    if meta.local_recv_ts_us <= 0 {
        return Err(MetadataInvalidReason::InvalidLocalReceiveTimestamp);
    }
    if let Some(source_ts_us) = meta.source_ts_us {
        if source_ts_us <= 0 {
            return Err(MetadataInvalidReason::InvalidSourceTimestamp);
        }
        if source_ts_us > meta.local_recv_ts_us {
            return Err(MetadataInvalidReason::SourceAfterLocalReceive {
                source_ts_us,
                local_recv_ts_us: meta.local_recv_ts_us,
            });
        }
    }
    Ok(())
}

fn aligned_source(
    meta: &DerivativeMeta,
    decision_ts_us: i64,
    freshness_limit_us: i64,
) -> Result<(FeatureSource, i64), MetadataInvalidReason> {
    if freshness_limit_us < 0 {
        return Err(MetadataInvalidReason::InvalidFreshnessLimit {
            limit_us: freshness_limit_us,
        });
    }
    let effective_ts_us = effective_ts_us(meta);
    if meta.local_recv_ts_us > decision_ts_us {
        return Err(MetadataInvalidReason::FutureTimestamp {
            source_ts_us: meta.local_recv_ts_us,
            decision_ts_us,
        });
    }
    if effective_ts_us > decision_ts_us {
        return Err(MetadataInvalidReason::FutureTimestamp {
            source_ts_us: effective_ts_us,
            decision_ts_us,
        });
    }
    // Availability and freshness are causal at local receipt; the exchange
    // timestamp remains separately preserved as effective-time provenance.
    let age_us = decision_ts_us - meta.local_recv_ts_us;
    if age_us > freshness_limit_us {
        return Err(MetadataInvalidReason::Stale {
            age_us,
            limit_us: freshness_limit_us,
        });
    }
    let effective_ts_source = if meta.source_ts_us.is_some() {
        EffectiveTimestampSource::ExchangeEvent
    } else {
        EffectiveTimestampSource::LocalReceive
    };
    Ok((
        FeatureSource {
            event_id: meta.event_id,
            adapter: meta.venue,
            symbol: meta.symbol.clone(),
            source_sequence: None,
            exchange_event_ts_us: meta.source_ts_us,
            exchange_trade_ts_us: None,
            local_recv_ts_us: meta.local_recv_ts_us,
            effective_ts_us,
            effective_ts_source,
        },
        age_us,
    ))
}

fn effective_ts_us(meta: &DerivativeMeta) -> i64 {
    meta.source_ts_us.unwrap_or(meta.local_recv_ts_us)
}

fn exact(value: i128) -> Result<ExactDecimal, MetadataInvalidReason> {
    ExactDecimal::from_scaled(value).map_err(|_| MetadataInvalidReason::ArithmeticOverflow)
}

fn hourly_linear_rate(
    raw_rate: ExactDecimal,
    interval_secs: u32,
) -> Result<ExactDecimal, MetadataInvalidReason> {
    if interval_secs == 0 {
        return Err(MetadataInvalidReason::InvalidFundingInterval);
    }
    exact_ratio_once(
        BigInt::from(raw_rate.scaled()) * BigInt::from(3_600_u32),
        BigInt::from(interval_secs),
    )
}

fn exact_ratio_once(
    numerator: BigInt,
    denominator: BigInt,
) -> Result<ExactDecimal, MetadataInvalidReason> {
    if denominator.is_zero() {
        return Err(MetadataInvalidReason::InvalidFundingInterval);
    }
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    let rounded = if remainder.is_zero() {
        quotient
    } else if remainder.abs() * BigInt::from(2) >= denominator.abs() {
        quotient + if numerator.is_negative() { -1 } else { 1 }
    } else {
        quotient
    };
    rounded
        .to_i128()
        .ok_or(MetadataInvalidReason::ArithmeticOverflow)
        .and_then(exact)
}
