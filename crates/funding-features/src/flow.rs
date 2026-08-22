use std::collections::{HashSet, VecDeque};

use funding_core::{
    config::{DecimalRounding, ExactDecimal},
    feature::{
        EffectiveTimestampSource, FeatureInvalidReason, FeatureSource, FeatureValidity,
        FlowFeatures, FlowInputState, FlowPolicy, OutOfOrderPolicy, TradeDedupePolicy,
    },
};
use md_core::{
    model::{AdapterId, CanonicalSymbol, NormalizedEvent, TakerSide, TradeTick},
    validation::{TimestampField, ValidationError, validate_event},
};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use uuid::Uuid;

const BURST_WINDOW_US: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TradePushOutcome {
    Accepted,
    Duplicate,
    RejectedOutOfOrder {
        previous_ts_us: i64,
        current_ts_us: i64,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct VenueTradeKey {
    adapter: AdapterId,
    symbol: CanonicalSymbol,
    trade_id: String,
}

#[derive(Debug, Clone)]
struct SeenTrade {
    event_id: Uuid,
    venue_key: VenueTradeKey,
    ts_us: i64,
}

#[derive(Debug, Clone)]
struct CompactTrade {
    ts_us: i64,
    price: ExactDecimal,
    quantity: ExactDecimal,
    side: TakerSide,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RejectionKind {
    Duplicate,
    OutOfOrder,
}

#[derive(Debug, Clone, Copy)]
struct Rejection {
    ts_us: i64,
    kind: RejectionKind,
}

pub struct TradeWindow {
    horizon_us: i64,
    identity: Option<(AdapterId, CanonicalSymbol)>,
    trades: VecDeque<CompactTrade>,
    seen: VecDeque<SeenTrade>,
    seen_event_ids: HashSet<Uuid>,
    seen_venue_keys: HashSet<VenueTradeKey>,
    rejections: VecDeque<Rejection>,
    ordering_watermark_ts_us: Option<i64>,
    source_watermark: Option<FeatureSource>,
    last_snapshot_ts_us: Option<i64>,
}

impl TradeWindow {
    pub fn new(horizon_us: i64) -> Result<Self, FeatureInvalidReason> {
        if horizon_us <= 0 {
            return Err(FeatureInvalidReason::NonPositiveValue);
        }
        Ok(Self {
            horizon_us,
            identity: None,
            trades: VecDeque::new(),
            seen: VecDeque::new(),
            seen_event_ids: HashSet::new(),
            seen_venue_keys: HashSet::new(),
            rejections: VecDeque::new(),
            ordering_watermark_ts_us: None,
            source_watermark: None,
            last_snapshot_ts_us: None,
        })
    }

    pub fn observe_watermark(&mut self, source: FeatureSource) -> Result<(), FeatureInvalidReason> {
        self.ensure_identity(source.adapter, &source.symbol)?;
        self.update_source_watermark(source);
        Ok(())
    }

    pub fn push(&mut self, trade: &TradeTick) -> Result<TradePushOutcome, FeatureInvalidReason> {
        validate_event(&NormalizedEvent::Trade(trade.clone()))
            .map_err(|error| map_trade_error(error, trade))?;
        let price = ExactDecimal::from_scaled(trade.price)
            .map_err(|_| FeatureInvalidReason::ArithmeticOverflow)?;
        let quantity = ExactDecimal::from_scaled(trade.quantity)
            .map_err(|_| FeatureInvalidReason::ArithmeticOverflow)?;
        if quantity.scaled() <= 0 {
            return Err(FeatureInvalidReason::InvalidQuantity);
        }
        if price.scaled() <= 0 {
            return Err(FeatureInvalidReason::NonPositiveValue);
        }
        self.ensure_identity(trade.meta.adapter, &trade.meta.symbol)?;

        let ts_us = trade_timestamp(trade);
        if let Some(watermark) = self.ordering_watermark_ts_us {
            self.evict_before(watermark.saturating_sub(self.horizon_us));
        }
        let venue_key = VenueTradeKey {
            adapter: trade.meta.adapter,
            symbol: trade.meta.symbol.clone(),
            trade_id: trade.trade_id.clone(),
        };
        if self.seen_event_ids.contains(&trade.meta.event_id)
            || self.seen_venue_keys.contains(&venue_key)
        {
            self.rejections.push_back(Rejection {
                ts_us,
                kind: RejectionKind::Duplicate,
            });
            return Ok(TradePushOutcome::Duplicate);
        }

        self.seen_event_ids.insert(trade.meta.event_id);
        self.seen_venue_keys.insert(venue_key.clone());
        self.seen.push_back(SeenTrade {
            event_id: trade.meta.event_id,
            venue_key,
            ts_us,
        });

        if let Some(previous_ts_us) = self.ordering_watermark_ts_us
            && ts_us < previous_ts_us
        {
            self.rejections.push_back(Rejection {
                ts_us,
                kind: RejectionKind::OutOfOrder,
            });
            return Ok(TradePushOutcome::RejectedOutOfOrder {
                previous_ts_us,
                current_ts_us: ts_us,
            });
        }

        self.ordering_watermark_ts_us = Some(ts_us);
        self.trades.push_back(CompactTrade {
            ts_us,
            price,
            quantity,
            side: trade.taker_side,
        });
        self.update_source_watermark(feature_source(trade));
        self.evict_before(ts_us.saturating_sub(self.horizon_us));
        Ok(TradePushOutcome::Accepted)
    }

    pub fn snapshot(&mut self, window_end_ts_us: i64) -> FlowFeatures {
        if let Some(previous_ts_us) = self.last_snapshot_ts_us
            && window_end_ts_us < previous_ts_us
        {
            return self.invalid_snapshot(
                window_end_ts_us,
                FeatureInvalidReason::RegressingTimestamp {
                    previous_ts_us,
                    current_ts_us: window_end_ts_us,
                },
            );
        }
        self.last_snapshot_ts_us = Some(window_end_ts_us);

        if let Some(source) = &self.source_watermark
            && source.local_recv_ts_us > window_end_ts_us
        {
            return self.invalid_snapshot(
                window_end_ts_us,
                FeatureInvalidReason::FutureTimestamp {
                    source_ts_us: source.local_recv_ts_us,
                    decision_ts_us: window_end_ts_us,
                },
            );
        }

        let Some(window_start_ts_us) = window_end_ts_us.checked_sub(self.horizon_us) else {
            return self
                .invalid_snapshot(window_end_ts_us, FeatureInvalidReason::ArithmeticOverflow);
        };
        self.evict_before(window_start_ts_us);

        if self.source_watermark.is_none() {
            return FlowFeatures::no_input(self.horizon_us, window_end_ts_us, policy());
        }

        let active: Vec<&CompactTrade> = self
            .trades
            .iter()
            .filter(|trade| trade.ts_us <= window_end_ts_us)
            .collect();
        if active.is_empty() {
            let mut result = FlowFeatures::zero_activity(
                self.horizon_us,
                window_end_ts_us,
                self.source_watermark
                    .clone()
                    .expect("source presence checked above"),
                policy(),
            );
            let (duplicates, out_of_order) = self.rejection_counts(window_end_ts_us);
            result.duplicate_trade_count = duplicates;
            result.out_of_order_trade_count = out_of_order;
            return result;
        }

        self.compute_activity(&active, window_end_ts_us)
            .unwrap_or_else(|reason| self.invalid_snapshot(window_end_ts_us, reason))
    }

    fn compute_activity(
        &self,
        active: &[&CompactTrade],
        window_end_ts_us: i64,
    ) -> Result<FlowFeatures, FeatureInvalidReason> {
        let mut buy_base = BigInt::zero();
        let mut sell_base = BigInt::zero();
        let mut unknown_base = BigInt::zero();
        let mut buy_notional_raw = BigInt::zero();
        let mut sell_notional_raw = BigInt::zero();
        let mut unknown_notional_raw = BigInt::zero();
        let mut buy_count = 0_u64;
        let mut sell_count = 0_u64;
        let mut unknown_count = 0_u64;

        for trade in active {
            let quantity = BigInt::from(trade.quantity.scaled());
            let notional = BigInt::from(trade.price.scaled()) * &quantity;
            match trade.side {
                TakerSide::Buy => {
                    buy_base += quantity;
                    buy_notional_raw += notional;
                    buy_count = buy_count
                        .checked_add(1)
                        .ok_or(FeatureInvalidReason::ArithmeticOverflow)?;
                }
                TakerSide::Sell => {
                    sell_base += quantity;
                    sell_notional_raw += notional;
                    sell_count = sell_count
                        .checked_add(1)
                        .ok_or(FeatureInvalidReason::ArithmeticOverflow)?;
                }
                TakerSide::Unknown => {
                    unknown_base += quantity;
                    unknown_notional_raw += notional;
                    unknown_count = unknown_count
                        .checked_add(1)
                        .ok_or(FeatureInvalidReason::ArithmeticOverflow)?;
                }
            }
        }

        let buy_base_volume = big_to_exact(&buy_base)?;
        let sell_base_volume = big_to_exact(&sell_base)?;
        let unknown_base_volume = big_to_exact(&unknown_base)?;
        let buy_quote_notional = scaled_notional(buy_notional_raw)?;
        let sell_quote_notional = scaled_notional(sell_notional_raw)?;
        let unknown_quote_notional = scaled_notional(unknown_notional_raw)?;
        let signed_base = &buy_base - &sell_base;
        let cumulative_volume_delta = big_to_exact(&signed_base)?;
        let signed_denominator = &buy_base + &sell_base;
        let signed_volume_imbalance = if signed_denominator.is_zero() {
            None
        } else {
            Some(exact_ratio(
                signed_base * BigInt::from(ExactDecimal::SCALE),
                signed_denominator,
                DecimalRounding::TowardZero,
            )?)
        };
        let total_count = buy_count
            .checked_add(sell_count)
            .and_then(|count| count.checked_add(unknown_count))
            .ok_or(FeatureInvalidReason::ArithmeticOverflow)?;
        let mean_trade_size = Some(exact_ratio(
            buy_base + sell_base + unknown_base,
            BigInt::from(total_count),
            DecimalRounding::HalfAwayFromZero,
        )?);

        let burst_start_ts_us = window_end_ts_us.saturating_sub(BURST_WINDOW_US);
        let burst_count = u64::try_from(
            active
                .iter()
                .filter(|trade| trade.ts_us >= burst_start_ts_us)
                .count(),
        )
        .map_err(|_| FeatureInvalidReason::ArithmeticOverflow)?;
        let burst_trade_rate_per_second = Some(
            ExactDecimal::from_scaled(
                i128::from(burst_count)
                    .checked_mul(ExactDecimal::SCALE)
                    .ok_or(FeatureInvalidReason::ArithmeticOverflow)?,
            )
            .map_err(|_| FeatureInvalidReason::ArithmeticOverflow)?,
        );
        let first_trade_ts_us = active.first().map(|trade| trade.ts_us);
        let last_trade_ts_us = active.last().map(|trade| trade.ts_us);
        let mean_inter_trade_us = if active.len() > 1 {
            let elapsed = last_trade_ts_us
                .expect("active is nonempty")
                .checked_sub(first_trade_ts_us.expect("active is nonempty"))
                .ok_or(FeatureInvalidReason::ArithmeticOverflow)?;
            let interval_count = i64::try_from(active.len() - 1)
                .map_err(|_| FeatureInvalidReason::ArithmeticOverflow)?;
            Some(elapsed / interval_count)
        } else {
            None
        };
        let (duplicate_trade_count, out_of_order_trade_count) =
            self.rejection_counts(window_end_ts_us);

        Ok(FlowFeatures {
            window_us: self.horizon_us,
            window_end_ts_us,
            input_state: FlowInputState::Activity,
            policy: policy(),
            source_watermark: self.source_watermark.clone(),
            first_trade_ts_us,
            last_trade_ts_us,
            buy_base_volume,
            sell_base_volume,
            unknown_base_volume,
            buy_quote_notional,
            sell_quote_notional,
            unknown_quote_notional,
            buy_trade_count: buy_count,
            sell_trade_count: sell_count,
            unknown_trade_count: unknown_count,
            duplicate_trade_count,
            out_of_order_trade_count,
            mean_trade_size,
            signed_volume_imbalance,
            cumulative_volume_delta,
            burst_count,
            burst_trade_rate_per_second,
            mean_inter_trade_us,
            validity: FeatureValidity::Valid,
        })
    }

    fn rejection_counts(&self, window_end_ts_us: i64) -> (u64, u64) {
        self.rejections
            .iter()
            .filter(|rejection| rejection.ts_us <= window_end_ts_us)
            .fold(
                (0_u64, 0_u64),
                |(duplicate, out_of_order), rejection| match rejection.kind {
                    RejectionKind::Duplicate => (duplicate.saturating_add(1), out_of_order),
                    RejectionKind::OutOfOrder => (duplicate, out_of_order.saturating_add(1)),
                },
            )
    }

    fn evict_before(&mut self, cutoff_ts_us: i64) {
        while self
            .trades
            .front()
            .is_some_and(|trade| trade.ts_us < cutoff_ts_us)
        {
            self.trades.pop_front();
        }
        self.rejections
            .retain(|rejection| rejection.ts_us >= cutoff_ts_us);
        self.seen.retain(|seen| seen.ts_us >= cutoff_ts_us);
        self.seen_event_ids.clear();
        self.seen_venue_keys.clear();
        for seen in &self.seen {
            self.seen_event_ids.insert(seen.event_id);
            self.seen_venue_keys.insert(seen.venue_key.clone());
        }
    }

    fn update_source_watermark(&mut self, source: FeatureSource) {
        let should_update = self.source_watermark.as_ref().is_none_or(|current| {
            (source.local_recv_ts_us, source.event_id)
                > (current.local_recv_ts_us, current.event_id)
        });
        if should_update {
            self.source_watermark = Some(source);
        }
    }

    fn ensure_identity(
        &mut self,
        adapter: AdapterId,
        symbol: &CanonicalSymbol,
    ) -> Result<(), FeatureInvalidReason> {
        if let Some((expected_adapter, expected_symbol)) = &self.identity {
            if *expected_adapter != adapter || expected_symbol != symbol {
                return Err(FeatureInvalidReason::FlowIdentityMismatch {
                    expected_adapter: *expected_adapter,
                    expected_symbol: expected_symbol.clone(),
                    actual_adapter: adapter,
                    actual_symbol: symbol.clone(),
                });
            }
        } else {
            self.identity = Some((adapter, symbol.clone()));
        }
        Ok(())
    }

    fn invalid_snapshot(
        &self,
        window_end_ts_us: i64,
        reason: FeatureInvalidReason,
    ) -> FlowFeatures {
        let zero = zero();
        let has_activity = !self.trades.is_empty();
        let input_state = if self.source_watermark.is_none() {
            FlowInputState::NoInput
        } else if has_activity {
            FlowInputState::Activity
        } else {
            FlowInputState::ZeroActivity
        };
        FlowFeatures {
            window_us: self.horizon_us,
            window_end_ts_us,
            input_state,
            policy: policy(),
            source_watermark: self.source_watermark.clone(),
            first_trade_ts_us: None,
            last_trade_ts_us: None,
            buy_base_volume: zero,
            sell_base_volume: zero,
            unknown_base_volume: zero,
            buy_quote_notional: zero,
            sell_quote_notional: zero,
            unknown_quote_notional: zero,
            buy_trade_count: 0,
            sell_trade_count: 0,
            unknown_trade_count: 0,
            duplicate_trade_count: 0,
            out_of_order_trade_count: 0,
            mean_trade_size: None,
            signed_volume_imbalance: None,
            cumulative_volume_delta: zero,
            burst_count: 0,
            burst_trade_rate_per_second: None,
            mean_inter_trade_us: None,
            validity: FeatureValidity::Invalid(reason),
        }
    }
}

fn trade_timestamp(trade: &TradeTick) -> i64 {
    trade
        .meta
        .exchange_trade_ts_us
        .or(trade.meta.exchange_event_ts_us)
        .unwrap_or(trade.meta.local_recv_ts_us)
}

fn feature_source(trade: &TradeTick) -> FeatureSource {
    let (effective_ts_us, effective_ts_source) =
        if let Some(value) = trade.meta.exchange_trade_ts_us {
            (value, EffectiveTimestampSource::ExchangeTrade)
        } else if let Some(value) = trade.meta.exchange_event_ts_us {
            (value, EffectiveTimestampSource::ExchangeEvent)
        } else {
            (
                trade.meta.local_recv_ts_us,
                EffectiveTimestampSource::LocalReceive,
            )
        };
    FeatureSource {
        event_id: trade.meta.event_id,
        adapter: trade.meta.adapter,
        symbol: trade.meta.symbol.clone(),
        source_sequence: trade.meta.source_sequence,
        exchange_event_ts_us: trade.meta.exchange_event_ts_us,
        exchange_trade_ts_us: trade.meta.exchange_trade_ts_us,
        local_recv_ts_us: trade.meta.local_recv_ts_us,
        effective_ts_us,
        effective_ts_source,
    }
}

fn map_trade_error(error: ValidationError, trade: &TradeTick) -> FeatureInvalidReason {
    match error {
        ValidationError::NonPositiveTradeQuantity { .. } => FeatureInvalidReason::InvalidQuantity,
        ValidationError::NonPositiveTradePrice { .. }
        | ValidationError::NonPositiveLocalTimestamp { .. } => {
            FeatureInvalidReason::NonPositiveValue
        }
        ValidationError::SourceTimestampOutOfRange { field, value, .. } => {
            let source_ts_us = match field {
                TimestampField::ExchangeEvent => value,
                TimestampField::ExchangeTrade => trade_timestamp(trade),
            };
            FeatureInvalidReason::SourceTimestampOutOfRange {
                source_ts_us,
                local_recv_ts_us: trade.meta.local_recv_ts_us,
            }
        }
        ValidationError::EmptyBookSide { .. }
        | ValidationError::NonPositiveBookPrice { .. }
        | ValidationError::NonPositiveBookQuantity { .. }
        | ValidationError::UnsortedBook { .. }
        | ValidationError::CrossedBook { .. } => FeatureInvalidReason::NonPositiveValue,
    }
}

fn scaled_notional(raw: BigInt) -> Result<ExactDecimal, FeatureInvalidReason> {
    exact_ratio(
        raw,
        BigInt::from(ExactDecimal::SCALE),
        DecimalRounding::HalfAwayFromZero,
    )
}

fn exact_ratio(
    mut numerator: BigInt,
    mut denominator: BigInt,
    rounding: DecimalRounding,
) -> Result<ExactDecimal, FeatureInvalidReason> {
    if denominator.is_zero() {
        return Err(FeatureInvalidReason::ArithmeticOverflow);
    }
    if denominator.is_negative() {
        numerator = -numerator;
        denominator = -denominator;
    }
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    let rounded = if remainder.is_zero() {
        quotient
    } else {
        let direction = if numerator.is_negative() { -1 } else { 1 };
        match rounding {
            DecimalRounding::TowardZero => quotient,
            DecimalRounding::Floor if numerator.is_negative() => quotient - 1,
            DecimalRounding::Floor => quotient,
            DecimalRounding::Ceiling if numerator.is_positive() => quotient + 1,
            DecimalRounding::Ceiling => quotient,
            DecimalRounding::HalfAwayFromZero
                if remainder.abs() * BigInt::from(2) >= denominator.abs() =>
            {
                quotient + direction
            }
            DecimalRounding::HalfAwayFromZero => quotient,
        }
    };
    big_to_exact(&rounded)
}

fn big_to_exact(value: &BigInt) -> Result<ExactDecimal, FeatureInvalidReason> {
    value
        .to_i128()
        .ok_or(FeatureInvalidReason::ArithmeticOverflow)
        .and_then(|scaled| {
            ExactDecimal::from_scaled(scaled).map_err(|_| FeatureInvalidReason::ArithmeticOverflow)
        })
}

fn zero() -> ExactDecimal {
    ExactDecimal::from_scaled(0).expect("zero is Decimal128-representable")
}

fn policy() -> FlowPolicy {
    FlowPolicy {
        dedupe: TradeDedupePolicy::EventIdAndVenueTradeId,
        out_of_order: OutOfOrderPolicy::RejectRegressingExchangeTime,
    }
}
