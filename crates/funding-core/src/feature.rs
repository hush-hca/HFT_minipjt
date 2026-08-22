use md_core::model::{AdapterId, CanonicalSymbol};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::ExactDecimal;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeatureSource {
    pub event_id: Uuid,
    pub exchange_event_ts_us: Option<i64>,
    pub local_recv_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookIdentity {
    pub event_id: Uuid,
    pub exchange_event_ts_us: Option<i64>,
    pub local_recv_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureValidity {
    Valid,
    Invalid(FeatureInvalidReason),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureInvalidReason {
    MissingBook,
    NoInput,
    NonPositiveValue,
    ArithmeticOverflow,
    InvalidQuantity,
    Stale {
        age_us: i64,
        limit_us: i64,
    },
    FutureTimestamp {
        source_ts_us: i64,
        decision_ts_us: i64,
    },
    RegressingTimestamp {
        previous_ts_us: i64,
        current_ts_us: i64,
    },
    MissingConversion,
    InsufficientDepth {
        requested_base: ExactDecimal,
        available_base: ExactDecimal,
    },
    MissingInstrumentRule,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookInvalidReason {
    EmptyBook,
    CrossedBook,
    LockedBook,
    NonPositivePrice,
    NegativeQuantity,
    FutureTimestamp {
        source_ts_us: i64,
        decision_ts_us: i64,
    },
    RegressingTimestamp {
        previous_ts_us: i64,
        current_ts_us: i64,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuralBookValidity {
    Valid,
    Invalid(BookInvalidReason),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutableQuoteSide {
    SellIntoBids,
    BuyFromAsks,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteInvalidReason {
    StructuralBookInvalid,
    InvalidQuantity,
    ArithmeticOverflow,
    FutureTimestamp {
        source_ts_us: i64,
        decision_ts_us: i64,
    },
    RegressingTimestamp {
        previous_ts_us: i64,
        current_ts_us: i64,
    },
    InsufficientDepth {
        requested_base: ExactDecimal,
        available_base: ExactDecimal,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteValidity {
    Valid,
    Invalid(QuoteInvalidReason),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutableQuote {
    pub side: ExecutableQuoteSide,
    pub requested_base: ExactDecimal,
    pub available_base: ExactDecimal,
    pub average_price: Option<ExactDecimal>,
    pub quote_notional: Option<ExactDecimal>,
    pub levels_consumed: u16,
    pub validity: QuoteValidity,
}

impl ExecutableQuote {
    pub fn invalid(
        side: ExecutableQuoteSide,
        requested_base: ExactDecimal,
        available_base: ExactDecimal,
        reason: QuoteInvalidReason,
    ) -> Self {
        Self {
            side,
            requested_base,
            available_base,
            average_price: None,
            quote_notional: None,
            levels_consumed: 0,
            validity: QuoteValidity::Invalid(reason),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookFeatures {
    pub source: FeatureSource,
    pub previous_book: Option<BookIdentity>,
    pub structural_validity: StructuralBookValidity,
    pub sell_into_bids: ExecutableQuote,
    pub buy_from_asks: ExecutableQuote,
    pub mid: Option<ExactDecimal>,
    pub microprice: Option<ExactDecimal>,
    pub imbalance_1: Option<ExactDecimal>,
    pub imbalance_5: Option<ExactDecimal>,
    pub imbalance_10: Option<ExactDecimal>,
    pub imbalance_20: Option<ExactDecimal>,
    pub snapshot_ofi: Option<ExactDecimal>,
    pub depth_delta_bid: Option<ExactDecimal>,
    pub depth_delta_ask: Option<ExactDecimal>,
    pub validity: FeatureValidity,
}

impl BookFeatures {
    pub fn invalid(
        source: FeatureSource,
        previous_book: Option<BookIdentity>,
        structural_validity: StructuralBookValidity,
        sell_into_bids: ExecutableQuote,
        buy_from_asks: ExecutableQuote,
        reason: FeatureInvalidReason,
    ) -> Self {
        Self {
            source,
            previous_book,
            structural_validity,
            sell_into_bids,
            buy_from_asks,
            mid: None,
            microprice: None,
            imbalance_1: None,
            imbalance_5: None,
            imbalance_10: None,
            imbalance_20: None,
            snapshot_ofi: None,
            depth_delta_bid: None,
            depth_delta_ask: None,
            validity: FeatureValidity::Invalid(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowInputState {
    NoInput,
    ZeroActivity,
    Activity,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeDedupePolicy {
    EventIdAndVenueTradeId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutOfOrderPolicy {
    RejectRegressingExchangeTime,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct FlowPolicy {
    pub dedupe: TradeDedupePolicy,
    pub out_of_order: OutOfOrderPolicy,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FlowFeatures {
    pub window_us: i64,
    pub window_end_ts_us: i64,
    pub input_state: FlowInputState,
    pub policy: FlowPolicy,
    pub source_watermark: Option<FeatureSource>,
    pub first_trade_ts_us: Option<i64>,
    pub last_trade_ts_us: Option<i64>,
    pub buy_base_volume: ExactDecimal,
    pub sell_base_volume: ExactDecimal,
    pub unknown_base_volume: ExactDecimal,
    pub buy_quote_notional: ExactDecimal,
    pub sell_quote_notional: ExactDecimal,
    pub unknown_quote_notional: ExactDecimal,
    pub buy_trade_count: u64,
    pub sell_trade_count: u64,
    pub unknown_trade_count: u64,
    pub duplicate_trade_count: u64,
    pub out_of_order_trade_count: u64,
    pub mean_trade_size: Option<ExactDecimal>,
    pub signed_volume_imbalance: Option<ExactDecimal>,
    pub cumulative_volume_delta: ExactDecimal,
    pub burst_count: u64,
    pub mean_inter_trade_us: Option<i64>,
    pub validity: FeatureValidity,
}

impl FlowFeatures {
    pub fn no_input(window_us: i64, end_us: i64, policy: FlowPolicy) -> Self {
        Self::empty(
            window_us,
            end_us,
            FlowInputState::NoInput,
            None,
            policy,
            FeatureValidity::Invalid(FeatureInvalidReason::NoInput),
        )
    }

    pub fn zero_activity(
        window_us: i64,
        end_us: i64,
        source: FeatureSource,
        policy: FlowPolicy,
    ) -> Self {
        Self::empty(
            window_us,
            end_us,
            FlowInputState::ZeroActivity,
            Some(source),
            policy,
            FeatureValidity::Valid,
        )
    }

    fn empty(
        window_us: i64,
        end_us: i64,
        state: FlowInputState,
        source: Option<FeatureSource>,
        policy: FlowPolicy,
        validity: FeatureValidity,
    ) -> Self {
        let zero = ExactDecimal::from_scaled(0).expect("zero is representable");
        Self {
            window_us,
            window_end_ts_us: end_us,
            input_state: state,
            policy,
            source_watermark: source,
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
            mean_inter_trade_us: None,
            validity,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceKind {
    SpotMid,
    PerpetualMid,
    Mark,
    Index,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamedPrice {
    pub venue: AdapterId,
    pub kind: PriceKind,
    pub value: ExactDecimal,
    pub source: FeatureSource,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BasisFeature {
    pub symbol: CanonicalSymbol,
    pub reference: NamedPrice,
    pub compared: NamedPrice,
    pub basis_bps: ExactDecimal,
    pub validity: FeatureValidity,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FeatureEvent {
    Book {
        venue: AdapterId,
        symbol: CanonicalSymbol,
        decision_ts_us: i64,
        value: Box<BookFeatures>,
    },
    Flow {
        venue: AdapterId,
        symbol: CanonicalSymbol,
        decision_ts_us: i64,
        value: Box<FlowFeatures>,
    },
    Basis(BasisFeature),
}
