use md_core::model::{AdapterId, CanonicalSymbol};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct VenueOrderId(pub String);
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct FillId(pub String);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum TimeInForce {
    GoodTilCanceled,
    ImmediateOrCancel,
    FillOrKill,
    PostOnly,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub venue: AdapterId,
    pub client_order_id: ClientOrderId,
    pub symbol: CanonicalSymbol,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub quantity: i128,
    pub limit_price: Option<i128>,
    pub reduce_only: bool,
    pub created_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFill {
    pub venue: AdapterId,
    pub client_order_id: ClientOrderId,
    pub venue_order_id: Option<VenueOrderId>,
    pub fill_id: FillId,
    pub price: i128,
    pub quantity: i128,
    pub fee: i128,
    pub fee_asset: String,
    pub source_ts_us: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub signed_quantity: i128,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BalanceSnapshot {
    pub venue: AdapterId,
    pub asset: String,
    pub total: i128,
    pub available: i128,
    pub source_ts_us: i64,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundingIncome {
    pub venue: AdapterId,
    pub income_id: String,
    pub symbol: CanonicalSymbol,
    pub amount: i128,
    pub source_ts_us: i64,
}
