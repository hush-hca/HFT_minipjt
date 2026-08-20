use serde::{Deserialize, Serialize};

use crate::meta::DerivativeMeta;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractKind {
    Perpetual,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    OneWay,
    Hedge,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountMode {
    Classic,
    Unified,
    Portfolio,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EligibilityReason {
    BinanceUnavailable,
    BybitUnavailable,
    TestnetUnavailable,
    Inactive,
    UnsupportedContract,
    UnsupportedSettlement,
    RuleInvalid,
    FundingScheduleUnknown,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InstrumentSpec {
    pub meta: DerivativeMeta,
    pub contract_kind: ContractKind,
    pub settlement_asset: String,
    pub contract_multiplier: i128,
    pub tick_size: i128,
    pub quantity_step: i128,
    pub min_quantity: i128,
    pub max_quantity: Option<i128>,
    pub min_notional: i128,
    pub funding_interval_secs: u32,
    pub price_lower_bound: Option<i128>,
    pub price_upper_bound: Option<i128>,
    pub supported_position_modes: Vec<PositionMode>,
    pub supported_account_modes: Vec<AccountMode>,
}
