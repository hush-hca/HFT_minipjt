use md_core::model::{AdapterId, CanonicalSymbol, TimestampPrecision};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DerivativeMeta {
    pub schema_version: u16,
    pub event_id: Uuid,
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub venue_symbol: String,
    pub source_ts_us: Option<i64>,
    pub source_ts_precision: TimestampPrecision,
    pub local_recv_ts_us: i64,
}
