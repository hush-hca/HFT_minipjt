use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FamilyCount {
    pub events: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExcludedSymbol {
    pub symbol: String,
    pub venue: Option<String>,
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicOnlyRequestSummary {
    pub requests: u64,
    pub credential_headers: u64,
    pub authenticated_requests: u64,
    pub no_credentials_client_invariant: bool,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchedulerSummary {
    pub rate_limit_blocks: u64,
    pub budget_rejections: u64,
    pub abandoned_permits: u64,
    pub pending_response_completions: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase2aStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase2aReport {
    pub schema_version: u16,
    pub status: Phase2aStatus,
    pub public_data_only: bool,
    pub requested_symbols: Vec<String>,
    pub common_mainnet_symbols: Vec<String>,
    pub excluded_mainnet: Vec<ExcludedSymbol>,
    pub common_testnet_symbols: Vec<String>,
    pub excluded_testnet: Vec<ExcludedSymbol>,
    pub unavailable_capabilities: BTreeMap<String, String>,
    pub per_family: BTreeMap<String, FamilyCount>,
    pub reconnects: u64,
    pub sequence_gaps: u64,
    pub parser_rejects: u64,
    pub stale_intervals: u64,
    pub scheduler: SchedulerSummary,
    pub public_only_requests: PublicOnlyRequestSummary,
    pub health_errors: Vec<String>,
    pub missing_event_families: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub finalized_paths: Vec<PathBuf>,
    pub output_root: PathBuf,
    pub report_path: PathBuf,
}
