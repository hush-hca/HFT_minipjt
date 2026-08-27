use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ModeLabel {
    Monitor,
    Paper,
    Testnet,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ControlAvailability {
    Enabled,
    Disabled { code: String },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OperatorCommand {
    SetMode(ModeLabel),
    ArmTestnet { confirmation: String },
    SetStrategyEnabled(bool),
    CancelAll { confirmation: String },
    RequestClosePositions { confirmation: String },
    KillNewOrderFlow { confirmation: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpportunityRow {
    pub symbol: String,
    pub short_venue: String,
    pub short_rate_ppm: i128,
    pub short_interval_secs: u32,
    pub long_venue: String,
    pub long_rate_ppm: i128,
    pub long_interval_secs: u32,
    pub raw_gap_ppm: i128,
    pub indicative_apr_bps: i128,
    pub conservative_net_usd_micros: Option<i128>,
    pub capacity_usd_micros: Option<i128>,
    pub freshness_ms: u64,
    pub exclusion: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookLevel {
    pub price: i128,
    pub quantity: i128,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MarketSummary {
    pub symbol: String,
    pub venue: String,
    pub book_events: u64,
    pub trade_events: u64,
    pub freshness_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketDetailView {
    pub symbol: String,
    pub venue: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub mid_price: Option<i128>,
    pub micro_price: Option<i128>,
    pub mid_history: Vec<(i64, i128)>,
    pub micro_history: Vec<(i64, i128)>,
    pub basis_bps: Option<i128>,
    pub open_interest: Option<i128>,
    pub top_trader_ratio_ppm: Option<i128>,
    pub cvd: Option<i128>,
    pub order_flow_imbalance_ppm: Option<i128>,
    pub latency_us: Option<i64>,
    pub freshness_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyOrdersView {
    pub availability: ControlAvailability,
    pub state: String,
    pub transition_reason: String,
    pub residual_delta: Option<i128>,
    pub predicted_funding: Option<i128>,
    pub confirmed_funding: Option<i128>,
    pub pnl_total: Option<i128>,
    pub reconciliation: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SystemHealthView {
    pub frames_per_second: u64,
    pub events_per_second: u64,
    pub features_per_second: u64,
    pub orders_per_second: u64,
    pub parse_failures: u64,
    pub validation_failures: u64,
    pub reconnects: u64,
    pub sequence_gaps: u64,
    pub backpressure_drops: u64,
    pub ui_input_drops: u64,
    pub ui_snapshots_superseded: u64,
    pub arrow_status: String,
    pub sqlite_status: String,
    pub public_connections: String,
    pub private_connections: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskControlsView {
    pub mode: ModeLabel,
    pub availability: ControlAvailability,
    pub banner: String,
    pub strategy_enabled: bool,
    pub kill_switch_active: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UiSnapshot {
    pub sequence: u64,
    pub generated_at_us: i64,
    pub opportunities: Vec<OpportunityRow>,
    pub markets: Vec<MarketSummary>,
    pub market: MarketDetailView,
    pub strategy: StrategyOrdersView,
    pub health: SystemHealthView,
    pub risk: RiskControlsView,
}

impl UiSnapshot {
    pub fn starting() -> Self {
        let mut snapshot = Self::demo();
        snapshot.opportunities.clear();
        snapshot.markets.clear();
        snapshot.market.symbol = "WAITING FOR MARKET DATA".into();
        snapshot.market.venue = "UNAVAILABLE".into();
        snapshot.market.bids.clear();
        snapshot.market.asks.clear();
        snapshot.market.mid_price = None;
        snapshot.market.micro_price = None;
        snapshot.market.mid_history.clear();
        snapshot.market.micro_history.clear();
        snapshot.market.basis_bps = None;
        snapshot.market.open_interest = None;
        snapshot.market.top_trader_ratio_ppm = None;
        snapshot.market.cvd = None;
        snapshot.market.order_flow_imbalance_ppm = None;
        snapshot.market.latency_us = None;
        snapshot.market.freshness_ms = u64::MAX;
        snapshot
    }

    pub fn demo() -> Self {
        let unavailable = ControlAvailability::Disabled {
            code: "EXECUTION_ENGINE_UNAVAILABLE".into(),
        };
        const SCALE: i128 = 1_000_000_000_000_000_000;
        let levels = |side: i128| {
            (0..20)
                .map(|i| BookLevel {
                    price: 100_000 * SCALE + side * i * SCALE / 10,
                    quantity: SCALE + i * SCALE / 12,
                })
                .collect()
        };
        Self {
            sequence: 0,
            generated_at_us: 0,
            opportunities: vec![
                OpportunityRow {
                    symbol: "BTC/USDT".into(),
                    short_venue: "Binance USD-M".into(),
                    short_rate_ppm: 112,
                    short_interval_secs: 28_800,
                    long_venue: "Bybit Linear".into(),
                    long_rate_ppm: 31,
                    long_interval_secs: 28_800,
                    raw_gap_ppm: 81,
                    indicative_apr_bps: 887,
                    conservative_net_usd_micros: Some(61_000),
                    capacity_usd_micros: Some(25_000_000_000),
                    freshness_ms: 180,
                    exclusion: None,
                },
                OpportunityRow {
                    symbol: "ETH/USDT".into(),
                    short_venue: "Binance USD-M".into(),
                    short_rate_ppm: 74,
                    short_interval_secs: 28_800,
                    long_venue: "Bybit Linear".into(),
                    long_rate_ppm: 22,
                    long_interval_secs: 28_800,
                    raw_gap_ppm: 52,
                    indicative_apr_bps: 569,
                    conservative_net_usd_micros: Some(34_000),
                    capacity_usd_micros: Some(18_000_000_000),
                    freshness_ms: 210,
                    exclusion: None,
                },
            ],
            markets: vec![MarketSummary {
                symbol: "BTC/USDT".into(),
                venue: "Binance USD-M".into(),
                book_events: 1,
                trade_events: 1,
                freshness_ms: 180,
            }],
            market: MarketDetailView {
                symbol: "BTC/USDT".into(),
                venue: "Binance USD-M".into(),
                bids: levels(-1),
                asks: levels(1),
                mid_price: Some(100_000 * SCALE),
                micro_price: Some(100_000 * SCALE + SCALE / 25),
                mid_history: (0..120)
                    .map(|i| (i, 100_000 * SCALE + i128::from(i % 13) * SCALE / 100))
                    .collect(),
                micro_history: (0..120)
                    .map(|i| (i, 100_000 * SCALE + i128::from(i % 11) * SCALE / 100))
                    .collect(),
                basis_bps: Some(14),
                open_interest: Some(8_420_000 * SCALE),
                top_trader_ratio_ppm: Some(1_180_000),
                cvd: Some(284 * SCALE),
                order_flow_imbalance_ppm: Some(83 * SCALE),
                latency_us: Some(3_240),
                freshness_ms: 180,
            },
            strategy: StrategyOrdersView {
                availability: unavailable.clone(),
                state: "UNAVAILABLE".into(),
                transition_reason: "EXECUTION_ENGINE_UNAVAILABLE".into(),
                residual_delta: None,
                predicted_funding: None,
                confirmed_funding: None,
                pnl_total: None,
                reconciliation: "NOT_INSTALLED".into(),
            },
            health: SystemHealthView {
                frames_per_second: 0,
                events_per_second: 0,
                features_per_second: 0,
                orders_per_second: 0,
                parse_failures: 0,
                validation_failures: 0,
                reconnects: 0,
                sequence_gaps: 0,
                backpressure_drops: 0,
                ui_input_drops: 0,
                ui_snapshots_superseded: 0,
                arrow_status: "STARTING".into(),
                sqlite_status: "NOT_INSTALLED".into(),
                public_connections: "STARTING".into(),
                private_connections: "NOT_INSTALLED".into(),
            },
            risk: RiskControlsView {
                mode: ModeLabel::Monitor,
                availability: unavailable,
                banner: "testnet research defaults — read-only monitor".into(),
                strategy_enabled: false,
                kill_switch_active: true,
            },
        }
    }

    pub fn debug_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "sequence={} opportunities={} mode={:?}",
            self.sequence,
            self.opportunities.len(),
            self.risk.mode
        );
        out
    }
}

pub fn decimal(value: Option<i128>, scale: i128, suffix: &str) -> String {
    let Some(value) = value else {
        return "—".into();
    };
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.abs();
    format!(
        "{sign}{}.{:04}{suffix}",
        magnitude / scale,
        (magnitude % scale) * 10_000 / scale
    )
}
