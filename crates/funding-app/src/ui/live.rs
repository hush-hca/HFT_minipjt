use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

use collector::MarketEventObserver;
use funding_core::config::ExactDecimal;
use funding_core::public::DerivativeEvent;
use funding_features::{book::compute_book_features, flow::TradeWindow};
use md_core::model::{AdapterId, BookSnapshot, NormalizedEvent};

use super::bridge::UiSnapshotPublisher;
use super::model::{BookLevel, MarketDetailView, MarketSummary, OpportunityRow, UiSnapshot};

type MarketKey = (u8, String);

#[derive(Debug, Clone, Eq, PartialEq)]
struct SelectedMarket {
    venue: String,
    symbol: String,
}

#[derive(Debug, Clone)]
pub struct MarketSelection {
    selected: Arc<Mutex<SelectedMarket>>,
}

impl MarketSelection {
    pub fn new(venue: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            selected: Arc::new(Mutex::new(SelectedMarket {
                venue: venue.into(),
                symbol: symbol.into(),
            })),
        }
    }

    pub fn select(&self, venue: impl Into<String>, symbol: impl Into<String>) {
        *self
            .selected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = SelectedMarket {
            venue: venue.into(),
            symbol: symbol.into(),
        };
    }

    fn current(&self) -> SelectedMarket {
        self.selected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

pub struct SharedLiveUiObserver {
    state: Arc<Mutex<LiveUiState>>,
    input_drops: Arc<AtomicU64>,
}

impl SharedLiveUiObserver {
    pub fn new(state: Arc<Mutex<LiveUiState>>) -> Self {
        let input_drops = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .input_drop_counter();
        Self { state, input_drops }
    }
}

impl MarketEventObserver for SharedLiveUiObserver {
    fn observe(&self, event: &NormalizedEvent) {
        match self.state.try_lock() {
            Ok(mut state) => state.market(event),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner().market(event),
            Err(std::sync::TryLockError::WouldBlock) => {
                self.input_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

struct MarketState {
    detail: MarketDetailView,
    previous_book: Option<BookSnapshot>,
    trade_window: TradeWindow,
    book_events: u64,
    trade_events: u64,
    last_event_ts_us: i64,
}

impl MarketState {
    fn new(venue: String, symbol: String) -> Self {
        Self {
            detail: MarketDetailView {
                symbol,
                venue,
                bids: Vec::new(),
                asks: Vec::new(),
                mid_price: None,
                micro_price: None,
                mid_history: Vec::new(),
                micro_history: Vec::new(),
                basis_bps: None,
                open_interest: None,
                top_trader_ratio_ppm: None,
                cvd: None,
                order_flow_imbalance_ppm: None,
                latency_us: None,
                freshness_ms: u64::MAX,
            },
            previous_book: None,
            trade_window: TradeWindow::new(5_000_000).expect("positive flow horizon"),
            book_events: 0,
            trade_events: 0,
            last_event_ts_us: 0,
        }
    }
}

#[derive(Default)]
struct DerivativeMetrics {
    basis_bps: Option<i128>,
    open_interest: Option<i128>,
    top_trader_ratio_ppm: Option<i128>,
}

pub struct LiveUiState {
    snapshot: UiSnapshot,
    publisher: UiSnapshotPublisher,
    selection: MarketSelection,
    markets: BTreeMap<MarketKey, MarketState>,
    derivatives: BTreeMap<MarketKey, DerivativeMetrics>,
    funding: BTreeMap<(String, u8), (i128, u32, u64)>,
    started: Instant,
    market_events: u64,
    derivative_events: u64,
    input_drops: Arc<AtomicU64>,
}

impl LiveUiState {
    pub fn new(
        publisher: UiSnapshotPublisher,
        initial: UiSnapshot,
        selection: MarketSelection,
    ) -> Self {
        Self {
            snapshot: initial,
            publisher,
            selection,
            markets: BTreeMap::new(),
            derivatives: BTreeMap::new(),
            funding: BTreeMap::new(),
            started: Instant::now(),
            market_events: 0,
            derivative_events: 0,
            input_drops: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn input_drop_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.input_drops)
    }

    pub fn market(&mut self, event: &NormalizedEvent) {
        self.market_events = self.market_events.saturating_add(1);
        let meta = event.meta();
        let symbol = symbol_name(&meta.symbol.base, &meta.symbol.quote);
        let venue = venue_name(meta.adapter).to_owned();
        let key = (venue_key(meta.adapter), symbol.clone());
        let state = self
            .markets
            .entry(key)
            .or_insert_with(|| MarketState::new(venue, symbol));
        state.last_event_ts_us = meta.local_recv_ts_us;
        match event {
            NormalizedEvent::Book(book) => update_book(state, book),
            NormalizedEvent::Trade(trade) => {
                state.trade_events = state.trade_events.saturating_add(1);
                if state.trade_window.push(trade).is_ok() {
                    let flow = state.trade_window.snapshot(trade.meta.local_recv_ts_us);
                    state.detail.cvd = Some(flow.cumulative_volume_delta.scaled());
                }
            }
        }
        self.publish();
    }

    pub fn derivative(&mut self, event: &DerivativeEvent) {
        self.derivative_events = self.derivative_events.saturating_add(1);
        let meta = event.meta();
        let symbol = symbol_name(&meta.symbol.base, &meta.symbol.quote);
        let key = (venue_key(meta.venue), symbol.clone());
        let metrics = self.derivatives.entry(key).or_default();
        match event {
            DerivativeEvent::FundingEstimate(value) => {
                self.funding.insert(
                    (symbol, venue_key(meta.venue)),
                    (
                        value.rate,
                        value.interval_secs,
                        age_ms(meta.local_recv_ts_us),
                    ),
                );
                self.rebuild_opportunities();
            }
            DerivativeEvent::MarkIndex(value) => {
                metrics.basis_bps = value
                    .mark_price
                    .checked_sub(value.index_price)
                    .and_then(|delta| delta.checked_mul(10_000))
                    .and_then(|scaled| scaled.checked_div(value.index_price));
            }
            DerivativeEvent::OpenInterest(value) => {
                metrics.open_interest = Some(value.open_interest);
            }
            DerivativeEvent::TraderRatio(value) => {
                metrics.top_trader_ratio_ppm =
                    value.long_short_ratio.checked_div(1_000_000_000_000);
            }
            _ => {}
        }
        self.publish();
    }

    fn rebuild_opportunities(&mut self) {
        let mut by_symbol = BTreeMap::<String, Vec<(u8, i128, u32, u64)>>::new();
        for ((symbol, venue), (rate, interval, age)) in &self.funding {
            by_symbol
                .entry(symbol.clone())
                .or_default()
                .push((*venue, *rate, *interval, *age));
        }
        let mut rows = Vec::new();
        for (symbol, mut values) in by_symbol {
            if values.len() < 2 {
                continue;
            }
            values.sort_by_key(|(_, rate, _, _)| *rate);
            let long = values.first().copied().expect("two funding values");
            let short = values.last().copied().expect("two funding values");
            let gap = short.1.saturating_sub(long.1);
            let gap_ppm = gap / 1_000_000_000_000;
            let settlements_per_year = 31_536_000_i128 / i128::from(short.2.max(long.2));
            let apr_bps = gap_ppm.saturating_mul(settlements_per_year) / 100;
            rows.push(OpportunityRow {
                symbol,
                short_venue: venue_name_from_key(short.0).into(),
                short_rate_ppm: short.1 / 1_000_000_000_000,
                short_interval_secs: short.2,
                long_venue: venue_name_from_key(long.0).into(),
                long_rate_ppm: long.1 / 1_000_000_000_000,
                long_interval_secs: long.2,
                raw_gap_ppm: gap_ppm,
                indicative_apr_bps: apr_bps,
                conservative_net_usd_micros: None,
                capacity_usd_micros: None,
                freshness_ms: short.3.max(long.3),
                exclusion: Some("COST_MODEL_PENDING".into()),
            });
        }
        rows.sort_by_key(|row| std::cmp::Reverse(row.raw_gap_ppm));
        self.snapshot.opportunities = rows;
    }

    fn publish(&mut self) {
        self.refresh_market_snapshot();
        self.snapshot.sequence = self.snapshot.sequence.saturating_add(1);
        self.snapshot.generated_at_us = now_us();
        let elapsed = self.started.elapsed().as_secs().max(1);
        self.snapshot.health.frames_per_second = self.market_events / elapsed;
        self.snapshot.health.events_per_second =
            self.market_events.saturating_add(self.derivative_events) / elapsed;
        self.snapshot.health.features_per_second = self.derivative_events / elapsed;
        self.snapshot.health.public_connections = "RECEIVING".into();
        self.snapshot.health.arrow_status = "WRITING".into();
        self.snapshot.health.ui_input_drops = self.input_drops.load(Ordering::Relaxed);
        self.snapshot.health.ui_snapshots_superseded = self.publisher.superseded_count();
        self.publisher.publish(self.snapshot.clone());
    }

    fn refresh_market_snapshot(&mut self) {
        self.snapshot.markets = self
            .markets
            .values()
            .map(|state| MarketSummary {
                symbol: state.detail.symbol.clone(),
                venue: state.detail.venue.clone(),
                book_events: state.book_events,
                trade_events: state.trade_events,
                freshness_ms: age_ms(state.last_event_ts_us),
            })
            .collect();
        self.snapshot.markets.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then(left.venue.cmp(&right.venue))
        });

        let requested = self.selection.current();
        let requested_key = market_key_from_names(&requested.venue, &requested.symbol);
        let selected_key = requested_key
            .filter(|key| self.markets.contains_key(key))
            .or_else(|| self.markets.keys().next().cloned());
        let Some(key) = selected_key else {
            return;
        };
        let state = self.markets.get(&key).expect("selected market exists");
        let mut detail = state.detail.clone();
        detail.freshness_ms = age_ms(state.last_event_ts_us);
        if let Some(metrics) = self.derivatives.get(&key) {
            detail.basis_bps = metrics.basis_bps;
            detail.open_interest = metrics.open_interest;
            detail.top_trader_ratio_ppm = metrics.top_trader_ratio_ppm;
        }
        self.snapshot.market = detail;
    }
}

fn update_book(state: &mut MarketState, book: &BookSnapshot) {
    state.book_events = state.book_events.saturating_add(1);
    state.detail.bids = book
        .bids
        .iter()
        .take(20)
        .map(|level| BookLevel {
            price: level.price,
            quantity: level.quantity,
        })
        .collect();
    state.detail.asks = book
        .asks
        .iter()
        .take(20)
        .map(|level| BookLevel {
            price: level.price,
            quantity: level.quantity,
        })
        .collect();
    let features = compute_book_features(
        state.previous_book.as_ref(),
        book,
        ExactDecimal::from_scaled(ExactDecimal::SCALE).expect("one base unit is representable"),
        book.meta.local_recv_ts_us,
        5_000_000,
    );
    let mid = features.mid.map(ExactDecimal::scaled);
    let micro = features.microprice.map(ExactDecimal::scaled);
    state.detail.mid_price = mid;
    state.detail.micro_price = micro;
    state.detail.order_flow_imbalance_ppm = features.snapshot_ofi.map(ExactDecimal::scaled);
    if let Some(value) = mid {
        append_point(
            &mut state.detail.mid_history,
            book.meta.local_recv_ts_us,
            value,
        );
    }
    if let Some(value) = micro {
        append_point(
            &mut state.detail.micro_history,
            book.meta.local_recv_ts_us,
            value,
        );
    }
    state.detail.latency_us = book
        .meta
        .exchange_event_ts_us
        .map(|source| book.meta.local_recv_ts_us.saturating_sub(source));
    state.previous_book = Some(book.clone());
}

fn append_point(points: &mut Vec<(i64, i128)>, timestamp: i64, value: i128) {
    if points.last().is_some_and(|(last, _)| *last == timestamp) {
        if let Some(last) = points.last_mut() {
            *last = (timestamp, value);
        }
    } else {
        points.push((timestamp, value));
    }
    if points.len() > 3_600 {
        points.drain(..points.len() - 3_600);
    }
}

fn market_key_from_names(venue: &str, symbol: &str) -> Option<MarketKey> {
    let key = match venue {
        "Upbit Spot" => 0,
        "Bithumb Spot" => 1,
        "Binance Spot" => 2,
        "Binance USD-M" => 3,
        "Bybit Linear" => 4,
        _ => return None,
    };
    Some((key, symbol.to_owned()))
}

fn venue_key(venue: AdapterId) -> u8 {
    match venue {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}

fn venue_name(venue: AdapterId) -> &'static str {
    venue_name_from_key(venue_key(venue))
}

fn venue_name_from_key(venue: u8) -> &'static str {
    match venue {
        0 => "Upbit Spot",
        1 => "Bithumb Spot",
        2 => "Binance Spot",
        3 => "Binance USD-M",
        4 => "Bybit Linear",
        _ => "Unsupported venue",
    }
}

fn symbol_name(base: &str, quote: &str) -> String {
    format!("{base}/{quote}")
}

fn age_ms(timestamp_us: i64) -> u64 {
    u64::try_from(now_us().saturating_sub(timestamp_us).max(0) / 1_000).unwrap_or(u64::MAX)
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| i64::try_from(value.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}
