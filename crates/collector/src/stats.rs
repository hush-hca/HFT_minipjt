use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use md_core::model::AdapterId;
use md_exchanges::{GapReason, ReconnectReason, RejectReason, RuntimeStats};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
pub struct ReconnectCounts {
    pub peer_closed: u64,
    pub idle_timeout: u64,
    pub protocol: u64,
    pub parse_threshold: u64,
    pub proactive_rotation: u64,
    pub backpressure: u64,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
pub struct ReceiveLagPercentiles {
    pub samples: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct GapRecord {
    pub start_ts_us: i64,
    pub end_ts_us: Option<i64>,
    pub reason: GapReason,
}

impl Serialize for GapRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GapRecord", 3)?;
        state.serialize_field("start_ts_us", &self.start_ts_us)?;
        state.serialize_field("end_ts_us", &self.end_ts_us)?;
        state.serialize_field("reason", gap_reason_name(self.reason))?;
        state.end()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AdapterSnapshot {
    pub adapter: String,
    pub connected: bool,
    pub connected_since_ts_us: Option<i64>,
    pub uptime_us: u64,
    pub frames: u64,
    pub bytes: u64,
    pub interval_ms: u64,
    pub frames_per_sec: f64,
    pub bytes_per_sec: f64,
    pub books_per_sec: f64,
    pub book_rows_per_sec: f64,
    pub trades_per_sec: f64,
    pub books: u64,
    pub book_rows: u64,
    pub trades: u64,
    pub parse_errors: u64,
    pub validation_errors: u64,
    pub rejected_events: u64,
    pub rejected_parse: u64,
    pub rejected_validation: u64,
    pub rejected_backpressure: u64,
    pub reconnects: ReconnectCounts,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub queue_high_water: usize,
    pub backpressure_disconnects: u64,
    pub receive_lag_us: ReceiveLagPercentiles,
    pub current_partition: Option<String>,
    pub rows_written: u64,
    pub known_gap_duration_us: u64,
    pub gaps: Vec<GapRecord>,
}

struct AdapterStats {
    connected: AtomicBool,
    connected_since_ts_us: AtomicI64,
    frames: AtomicU64,
    bytes: AtomicU64,
    books: AtomicU64,
    book_rows: AtomicU64,
    trades: AtomicU64,
    parse_errors: AtomicU64,
    validation_errors: AtomicU64,
    rejected_events: AtomicU64,
    rejected_parse: AtomicU64,
    rejected_validation: AtomicU64,
    rejected_backpressure: AtomicU64,
    reconnect_peer_closed: AtomicU64,
    reconnect_idle_timeout: AtomicU64,
    reconnect_protocol: AtomicU64,
    reconnect_parse_threshold: AtomicU64,
    reconnect_proactive_rotation: AtomicU64,
    reconnect_backpressure: AtomicU64,
    queue_depth: AtomicUsize,
    queue_high_water: AtomicUsize,
    backpressure_disconnects: AtomicU64,
    latency: Mutex<Histogram<u64>>,
    current_partition: Mutex<Option<String>>,
    rows_written: AtomicU64,
    gaps: Mutex<GapState>,
}

impl Default for AdapterStats {
    fn default() -> Self {
        Self {
            connected: AtomicBool::new(false),
            connected_since_ts_us: AtomicI64::new(-1),
            frames: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            books: AtomicU64::new(0),
            book_rows: AtomicU64::new(0),
            trades: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            validation_errors: AtomicU64::new(0),
            rejected_events: AtomicU64::new(0),
            rejected_parse: AtomicU64::new(0),
            rejected_validation: AtomicU64::new(0),
            rejected_backpressure: AtomicU64::new(0),
            reconnect_peer_closed: AtomicU64::new(0),
            reconnect_idle_timeout: AtomicU64::new(0),
            reconnect_protocol: AtomicU64::new(0),
            reconnect_parse_threshold: AtomicU64::new(0),
            reconnect_proactive_rotation: AtomicU64::new(0),
            reconnect_backpressure: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            queue_high_water: AtomicUsize::new(0),
            backpressure_disconnects: AtomicU64::new(0),
            latency: Mutex::new(Histogram::new(3).expect("valid histogram precision")),
            current_partition: Mutex::new(None),
            rows_written: AtomicU64::new(0),
            gaps: Mutex::new(GapState::default()),
        }
    }
}

#[derive(Default)]
struct GapState {
    active: Option<GapRecord>,
    completed: Vec<GapRecord>,
}

#[derive(Clone)]
pub struct StatsRegistry {
    queue_capacity: usize,
    adapters: Arc<[AdapterStats; 5]>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl std::fmt::Debug for StatsRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatsRegistry")
            .field("queue_capacity", &self.queue_capacity)
            .finish_non_exhaustive()
    }
}

impl StatsRegistry {
    pub fn new(queue_capacity: usize) -> Self {
        Self::with_clock(queue_capacity, system_time_us)
    }

    pub fn with_clock(
        queue_capacity: usize,
        clock: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            queue_capacity,
            adapters: Arc::new(std::array::from_fn(|_| AdapterStats::default())),
            clock: Arc::new(clock),
        }
    }

    pub fn snapshot(&self, adapter: AdapterId) -> AdapterSnapshot {
        let value = self.entry(adapter);
        let latency = value
            .latency
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let receive_lag_us = if latency.is_empty() {
            ReceiveLagPercentiles::default()
        } else {
            ReceiveLagPercentiles {
                samples: latency.len(),
                p50: latency.value_at_quantile(0.50),
                p95: latency.value_at_quantile(0.95),
                p99: latency.value_at_quantile(0.99),
            }
        };
        drop(latency);

        let gaps = value.gaps.lock().unwrap_or_else(|error| error.into_inner());
        let mut gap_records = gaps.completed.clone();
        if let Some(active) = gaps.active {
            gap_records.push(active);
        }
        let connected_since = value.connected_since_ts_us.load(Ordering::Relaxed);
        let connected = value.connected.load(Ordering::Relaxed);
        let now = (gaps.active.is_some() || connected).then(|| (self.clock)());
        let known_gap_duration_us = gap_records.iter().fold(0_u64, |total, gap| {
            let end = gap.end_ts_us.or(now).unwrap_or(gap.start_ts_us);
            total.saturating_add(end.saturating_sub(gap.start_ts_us).max(0) as u64)
        });

        let current_partition = value
            .current_partition
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        AdapterSnapshot {
            adapter: adapter_name(adapter).to_owned(),
            connected,
            connected_since_ts_us: (connected_since >= 0).then_some(connected_since),
            uptime_us: if connected_since >= 0 {
                now.unwrap_or(connected_since)
                    .saturating_sub(connected_since)
                    .max(0) as u64
            } else {
                0
            },
            frames: value.frames.load(Ordering::Relaxed),
            bytes: value.bytes.load(Ordering::Relaxed),
            interval_ms: 0,
            frames_per_sec: 0.0,
            bytes_per_sec: 0.0,
            books_per_sec: 0.0,
            book_rows_per_sec: 0.0,
            trades_per_sec: 0.0,
            books: value.books.load(Ordering::Relaxed),
            book_rows: value.book_rows.load(Ordering::Relaxed),
            trades: value.trades.load(Ordering::Relaxed),
            parse_errors: value.parse_errors.load(Ordering::Relaxed),
            validation_errors: value.validation_errors.load(Ordering::Relaxed),
            rejected_events: value.rejected_events.load(Ordering::Relaxed),
            rejected_parse: value.rejected_parse.load(Ordering::Relaxed),
            rejected_validation: value.rejected_validation.load(Ordering::Relaxed),
            rejected_backpressure: value.rejected_backpressure.load(Ordering::Relaxed),
            reconnects: ReconnectCounts {
                peer_closed: value.reconnect_peer_closed.load(Ordering::Relaxed),
                idle_timeout: value.reconnect_idle_timeout.load(Ordering::Relaxed),
                protocol: value.reconnect_protocol.load(Ordering::Relaxed),
                parse_threshold: value.reconnect_parse_threshold.load(Ordering::Relaxed),
                proactive_rotation: value.reconnect_proactive_rotation.load(Ordering::Relaxed),
                backpressure: value.reconnect_backpressure.load(Ordering::Relaxed),
            },
            queue_capacity: self.queue_capacity,
            queue_depth: value.queue_depth.load(Ordering::Relaxed),
            queue_high_water: value.queue_high_water.load(Ordering::Relaxed),
            backpressure_disconnects: value.backpressure_disconnects.load(Ordering::Relaxed),
            receive_lag_us,
            current_partition,
            rows_written: value.rows_written.load(Ordering::Relaxed),
            known_gap_duration_us,
            gaps: gap_records,
        }
    }

    pub fn snapshots(&self) -> Vec<AdapterSnapshot> {
        ALL_ADAPTERS
            .iter()
            .map(|adapter| self.snapshot(*adapter))
            .collect()
    }

    pub fn mark_stopped(&self, adapter: AdapterId) {
        let stats = self.entry(adapter);
        stats.connected.store(false, Ordering::Relaxed);
        stats.connected_since_ts_us.store(-1, Ordering::Relaxed);
    }

    pub fn on_rows_written(&self, adapter: AdapterId, partition: String, rows: u64) {
        let stats = self.entry(adapter);
        stats.rows_written.fetch_add(rows, Ordering::Relaxed);
        *stats
            .current_partition
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(partition);
    }

    fn entry(&self, adapter: AdapterId) -> &AdapterStats {
        &self.adapters[adapter_index(adapter)]
    }
}

impl RuntimeStats for StatsRegistry {
    fn on_frame(&self, adapter: AdapterId, bytes: u32) {
        let stats = self.entry(adapter);
        stats.frames.fetch_add(1, Ordering::Relaxed);
        stats.bytes.fetch_add(u64::from(bytes), Ordering::Relaxed);
    }

    fn on_events(&self, adapter: AdapterId, books: u64, book_rows: u64, trades: u64) {
        let stats = self.entry(adapter);
        stats.books.fetch_add(books, Ordering::Relaxed);
        stats.book_rows.fetch_add(book_rows, Ordering::Relaxed);
        stats.trades.fetch_add(trades, Ordering::Relaxed);
    }

    fn on_parse_error(&self, adapter: AdapterId) {
        self.entry(adapter)
            .parse_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn on_receive_lag_us(&self, adapter: AdapterId, lag_us: u64) {
        let mut histogram = self
            .entry(adapter)
            .latency
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _ = histogram.record(lag_us.max(1));
    }

    fn on_queue_depth(&self, adapter: AdapterId, depth: usize) {
        let stats = self.entry(adapter);
        stats.queue_depth.store(depth, Ordering::Relaxed);
        stats.queue_high_water.fetch_max(depth, Ordering::Relaxed);
    }

    fn on_reconnect(&self, adapter: AdapterId, reason: ReconnectReason) {
        let stats = self.entry(adapter);
        match reason {
            ReconnectReason::PeerClosed => &stats.reconnect_peer_closed,
            ReconnectReason::IdleTimeout => &stats.reconnect_idle_timeout,
            ReconnectReason::Protocol => &stats.reconnect_protocol,
            ReconnectReason::ParseThreshold => &stats.reconnect_parse_threshold,
            ReconnectReason::ProactiveRotation => &stats.reconnect_proactive_rotation,
            ReconnectReason::Backpressure => &stats.reconnect_backpressure,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn on_rejected_event(&self, adapter: AdapterId, reason: RejectReason) {
        let stats = self.entry(adapter);
        stats.rejected_events.fetch_add(1, Ordering::Relaxed);
        match reason {
            RejectReason::Parse => &stats.rejected_parse,
            RejectReason::Validation => {
                stats.validation_errors.fetch_add(1, Ordering::Relaxed);
                &stats.rejected_validation
            }
            RejectReason::Backpressure => &stats.rejected_backpressure,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn on_backpressure_disconnect(&self, adapter: AdapterId) {
        self.entry(adapter)
            .backpressure_disconnects
            .fetch_add(1, Ordering::Relaxed);
    }

    fn open_gap(&self, adapter: AdapterId, reason: GapReason) {
        let stats = self.entry(adapter);
        stats.connected.store(false, Ordering::Relaxed);
        stats.connected_since_ts_us.store(-1, Ordering::Relaxed);
        let mut gaps = stats.gaps.lock().unwrap_or_else(|error| error.into_inner());
        if gaps.active.is_none() {
            gaps.active = Some(GapRecord {
                start_ts_us: (self.clock)(),
                end_ts_us: None,
                reason,
            });
        }
    }

    fn close_gap(&self, adapter: AdapterId) {
        let now = (self.clock)();
        let stats = self.entry(adapter);
        stats.connected.store(true, Ordering::Relaxed);
        stats.connected_since_ts_us.store(now, Ordering::Relaxed);
        let mut gaps = stats.gaps.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(mut gap) = gaps.active.take() {
            gap.end_ts_us = Some(now.max(gap.start_ts_us));
            gaps.completed.push(gap);
        }
    }
}

pub const ALL_ADAPTERS: [AdapterId; 4] = [
    AdapterId::UpbitSpot,
    AdapterId::BithumbSpot,
    AdapterId::BinanceSpot,
    AdapterId::BinanceUsdm,
];

pub fn adapter_name(adapter: AdapterId) -> &'static str {
    match adapter {
        AdapterId::UpbitSpot => "upbit_spot",
        AdapterId::BithumbSpot => "bithumb_spot",
        AdapterId::BinanceSpot => "binance_spot",
        AdapterId::BinanceUsdm => "binance_usdm",
        AdapterId::BybitLinear => "bybit_linear",
    }
}

fn adapter_index(adapter: AdapterId) -> usize {
    match adapter {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}

fn gap_reason_name(reason: GapReason) -> &'static str {
    match reason {
        GapReason::Disconnected => "disconnected",
        GapReason::Backpressure => "backpressure",
    }
}

fn system_time_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
