use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use collector::{
    AdapterSnapshot, AdapterSupervisor, CollectorApp, DiscoveryFuture, GapRecord, MarketDiscovery,
    RunReport, SnapshotEmitter, StatsRegistry, SupervisorFuture,
};
use md_core::config::{AdapterConfig, CollectorConfig, RetryConfig};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
    TimestampPrecision,
};
use md_exchanges::{
    AdapterRuntime, DiscoveryResult, GapReason, ReconnectReason, RejectReason, RuntimeOptions,
    RuntimeStats,
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[test]
fn statistics_snapshot_contains_all_health_signals() {
    let stats = StatsRegistry::new(128);
    let adapter = AdapterId::BinanceSpot;

    stats.on_frame(adapter, 512);
    stats.on_events(adapter, 2, 80, 3);
    stats.on_parse_error(adapter);
    stats.on_rejected_event(adapter, RejectReason::Parse);
    stats.on_rejected_event(adapter, RejectReason::Validation);
    stats.on_rejected_event(adapter, RejectReason::Backpressure);
    stats.on_reconnect(adapter, ReconnectReason::PeerClosed);
    stats.on_queue_depth(adapter, 97);
    stats.on_queue_depth(adapter, 12);
    stats.on_backpressure_disconnect(adapter);
    for lag in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] {
        stats.on_receive_lag_us(adapter, lag);
    }

    let snapshot = stats.snapshot(adapter);
    assert_eq!(snapshot.frames, 1);
    assert_eq!(snapshot.bytes, 512);
    assert_eq!(snapshot.books, 2);
    assert_eq!(snapshot.book_rows, 80);
    assert_eq!(snapshot.trades, 3);
    assert_eq!(snapshot.parse_errors, 1);
    assert_eq!(snapshot.validation_errors, 1);
    assert_eq!(snapshot.rejected_events, 3);
    assert_eq!(snapshot.rejected_parse, 1);
    assert_eq!(snapshot.rejected_validation, 1);
    assert_eq!(snapshot.rejected_backpressure, 1);
    assert_eq!(snapshot.reconnects.peer_closed, 1);
    assert_eq!(snapshot.queue_capacity, 128);
    assert_eq!(snapshot.queue_high_water, 97);
    assert_eq!(snapshot.backpressure_disconnects, 1);
    assert!(snapshot.receive_lag_us.p50 >= 40);
    assert!(snapshot.receive_lag_us.p95 >= 90);
    assert!(snapshot.receive_lag_us.p99 >= 90);
}

#[test]
fn gaps_keep_exact_start_end_and_reason() {
    let times = [1_000_i64, 2_500_i64];
    let stats = StatsRegistry::with_clock(16, move || {
        static INDEX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        times[INDEX
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .min(1)]
    });
    let adapter = AdapterId::UpbitSpot;

    stats.open_gap(adapter, GapReason::Disconnected);
    stats.close_gap(adapter);

    assert_eq!(
        stats.snapshot(adapter).gaps,
        vec![GapRecord {
            start_ts_us: 1_000,
            end_ts_us: Some(2_500),
            reason: GapReason::Disconnected,
        }]
    );
}

#[test]
fn run_report_writes_pretty_json_atomically() {
    let root = std::env::temp_dir().join(format!("collector-report-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("run-report.json");
    let report = RunReport::empty(100, 200);

    report.write_json(&path).unwrap();
    RunReport::empty(300, 400).write_json(&path).unwrap();

    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(value["started_at_us"], 300);
    assert_eq!(value["ended_at_us"], 400);
    assert_eq!(value["status"], "completed");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn report_rejects_parent_that_is_a_file() {
    let path = PathBuf::from("Cargo.toml").join("run-report.json");
    assert!(RunReport::empty(0, 1).write_json(&path).is_err());
}

#[tokio::test]
async fn fatal_storage_cancels_the_adapter_and_keeps_original_error() {
    let root = unique_root("fatal-storage");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("upbit"), b"blocks partition directory").unwrap();
    let supervisor = Arc::new(FakeSupervisor::with_event(book_event()));
    let app = CollectorApp::with_services(
        config(root.clone(), false, 10),
        Arc::new(FakeDiscovery::all_available()),
        supervisor.clone(),
        Arc::new(RecordingEmitter::default()),
    )
    .unwrap();

    let error = app.run(CancellationToken::new()).await.unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("fatal storage failure"), "{message}");
    assert!(message.contains("upbit"), "{message}");
    assert!(supervisor.cancelled.load(Ordering::SeqCst));
    let failure_report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("run-report.json")).unwrap()).unwrap();
    assert_eq!(failure_report["status"], "failed");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn graceful_shutdown_drains_storage_then_writes_final_report() {
    let root = unique_root("drain");
    let supervisor = Arc::new(FakeSupervisor::with_event(book_event()));
    let emitter = Arc::new(RecordingEmitter::default());
    let app = CollectorApp::with_services(
        config(root.clone(), false, 10),
        Arc::new(FakeDiscovery::with_missing(vec![CanonicalSymbol::new(
            "ETH", "KRW",
        )])),
        supervisor.clone(),
        emitter,
    )
    .unwrap();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(app.run(shutdown.clone()));
    supervisor.started.notified().await;
    shutdown.cancel();

    let report = running.await.unwrap().unwrap();

    assert!(root.join("run-report.json").exists());
    assert!(
        root.join("upbit/spot/BTC-KRW/2024-09-10/00/books.arrow")
            .exists()
    );
    let upbit = report
        .adapters
        .iter()
        .find(|snapshot| snapshot.adapter == "upbit_spot")
        .unwrap();
    assert!(!upbit.connected);
    assert_eq!(upbit.queue_depth, 0);
    assert_eq!(upbit.rows_written, 2);
    assert!(upbit.current_partition.as_deref().is_some_and(|path| {
        path.ends_with("upbit\\spot\\BTC-KRW\\2024-09-10\\00")
            || path.ends_with("upbit/spot/BTC-KRW/2024-09-10/00")
    }));
    assert_eq!(report.missing_markets[0].symbols, ["ETH/KRW"]);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(start_paused = true)]
async fn configured_interval_emits_rates_and_a_final_snapshot() {
    let root = unique_root("interval");
    let supervisor = Arc::new(FakeSupervisor::without_event());
    let emitter = Arc::new(RecordingEmitter::default());
    let app = CollectorApp::with_services(
        config(root.clone(), false, 10),
        Arc::new(FakeDiscovery::all_available()),
        supervisor.clone(),
        emitter.clone(),
    )
    .unwrap();
    let stats = app.stats();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(app.run(shutdown.clone()));
    supervisor.started.notified().await;
    stats.on_frame(AdapterId::UpbitSpot, 100);
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    shutdown.cancel();

    running.await.unwrap().unwrap();

    let upbit = emitter
        .records
        .lock()
        .unwrap()
        .iter()
        .filter(|snapshot| snapshot.adapter == "upbit_spot")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(upbit.len(), 3, "two intervals plus one final snapshot");
    assert_eq!(upbit[0].interval_ms, 10_000);
    assert!((upbit[0].frames_per_sec - 0.1).abs() < f64::EPSILON);
    assert!(!upbit.last().unwrap().connected);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn strict_discovery_failure_happens_before_storage_is_opened() {
    let root = unique_root("strict");
    let app = CollectorApp::with_services(
        config(root.clone(), true, 10),
        Arc::new(FakeDiscovery::failing()),
        Arc::new(FakeSupervisor::without_event()),
        Arc::new(RecordingEmitter::default()),
    )
    .unwrap();

    let error = app.run(CancellationToken::new()).await.unwrap_err();

    assert!(format!("{error:#}").contains("strict market discovery failed"));
    assert!(!root.exists());
}

#[derive(Clone)]
struct FakeDiscovery {
    fail: bool,
    missing: Vec<CanonicalSymbol>,
}

impl FakeDiscovery {
    fn all_available() -> Self {
        Self {
            fail: false,
            missing: Vec::new(),
        }
    }

    fn with_missing(missing: Vec<CanonicalSymbol>) -> Self {
        Self {
            fail: false,
            missing,
        }
    }

    fn failing() -> Self {
        Self {
            fail: true,
            missing: Vec::new(),
        }
    }
}

impl MarketDiscovery for FakeDiscovery {
    fn discover<'a>(
        &'a self,
        _adapter: AdapterId,
        config: &'a CollectorConfig,
    ) -> DiscoveryFuture<'a> {
        let fail = self.fail;
        let missing = self.missing.clone();
        Box::pin(async move {
            if fail {
                anyhow::bail!("injected discovery failure");
            }
            let requested = config
                .assets
                .iter()
                .map(|asset| CanonicalSymbol::new(asset, "KRW"))
                .collect::<Vec<_>>();
            let available = requested
                .iter()
                .filter(|symbol| !missing.contains(symbol))
                .cloned()
                .collect();
            Ok(DiscoveryResult {
                requested,
                available,
                missing,
            })
        })
    }
}

struct FakeSupervisor {
    event: Option<NormalizedEvent>,
    started: Arc<Notify>,
    cancelled: Arc<AtomicBool>,
}

impl FakeSupervisor {
    fn with_event(event: NormalizedEvent) -> Self {
        Self {
            event: Some(event),
            started: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn without_event() -> Self {
        Self {
            event: None,
            started: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl AdapterSupervisor for FakeSupervisor {
    fn run(
        &self,
        runtime: AdapterRuntime,
        _options: RuntimeOptions,
        tx: mpsc::Sender<NormalizedEvent>,
        shutdown: CancellationToken,
        stats: Arc<dyn RuntimeStats>,
    ) -> SupervisorFuture {
        let event = self.event.clone();
        let started = Arc::clone(&self.started);
        let cancelled = Arc::clone(&self.cancelled);
        Box::pin(async move {
            stats.close_gap(runtime.id);
            if let Some(event) = event {
                tx.send(event).await?;
                stats.on_queue_depth(runtime.id, tx.max_capacity() - tx.capacity());
            }
            started.notify_one();
            shutdown.cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[derive(Default)]
struct RecordingEmitter {
    records: Mutex<Vec<AdapterSnapshot>>,
}

impl SnapshotEmitter for RecordingEmitter {
    fn emit(&self, snapshot: &AdapterSnapshot) {
        self.records.lock().unwrap().push(snapshot.clone());
    }
}

fn config(output_root: PathBuf, strict_symbols: bool, stats_interval_secs: u64) -> CollectorConfig {
    let mut adapters = BTreeMap::new();
    for name in ["upbit_spot", "bithumb_spot", "binance_spot", "binance_usdm"] {
        adapters.insert(
            name.to_owned(),
            AdapterConfig {
                enabled: name == "upbit_spot",
                quote: if name.starts_with("binance") {
                    "USDT"
                } else {
                    "KRW"
                }
                .to_owned(),
                rest_url: "https://example.com/markets".to_owned(),
                websocket_url: "wss://example.com/stream".to_owned(),
                proactive_reconnect_secs: None,
            },
        );
    }
    CollectorConfig {
        output_root,
        assets: vec!["BTC".to_owned(), "ETH".to_owned()],
        strict_symbols,
        channel_capacity: 16,
        batch_rows: 8,
        flush_interval_ms: 1_000,
        enqueue_timeout_ms: 5_000,
        stats_interval_secs,
        retry: RetryConfig {
            initial_ms: 1,
            max_ms: 2,
            reset_after_secs: 5,
        },
        adapters,
    }
}

fn book_event() -> NormalizedEvent {
    NormalizedEvent::Book(BookSnapshot {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter: AdapterId::UpbitSpot,
            symbol: CanonicalSymbol::new("BTC", "KRW"),
            source_symbol: "KRW-BTC".to_owned(),
            source_stream: "orderbook".to_owned(),
            source_sequence: None,
            exchange_event_ts_us: Some(1_725_929_934_000_000),
            exchange_trade_ts_us: None,
            event_ts_precision: TimestampPrecision::Millisecond,
            trade_ts_precision: TimestampPrecision::Unavailable,
            local_recv_ts_us: 1_725_929_934_373_000,
            raw_size_bytes: 100,
        },
        bids: vec![PriceLevel {
            price: 100_000_000_000_000_000_000,
            quantity: 1_000_000_000_000_000_000,
        }],
        asks: vec![PriceLevel {
            price: 101_000_000_000_000_000_000,
            quantity: 1_000_000_000_000_000_000,
        }],
    })
}

fn unique_root(label: &str) -> PathBuf {
    static ID: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "collector-task9-{label}-{}-{}",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ))
}
