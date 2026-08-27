use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use md_core::config::CollectorConfig;
use md_core::model::{AdapterId, CanonicalSymbol, NormalizedEvent};
use md_exchanges::{
    AdapterRuntime, BinanceSpotParser, BinanceUsdmParser, BithumbParser, DiscoveryResult,
    FrameParser, RuntimeOptions, RuntimeStats, UpbitParser, build_combined_stream_url,
    build_subscription, discover_markets, run_supervised_with_options,
};
use md_storage::{PartitionKey, PartitionRouter, StorageConfig, recover_partial};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::report::{MissingMarkets, RecoveryRecord, RunReport};
use crate::stats::{ALL_ADAPTERS, AdapterSnapshot, StatsRegistry, adapter_name};

pub type DiscoveryFuture<'a> = Pin<Box<dyn Future<Output = Result<DiscoveryResult>> + Send + 'a>>;
pub type SupervisorFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
type RuntimeSpec = (AdapterRuntime, RuntimeOptions);
type PreparedRuntimes = (Vec<RuntimeSpec>, Vec<MissingMarkets>);

pub trait MarketDiscovery: Send + Sync {
    fn discover<'a>(
        &'a self,
        adapter: AdapterId,
        config: &'a CollectorConfig,
    ) -> DiscoveryFuture<'a>;
}

pub trait AdapterSupervisor: Send + Sync {
    fn run(
        &self,
        runtime: AdapterRuntime,
        options: RuntimeOptions,
        tx: mpsc::Sender<NormalizedEvent>,
        shutdown: CancellationToken,
        stats: Arc<dyn RuntimeStats>,
    ) -> SupervisorFuture;
}

pub trait SnapshotEmitter: Send + Sync {
    fn emit(&self, snapshot: &AdapterSnapshot);
}

/// Receives normalized market events on the storage consumer task.
///
/// Implementations must return immediately: collection and Arrow persistence
/// intentionally never await a UI or analytics consumer.
pub trait MarketEventObserver: Send + Sync {
    fn observe(&self, event: &NormalizedEvent);
}

#[derive(Default)]
struct LiveDiscovery;

impl MarketDiscovery for LiveDiscovery {
    fn discover<'a>(
        &'a self,
        adapter: AdapterId,
        config: &'a CollectorConfig,
    ) -> DiscoveryFuture<'a> {
        Box::pin(async move {
            let client = reqwest::Client::new();
            discover_markets(adapter, &client, config)
                .await
                .map_err(Into::into)
        })
    }
}

#[derive(Default)]
struct LiveSupervisor;

impl AdapterSupervisor for LiveSupervisor {
    fn run(
        &self,
        runtime: AdapterRuntime,
        options: RuntimeOptions,
        tx: mpsc::Sender<NormalizedEvent>,
        shutdown: CancellationToken,
        stats: Arc<dyn RuntimeStats>,
    ) -> SupervisorFuture {
        Box::pin(async move {
            run_supervised_with_options(runtime, tx, shutdown, stats, options)
                .await
                .map_err(Into::into)
        })
    }
}

#[derive(Default)]
struct TracingSnapshotEmitter;

impl SnapshotEmitter for TracingSnapshotEmitter {
    fn emit(&self, snapshot: &AdapterSnapshot) {
        match serde_json::to_string(snapshot) {
            Ok(json) => info!(target: "collector::stats", snapshot = %json, "adapter statistics"),
            Err(error) => {
                warn!(target: "collector::stats", %error, "failed to encode adapter statistics")
            }
        }
    }
}

#[derive(Default)]
struct NoopMarketEventObserver;

impl MarketEventObserver for NoopMarketEventObserver {
    fn observe(&self, _event: &NormalizedEvent) {}
}

pub struct CollectorApp {
    config: CollectorConfig,
    discovery: Arc<dyn MarketDiscovery>,
    supervisor: Arc<dyn AdapterSupervisor>,
    emitter: Arc<dyn SnapshotEmitter>,
    event_observer: Arc<dyn MarketEventObserver>,
    stats: Arc<StatsRegistry>,
}

impl std::fmt::Debug for CollectorApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CollectorApp")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CollectorApp {
    pub fn new(config: CollectorConfig) -> Result<Self> {
        Self::with_services(
            config,
            Arc::new(LiveDiscovery),
            Arc::new(LiveSupervisor),
            Arc::new(TracingSnapshotEmitter),
        )
    }

    pub fn with_services(
        config: CollectorConfig,
        discovery: Arc<dyn MarketDiscovery>,
        supervisor: Arc<dyn AdapterSupervisor>,
        emitter: Arc<dyn SnapshotEmitter>,
    ) -> Result<Self> {
        config.validate()?;
        let stats = Arc::new(StatsRegistry::new(config.channel_capacity));
        Ok(Self {
            config,
            discovery,
            supervisor,
            emitter,
            event_observer: Arc::new(NoopMarketEventObserver),
            stats,
        })
    }

    pub fn with_event_observer(mut self, observer: Arc<dyn MarketEventObserver>) -> Self {
        self.event_observer = observer;
        self
    }

    pub fn stats(&self) -> Arc<StatsRegistry> {
        Arc::clone(&self.stats)
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<RunReport> {
        let started_at_us = system_time_us();
        let discovered = self.discover_enabled().await?;
        let (runtime_specs, missing_markets) = self.prepare_runtimes(discovered)?;

        let recovery = recover_partials(&self.config.output_root)?;
        let router = PartitionRouter::open(StorageConfig {
            output_root: self.config.output_root.clone(),
            batch_rows: self.config.batch_rows,
            flush_interval: Duration::from_millis(self.config.flush_interval_ms),
        })?;

        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let internal_shutdown = CancellationToken::new();
        let mut storage = tokio::spawn(storage_loop(
            router,
            rx,
            Duration::from_millis(self.config.flush_interval_ms),
            Arc::clone(&self.stats),
            Arc::clone(&self.event_observer),
        ));
        let stats_shutdown = CancellationToken::new();
        let stats_task = spawn_stats_task(
            Arc::clone(&self.stats),
            Arc::clone(&self.emitter),
            Duration::from_secs(self.config.stats_interval_secs),
            stats_shutdown.child_token(),
        );

        let mut adapters = JoinSet::new();
        let mut launched_adapters = Vec::new();
        for (runtime, options) in runtime_specs {
            launched_adapters.push(runtime.id);
            let future = self.supervisor.run(
                runtime,
                options,
                tx.clone(),
                internal_shutdown.child_token(),
                Arc::clone(&self.stats) as Arc<dyn RuntimeStats>,
            );
            adapters.spawn(future);
        }

        let mut storage_result = None;
        let run_error = tokio::select! {
            () = shutdown.cancelled() => None,
            result = &mut storage => {
                storage_result = Some(flatten_storage_join(result));
                Some(anyhow!("storage router stopped before shutdown"))
            }
            adapter = adapters.join_next(), if !adapters.is_empty() => {
                match adapter {
                    Some(Ok(Ok(()))) => Some(anyhow!("adapter supervisor stopped before shutdown")),
                    Some(Ok(Err(error))) => Some(error.context("adapter supervisor failed")),
                    Some(Err(error)) => Some(anyhow!(error).context("adapter supervisor task panicked")),
                    None => Some(anyhow!("all adapter supervisors stopped before shutdown")),
                }
            }
        };

        internal_shutdown.cancel();
        while let Some(result) = adapters.join_next().await {
            if let Err(error) = result {
                warn!(%error, "adapter supervisor task failed during shutdown");
            } else if let Ok(Err(error)) = result {
                warn!(%error, "adapter supervisor returned an error during shutdown");
            }
        }
        for adapter in launched_adapters {
            self.stats.mark_stopped(adapter);
        }
        drop(tx);

        let storage_result = match storage_result {
            Some(result) => result,
            None => flatten_storage_join(storage.await),
        };
        stats_shutdown.cancel();
        let _ = stats_task.await;

        let ended_at_us = system_time_us();
        let mut report = RunReport::empty(started_at_us, ended_at_us);
        let failure = match storage_result {
            Err(error) => Some(error.context("fatal storage failure")),
            Ok(()) => run_error,
        };
        if failure.is_some() {
            report.status = "failed".to_owned();
        }
        report.adapters = self.stats.snapshots();
        report.missing_markets = missing_markets;
        report.recovery = recovery;
        let report_result = report.write_json(&self.config.output_root.join("run-report.json"));
        if let Some(error) = failure {
            if let Err(report_error) = report_result {
                warn!(%report_error, "failed to write final failure report");
            }
            return Err(error);
        }
        report_result?;
        Ok(report)
    }

    async fn discover_enabled(&self) -> Result<Vec<(AdapterId, DiscoveryResult)>> {
        let mut results = Vec::new();
        for adapter in ALL_ADAPTERS {
            let config = self.adapter_config(adapter)?;
            if !config.enabled {
                continue;
            }
            match self.discovery.discover(adapter, &self.config).await {
                Ok(result) => results.push((adapter, result)),
                Err(error) if !self.config.strict_symbols => {
                    warn!(adapter = adapter_name(adapter), %error, "market discovery failed; adapter disabled for this run");
                    results.push((
                        adapter,
                        DiscoveryResult {
                            requested: requested_symbols(&self.config.assets, &config.quote),
                            available: Vec::new(),
                            missing: requested_symbols(&self.config.assets, &config.quote),
                        },
                    ));
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "strict market discovery failed for {}",
                            adapter_name(adapter)
                        )
                    });
                }
            }
        }
        Ok(results)
    }

    fn prepare_runtimes(
        &self,
        discovered: Vec<(AdapterId, DiscoveryResult)>,
    ) -> Result<PreparedRuntimes> {
        let mut runtimes = Vec::new();
        let mut missing_markets = Vec::new();
        for (adapter, discovery) in discovered {
            if !discovery.missing.is_empty() {
                missing_markets.push(MissingMarkets {
                    adapter: adapter_name(adapter).to_owned(),
                    symbols: discovery.missing.iter().map(symbol_name).collect(),
                });
            }
            if discovery.available.is_empty() {
                continue;
            }
            let config = self.adapter_config(adapter)?;
            let (websocket_url, subscription) = match adapter {
                AdapterId::UpbitSpot | AdapterId::BithumbSpot => (
                    config.websocket_url.clone(),
                    build_subscription(adapter, &discovery.available, Uuid::now_v7())?,
                ),
                AdapterId::BinanceSpot | AdapterId::BinanceUsdm => (
                    build_combined_stream_url(&config.websocket_url, &discovery.available)?
                        .to_string(),
                    String::new(),
                ),
                AdapterId::BybitLinear => {
                    return Err(anyhow!(
                        "adapter BybitLinear is not supported by the Phase 1 collector runtime"
                    ));
                }
            };
            let parser: Arc<dyn FrameParser> = match adapter {
                AdapterId::UpbitSpot => Arc::new(UpbitParser),
                AdapterId::BithumbSpot => Arc::new(BithumbParser),
                AdapterId::BinanceSpot => Arc::new(BinanceSpotParser),
                AdapterId::BinanceUsdm => Arc::new(BinanceUsdmParser),
                AdapterId::BybitLinear => {
                    return Err(anyhow!(
                        "adapter BybitLinear is not supported by the Phase 1 collector runtime"
                    ));
                }
            };
            let proactive = config
                .proactive_reconnect_secs
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(365 * 24 * 60 * 60));
            let runtime =
                AdapterRuntime::new(adapter, websocket_url, subscription, proactive, parser);
            let mut options = RuntimeOptions::default();
            options.initial_backoff = Duration::from_millis(self.config.retry.initial_ms);
            options.max_backoff = Duration::from_millis(self.config.retry.max_ms);
            options.healthy_reset = Duration::from_secs(self.config.retry.reset_after_secs);
            options.send_timeout = Duration::from_millis(self.config.enqueue_timeout_ms);
            runtimes.push((runtime, options));
        }
        Ok((runtimes, missing_markets))
    }

    fn adapter_config(&self, adapter: AdapterId) -> Result<&md_core::config::AdapterConfig> {
        self.config
            .adapters
            .get(adapter_name(adapter))
            .ok_or_else(|| {
                anyhow!(
                    "missing adapter configuration for {}",
                    adapter_name(adapter)
                )
            })
    }
}

async fn storage_loop(
    mut router: PartitionRouter,
    mut receiver: mpsc::Receiver<NormalizedEvent>,
    flush_interval: Duration,
    stats: Arc<StatsRegistry>,
    event_observer: Arc<dyn MarketEventObserver>,
) -> Result<()> {
    let mut flush = tokio::time::interval(flush_interval);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush.tick().await;
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                Some(event) => {
                    let depth = receiver.len();
                    for adapter in ALL_ADAPTERS {
                        stats.on_queue_depth(adapter, depth);
                    }
                    let adapter = event.meta().adapter;
                    let key = match PartitionKey::for_event(&event) {
                        Ok(key) => key,
                        Err(error) => {
                            let _ = router.shutdown().await;
                            return Err(anyhow!(error)).with_context(|| {
                                format!("invalid storage partition for {}", adapter_name(adapter))
                            });
                        }
                    };
                    let (partition, rows) = match &event {
                        NormalizedEvent::Book(book) => (
                            key.book_path(Path::new(""))
                                .parent()
                                .unwrap_or_else(|| Path::new(""))
                                .display()
                                .to_string(),
                            u64::try_from(book.bids.len().saturating_add(book.asks.len()))
                                .unwrap_or(u64::MAX),
                        ),
                        NormalizedEvent::Trade(_) => (
                            key.trade_path(Path::new(""))
                                .parent()
                                .unwrap_or_else(|| Path::new(""))
                                .display()
                                .to_string(),
                            1,
                        ),
                    };
                    event_observer.observe(&event);
                    if let Err(error) = router.push(event).await {
                        let _ = router.shutdown().await;
                        return Err(anyhow!(error)).with_context(|| {
                            format!("storage push failed for {}", adapter_name(adapter))
                        });
                    }
                    stats.on_rows_written(adapter, partition, rows);
                },
                None => break,
            },
            _ = flush.tick() => {
                    if let Err(error) = router.flush_due(StdInstant::now()).await {
                    let _ = router.shutdown().await;
                    return Err(error.into());
                }
            },
        }
    }
    router.shutdown().await?;
    Ok(())
}

fn spawn_stats_task(
    stats: Arc<StatsRegistry>,
    emitter: Arc<dyn SnapshotEmitter>,
    interval: Duration,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut previous = stats.snapshots();
        let mut previous_at = Instant::now();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    emit_interval(stats.as_ref(), emitter.as_ref(), &mut previous, &mut previous_at);
                    return;
                },
                _ = ticker.tick() => emit_interval(stats.as_ref(), emitter.as_ref(), &mut previous, &mut previous_at),
            }
        }
    })
}

fn emit_interval(
    stats: &StatsRegistry,
    emitter: &dyn SnapshotEmitter,
    previous: &mut Vec<AdapterSnapshot>,
    previous_at: &mut Instant,
) {
    let now = Instant::now();
    let interval_ms = u64::try_from(now.saturating_duration_since(*previous_at).as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let current = stats.snapshots();
    for (mut snapshot, old) in current.iter().cloned().zip(previous.iter()) {
        snapshot.interval_ms = interval_ms;
        snapshot.frames_per_sec =
            per_second(snapshot.frames.saturating_sub(old.frames), interval_ms);
        snapshot.bytes_per_sec = per_second(snapshot.bytes.saturating_sub(old.bytes), interval_ms);
        snapshot.books_per_sec = per_second(snapshot.books.saturating_sub(old.books), interval_ms);
        snapshot.book_rows_per_sec = per_second(
            snapshot.book_rows.saturating_sub(old.book_rows),
            interval_ms,
        );
        snapshot.trades_per_sec =
            per_second(snapshot.trades.saturating_sub(old.trades), interval_ms);
        emitter.emit(&snapshot);
    }
    *previous = current;
    *previous_at = now;
}

fn per_second(delta: u64, interval_ms: u64) -> f64 {
    delta as f64 * 1_000.0 / interval_ms.max(1) as f64
}

fn flatten_storage_join(result: Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match result {
        Ok(result) => result,
        Err(error) => Err(anyhow!(error).context("storage router task panicked")),
    }
}

fn recover_partials(root: &Path) -> Result<Vec<RecoveryRecord>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut directories = vec![root.to_path_buf()];
    let mut paths = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("failed to scan {} for partial files", directory.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(path);
            } else if file_type.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".arrow.partial"))
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let outcome = recover_partial(&path)
                .with_context(|| format!("failed to recover {}", path.display()))?;
            Ok(RecoveryRecord {
                path,
                batches_kept: outcome.batches_kept,
                rows_kept: outcome.rows_kept,
                bytes_kept: outcome.bytes_kept,
                bytes_rejected: outcome.bytes_rejected,
                corrupt_path: outcome.corrupt_path,
            })
        })
        .collect()
}

fn requested_symbols(assets: &[String], quote: &str) -> Vec<CanonicalSymbol> {
    assets
        .iter()
        .map(|asset| CanonicalSymbol::new(asset, quote))
        .collect()
}

fn symbol_name(symbol: &CanonicalSymbol) -> String {
    format!("{}/{}", symbol.base, symbol.quote)
}

fn system_time_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
