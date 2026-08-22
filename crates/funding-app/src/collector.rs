use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use funding_core::config::{EndpointSet, FundingConfig};
use funding_core::instrument::{
    AccountMode, ContractKind, FundingRateBoundsProvenance, InstrumentSpec, PositionMode,
};
use funding_core::meta::DerivativeMeta;
use funding_core::public::{
    DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
    FundingSettlement, MarkIndexSnapshot, OpenInterestSnapshot, OpenInterestUnit,
    QuoteConversionSnapshot, QuoteSide, TraderMetricKind, TraderRatioSnapshot,
};
use futures_util::{SinkExt, StreamExt};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
    TimestampPrecision,
};
use md_exchanges::derivatives::binance::{
    self, EffectiveFundingRule, FundingHistoryRules, FundingRules, FundingSchedule,
    LegacyRateTypePolicy, PublicCapability,
};
use md_exchanges::derivatives::bybit::{self, BybitTickerParser};
use md_exchanges::derivatives::discovery::{
    DerivativeDiscovery, DiscoveryRequestObserver, Environment, IneligibleInstrument,
    discover_derivatives_observed,
};
use md_exchanges::derivatives::scheduler::{
    BudgetError, HealthSignal, Permit, RequestClass, RestScheduler, SchedulerMode,
};
use md_exchanges::{
    AdapterRuntime, BinanceUsdmParser, BithumbParser, BybitLinearParser, FrameParser, RuntimeStats,
    UpbitParser, build_combined_stream_url, build_subscription, run_supervised,
};
use md_storage::{
    DerivativeEventFamily, DerivativePartitionRouter, PartitionRouter, StorageConfig,
};
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::report::{
    ExcludedSymbol, FamilyCount, Phase2aReport, PublicOnlyRequestSummary, SchedulerSummary,
};

const BINANCE_LIMIT_PER_MINUTE: u32 = 2_400;
const BYBIT_PUBLIC_REQUESTS_PER_SECOND: u32 = 10;
const QUOTE_STALE_AFTER: Duration = Duration::from_secs(5);
const BINANCE_TOP_TRADER_CODE: &str = "BINANCE_TOP_TRADER_REQUIRES_API_KEY";
const NETWORK_TIMEOUT: Duration = Duration::from_secs(10);
const WS_READ_TIMEOUT: Duration = Duration::from_secs(120);

type FundingRuleStore =
    Arc<Mutex<std::collections::HashMap<(AdapterId, CanonicalSymbol), FundingRules>>>;

fn funding_rule_store(discovery: &DerivativeDiscovery) -> Result<FundingRuleStore> {
    let mut rules = std::collections::HashMap::new();
    for common in &discovery.eligible {
        rules.insert(
            (AdapterId::BinanceUsdm, common.symbol.clone()),
            FundingRules::from_instrument(&common.binance)?,
        );
        rules.insert(
            (AdapterId::BybitLinear, common.symbol.clone()),
            FundingRules::from_instrument(&common.bybit)?,
        );
    }
    Ok(Arc::new(Mutex::new(rules)))
}

fn update_funding_rules(store: &FundingRuleStore, discovery: &DerivativeDiscovery) -> Result<()> {
    let mut locked = store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for common in &discovery.eligible {
        locked.insert(
            (AdapterId::BinanceUsdm, common.symbol.clone()),
            FundingRules::from_instrument(&common.binance)?,
        );
        locked.insert(
            (AdapterId::BybitLinear, common.symbol.clone()),
            FundingRules::from_instrument(&common.bybit)?,
        );
    }
    Ok(())
}

fn current_funding_rules(
    store: &FundingRuleStore,
    venue: AdapterId,
    symbol: &CanonicalSymbol,
) -> FundingRules {
    store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&(venue, symbol.clone()))
        .copied()
        .unwrap_or_default()
}

#[derive(Debug, Clone, Default)]
pub struct SyntheticPublicSource {
    fail_producer: bool,
}

impl SyntheticPublicSource {
    pub fn complete_fixture() -> Self {
        Self::default()
    }

    pub fn failing_fixture() -> Self {
        Self {
            fail_producer: true,
        }
    }
}

enum Source {
    Network {
        upbit_websocket: String,
        bithumb_websocket: String,
    },
    Synthetic(SyntheticPublicSource),
}

pub struct Phase2Collector {
    config: FundingConfig,
    source: Source,
}

impl Phase2Collector {
    pub fn new(config: FundingConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            source: Source::Network {
                upbit_websocket: "wss://api.upbit.com/websocket/v1".to_owned(),
                bithumb_websocket: "wss://pubwss.bithumb.com/pub/ws".to_owned(),
            },
        })
    }

    /// Overrides only the two unauthenticated quote-reference WebSockets.
    /// Intended for deterministic loopback verification; derivative REST/WS
    /// endpoints continue to come from the validated configuration.
    pub fn with_public_quote_websockets(
        mut self,
        upbit_websocket: impl Into<String>,
        bithumb_websocket: impl Into<String>,
    ) -> Self {
        self.source = Source::Network {
            upbit_websocket: upbit_websocket.into(),
            bithumb_websocket: bithumb_websocket.into(),
        };
        self
    }

    pub fn with_source(config: FundingConfig, source: SyntheticPublicSource) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            source: Source::Synthetic(source),
        })
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<Phase2aReport> {
        fs::create_dir_all(&self.config.output_root)
            .with_context(|| format!("failed to create {}", self.config.output_root.display()))?;
        let storage = StorageConfig {
            output_root: self.config.output_root.clone(),
            batch_rows: self.config.batch_rows,
            flush_interval: Duration::from_millis(self.config.flush_interval_ms),
        };
        let market_router = PartitionRouter::open(storage.clone())?;
        let derivative_router = DerivativePartitionRouter::open(storage)?;
        let (market_tx, market_rx) = mpsc::channel(self.config.channel_capacity);
        let (derivative_tx, derivative_rx) = mpsc::channel(self.config.channel_capacity);
        let metrics = Arc::new(Metrics::default());

        // Durability consumers are alive before discovery or any producer starts.
        let market_store = tokio::spawn(market_storage_loop(
            market_router,
            market_rx,
            derivative_tx.clone(),
            Arc::clone(&metrics),
            Duration::from_millis(self.config.flush_interval_ms),
        ));
        let derivative_store = tokio::spawn(derivative_storage_loop(
            derivative_router,
            derivative_rx,
            Arc::clone(&metrics),
            Duration::from_millis(self.config.flush_interval_ms),
        ));

        let mut basis = ReportBasis::new(&self.config);
        // Startup discovery and every producer must observe the caller's
        // deadline/cancellation even before the supervision select is reached.
        let producer_shutdown = shutdown.child_token();
        let mut producers = JoinSet::new();
        let preparation = match self.source {
            Source::Synthetic(source) => {
                let result =
                    prepare_synthetic(&mut basis, &derivative_tx, &market_tx, &metrics).await;
                if result.is_ok() {
                    if source.fail_producer {
                        producers.spawn(async { Err(anyhow!("injected producer failure")) });
                    } else {
                        producers.spawn(wait_for_cancel(producer_shutdown.clone()));
                    }
                }
                result
            }
            Source::Network {
                upbit_websocket,
                bithumb_websocket,
            } => {
                let context = NetworkContext {
                    derivative_tx: derivative_tx.clone(),
                    market_tx: market_tx.clone(),
                    shutdown: producer_shutdown.clone(),
                    metrics: Arc::clone(&metrics),
                    upbit_websocket,
                    bithumb_websocket,
                };
                prepare_network(&self.config, &mut basis, context, &mut producers).await
            }
        };
        if let Err(error) = preparation {
            metrics.error(format!("startup failed: {error:#}"));
            producer_shutdown.cancel();
            while producers.join_next().await.is_some() {}
            drop(market_tx);
            let market_result = market_store.await.context("market storage task panicked")?;
            drop(derivative_tx);
            let derivative_result = derivative_store
                .await
                .context("derivative storage task panicked")?;
            market_result?;
            derivative_result?;
            let report_path = self.config.output_root.join("phase2a-report.json");
            let snapshot = metrics.snapshot();
            let failure_report = build_report(
                &self.config,
                basis,
                snapshot,
                finalized_arrow_paths(&self.config.output_root)?,
                report_path.clone(),
            );
            atomic_json(&report_path, &failure_report)?;
            return Err(error);
        }

        let mut producer_failure = None;
        tokio::select! {
            () = shutdown.cancelled() => {}
            result = producers.join_next() => {
                if shutdown.is_cancelled()
                    && matches!(result, Some(Ok(Ok(()))))
                {
                    // A child producer may observe the same caller cancellation
                    // just before this select observes the parent token.
                } else {
                let message = match result {
                    Some(Ok(Ok(()))) => "producer exited before shutdown".to_owned(),
                    Some(Ok(Err(error))) => format!("producer failed before shutdown: {error}"),
                    Some(Err(error)) => format!("producer task panicked before shutdown: {error}"),
                    None => "all producers exited before shutdown".to_owned(),
                };
                metrics.error(message.clone());
                producer_failure = Some(anyhow!(message));
                }
            }
        }
        producer_shutdown.cancel();
        while let Some(result) = producers.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => metrics.error(error.to_string()),
                Err(error) => metrics.error(format!("producer task panicked: {error}")),
            }
        }
        drop(market_tx);
        let mut terminal_error = producer_failure;
        match market_store.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                metrics.error(format!("market storage failed: {error}"));
                terminal_error = Some(anyhow!(error));
            }
            Err(error) => {
                metrics.error(format!("market storage task panicked: {error}"));
                terminal_error = Some(anyhow!(error));
            }
        }
        drop(derivative_tx);
        match derivative_store.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                metrics.error(format!("derivative storage failed: {error}"));
                terminal_error.get_or_insert_with(|| anyhow!(error));
            }
            Err(error) => {
                metrics.error(format!("derivative storage task panicked: {error}"));
                terminal_error.get_or_insert_with(|| anyhow!(error));
            }
        }

        // Completion is deliberately derived from this run's in-memory
        // producer metrics, never by scanning a pre-existing output tree.
        // This prevents an earlier Arrow hour from masking a failed startup.
        let completion_snapshot = metrics.snapshot();
        let missing_families: Vec<_> = [
            "instrument",
            "mark_index",
            "funding_estimate",
            "funding_settlement",
            "open_interest",
            "trader_ratio",
            "quote_conversion",
        ]
        .into_iter()
        .filter(|family| {
            completion_snapshot
                .per_family
                .get(*family)
                .is_none_or(|count| count.events == 0)
        })
        .collect();
        if !missing_families.is_empty() {
            let error = anyhow!(
                "run completion gate failed; missing event families: {}",
                missing_families.join(", ")
            );
            metrics.error(error.to_string());
            terminal_error.get_or_insert(error);
        }
        let missing_evidence = expected_run_evidence(&self.config, &basis)
            .into_iter()
            .filter(|identity| !completion_snapshot.event_identities.contains(identity))
            .collect::<Vec<_>>();
        if !missing_evidence.is_empty() {
            let error = anyhow!(
                "run completion gate failed; missing venue/symbol evidence: {}",
                missing_evidence.join(", ")
            );
            metrics.error(error.to_string());
            terminal_error.get_or_insert(error);
        }

        match md_storage::validate_path(&self.config.output_root) {
            Ok(validation) if validation.is_valid() => {}
            Ok(validation) => {
                let error = anyhow!(
                    "finalized Phase 2A Arrow validation failed: {:?}",
                    validation.errors
                );
                metrics.error(error.to_string());
                terminal_error.get_or_insert(error);
            }
            Err(error) => {
                metrics.error(format!("dataset validation failed: {error}"));
                terminal_error.get_or_insert_with(|| anyhow!(error));
            }
        }
        let finalized_paths = finalized_arrow_paths(&self.config.output_root)?;
        let snapshot = metrics.snapshot();
        let report_path = self.config.output_root.join("phase2a-report.json");
        let report = build_report(
            &self.config,
            basis,
            snapshot,
            finalized_paths,
            report_path.clone(),
        );
        atomic_json(&report_path, &report)?;
        if let Some(error) = terminal_error {
            Err(error)
        } else {
            Ok(report)
        }
    }
}

fn build_report(
    config: &FundingConfig,
    basis: ReportBasis,
    snapshot: MetricState,
    finalized_paths: Vec<PathBuf>,
    report_path: PathBuf,
) -> Phase2aReport {
    Phase2aReport {
        schema_version: 1,
        public_data_only: true,
        requested_symbols: basis.requested,
        common_mainnet_symbols: basis.mainnet,
        excluded_mainnet: basis.mainnet_excluded,
        common_testnet_symbols: basis.testnet,
        excluded_testnet: basis.testnet_excluded,
        unavailable_capabilities: BTreeMap::from([(
            "binance_top_trader_ratios".to_owned(),
            BINANCE_TOP_TRADER_CODE.to_owned(),
        )]),
        per_family: snapshot.per_family,
        reconnects: snapshot.reconnects,
        sequence_gaps: snapshot.sequence_gaps,
        parser_rejects: snapshot.parser_rejects,
        stale_intervals: snapshot.stale_intervals,
        scheduler: SchedulerSummary {
            rate_limit_blocks: snapshot.rate_limit_blocks,
            budget_rejections: snapshot.budget_rejections,
            abandoned_permits: snapshot.abandoned_permits,
            pending_response_completions: snapshot.pending_response_completions,
        },
        public_only_requests: PublicOnlyRequestSummary {
            requests: snapshot.requests,
            credential_headers: snapshot.credential_headers,
            authenticated_requests: snapshot.authenticated_requests,
            no_credentials_client_invariant: snapshot.credential_headers == 0
                && snapshot.authenticated_requests == 0,
        },
        health_errors: snapshot.errors,
        finalized_paths,
        output_root: config.output_root.clone(),
        report_path,
    }
}

#[derive(Default)]
struct ReportBasis {
    requested: Vec<String>,
    mainnet: Vec<String>,
    mainnet_excluded: Vec<ExcludedSymbol>,
    testnet: Vec<String>,
    testnet_excluded: Vec<ExcludedSymbol>,
}

impl ReportBasis {
    fn new(config: &FundingConfig) -> Self {
        Self {
            requested: config.assets.iter().map(|v| format!("{v}/USDT")).collect(),
            ..Self::default()
        }
    }

    fn apply(&mut self, environment: Environment, discovery: &DerivativeDiscovery) {
        let eligible = discovery
            .eligible
            .iter()
            .map(|v| symbol_name(&v.symbol))
            .collect();
        let excluded = discovery.excluded.iter().map(excluded).collect();
        match environment {
            Environment::Mainnet => {
                self.mainnet = eligible;
                self.mainnet_excluded = excluded;
            }
            Environment::Testnet => {
                self.testnet = eligible;
                self.testnet_excluded = excluded;
            }
        }
    }
}

struct NetworkContext {
    derivative_tx: mpsc::Sender<DerivativeEvent>,
    market_tx: mpsc::Sender<NormalizedEvent>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    upbit_websocket: String,
    bithumb_websocket: String,
}

#[derive(Clone)]
struct PublicHttpClient {
    inner: Client,
    metrics: Arc<Metrics>,
}

impl PublicHttpClient {
    fn build(metrics: Arc<Metrics>) -> Result<Self> {
        let inner = Client::builder()
            .user_agent("funding-app/0.1 public-only")
            .connect_timeout(NETWORK_TIMEOUT)
            .timeout(NETWORK_TIMEOUT)
            .build()?;
        Ok(Self { inner, metrics })
    }

    fn raw(&self) -> &Client {
        &self.inner
    }

    fn get(&self, url: Url) -> Result<reqwest::RequestBuilder> {
        self.get_with_headers(url, &HeaderMap::new())
    }

    fn get_with_headers(&self, url: Url, headers: &HeaderMap) -> Result<reqwest::RequestBuilder> {
        let violations = headers
            .keys()
            .filter(|name| {
                let name = name.as_str().to_ascii_lowercase();
                name == "authorization"
                    || name == "proxy-authorization"
                    || name.contains("api-key")
                    || name.contains("apikey")
            })
            .count();
        if violations != 0 {
            self.metrics.credential_violation(violations as u64);
            return Err(anyhow!(
                "public-only HTTP client rejected credential headers"
            ));
        }
        self.metrics.rest_attempt();
        Ok(self.inner.get(url).headers(headers.clone()))
    }
}

async fn prepare_network(
    config: &FundingConfig,
    basis: &mut ReportBasis,
    context: NetworkContext,
    producers: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let NetworkContext {
        derivative_tx,
        market_tx,
        shutdown,
        metrics,
        upbit_websocket,
        bithumb_websocket,
    } = context;
    let client = PublicHttpClient::build(Arc::clone(&metrics))?;
    let binance_scheduler = Arc::new(RestScheduler::binance_weighted(
        BINANCE_LIMIT_PER_MINUTE,
        config.poll.reserved_order_weight,
        0,
    )?);
    let bybit_scheduler = Arc::new(RestScheduler::bybit_endpoint(
        BYBIT_PUBLIC_REQUESTS_PER_SECOND,
    )?);
    metrics.register_schedulers(Arc::clone(&binance_scheduler), Arc::clone(&bybit_scheduler));
    let mainnet = scheduled_discovery(
        &client,
        config,
        Environment::Mainnet,
        &binance_scheduler,
        &bybit_scheduler,
        &shutdown,
        &metrics,
    )
    .await?;
    basis.apply(Environment::Mainnet, &mainnet);
    let funding_rules = funding_rule_store(&mainnet)?;
    match scheduled_discovery(
        &client,
        config,
        Environment::Testnet,
        &binance_scheduler,
        &bybit_scheduler,
        &shutdown,
        &metrics,
    )
    .await
    {
        Ok(testnet) => basis.apply(Environment::Testnet, &testnet),
        Err(error) => {
            basis.testnet_excluded = config
                .assets
                .iter()
                .map(|base| ExcludedSymbol {
                    symbol: format!("{base}/USDT"),
                    venue: None,
                    code: "TESTNET_DISCOVERY_FAILED".into(),
                    detail: error.to_string(),
                })
                .collect()
        }
    }
    for common in &mainnet.eligible {
        derivative_tx
            .send(DerivativeEvent::Instrument(Box::new(
                common.binance.clone(),
            )))
            .await?;
        derivative_tx
            .send(DerivativeEvent::Instrument(Box::new(common.bybit.clone())))
            .await?;
    }

    let poll_context = PollContext {
        client: client.clone(),
        binance_scheduler: Arc::clone(&binance_scheduler),
        bybit_scheduler: Arc::clone(&bybit_scheduler),
        tx: derivative_tx.clone(),
        shutdown: shutdown.clone(),
        metrics: Arc::clone(&metrics),
        settlements: Arc::new(Mutex::new(HashSet::new())),
        funding_rules: Arc::clone(&funding_rules),
    };
    spawn_instrument_refresh(
        InstrumentRefreshContext {
            config: config.clone(),
            client: client.clone(),
            tx: derivative_tx.clone(),
            shutdown: shutdown.clone(),
            metrics: Arc::clone(&metrics),
            schedulers: (Arc::clone(&binance_scheduler), Arc::clone(&bybit_scheduler)),
            funding_rules: Arc::clone(&funding_rules),
        },
        producers,
    );
    let eligible_count = mainnet.eligible.len();
    for (symbol_index, common) in mainnet.eligible.into_iter().enumerate() {
        spawn_derivative_ws(
            DerivativeWsSpec {
                symbol: common.symbol.clone(),
                endpoints: config.venues["binance_usdm"].mainnet.clone(),
                venue: AdapterId::BinanceUsdm,
                funding_rules: Arc::clone(&funding_rules),
            },
            derivative_tx.clone(),
            shutdown.clone(),
            Arc::clone(&metrics),
            producers,
        );
        spawn_derivative_ws(
            DerivativeWsSpec {
                symbol: common.symbol.clone(),
                endpoints: config.venues["bybit_linear"].mainnet.clone(),
                venue: AdapterId::BybitLinear,
                funding_rules: Arc::clone(&funding_rules),
            },
            derivative_tx.clone(),
            shutdown.clone(),
            Arc::clone(&metrics),
            producers,
        );
        spawn_market_ws(
            common.symbol.clone(),
            config.venues["binance_usdm"].mainnet.clone(),
            AdapterId::BinanceUsdm,
            market_tx.clone(),
            shutdown.clone(),
            Arc::clone(&metrics),
            producers,
        )?;
        spawn_market_ws(
            common.symbol.clone(),
            config.venues["bybit_linear"].mainnet.clone(),
            AdapterId::BybitLinear,
            market_tx.clone(),
            shutdown.clone(),
            Arc::clone(&metrics),
            producers,
        )?;
        spawn_rest_pollers(
            common.symbol,
            symbol_index,
            eligible_count,
            config,
            &poll_context,
            producers,
        );
    }
    spawn_quote_feeds(
        config,
        market_tx,
        shutdown.clone(),
        Arc::clone(&metrics),
        producers,
        &upbit_websocket,
        &bithumb_websocket,
    )?;
    let monitored_quote_venues = config.quote_conversions[0]
        .venues
        .iter()
        .filter_map(|venue| match venue.as_str() {
            "upbit_spot" => Some(AdapterId::UpbitSpot),
            "bithumb_spot" => Some(AdapterId::BithumbSpot),
            _ => None,
        })
        .collect();
    spawn_quote_freshness_monitor(
        shutdown,
        Arc::clone(&metrics),
        monitored_quote_venues,
        producers,
    );
    debug_assert_eq!(
        binance::top_trader_public_capability(),
        PublicCapability::UnavailableRequiresApiKey {
            code: BINANCE_TOP_TRADER_CODE
        }
    );
    Ok(())
}

struct InstrumentRefreshContext {
    config: FundingConfig,
    client: PublicHttpClient,
    tx: mpsc::Sender<DerivativeEvent>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    schedulers: (Arc<RestScheduler>, Arc<RestScheduler>),
    funding_rules: FundingRuleStore,
}

fn spawn_instrument_refresh(
    context: InstrumentRefreshContext,
    producers: &mut JoinSet<Result<()>>,
) {
    let InstrumentRefreshContext {
        config,
        client,
        tx,
        shutdown,
        metrics,
        schedulers: (binance_scheduler, bybit_scheduler),
        funding_rules,
    } = context;
    producers.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(
            config.poll.instrument_secs.max(900),
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await; // Startup discovery already emitted the first instrument snapshot.
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                _ = interval.tick() => match scheduled_discovery(&client, &config, Environment::Mainnet, &binance_scheduler, &bybit_scheduler, &shutdown, &metrics).await {
                    Ok(discovery) => {
                        update_funding_rules(&funding_rules, &discovery)?;
                        for common in discovery.eligible {
                        if tx.send(DerivativeEvent::Instrument(Box::new(common.binance))).await.is_err()
                            || tx.send(DerivativeEvent::Instrument(Box::new(common.bybit))).await.is_err()
                        {
                            return Ok(());
                        }
                        }
                    },
                    Err(error) => metrics.error(format!("instrument refresh failed: {error}")),
                }
            }
        }
    });
}

async fn scheduled_discovery(
    client: &PublicHttpClient,
    config: &FundingConfig,
    environment: Environment,
    binance_scheduler: &RestScheduler,
    bybit_scheduler: &RestScheduler,
    shutdown: &CancellationToken,
    metrics: &Metrics,
) -> Result<DerivativeDiscovery> {
    let observer = ScheduledDiscoveryObserver {
        binance_scheduler,
        bybit_scheduler,
        metrics,
        pending: Mutex::new(None),
    };
    let result = tokio::select! {
        () = shutdown.cancelled() => Err(anyhow!("derivative discovery cancelled")),
        result = tokio::time::timeout(NETWORK_TIMEOUT, discover_derivatives_observed(client.raw(), config, environment, &observer)) => match result {
            Ok(value) => value.map_err(Into::into),
            Err(_) => Err(anyhow!("derivative discovery timed out")),
        }
    };
    if result.is_err() {
        observer.abandon_pending()?;
    }
    result
}

struct PendingDiscoveryRequest {
    adapter: AdapterId,
    url: String,
    permit: Permit,
}

struct ScheduledDiscoveryObserver<'a> {
    binance_scheduler: &'a RestScheduler,
    bybit_scheduler: &'a RestScheduler,
    metrics: &'a Metrics,
    pending: Mutex<Option<PendingDiscoveryRequest>>,
}

impl ScheduledDiscoveryObserver<'_> {
    fn scheduler(&self, adapter: AdapterId) -> Result<&RestScheduler, String> {
        match adapter {
            AdapterId::BinanceUsdm => Ok(self.binance_scheduler),
            AdapterId::BybitLinear => Ok(self.bybit_scheduler),
            _ => Err(format!("unsupported discovery adapter {adapter:?}")),
        }
    }

    fn take_pending(
        &self,
        adapter: AdapterId,
        url: &Url,
    ) -> Result<PendingDiscoveryRequest, String> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| "discovery permit lock poisoned".to_owned())?
            .take()
            .ok_or_else(|| "discovery request has no pending permit".to_owned())?;
        if pending.adapter != adapter || pending.url != url.as_str() {
            return Err("discovery response does not match its pending request".to_owned());
        }
        Ok(pending)
    }

    fn abandon_pending(&self) -> Result<(), anyhow::Error> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| anyhow!("discovery permit lock poisoned"))?
            .take();
        if let Some(pending) = pending {
            self.scheduler(pending.adapter)
                .map_err(|message| anyhow!(message))?
                .abandon_permit(&pending.permit)?;
            self.metrics.abandoned();
        }
        Ok(())
    }
}

impl DiscoveryRequestObserver for ScheduledDiscoveryObserver<'_> {
    fn before_request(&self, adapter: AdapterId, url: &Url) -> Result<(), String> {
        let scheduler = self.scheduler(adapter)?;
        let permit = scheduler
            .acquire(RequestClass::MarketData, 1, Instant::now())
            .map_err(|error| {
                self.metrics.budget_reject();
                error.to_string()
            })?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| "discovery permit lock poisoned".to_owned())?;
        if pending.is_some() {
            scheduler
                .abandon_permit(&permit)
                .map_err(|error| error.to_string())?;
            self.metrics.abandoned();
            return Err("discovery requests must be serialized".to_owned());
        }
        *pending = Some(PendingDiscoveryRequest {
            adapter,
            url: url.as_str().to_owned(),
            permit,
        });
        self.metrics.rest_attempt();
        Ok(())
    }

    fn complete_request(
        &self,
        adapter: AdapterId,
        url: &Url,
        headers: &HeaderMap,
        status: reqwest::StatusCode,
        bybit_ret_code: Option<i64>,
    ) -> Result<(), String> {
        let pending = self.take_pending(adapter, url)?;
        let scheduler = self.scheduler(adapter)?;
        match scheduler.record_response_at(
            &pending.permit,
            headers,
            status,
            bybit_ret_code,
            Instant::now(),
            now_ms(),
        ) {
            Ok(Some(_)) => {
                self.metrics.rate_block();
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => {
                if scheduler.abandon_permit(&pending.permit).is_ok() {
                    self.metrics.abandoned();
                }
                Err(error.to_string())
            }
        }
    }

    fn abandon_request(&self, adapter: AdapterId, url: &Url) -> Result<(), String> {
        let pending = self.take_pending(adapter, url)?;
        self.scheduler(adapter)?
            .abandon_permit(&pending.permit)
            .map_err(|error| error.to_string())?;
        self.metrics.abandoned();
        Ok(())
    }
}

struct DerivativeWsSpec {
    symbol: CanonicalSymbol,
    endpoints: EndpointSet,
    venue: AdapterId,
    funding_rules: FundingRuleStore,
}

fn spawn_derivative_ws(
    spec: DerivativeWsSpec,
    tx: mpsc::Sender<DerivativeEvent>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    producers: &mut JoinSet<Result<()>>,
) {
    producers.spawn(async move {
        let DerivativeWsSpec {
            symbol,
            endpoints,
            venue,
            funding_rules,
        } = spec;
        let source_symbol = format!("{}{}", symbol.base, symbol.quote);
        let mut reconnect_failures = 0_u32;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            metrics.ws_connect_attempt();
            let connected = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                result = tokio::time::timeout(NETWORK_TIMEOUT, connect_async(&endpoints.public_websocket_url)) => match result {
                    Ok(result) => result,
                    Err(_) => {
                        metrics.reconnect();
                        if ws_reconnect_pause(&shutdown, &mut reconnect_failures, false).await {
                            return Ok(());
                        }
                        continue;
                    }
                }
            };
            let (mut socket, _) = match connected {
                Ok(value) => value,
                Err(error) => {
                    metrics.reconnect();
                    tracing::debug!(?venue, %error, "public derivative websocket connect failed");
                    if ws_reconnect_pause(&shutdown, &mut reconnect_failures, false).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let subscription = match venue {
                AdapterId::BinanceUsdm => json!({"method":"SUBSCRIBE","params":[format!("{}@markPrice@1s", source_symbol.to_ascii_lowercase())],"id":1}),
                AdapterId::BybitLinear => json!({"op":"subscribe","args":[format!("tickers.{source_symbol}")]}),
                _ => unreachable!(),
            };
            metrics.ws_subscription_attempt();
            let subscribed = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                result = tokio::time::timeout(NETWORK_TIMEOUT, socket.send(Message::Text(subscription.to_string().into()))) => matches!(result, Ok(Ok(()))),
            };
            if !subscribed {
                metrics.reconnect();
                if ws_reconnect_pause(&shutdown, &mut reconnect_failures, false).await {
                    return Ok(());
                }
                continue;
            }
            let mut bybit_parser = BybitTickerParser::new(symbol.clone());
            let mut reconnect = false;
            let connected_at = Instant::now();
            let mut healthy_events = 0_u32;
            while !reconnect {
                tokio::select! {
                    () = shutdown.cancelled() => {
                        let _ = tokio::time::timeout(NETWORK_TIMEOUT, socket.close(None)).await;
                        return Ok(());
                    },
                    frame = tokio::time::timeout(WS_READ_TIMEOUT, socket.next()) => match frame {
                        Err(_) => reconnect = true,
                        Ok(frame) => match frame {
                        Some(Ok(Message::Text(text))) => {
                            let recv = now_us();
                            let mut bytes = text.as_bytes().to_vec();
                            let parsed = match venue {
                                AdapterId::BinanceUsdm => binance::parse_mark_funding_with_rules(
                                    &mut bytes,
                                    recv,
                                    current_funding_rules(&funding_rules, venue, &symbol),
                                ),
                                AdapterId::BybitLinear => bybit_parser.parse(&mut bytes, recv),
                                _ => unreachable!(),
                            };
                            match parsed {
                                Ok(events) => for event in events {
                                    let sent = tokio::select! {
                                        () = shutdown.cancelled() => return Ok(()),
                                        result = tokio::time::timeout(NETWORK_TIMEOUT, tx.send(event)) => matches!(result, Ok(Ok(()))),
                                    };
                                    if !sent {
                                        return Err(anyhow!("derivative event channel is unavailable or blocked"));
                                    }
                                    healthy_events = healthy_events.saturating_add(1);
                                },
                                Err(error) => {
                                    // Subscription acknowledgements are control frames, not rejects.
                                    if !text.contains("\"success\"") && !text.contains("\"ret_msg\"") && !text.contains("\"result\":null") {
                                        metrics.parser_reject();
                                        tracing::debug!(?venue, %error, "derivative websocket frame rejected");
                                        if venue == AdapterId::BybitLinear && is_derivative_sequence_gap(&error) {
                                            metrics.sequence_gap();
                                            reconnect = true;
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(value))) => {
                            if !matches!(tokio::time::timeout(NETWORK_TIMEOUT, socket.send(Message::Pong(value))).await, Ok(Ok(()))) {
                                reconnect = true;
                            }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => reconnect = true,
                        _ => {}
                    }}
                }
            }
            bybit_parser.reset();
            metrics.reconnect();
            let healthy = healthy_events >= 3 || connected_at.elapsed() >= Duration::from_secs(30);
            if ws_reconnect_pause(&shutdown, &mut reconnect_failures, healthy).await {
                return Ok(());
            }
        }
    });
}

fn is_derivative_sequence_gap(error: &binance::DerivativeParseError) -> bool {
    matches!(
        error,
        binance::DerivativeParseError::SnapshotRequired
            | binance::DerivativeParseError::SequenceRegression { .. }
    )
}

async fn ws_reconnect_pause(
    shutdown: &CancellationToken,
    failures: &mut u32,
    healthy: bool,
) -> bool {
    if healthy {
        *failures = 0;
    } else {
        *failures = failures.saturating_add(1);
    }
    let delay = ws_backoff_delay(*failures);
    tokio::select! {
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

fn ws_backoff_delay(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(6);
    let base_ms = 100_u64.saturating_mul(1_u64 << shift).min(5_000);
    let jitter_ms = (u64::from(failures).wrapping_mul(97)) % 101;
    Duration::from_millis((base_ms + jitter_ms).min(5_000))
}

fn spawn_market_ws(
    symbol: CanonicalSymbol,
    endpoints: EndpointSet,
    venue: AdapterId,
    tx: mpsc::Sender<NormalizedEvent>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    producers: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let (url, subscription, parser): (String, String, Arc<dyn FrameParser>) = match venue {
        AdapterId::BinanceUsdm => (
            build_combined_stream_url(&endpoints.public_websocket_url, std::slice::from_ref(&symbol))?.to_string(),
            String::new(), Arc::new(BinanceUsdmParser),
        ),
        AdapterId::BybitLinear => (
            endpoints.public_websocket_url,
            json!({"op":"subscribe","args":[format!("orderbook.50.{}{}", symbol.base, symbol.quote), format!("publicTrade.{}{}", symbol.base, symbol.quote)]}).to_string(),
            Arc::new(BybitLinearParser::new(symbol)),
        ),
        _ => unreachable!(),
    };
    let runtime = AdapterRuntime::new(
        venue,
        url,
        subscription,
        Duration::from_secs(23 * 60 * 60),
        parser,
    );
    producers.spawn(async move {
        run_supervised(runtime, tx, shutdown, metrics)
            .await
            .map_err(Into::into)
    });
    Ok(())
}

fn spawn_quote_feeds(
    config: &FundingConfig,
    tx: mpsc::Sender<NormalizedEvent>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    producers: &mut JoinSet<Result<()>>,
    upbit_websocket: &str,
    bithumb_websocket: &str,
) -> Result<()> {
    for conversion in &config.quote_conversions {
        let symbol = CanonicalSymbol::new(&conversion.base, &conversion.quote);
        for venue_name in &conversion.venues {
            let (id, url, parser): (_, _, Arc<dyn FrameParser>) = match venue_name.as_str() {
                "upbit_spot" => (
                    AdapterId::UpbitSpot,
                    upbit_websocket.to_owned(),
                    Arc::new(UpbitParser),
                ),
                "bithumb_spot" => (
                    AdapterId::BithumbSpot,
                    bithumb_websocket.to_owned(),
                    Arc::new(BithumbParser),
                ),
                _ => continue,
            };
            let subscription =
                build_subscription(id, std::slice::from_ref(&symbol), Uuid::now_v7())?;
            let runtime = AdapterRuntime::new(
                id,
                url,
                subscription,
                Duration::from_secs(23 * 60 * 60),
                parser,
            );
            let tx = tx.clone();
            let shutdown = shutdown.clone();
            let metrics = Arc::clone(&metrics);
            producers.spawn(async move {
                run_supervised(runtime, tx, shutdown, metrics)
                    .await
                    .map_err(Into::into)
            });
        }
    }
    Ok(())
}

fn spawn_quote_freshness_monitor(
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    venues: Vec<AdapterId>,
    producers: &mut JoinSet<Result<()>>,
) {
    producers.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                _ = interval.tick() => metrics.check_quote_staleness(now_us(), &venues),
            }
        }
    });
}

#[derive(Clone)]
struct PollContext {
    client: PublicHttpClient,
    binance_scheduler: Arc<RestScheduler>,
    bybit_scheduler: Arc<RestScheduler>,
    tx: mpsc::Sender<DerivativeEvent>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    settlements: Arc<Mutex<HashSet<(AdapterId, CanonicalSymbol, i64)>>>,
    funding_rules: FundingRuleStore,
}

fn spawn_rest_pollers(
    symbol: CanonicalSymbol,
    symbol_index: usize,
    symbol_count: usize,
    config: &FundingConfig,
    context: &PollContext,
    producers: &mut JoinSet<Result<()>>,
) {
    let binance = config.venues["binance_usdm"].mainnet.rest_url.clone();
    let bybit = config.venues["bybit_linear"].mainnet.rest_url.clone();
    let oi_every = Duration::from_secs(config.poll.open_interest_secs.max(5));
    let ratio_every = Duration::from_secs(config.poll.trader_ratio_secs.max(300));
    let funding_every = Duration::from_secs(config.poll.funding_metadata_secs.max(900));
    let poll_specs = vec![
        PollSpec::new(
            AdapterId::BinanceUsdm,
            RestKind::OpenInterest,
            oi_every,
            1,
            startup_delay(symbol_index, 0, 2, BINANCE_LIMIT_PER_MINUTE / 60),
        ),
        PollSpec::new(
            AdapterId::BybitLinear,
            RestKind::OpenInterest,
            oi_every,
            1,
            startup_delay(symbol_index, 0, 3, BYBIT_PUBLIC_REQUESTS_PER_SECOND),
        ),
        PollSpec::new(
            AdapterId::BybitLinear,
            RestKind::TraderRatio,
            ratio_every,
            1,
            startup_delay(symbol_index, 1, 3, BYBIT_PUBLIC_REQUESTS_PER_SECOND),
        ),
        PollSpec::new(
            AdapterId::BinanceUsdm,
            RestKind::FundingHistory,
            funding_every,
            1,
            startup_delay(symbol_index, 1, 2, BINANCE_LIMIT_PER_MINUTE / 60),
        ),
        PollSpec::new(
            AdapterId::BybitLinear,
            RestKind::FundingHistory,
            funding_every,
            1,
            startup_delay(symbol_index, 2, 3, BYBIT_PUBLIC_REQUESTS_PER_SECOND),
        ),
    ];
    debug_assert!(symbol_index < symbol_count);
    for spec in poll_specs {
        let symbol = symbol.clone();
        let context = context.clone();
        let base = if spec.venue == AdapterId::BinanceUsdm {
            binance.clone()
        } else {
            bybit.clone()
        };
        let scheduler = if spec.venue == AdapterId::BinanceUsdm {
            Arc::clone(&context.binance_scheduler)
        } else {
            Arc::clone(&context.bybit_scheduler)
        };
        producers.spawn(async move { poll_loop(symbol, base, spec, scheduler, context).await });
    }
}

#[derive(Clone, Copy)]
enum RestKind {
    OpenInterest,
    TraderRatio,
    FundingHistory,
}

#[derive(Clone, Copy)]
struct PollSpec {
    venue: AdapterId,
    kind: RestKind,
    every: Duration,
    weight: u32,
    startup_delay: Duration,
}
impl PollSpec {
    fn new(
        venue: AdapterId,
        kind: RestKind,
        every: Duration,
        weight: u32,
        startup_delay: Duration,
    ) -> Self {
        Self {
            venue,
            kind,
            every,
            weight,
            startup_delay,
        }
    }
}

async fn poll_loop(
    symbol: CanonicalSymbol,
    base: String,
    spec: PollSpec,
    scheduler: Arc<RestScheduler>,
    context: PollContext,
) -> Result<()> {
    tokio::select! {
        () = context.shutdown.cancelled() => return Ok(()),
        () = tokio::time::sleep(spec.startup_delay) => {}
    }
    let mut failure_streak = 0_u32;
    loop {
        let succeeded = match poll_once(&symbol, &base, spec, &scheduler, &context).await {
            Ok(events) => {
                for event in events {
                    if !context.accept_settlement(&event) {
                        continue;
                    }
                    if context.tx.send(event).await.is_err() {
                        return Ok(());
                    }
                }
                failure_streak = 0;
                true
            }
            Err(error) => {
                if !is_scheduler_wait(&error) {
                    context.metrics.error(error.to_string());
                    failure_streak = failure_streak.saturating_add(1);
                }
                false
            }
        };
        let next_delay = if succeeded {
            spec.every
        } else {
            retry_delay(&scheduler, failure_streak)
        };
        tokio::select! {
            () = context.shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(next_delay) => {}
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("public REST response is rate-limited until {until:?}")]
struct RateLimitedPoll {
    until: Option<Instant>,
}

fn is_scheduler_wait(error: &anyhow::Error) -> bool {
    error.downcast_ref::<BudgetError>().is_some()
        || error.downcast_ref::<RateLimitedPoll>().is_some()
}

fn retry_delay(scheduler: &RestScheduler, failure_streak: u32) -> Duration {
    let now = Instant::now();
    if let Some(until) = scheduler.snapshot(now).blocked_until {
        return until
            .saturating_duration_since(now)
            .max(Duration::from_millis(10));
    }
    let snapshot = scheduler.snapshot(now);
    if failure_streak == 0 {
        return match snapshot.mode {
            SchedulerMode::BinanceWeightedMinute => Duration::from_secs(60),
            SchedulerMode::BybitEndpointRollingSecond => Duration::from_secs(1),
        };
    }
    let shift = failure_streak.saturating_sub(1).min(5);
    let base_ms = 250_u64.saturating_mul(1_u64 << shift).min(8_000);
    let jitter_ms = (u64::from(failure_streak).wrapping_mul(137)) % 251;
    Duration::from_millis((base_ms + jitter_ms).min(10_000))
}

fn startup_delay(
    symbol_index: usize,
    family_index: usize,
    families_per_symbol: usize,
    requests_per_second: u32,
) -> Duration {
    let ordinal = symbol_index
        .saturating_mul(families_per_symbol)
        .saturating_add(family_index);
    let millis = u64::try_from(ordinal)
        .unwrap_or(u64::MAX)
        .saturating_mul(1_000)
        / u64::from(requests_per_second.max(1));
    Duration::from_millis(millis.min(29_999))
}

impl PollContext {
    fn accept_settlement(&self, event: &DerivativeEvent) -> bool {
        let DerivativeEvent::FundingSettlement(settlement) = event else {
            return true;
        };
        self.settlements
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((
                settlement.meta.venue,
                settlement.meta.symbol.clone(),
                settlement.settlement_ts_us,
            ))
    }
}

async fn poll_once(
    symbol: &CanonicalSymbol,
    base: &str,
    spec: PollSpec,
    scheduler: &RestScheduler,
    context: &PollContext,
) -> Result<Vec<DerivativeEvent>> {
    let permit = match scheduler.acquire(RequestClass::MarketData, spec.weight, Instant::now()) {
        Ok(permit) => permit,
        Err(error) => {
            context.metrics.budget_reject();
            return Err(error.into());
        }
    };
    let venue_symbol = format!("{}{}", symbol.base, symbol.quote);
    let url = rest_url(base, spec.venue, spec.kind, &venue_symbol)?;
    let request = context.client.get(url)?;
    let response = tokio::select! {
        () = context.shutdown.cancelled() => {
            scheduler.abandon_permit(&permit)?;
            context.metrics.abandoned();
            return Err(anyhow!("public REST request cancelled"));
        }
        response = tokio::time::timeout(NETWORK_TIMEOUT, request.send()) => match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                scheduler.abandon_permit(&permit)?;
                context.metrics.abandoned();
                return Err(error.into());
            }
            Err(_) => {
                scheduler.abandon_permit(&permit)?;
                context.metrics.abandoned();
                return Err(anyhow!("public REST request timed out"));
            }
        }
    };
    let status = response.status();
    let headers = response.headers().clone();
    if !status.is_success() {
        let signal =
            complete_response(scheduler, &permit, &headers, status, None, &context.metrics)?;
        if signal {
            context.metrics.rate_block();
            return Err(RateLimitedPoll {
                until: scheduler.snapshot(Instant::now()).blocked_until,
            }
            .into());
        }
        bail!("public REST response rejected for {venue_symbol}: status={status}");
    }
    let bytes = tokio::select! {
        () = context.shutdown.cancelled() => {
            scheduler.abandon_permit(&permit)?;
            context.metrics.abandoned();
            return Err(anyhow!("public REST body read cancelled"));
        }
        bytes = tokio::time::timeout(NETWORK_TIMEOUT, response.bytes()) => match bytes {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                scheduler.abandon_permit(&permit)?;
                context.metrics.abandoned();
                return Err(error.into());
            }
            Err(_) => {
                scheduler.abandon_permit(&permit)?;
                context.metrics.abandoned();
                return Err(anyhow!("public REST body read timed out"));
            }
        }
    };
    let mut payload = bytes.to_vec();
    let bybit_ret_code = if spec.venue == AdapterId::BybitLinear {
        serde_json::from_slice::<serde_json::Value>(&payload)
            .ok()
            .and_then(|value| value.get("retCode").and_then(serde_json::Value::as_i64))
    } else {
        None
    };
    if spec.venue == AdapterId::BybitLinear && bybit_ret_code.is_none() {
        scheduler.abandon_permit(&permit)?;
        context.metrics.abandoned();
        return Err(anyhow!(
            "Bybit successful REST response has no integer retCode"
        ));
    }
    if bybit_ret_code.is_some_and(|code| code != 0) {
        let signal = complete_response(
            scheduler,
            &permit,
            &headers,
            status,
            bybit_ret_code,
            &context.metrics,
        )?;
        if signal {
            context.metrics.rate_block();
        }
        return Err(RateLimitedPoll {
            until: scheduler.snapshot(Instant::now()).blocked_until,
        }
        .into());
    }
    let recv = now_us();
    let parsed = match (spec.venue, spec.kind) {
        (AdapterId::BinanceUsdm, RestKind::OpenInterest) => {
            binance::parse_open_interest(&mut payload, recv)
        }
        (AdapterId::BinanceUsdm, RestKind::FundingHistory) => {
            binance::parse_funding_history_with_rules(
                &mut payload,
                recv,
                FundingHistoryRules {
                    schedule: FundingSchedule::new(vec![EffectiveFundingRule {
                        effective_from_ts_us: 0,
                        rules: current_funding_rules(&context.funding_rules, spec.venue, symbol),
                    }])?,
                    legacy_rate_type: LegacyRateTypePolicy::AcceptMissing,
                },
            )
        }
        (AdapterId::BybitLinear, RestKind::OpenInterest) => {
            bybit::parse_open_interest(&mut payload, recv)
        }
        (AdapterId::BybitLinear, RestKind::TraderRatio) => {
            bybit::parse_long_short_ratio(&mut payload, recv)
        }
        (AdapterId::BybitLinear, RestKind::FundingHistory) => {
            bybit::parse_funding_history_with_rules(
                &mut payload,
                recv,
                current_funding_rules(&context.funding_rules, spec.venue, symbol),
            )
        }
        _ => return Err(anyhow!("unsupported public REST poll")),
    };
    let events = match parsed {
        Ok(events) => events,
        Err(error) => {
            scheduler.abandon_permit(&permit)?;
            context.metrics.abandoned();
            return Err(error.into());
        }
    };
    let signal = complete_response(
        scheduler,
        &permit,
        &headers,
        status,
        bybit_ret_code,
        &context.metrics,
    )?;
    if signal {
        context.metrics.rate_block();
    }
    Ok(events)
}

fn complete_response(
    scheduler: &RestScheduler,
    permit: &md_exchanges::derivatives::scheduler::Permit,
    headers: &HeaderMap,
    status: reqwest::StatusCode,
    bybit_ret_code: Option<i64>,
    metrics: &Metrics,
) -> Result<bool> {
    match scheduler.record_response_at(
        permit,
        headers,
        status,
        bybit_ret_code,
        Instant::now(),
        now_ms(),
    ) {
        Ok(signal) => Ok(matches!(
            signal,
            Some(HealthSignal::RateLimited { .. } | HealthSignal::IpBanned { .. })
        )),
        Err(error) => {
            if scheduler.abandon_permit(permit).is_ok() {
                metrics.abandoned();
            }
            Err(error.into())
        }
    }
}

fn rest_url(base: &str, venue: AdapterId, kind: RestKind, symbol: &str) -> Result<url::Url> {
    let mut url = url::Url::parse(base)?;
    let path = match (venue, kind) {
        (AdapterId::BinanceUsdm, RestKind::OpenInterest) => "/fapi/v1/openInterest",
        (AdapterId::BinanceUsdm, RestKind::FundingHistory) => "/fapi/v1/fundingRate",
        (AdapterId::BybitLinear, RestKind::OpenInterest) => "/v5/market/open-interest",
        (AdapterId::BybitLinear, RestKind::TraderRatio) => "/v5/market/account-ratio",
        (AdapterId::BybitLinear, RestKind::FundingHistory) => "/v5/market/funding/history",
        _ => bail!("unsupported public REST route"),
    };
    url.set_path(path);
    url.set_query(None);
    let mut query = url.query_pairs_mut();
    query.append_pair("symbol", symbol);
    match (venue, kind) {
        (AdapterId::BinanceUsdm, RestKind::FundingHistory) => {
            query.append_pair("limit", "1");
        }
        (AdapterId::BybitLinear, RestKind::OpenInterest) => {
            query
                .append_pair("category", "linear")
                .append_pair("intervalTime", "5min")
                .append_pair("limit", "1");
        }
        (AdapterId::BybitLinear, RestKind::TraderRatio) => {
            query
                .append_pair("category", "linear")
                .append_pair("period", "5min")
                .append_pair("limit", "1");
        }
        (AdapterId::BybitLinear, RestKind::FundingHistory) => {
            query
                .append_pair("category", "linear")
                .append_pair("limit", "1");
        }
        _ => {}
    }
    drop(query);
    Ok(url)
}

async fn market_storage_loop(
    mut router: PartitionRouter,
    mut rx: mpsc::Receiver<NormalizedEvent>,
    derivative_tx: mpsc::Sender<DerivativeEvent>,
    metrics: Arc<Metrics>,
    flush_every: Duration,
) -> Result<()> {
    let mut flush = tokio::time::interval(flush_every);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Some(event) => {
                    if let NormalizedEvent::Book(book) = &event
                        && book.meta.symbol.base == "USDT" && book.meta.symbol.quote == "KRW"
                    {
                        for conversion in quote_conversions(book, &metrics) {
                            derivative_tx.send(conversion).await.map_err(|_| anyhow!("derivative storage channel closed"))?;
                        }
                    }
                    router.push(event).await?;
                }
                None => break,
            },
            _ = flush.tick() => router.flush_due(Instant::now()).await?,
        }
    }
    router.shutdown().await?;
    Ok(())
}

async fn derivative_storage_loop(
    mut router: DerivativePartitionRouter,
    mut rx: mpsc::Receiver<DerivativeEvent>,
    metrics: Arc<Metrics>,
    flush_every: Duration,
) -> Result<()> {
    let mut flush = tokio::time::interval(flush_every);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Some(event) => { metrics.event(&event); router.push(event).await?; }
                None => break,
            },
            _ = flush.tick() => router.flush_due(Instant::now()).await?,
        }
    }
    router.shutdown().await?;
    Ok(())
}

fn quote_conversions(book: &BookSnapshot, metrics: &Metrics) -> Vec<DerivativeEvent> {
    let age = now_us().saturating_sub(book.meta.local_recv_ts_us);
    if age > i64::try_from(QUOTE_STALE_AFTER.as_micros()).unwrap_or(i64::MAX) {
        metrics.stale();
        return Vec::new();
    }
    metrics.quote_received(book.meta.adapter, book.meta.local_recv_ts_us);
    let mut result = Vec::with_capacity(2);
    for (side, level) in [
        (QuoteSide::Bid, book.bids.first()),
        (QuoteSide::Ask, book.asks.first()),
    ] {
        if let Some(level) = level {
            result.push(DerivativeEvent::QuoteConversion(QuoteConversionSnapshot {
                meta: DerivativeMeta {
                    schema_version: 1,
                    event_id: Uuid::now_v7(),
                    venue: book.meta.adapter,
                    symbol: book.meta.symbol.clone(),
                    venue_symbol: book.meta.source_symbol.clone(),
                    source_ts_us: book.meta.exchange_event_ts_us,
                    source_ts_precision: book.meta.event_ts_precision,
                    local_recv_ts_us: book.meta.local_recv_ts_us,
                },
                side,
                price: level.price,
                executable_quantity: level.quantity,
            }));
        }
    }
    if result.len() != 2 {
        metrics.stale();
    }
    result
}

async fn prepare_synthetic(
    basis: &mut ReportBasis,
    derivative_tx: &mpsc::Sender<DerivativeEvent>,
    market_tx: &mpsc::Sender<NormalizedEvent>,
    metrics: &Metrics,
) -> Result<()> {
    basis.mainnet = vec!["BTC/USDT".into(), "ETH/USDT".into()];
    basis.testnet = vec!["BTC/USDT".into()];
    basis.testnet_excluded.push(ExcludedSymbol {
        symbol: "ETH/USDT".into(),
        venue: None,
        code: "TESTNET_UNAVAILABLE".into(),
        detail: "synthetic testnet omission".into(),
    });
    let ts = now_us();
    for base in ["BTC", "ETH"] {
        for venue in [AdapterId::BinanceUsdm, AdapterId::BybitLinear] {
            derivative_tx
                .send(DerivativeEvent::Instrument(Box::new(test_instrument_for(
                    venue, base, ts,
                ))))
                .await?;
            for event in synthetic_derivative_events(venue, base, ts) {
                derivative_tx.send(event).await?;
            }
        }
    }
    for venue in [AdapterId::UpbitSpot, AdapterId::BithumbSpot] {
        market_tx.send(test_quote_book(venue, ts)).await?;
    }
    metrics.reconnect();
    metrics.sequence_gap();
    metrics.abandoned();
    Ok(())
}

fn synthetic_derivative_events(venue: AdapterId, base: &str, ts: i64) -> Vec<DerivativeEvent> {
    let meta = test_meta(venue, base, "USDT", ts);
    let mut events = vec![
        DerivativeEvent::MarkIndex(MarkIndexSnapshot {
            meta: meta.clone(),
            mark_price: 100_000_000_000_000_000_000,
            index_price: 100_000_000_000_000_000_000,
        }),
        DerivativeEvent::FundingEstimate(FundingEstimate {
            meta: fresh(meta.clone()),
            rate: 100_000_000_000_000,
            rate_kind: FundingRateKind::IndicativeNext,
            basis: FundingBasis::MarkNotional,
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            next_funding_ts_us: ts + 28_800_000_000,
        }),
        DerivativeEvent::FundingSettlement(FundingSettlement {
            meta: fresh(meta.clone()),
            rate: 90_000_000_000_000,
            rate_kind: FundingRateKind::SettledActual,
            basis: FundingBasis::MarkNotional,
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            settlement_ts_us: ts,
        }),
        DerivativeEvent::OpenInterest(OpenInterestSnapshot {
            meta: fresh(meta.clone()),
            open_interest: 10_000_000_000_000_000_000,
            unit: OpenInterestUnit::BaseAsset,
            quote_notional: Some(1_000_000_000_000_000_000_000),
        }),
    ];
    if venue == AdapterId::BybitLinear {
        events.push(DerivativeEvent::TraderRatio(TraderRatioSnapshot {
            meta: fresh(meta),
            metric_kind: TraderMetricKind::BybitLongShortRatio,
            long_ratio: 600_000_000_000_000_000,
            short_ratio: 400_000_000_000_000_000,
            long_short_ratio: 1_500_000_000_000_000_000,
        }));
    }
    events
}

async fn wait_for_cancel(shutdown: CancellationToken) -> Result<()> {
    shutdown.cancelled().await;
    Ok(())
}

fn test_instrument_for(venue: AdapterId, base: &str, ts: i64) -> InstrumentSpec {
    InstrumentSpec {
        meta: test_meta(venue, base, "USDT", ts),
        contract_kind: ContractKind::Perpetual,
        settlement_asset: "USDT".into(),
        contract_multiplier: 1_000_000_000_000_000_000,
        tick_size: 100_000_000_000_000,
        quantity_step: 1_000_000_000_000_000,
        min_quantity: 1_000_000_000_000_000,
        max_quantity: Some(1_000_000_000_000_000_000),
        min_notional: 5_000_000_000_000_000_000,
        funding_interval_secs: 28_800,
        funding_interval_provenance: FundingIntervalProvenance::VenuePayload,
        funding_rate_floor: Some(-10_000_000_000_000_000),
        funding_rate_cap: Some(10_000_000_000_000_000),
        funding_rate_bounds_provenance: FundingRateBoundsProvenance::VenueFundingInfo,
        price_lower_bound: Some(1),
        price_upper_bound: Some(1_000_000_000_000_000_000_000_000),
        supported_position_modes: vec![PositionMode::OneWay, PositionMode::Hedge],
        supported_account_modes: vec![AccountMode::Classic],
    }
}

fn test_meta(venue: AdapterId, base: &str, quote: &str, ts: i64) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol: CanonicalSymbol::new(base, quote),
        venue_symbol: format!("{base}{quote}"),
        source_ts_us: Some(ts),
        source_ts_precision: TimestampPrecision::Microsecond,
        local_recv_ts_us: ts,
    }
}

fn fresh(mut meta: DerivativeMeta) -> DerivativeMeta {
    meta.event_id = Uuid::now_v7();
    meta
}

fn test_quote_book(venue: AdapterId, ts: i64) -> NormalizedEvent {
    NormalizedEvent::Book(BookSnapshot {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter: venue,
            symbol: CanonicalSymbol::new("USDT", "KRW"),
            source_symbol: "KRW-USDT".into(),
            source_stream: "orderbook".into(),
            source_sequence: Some(1),
            exchange_event_ts_us: Some(ts),
            exchange_trade_ts_us: None,
            event_ts_precision: TimestampPrecision::Microsecond,
            trade_ts_precision: TimestampPrecision::Unavailable,
            local_recv_ts_us: ts,
            raw_size_bytes: 1,
        },
        bids: vec![PriceLevel {
            price: 1_350_000_000_000_000_000_000,
            quantity: 100_000_000_000_000_000_000,
        }],
        asks: vec![PriceLevel {
            price: 1_351_000_000_000_000_000_000,
            quantity: 100_000_000_000_000_000_000,
        }],
    })
}

#[derive(Default)]
struct Metrics {
    state: Mutex<MetricState>,
    schedulers: Mutex<Vec<Arc<RestScheduler>>>,
}

#[derive(Default, Clone)]
struct MetricState {
    per_family: BTreeMap<String, FamilyCount>,
    event_identities: HashSet<String>,
    reconnects: u64,
    sequence_gaps: u64,
    parser_rejects: u64,
    stale_intervals: u64,
    rate_limit_blocks: u64,
    budget_rejections: u64,
    abandoned_permits: u64,
    pending_response_completions: usize,
    requests: u64,
    credential_headers: u64,
    authenticated_requests: u64,
    errors: Vec<String>,
    error_counts: BTreeMap<String, u64>,
    quote_last_recv_us: BTreeMap<String, i64>,
    quote_stale: BTreeMap<String, bool>,
}

impl Metrics {
    fn mutate(&self, f: impl FnOnce(&mut MetricState)) {
        f(&mut self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner));
    }
    fn snapshot(&self) -> MetricState {
        let mut snapshot = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        snapshot.pending_response_completions = self
            .schedulers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|scheduler| {
                scheduler
                    .snapshot(Instant::now())
                    .pending_response_completions
            })
            .sum();
        snapshot.errors = snapshot
            .error_counts
            .iter()
            .map(|(message, count)| format!("{message} (count={count})"))
            .collect();
        snapshot
    }
    fn event(&self, event: &DerivativeEvent) {
        self.mutate(|s| {
            let family = event_family(event);
            let v = s.per_family.entry(family.into()).or_default();
            v.events += 1;
            v.rows += 1;
            s.event_identities.insert(event_identity(event));
        });
    }
    fn reconnect(&self) {
        self.mutate(|s| s.reconnects += 1);
    }
    fn sequence_gap(&self) {
        self.mutate(|s| s.sequence_gaps += 1);
    }
    fn parser_reject(&self) {
        self.mutate(|s| s.parser_rejects += 1);
    }
    fn stale(&self) {
        self.mutate(|s| s.stale_intervals += 1);
    }
    fn rate_block(&self) {
        self.mutate(|s| s.rate_limit_blocks += 1);
    }
    fn budget_reject(&self) {
        self.mutate(|s| s.budget_rejections += 1);
    }
    fn abandoned(&self) {
        self.mutate(|s| s.abandoned_permits += 1);
    }
    fn rest_attempt(&self) {
        self.mutate(|s| s.requests += 1);
    }
    fn ws_connect_attempt(&self) {
        self.mutate(|s| s.requests += 1);
    }
    fn ws_subscription_attempt(&self) {
        self.mutate(|s| s.requests += 1);
    }
    fn credential_violation(&self, headers: u64) {
        self.mutate(|state| {
            state.credential_headers = state.credential_headers.saturating_add(headers);
            state.authenticated_requests = state.authenticated_requests.saturating_add(1);
        });
    }
    fn register_schedulers(&self, binance: Arc<RestScheduler>, bybit: Arc<RestScheduler>) {
        self.schedulers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend([binance, bybit]);
    }
    fn error(&self, error: String) {
        self.mutate(|state| {
            const MAX_DISTINCT_HEALTH_ERRORS: usize = 64;
            let mut message = error;
            message.truncate(512);
            if let Some(count) = state.error_counts.get_mut(&message) {
                *count = count.saturating_add(1);
            } else if state.error_counts.len() < MAX_DISTINCT_HEALTH_ERRORS - 1 {
                state.error_counts.insert(message, 1);
            } else {
                *state
                    .error_counts
                    .entry("additional distinct health errors suppressed".to_owned())
                    .or_default() += 1;
            }
        });
    }
    fn quote_received(&self, venue: AdapterId, recv_us: i64) {
        let venue = format!("{venue:?}");
        self.mutate(|state| {
            state.quote_last_recv_us.insert(venue.clone(), recv_us);
            state.quote_stale.insert(venue, false);
        });
    }
    fn check_quote_staleness(&self, now_us: i64, venues: &[AdapterId]) {
        let threshold = i64::try_from(QUOTE_STALE_AFTER.as_micros()).unwrap_or(i64::MAX);
        self.mutate(|state| {
            for venue in venues.iter().map(|venue| format!("{venue:?}")) {
                let stale = state
                    .quote_last_recv_us
                    .get(&venue)
                    .is_none_or(|last| now_us.saturating_sub(*last) > threshold);
                if stale && !state.quote_stale.get(&venue).copied().unwrap_or(false) {
                    state.stale_intervals += 1;
                    state.quote_stale.insert(venue, true);
                }
            }
        });
    }
}

impl RuntimeStats for Metrics {
    fn on_websocket_connect_attempt(&self, _: AdapterId) {
        self.ws_connect_attempt();
    }
    fn on_websocket_subscription_attempt(&self, _: AdapterId) {
        self.ws_subscription_attempt();
    }
    fn on_frame(&self, _: AdapterId, _: u32) {}
    fn on_events(&self, _: AdapterId, _: u64, _: u64, _: u64) {}
    fn on_parse_error(&self, _: AdapterId) {
        self.parser_reject();
    }
    fn on_receive_lag_us(&self, _: AdapterId, _: u64) {}
    fn on_queue_depth(&self, _: AdapterId, _: usize) {}
    fn on_reconnect(&self, _: AdapterId, _: md_exchanges::ReconnectReason) {
        self.reconnect();
    }
    fn on_rejected_event(&self, _: AdapterId, _: md_exchanges::RejectReason) {
        self.parser_reject();
    }
    fn on_backpressure_disconnect(&self, _: AdapterId) {}
    fn open_gap(&self, _: AdapterId, _: md_exchanges::GapReason) {}
    fn close_gap(&self, _: AdapterId) {}
    fn on_sequence_gap(&self, _: AdapterId) {
        self.sequence_gap();
    }
}

fn event_family(event: &DerivativeEvent) -> &'static str {
    match event {
        DerivativeEvent::Instrument(_) => DerivativeEventFamily::Instrument.as_str(),
        DerivativeEvent::MarkIndex(_) => DerivativeEventFamily::MarkIndex.as_str(),
        DerivativeEvent::FundingEstimate(_) => DerivativeEventFamily::FundingEstimate.as_str(),
        DerivativeEvent::FundingSettlement(_) => DerivativeEventFamily::FundingSettlement.as_str(),
        DerivativeEvent::OpenInterest(_) => DerivativeEventFamily::OpenInterest.as_str(),
        DerivativeEvent::TraderRatio(_) => DerivativeEventFamily::TraderRatio.as_str(),
        DerivativeEvent::QuoteConversion(_) => DerivativeEventFamily::QuoteConversion.as_str(),
    }
}

fn event_identity(event: &DerivativeEvent) -> String {
    let meta = event.meta();
    let mut identity = format!(
        "{}:{:?}:{}",
        event_family(event),
        meta.venue,
        symbol_name(&meta.symbol)
    );
    if let DerivativeEvent::QuoteConversion(conversion) = event {
        identity.push_str(match conversion.side {
            QuoteSide::Bid => ":bid",
            QuoteSide::Ask => ":ask",
        });
    }
    identity
}

fn expected_run_evidence(config: &FundingConfig, basis: &ReportBasis) -> Vec<String> {
    let mut expected = Vec::new();
    for symbol in &basis.mainnet {
        for venue in [AdapterId::BinanceUsdm, AdapterId::BybitLinear] {
            for family in [
                "instrument",
                "mark_index",
                "funding_estimate",
                "funding_settlement",
                "open_interest",
            ] {
                expected.push(format!("{family}:{venue:?}:{symbol}"));
            }
        }
        expected.push(format!(
            "trader_ratio:{:?}:{symbol}",
            AdapterId::BybitLinear
        ));
    }
    for conversion in &config.quote_conversions {
        let symbol = format!("{}/{}", conversion.base, conversion.quote);
        for venue in &conversion.venues {
            let adapter = match venue.as_str() {
                "upbit_spot" => AdapterId::UpbitSpot,
                "bithumb_spot" => AdapterId::BithumbSpot,
                _ => continue,
            };
            expected.push(format!("quote_conversion:{adapter:?}:{symbol}:bid"));
            expected.push(format!("quote_conversion:{adapter:?}:{symbol}:ask"));
        }
    }
    expected
}

fn excluded(value: &IneligibleInstrument) -> ExcludedSymbol {
    ExcludedSymbol {
        symbol: symbol_name(&value.symbol),
        venue: value.venue.map(|v| format!("{v:?}")),
        code: value.code.into(),
        detail: value.detail.clone(),
    }
}
fn symbol_name(symbol: &CanonicalSymbol) -> String {
    format!("{}/{}", symbol.base, symbol.quote)
}
fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| i64::try_from(v.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| i64::try_from(v.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}

fn finalized_arrow_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut result = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|v| v == "arrow") {
                result.push(path);
            }
        }
    }
    result.sort();
    Ok(result)
}

fn atomic_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    atomic_json_with_publisher(path, value, publish_report)
}

fn atomic_json_with_publisher(
    path: &Path,
    value: &impl serde::Serialize,
    publisher: impl FnOnce(&Path, &Path, Uuid) -> Result<()>,
) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("phase2a-report.json");
    let id = Uuid::now_v7();
    let temporary = path.with_file_name(format!(".{file_name}.{id}.tmp"));
    let result = (|| -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        publisher(&temporary, path, id)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn publish_report(temporary: &Path, target: &Path, _id: Uuid) -> Result<()> {
    fs::rename(temporary, target)
        .with_context(|| format!("failed to atomically replace {}", target.display()))?;
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    fs::File::open(parent)?.sync_all().with_context(|| {
        format!(
            "report replacement committed but parent-directory sync failed: {}",
            parent.display()
        )
    })?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn publish_report(temporary: &Path, target: &Path, _id: Uuid) -> Result<()> {
    fs::rename(temporary, target)
        .with_context(|| format!("failed to atomically replace {}", target.display()))?;
    Ok(())
}

#[cfg(windows)]
fn wide_path(path: &Path) -> std::io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
    if value.contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "report path contains NUL",
        ));
    }
    value.push(0);
    Ok(value)
}

#[cfg(windows)]
fn publish_report(temporary: &Path, target: &Path, id: Uuid) -> Result<()> {
    use std::ptr::null;
    use windows_sys::Win32::Foundation::ERROR_UNABLE_TO_MOVE_REPLACEMENT_2;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, ReplaceFileW,
    };

    let temporary_w = wide_path(temporary)?;
    let target_w = wide_path(target)?;
    if !target.try_exists()? {
        // SAFETY: both path buffers are NUL-terminated and remain alive for the call.
        let moved = unsafe {
            MoveFileExW(
                temporary_w.as_ptr(),
                target_w.as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved != 0 {
            return Ok(());
        }
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to publish new report {}", target.display()));
    }

    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("phase2a-report.json");
    let backup = target.with_file_name(format!(".{file_name}.{id}.previous"));
    let backup_w = wide_path(&backup)?;
    // SAFETY: all PCWSTR buffers are NUL-terminated and the reserved pointers are null.
    let replaced = unsafe {
        ReplaceFileW(
            target_w.as_ptr(),
            temporary_w.as_ptr(),
            backup_w.as_ptr(),
            0,
            null(),
            null(),
        )
    };
    if replaced != 0 {
        if let Err(error) = fs::remove_file(&backup) {
            tracing::warn!(path = %backup.display(), %error, "report committed but backup cleanup failed");
        }
        return Ok(());
    }

    let replace_error = std::io::Error::last_os_error();
    if replace_error.raw_os_error() == Some(ERROR_UNABLE_TO_MOVE_REPLACEMENT_2 as i32) {
        // Microsoft documents that error 1177 may leave the old target at backup.
        // SAFETY: all path buffers are valid and NUL-terminated.
        let restored = unsafe {
            MoveFileExW(
                backup_w.as_ptr(),
                target_w.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if restored == 0 {
            let restore_error = std::io::Error::last_os_error();
            bail!(
                "ReplaceFileW failed ({replace_error}); old-report restoration also failed ({restore_error}); backup={}",
                backup.display()
            );
        }
    }
    Err(replace_error).with_context(|| format!("failed to replace report {}", target.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_funding_settlement_is_emitted_once_but_next_timestamp_is_new() {
        let (tx, _rx) = mpsc::channel(1);
        let metrics = Arc::new(Metrics::default());
        let context = PollContext {
            client: PublicHttpClient::build(Arc::clone(&metrics)).unwrap(),
            binance_scheduler: Arc::new(RestScheduler::binance_weighted(1_200, 200, 0).unwrap()),
            bybit_scheduler: Arc::new(RestScheduler::bybit_endpoint(10).unwrap()),
            tx,
            shutdown: CancellationToken::new(),
            metrics,
            settlements: Arc::new(Mutex::new(HashSet::new())),
            funding_rules: Arc::new(Mutex::new(std::collections::HashMap::new())),
        };
        let ts = now_us();
        let settlement = |timestamp| {
            DerivativeEvent::FundingSettlement(FundingSettlement {
                meta: test_meta(AdapterId::BybitLinear, "BTC", "USDT", timestamp),
                rate: 1,
                rate_kind: FundingRateKind::SettledActual,
                basis: FundingBasis::MarkNotional,
                interval_secs: 28_800,
                interval_provenance: FundingIntervalProvenance::VenuePayload,
                settlement_ts_us: timestamp,
            })
        };
        assert!(context.accept_settlement(&settlement(ts)));
        assert!(!context.accept_settlement(&settlement(ts)));
        assert!(context.accept_settlement(&settlement(ts + 1)));
    }

    #[test]
    fn bybit_10006_completes_permit_and_blocks_scheduler() {
        let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
        let permit = scheduler
            .acquire(RequestClass::MarketData, 1, Instant::now())
            .unwrap();
        let metrics = Metrics::default();
        assert!(
            complete_response(
                &scheduler,
                &permit,
                &HeaderMap::new(),
                reqwest::StatusCode::OK,
                Some(10006),
                &metrics,
            )
            .unwrap()
        );
        let snapshot = scheduler.snapshot(Instant::now());
        assert_eq!(snapshot.pending_response_completions, 0);
        assert!(snapshot.blocked_until.is_some());
    }

    #[test]
    fn atomic_report_replaces_existing_without_leaving_backup_or_temp() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("phase2a-report.json");
        atomic_json(&path, &json!({"generation": 1})).unwrap();
        atomic_json(&path, &json!({"generation": 2})).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["generation"], 2);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_atomic_publication_preserves_existing_report_and_cleans_temp() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("phase2a-report.json");
        atomic_json(&path, &json!({"generation": 1})).unwrap();
        let expected = fs::read(&path).unwrap();
        let error = atomic_json_with_publisher(
            &path,
            &json!({"generation": 2}),
            |_temporary, _target, _id| Err(anyhow!("injected publication failure")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected publication failure"));
        assert_eq!(fs::read(&path).unwrap(), expected);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn startup_slots_are_capacity_bounded_and_finish_well_inside_sixty_seconds() {
        let delays = (0..20)
            .flat_map(|symbol| {
                (0..3).map(move |family| {
                    startup_delay(symbol, family, 3, BYBIT_PUBLIC_REQUESTS_PER_SECOND)
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(delays.len(), 60);
        assert!(delays.iter().all(|delay| *delay < Duration::from_secs(6)));
        let mut per_second = BTreeMap::<u64, usize>::new();
        for delay in delays {
            *per_second.entry(delay.as_secs()).or_default() += 1;
        }
        assert!(
            per_second
                .values()
                .all(|count| *count <= BYBIT_PUBLIC_REQUESTS_PER_SECOND as usize)
        );
    }

    #[test]
    fn long_rate_block_waits_without_unbounded_health_entries() {
        let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
        let permit = scheduler
            .acquire(RequestClass::MarketData, 1, Instant::now())
            .unwrap();
        let metrics = Metrics::default();
        assert!(
            complete_response(
                &scheduler,
                &permit,
                &HeaderMap::new(),
                reqwest::StatusCode::FORBIDDEN,
                None,
                &metrics,
            )
            .unwrap()
        );
        assert!(retry_delay(&scheduler, 0) > Duration::from_secs(590));
        for _ in 0..100 {
            metrics.error("same transport failure".to_owned());
        }
        let errors = metrics.snapshot().errors;
        assert_eq!(errors.len(), 1);
        assert!(errors[0].ends_with("(count=100)"));
    }

    #[test]
    fn public_http_client_rejects_and_measures_credential_headers() {
        let metrics = Arc::new(Metrics::default());
        let client = PublicHttpClient::build(Arc::clone(&metrics)).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-mbx-apikey",
            reqwest::header::HeaderValue::from_static("forbidden"),
        );
        assert!(
            client
                .get_with_headers(Url::parse("http://127.0.0.1/public").unwrap(), &headers)
                .is_err()
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests, 0);
        assert_eq!(snapshot.credential_headers, 1);
        assert_eq!(snapshot.authenticated_requests, 1);
    }

    #[test]
    fn websocket_backoff_is_bounded_and_increases_before_healthy_reset() {
        let delays = (1..=10).map(ws_backoff_delay).collect::<Vec<_>>();
        assert!(delays.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(delays.iter().all(|delay| *delay <= Duration::from_secs(5)));
    }

    #[test]
    fn ticker_timestamp_regression_is_not_counted_as_a_sequence_gap() {
        assert!(is_derivative_sequence_gap(
            &binance::DerivativeParseError::SequenceRegression {
                previous: 2,
                current: 1,
            }
        ));
        assert!(!is_derivative_sequence_gap(
            &binance::DerivativeParseError::TimestampRegression {
                previous: 2,
                current: 1,
            }
        ));
    }

    #[tokio::test]
    async fn derivative_websocket_accept_close_storm_is_backed_off() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                server_accepts.fetch_add(1, Ordering::SeqCst);
                if let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await {
                    let _ = socket.close(None).await;
                }
            }
        });
        let config =
            FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
        let mut endpoints = config.venues["bybit_linear"].mainnet.clone();
        endpoints.public_websocket_url = format!("ws://{address}");
        let (tx, _rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let metrics = Arc::new(Metrics::default());
        let mut producers = JoinSet::new();
        spawn_derivative_ws(
            DerivativeWsSpec {
                symbol: CanonicalSymbol::new("BTC", "USDT"),
                endpoints,
                venue: AdapterId::BybitLinear,
                funding_rules: Arc::new(Mutex::new(std::collections::HashMap::new())),
            },
            tx,
            shutdown.clone(),
            Arc::clone(&metrics),
            &mut producers,
        );
        tokio::time::sleep(Duration::from_millis(1_600)).await;
        shutdown.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), producers.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        result.unwrap();
        server.abort();
        let attempts = accepts.load(Ordering::SeqCst);
        assert!(
            (2..=5).contains(&attempts),
            "accept-close attempts={attempts}"
        );
        assert_eq!(metrics.snapshot().requests, attempts as u64 * 2);
    }
}
