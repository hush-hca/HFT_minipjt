# Phase 2A Derivatives Data and Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add validated Binance USDⓈ-M and Bybit Linear public derivatives data, instrument discovery, Bybit top-20 reconstructed books, weighted REST polling, and hourly Arrow persistence without regressing Phase 1.

**Architecture:** A new `funding-core` crate owns venue-neutral derivative types and Phase 2 configuration. `md-exchanges` parses venue payloads and contains Bybit's stateful depth-50 reconstruction, while `md-storage` writes derivative event families through a dedicated router; a new non-trading `funding-app collect` command orchestrates the pipeline.

**Tech Stack:** Rust 2024 (Rust 1.85+), Tokio, reqwest/rustls, tokio-tungstenite, simd-json, exact scale-18 `i128` decimals, Apache Arrow IPC 56, clap, serde/TOML, tracing, UUIDv7.

**Spec:** `docs/superpowers/specs/2026-08-20-funding-arbitrage-hft-phase-2-design.md`

## Global Constraints

- Preserve all Phase 1 public feeds, schemas, validation, CLI behavior, and tests.
- The configured mainnet monitor/paper universe is the existing ordered list of 20 assets intersected with active Binance USDⓈ-M and Bybit USDT perpetuals.
- Testnet discovery is independent; unavailable mainnet symbols are reported as `TESTNET_UNAVAILABLE` rather than treated as startup failures.
- Parse JSON market payloads with `simd-json`; capture local receive time before parsing.
- Persist financial values as `Decimal128(38,18)` backed by exact `i128`; never persist financial `f64` values.
- Store source timestamps and local receive timestamps as epoch microseconds with source precision retained.
- Bybit `orderbook.50` is reconstructed inside the adapter and only validated top-20 snapshots leave the adapter.
- Public derivatives events remain unauthenticated. No API keys, strategy decisions, paper orders, testnet orders, or GUI are introduced in this phase.
- REST polling must preserve order-entry headroom: a `429`, unknown endpoint weight, or exhausted budget slows/disables the poller.
- All network tests use deterministic loopback fake servers; quality gates run offline after dependencies are fetched once.

---

## File Map

| Path | Responsibility |
|---|---|
| `Cargo.toml` | Add `funding-core` and `funding-app` workspace members |
| `config/funding.toml` | Phase 2 assets, mainnet/testnet endpoints, polling intervals, queue and Arrow defaults |
| `crates/md-core/src/model.rs` | Add `AdapterId::BybitLinear` without changing existing event layouts |
| `crates/funding-core/src/config.rs` | Load and validate Phase 2 public-only configuration |
| `crates/funding-core/src/instrument.rs` | Instrument rules, account/position capability enums, eligibility reasons |
| `crates/funding-core/src/public.rs` | Normalized mark/index, funding, OI, ratio, conversion, and settlement events |
| `crates/md-exchanges/src/bybit.rs` | Bybit discovery, trades, and depth-50 snapshot/delta reconstruction |
| `crates/md-exchanges/src/derivatives/binance.rs` | Binance instrument, mark/funding, OI, and top-trader parsers |
| `crates/md-exchanges/src/derivatives/bybit.rs` | Bybit instrument, ticker/funding, OI, and long/short parsers |
| `crates/md-exchanges/src/derivatives/scheduler.rs` | Weighted, order-capacity-reserving REST scheduler |
| `crates/md-storage/src/derivative_schema.rs` | Canonical Arrow schemas and metadata for derivative event families |
| `crates/md-storage/src/derivative_batch.rs` | Atomic derivative-event-to-RecordBatch builders |
| `crates/md-storage/src/derivative_partition.rs` | Hourly derivative partition routing, flush, finalization, and recovery |
| `crates/funding-app/src/collector.rs` | Public adapter/poller orchestration and gap/error reporting |
| `crates/funding-app/src/main.rs` | `funding-app collect --config config/funding.toml --duration 60s` command |
| `crates/funding-app/tests/phase2a_e2e.rs` | Four loopback public services, reconnect, persistence, and validation |

---

### Task 1: Add the Phase 2 Core Types and Configuration

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/md-core/src/model.rs`
- Modify: `crates/md-core/src/validation.rs`
- Modify: `crates/md-exchanges/src/discovery.rs`
- Modify: `crates/md-storage/src/schema.rs`
- Modify: `crates/md-storage/src/partition.rs`
- Modify: `crates/collector/src/stats.rs`
- Modify: `crates/collector/src/app.rs`
- Create: `crates/funding-core/Cargo.toml`
- Create: `crates/funding-core/src/lib.rs`
- Create: `crates/funding-core/src/config.rs`
- Create: `crates/funding-core/src/instrument.rs`
- Create: `crates/funding-core/src/public.rs`
- Create: `crates/funding-core/tests/model.rs`
- Create: `config/funding.toml`

**Interfaces:**
- Produces: `FundingConfig::load(path: &Path) -> Result<FundingConfig, FundingConfigError>`
- Produces: `InstrumentSpec`, `DerivativeEvent`, `FundingRateKind`, `FundingBasis`, `TraderMetricKind`, and `EligibilityReason`
- Produces: `AdapterId::BybitLinear`; existing enum variants and serialized labels remain unchanged.

- [ ] **Step 1: Write the failing model/config test**

```rust
use funding_core::{
    config::FundingConfig,
    instrument::{ContractKind, InstrumentSpec},
    public::{DerivativeEvent, FundingBasis, FundingEstimate, FundingRateKind},
};
use md_core::model::{AdapterId, CanonicalSymbol, DECIMAL_SCALE};

#[test]
fn funding_config_and_types_preserve_venue_semantics() {
    let cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    assert_eq!(cfg.assets.len(), 20);
    assert_eq!(cfg.assets.first().unwrap(), "BTC");
    assert_eq!(cfg.assets.last().unwrap(), "OP");
    assert_eq!(DECIMAL_SCALE, 18);

    let spec = test_usdt_perpetual(
        AdapterId::BybitLinear,
        CanonicalSymbol::new("BTC", "USDT"),
    );
    assert_eq!(spec.contract_kind, ContractKind::Perpetual);
    assert_eq!(spec.contract_multiplier, 1_000_000_000_000_000_000);

    let event = DerivativeEvent::FundingEstimate(FundingEstimate {
        venue: AdapterId::BinanceUsdm,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        venue_symbol: "BTCUSDT".into(),
        rate: 100_000_000_000_000,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        next_funding_ts_us: 1_800_000_000_000_000,
        source_ts_us: Some(1_799_999_999_000_000),
        local_recv_ts_us: 1_799_999_999_100_000,
    });
    assert!(matches!(event, DerivativeEvent::FundingEstimate(_)));
}
```

- [ ] **Step 2: Run the focused test and confirm the red state**

Run: `cargo test -p funding-core --test model`

Expected: FAIL because the workspace member and `funding_core` types do not exist.

- [ ] **Step 3: Define the exact normalized types**

```rust
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ContractKind { Perpetual }

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PositionMode { OneWay, Hedge }

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AccountMode { Classic, Unified, Portfolio }

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InstrumentSpec {
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub venue_symbol: String,
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
    pub source_ts_us: Option<i64>,
    pub local_recv_ts_us: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FundingRateKind { IndicativeNext, SettledActual }

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FundingBasis { MarkNotional }

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TraderMetricKind { BinanceTopAccountRatio, BinanceTopPositionRatio, BybitLongShortRatio }

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DerivativeEvent {
    Instrument(InstrumentSpec),
    MarkIndex(MarkIndexSnapshot),
    FundingEstimate(FundingEstimate),
    FundingSettlement(FundingSettlement),
    OpenInterest(OpenInterestSnapshot),
    TraderRatio(TraderRatioSnapshot),
    QuoteConversion(QuoteConversionSnapshot),
}
```

Every event struct contains `venue`, `symbol`, `venue_symbol`, nullable venue source time, and `local_recv_ts_us`. Use exact `i128` for all rates, prices, quantities, and ratios. Implement `DerivativeEvent::partition_ts_us()` using source time when valid and local receive time otherwise.

- [ ] **Step 4: Implement and validate `FundingConfig`**

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FundingConfig {
    pub output_root: PathBuf,
    pub assets: Vec<String>,
    pub quote_conversions: Vec<QuoteConversionConfig>,
    pub channel_capacity: usize,
    pub batch_rows: usize,
    pub flush_interval_ms: u64,
    pub poll: PollConfig,
    pub venues: BTreeMap<String, VenueConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PollConfig {
    pub instrument_secs: u64,
    pub open_interest_secs: u64,
    pub trader_ratio_secs: u64,
    pub funding_metadata_secs: u64,
    pub reserved_order_weight: u32,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct VenueConfig {
    pub mainnet: EndpointSet,
    pub testnet: EndpointSet,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct EndpointSet {
    pub rest_url: String,
    pub public_websocket_url: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct QuoteConversionConfig {
    pub base: String,
    pub quote: String,
    pub venues: Vec<String>,
}
```

Require exactly the venue keys `binance_usdm` and `bybit_linear`, uppercase unique assets, positive queue/batch/interval values, HTTPS/WSS endpoints except loopback HTTP/WS, and `reserved_order_weight > 0`. Configure `USDT/KRW` on Upbit and Bithumb as the initial `quote_conversions` entry. Populate `config/funding.toml` with the approved 20 assets; mainnet/testnet endpoint groups are explicit and no credential values appear.

- [ ] **Step 5: Update existing exhaustive adapter mappings without widening the Phase 1 collector**

Map Bybit storage paths/metadata to `bybit/linear_futures`. Expand `StatsRegistry` internal adapter storage to five entries and map `BybitLinear` to index 4, but keep Phase 1 `ALL_ADAPTERS` at its original four so `collector` does not request missing Bybit configuration. Existing Phase 1 discovery/subscription and collector runtime matches return a typed unsupported-adapter error for `BybitLinear`; the Phase 2A-specific discovery/runtime added in later tasks owns Bybit. Add a regression test proving `collector collect` still launches exactly its original four adapters.

- [ ] **Step 6: Run core and Phase 1 regression tests**

Run: `cargo test -p funding-core --test model`

Run: `cargo test -p md-core`

Run: `cargo test -p collector --test app`

Expected: PASS; existing `AdapterId` and market-event tests remain green.

- [ ] **Step 7: Commit the core boundary**

```bash
git add Cargo.toml Cargo.lock config/funding.toml crates/md-core crates/funding-core
git commit -m "feat: add derivatives core models"
```

---

### Task 2: Implement Ordered Mainnet and Independent Testnet Discovery

**Files:**
- Create: `crates/md-exchanges/src/derivatives/mod.rs`
- Create: `crates/md-exchanges/src/derivatives/discovery.rs`
- Modify: `crates/md-exchanges/src/lib.rs`
- Modify: `crates/md-exchanges/Cargo.toml`
- Create: `crates/md-exchanges/tests/derivative_discovery.rs`
- Create: `crates/md-exchanges/tests/fixtures/bybit_linear_instruments.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_usdm_instruments_phase2.json`

**Interfaces:**
- Consumes: `InstrumentSpec`, `CanonicalSymbol`, and `AdapterId` from Task 1.
- Produces: `discover_derivatives(client, config, environment) -> Result<DerivativeDiscovery, DiscoveryError>`.
- Produces: stable `IneligibleInstrument { symbol, venue, code, detail }` records, including `TESTNET_UNAVAILABLE`.

- [ ] **Step 1: Write the failing intersection test**

```rust
#[test]
fn mainnet_and_testnet_discovery_are_independent_and_stable() {
    let requested = vec!["BTC".into(), "ETH".into(), "OP".into()];
    let binance = fixture_specs(AdapterId::BinanceUsdm, &["ETH", "BTC", "OP"]);
    let bybit = fixture_specs(AdapterId::BybitLinear, &["BTC", "ETH"]);
    let mainnet = intersect_active(&requested, &binance, &bybit, Environment::Mainnet);
    assert_eq!(bases(&mainnet.eligible), ["BTC", "ETH"]);

    let test_binance = fixture_specs(AdapterId::BinanceUsdm, &["BTC"]);
    let test_bybit = fixture_specs(AdapterId::BybitLinear, &["BTC"]);
    let testnet = intersect_active(&requested, &test_binance, &test_bybit, Environment::Testnet);
    assert_eq!(bases(&testnet.eligible), ["BTC"]);
    assert_eq!(testnet.excluded[0].code, "TESTNET_UNAVAILABLE");
    assert_eq!(testnet.excluded[1].code, "TESTNET_UNAVAILABLE");
}
```

- [ ] **Step 2: Verify the test fails on the missing interface**

Run: `cargo test -p md-exchanges --test derivative_discovery`

Expected: FAIL with unresolved derivative discovery imports.

- [ ] **Step 3: Parse and validate venue instrument responses**

```rust
pub enum Environment { Mainnet, Testnet }

pub struct DerivativeDiscovery {
    pub eligible: Vec<CommonInstrument>,
    pub excluded: Vec<IneligibleInstrument>,
}

pub struct CommonInstrument {
    pub symbol: CanonicalSymbol,
    pub binance: InstrumentSpec,
    pub bybit: InstrumentSpec,
}
```

Binance accepts only `status == "TRADING"`, `contractType == "PERPETUAL"`, `quoteAsset == "USDT"`. Bybit accepts only `status == "Trading"`, `contractType == "LinearPerpetual"`, `settleCoin == "USDT"`. Convert every tick, step, min/max quantity, min notional, multiplier, bound, and funding interval exactly; reject representable configured instruments when a required rule is absent or non-positive.

- [ ] **Step 4: Implement stable intersection and reason codes**

Iterate `requested` once; look up both venue specs by base/quote; append a common pair only when both are eligible. Emit `BINANCE_UNAVAILABLE`, `BYBIT_UNAVAILABLE`, `RULE_INVALID`, or, for a testnet pair absent on either side, `TESTNET_UNAVAILABLE`. Do not let extra or Unicode venue instruments disturb configured order.

- [ ] **Step 5: Run discovery and workspace tests**

Run: `cargo test -p md-exchanges --test derivative_discovery`

Run: `cargo test --workspace --offline`

Expected: PASS with BTC then ETH in configured order and all Phase 1 tests green.

- [ ] **Step 6: Commit discovery**

```bash
git add Cargo.lock crates/md-exchanges
git commit -m "feat: discover common perpetual instruments"
```

---

### Task 3: Reconstruct Bybit Depth and Parse Individual Trades

**Files:**
- Create: `crates/md-exchanges/src/bybit.rs`
- Modify: `crates/md-exchanges/src/runtime.rs`
- Modify: `crates/md-exchanges/src/lib.rs`
- Create: `crates/md-exchanges/tests/bybit_public.rs`
- Create: `crates/md-exchanges/tests/fixtures/bybit_book_snapshot.json`
- Create: `crates/md-exchanges/tests/fixtures/bybit_book_delta.json`
- Create: `crates/md-exchanges/tests/fixtures/bybit_trade.json`

**Interfaces:**
- Produces: `BybitLinearParser::new(symbol: CanonicalSymbol) -> Self` implementing existing `FrameParser`.
- Produces: `FrameParser::reset(&self)` default no-op; Bybit clears reconstructed depth on reconnect.
- Produces: validated `NormalizedEvent::{Book,Trade}` with `AdapterId::BybitLinear`.

- [ ] **Step 1: Write snapshot/delta/gap tests**

```rust
#[test]
fn bybit_snapshot_then_delta_emits_sorted_top_twenty() {
    let parser = BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"));
    let first = parser.parse(&mut fixture("bybit_book_snapshot.json"), 10_000_000).unwrap();
    assert_eq!(book(&first).bids.len(), 20);
    assert_eq!(book(&first).asks.len(), 20);

    let second = parser.parse(&mut fixture("bybit_book_delta.json"), 10_000_100).unwrap();
    assert!(book(&second).bids.windows(2).all(|w| w[0].price > w[1].price));
    assert!(book(&second).asks.windows(2).all(|w| w[0].price < w[1].price));
}

#[test]
fn bybit_sequence_gap_requires_reconnect_and_reset() {
    let parser = seeded_parser();
    let error = parser.parse(&mut delta_with_update_id(99), 20_000_000).unwrap_err();
    assert!(matches!(error, ParseError::SequenceGap { .. }));
    parser.reset();
    assert!(matches!(parser.parse(&mut delta_with_update_id(100), 20_000_100), Err(ParseError::SnapshotRequired)));
}
```

- [ ] **Step 2: Run the parser test and confirm failure**

Run: `cargo test -p md-exchanges --test bybit_public`

Expected: FAIL because `BybitLinearParser`, reset support, and fixtures do not exist.

- [ ] **Step 3: Implement stateful price-keyed reconstruction**

```rust
struct BybitBookState {
    bids: BTreeMap<i128, i128>,
    asks: BTreeMap<i128, i128>,
    last_update_id: Option<u64>,
    last_cross_sequence: Option<u64>,
    initialized: bool,
}
```

On `type == "snapshot"`, clear and replace both sides. On `type == "delta"`, require initialized state and monotonic `u`/`seq`; quantity zero deletes the price, positive quantity inserts/replaces it, and negative quantity is invalid. Sort bids descending and asks ascending, take 20 each, set source sequence to `u`, preserve Bybit timestamp fields, then call `md_core::validation::validate_book`. Return a typed sequence-gap error so the supervisor reconnects; call `parser.reset()` before every new session.

- [ ] **Step 4: Parse `publicTrade.{symbol}` as individual executions**

For every element of `data`, create one `TradeTick` using trade ID `i`, price `p`, size `v`, and taker side from `S`. Do not aggregate array entries. Convert millisecond timestamps with `ms_to_us`; keep local receive time captured before parsing.

- [ ] **Step 5: Run focused and runtime regression tests**

Run: `cargo test -p md-exchanges --test bybit_public`

Run: `cargo test -p md-exchanges --test runtime`

Expected: PASS, including reconnect reset and existing four adapters.

- [ ] **Step 6: Commit the Bybit public adapter**

```bash
git add crates/md-exchanges
git commit -m "feat: reconstruct bybit linear books"
```

---

### Task 4: Parse Derivatives Feeds and Enforce Weighted REST Budgets

**Files:**
- Create: `crates/md-exchanges/src/derivatives/binance.rs`
- Create: `crates/md-exchanges/src/derivatives/bybit.rs`
- Create: `crates/md-exchanges/src/derivatives/scheduler.rs`
- Create: `crates/md-exchanges/tests/derivative_events.rs`
- Create: `crates/md-exchanges/tests/rest_scheduler.rs`
- Create: `crates/md-exchanges/tests/fixtures/binance_mark_funding.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_open_interest.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_top_ratio.json`
- Create: `crates/md-exchanges/tests/fixtures/bybit_ticker_funding.json`
- Create: `crates/md-exchanges/tests/fixtures/bybit_open_interest.json`
- Create: `crates/md-exchanges/tests/fixtures/bybit_long_short.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_funding_history.json`
- Create: `crates/md-exchanges/tests/fixtures/bybit_funding_history.json`

**Interfaces:**
- Consumes: derivative event types from Task 1.
- Produces: `parse_*(&mut [u8], recv_us: i64) -> Result<Vec<DerivativeEvent>, DerivativeParseError>`.
- Produces: `RestScheduler::acquire(class, weight, now) -> Result<Permit, BudgetError>` and `record_response(headers, status)`.

- [ ] **Step 1: Write semantic and rate-budget tests**

```rust
#[test]
fn funding_estimate_keeps_interval_kind_and_next_timestamp() {
    let events = binance::parse_mark_funding(&mut fixture("binance_mark_funding.json"), 9_000_000).unwrap();
    let funding = funding_estimate(&events);
    assert_eq!(funding.rate_kind, FundingRateKind::IndicativeNext);
    assert_eq!(funding.interval_secs, 28_800);
    assert_eq!(funding.basis, FundingBasis::MarkNotional);
}

#[test]
fn funding_history_is_an_actual_settled_rate_not_an_estimate() {
    let events = bybit::parse_funding_history(&mut fixture("bybit_funding_history.json"), 9_000_000).unwrap();
    let settled = funding_settlement(&events);
    assert_eq!(settled.rate_kind, FundingRateKind::SettledActual);
    assert_eq!(settled.basis, FundingBasis::MarkNotional);
}

#[test]
fn market_data_cannot_consume_reserved_order_weight() {
    let scheduler = RestScheduler::new(1_200, 200);
    scheduler.acquire(RequestClass::MarketData, 1_000, Instant::now()).unwrap();
    assert!(matches!(scheduler.acquire(RequestClass::MarketData, 1, Instant::now()), Err(BudgetError::ReservedHeadroom)));
    assert!(scheduler.acquire(RequestClass::Order, 1, Instant::now()).is_ok());
}
```

- [ ] **Step 2: Confirm the tests fail on missing parsers/scheduler**

Run: `cargo test -p md-exchanges --test derivative_events --test rest_scheduler`

Expected: FAIL with unresolved functions and types.

- [ ] **Step 3: Implement exact event parsers**

Parse Binance mark/index/current funding fields from the documented mark-price stream, settled rates from funding history, OI from its REST response, and each top-trader series with its precise `TraderMetricKind`. Parse Bybit ticker funding fields, settled rates from funding history, OI series, and long/short ratio with `BybitLongShortRatio`. Require positive prices/notionals, rates within instrument bounds when bounds are present, nondecreasing timestamps, and exact decimals through `md_core::decimal`.

- [ ] **Step 4: Implement the weighted scheduler**

```rust
pub enum RequestClass { MarketData, Account, Order }

pub struct RestScheduler {
    limit_per_minute: AtomicU32,
    used_weight: AtomicU32,
    reserved_order_weight: u32,
    blocked_until: Mutex<Option<Instant>>,
}
```

Market-data permits require `used + requested <= limit - reserved`; account permits use only explicitly configured account headroom; order permits may use the reserve. Parse venue rate-limit headers when documented. On `429`, honor `Retry-After`, block the affected venue scheduler, and surface a health event. If endpoint weight is unknown, return `BudgetError::UnknownWeight` and leave the poller disabled.

- [ ] **Step 5: Run deterministic parser/scheduler tests**

Run: `cargo test -p md-exchanges --test derivative_events --test rest_scheduler`

Expected: PASS without wall-clock sleeps or external network access.

- [ ] **Step 6: Commit derivative parsers and budgets**

```bash
git add crates/md-exchanges
git commit -m "feat: normalize derivatives public data"
```

---

### Task 5: Persist and Validate Derivative Arrow Streams

**Files:**
- Create: `crates/md-storage/src/derivative_schema.rs`
- Create: `crates/md-storage/src/derivative_batch.rs`
- Create: `crates/md-storage/src/derivative_partition.rs`
- Modify: `crates/md-storage/src/lib.rs`
- Modify: `crates/md-storage/src/validate.rs`
- Modify: `crates/md-storage/Cargo.toml`
- Create: `crates/md-storage/tests/derivative_roundtrip.rs`
- Create: `crates/md-storage/tests/derivative_partition.rs`

**Interfaces:**
- Consumes: `DerivativeEvent` from Task 1.
- Produces: `DerivativePartitionRouter::push(event).await`, `flush_due(now).await`, and `shutdown().await`.
- Produces: canonical schemas with `event_family` metadata and the existing validator's structured issue format.

- [ ] **Step 1: Write round-trip and partition tests**

```rust
#[tokio::test]
async fn derivative_events_round_trip_by_family_and_utc_hour() {
    let root = test_root();
    let mut router = DerivativePartitionRouter::open(storage_config(&root)).unwrap();
    router.push(funding_event_at("BTC", 1_800_000_000_000_000)).await.unwrap();
    router.push(oi_event_at("BTC", 1_800_000_000_100_000)).await.unwrap();
    router.shutdown().await.unwrap();
    let report = md_storage::validate_path(&root).unwrap();
    assert!(report.is_valid(), "{:#?}", report.issues);
    assert_eq!(find_files(&root, "funding_estimate").len(), 1);
    assert_eq!(find_files(&root, "open_interest").len(), 1);
}
```

- [ ] **Step 2: Confirm missing router/schema failure**

Run: `cargo test -p md-storage --test derivative_roundtrip --test derivative_partition`

Expected: FAIL because derivative storage APIs do not exist.

- [ ] **Step 3: Define schemas and atomic builders**

Every schema starts with `schema_version`, `event_id`, `venue`, `market`, `base`, `quote`, `source_symbol`, nullable `exchange_event_ts_us`, `local_recv_ts_us`, and `source_precision`. Add event-specific fields with exact `Decimal128(38,18)`. Funding schemas require `rate_kind`, `funding_basis`, `interval_secs`, and settlement/next-funding timestamps. Ratio schemas require `metric_kind`. Build a complete row in temporary values, validate it, then mutate Arrow builders so rejected rows cannot leave unequal column lengths.

- [ ] **Step 4: Route and validate canonical paths**

Use:

```text
derivatives/<event_family>/<venue>/<market>/<BASE-QUOTE>/<YYYY-MM-DD>/<HH>/<event_family>.arrow
```

Rotate on source-event UTC hour, fall back to local receive time only when source time is absent, and retain `.arrow.partial` recovery. Extend `validate_path` to reject wrong family metadata, financial non-Decimal columns, invalid funding kinds, invalid intervals, and path/metadata disagreement.

- [ ] **Step 5: Run storage and full workspace tests**

Run: `cargo test -p md-storage --test derivative_roundtrip --test derivative_partition`

Run: `cargo test --workspace --offline`

Expected: PASS with finalized files and all prior 78+ tests green.

- [ ] **Step 6: Commit derivative storage**

```bash
git add Cargo.lock crates/md-storage
git commit -m "feat: persist derivatives arrow streams"
```

---

### Task 6: Build the Public-Only Phase 2 Collector

**Files:**
- Create: `crates/funding-app/Cargo.toml`
- Create: `crates/funding-app/src/lib.rs`
- Create: `crates/funding-app/src/main.rs`
- Create: `crates/funding-app/src/collector.rs`
- Create: `crates/funding-app/src/report.rs`
- Create: `crates/funding-app/tests/phase2a_e2e.rs`
- Modify: `README.md`
- Modify: `docs/data-schema.md`

**Interfaces:**
- Consumes: discovery, public parsers, scheduler, and derivative router from Tasks 2-5.
- Produces: `Phase2Collector::run(shutdown: CancellationToken) -> Result<Phase2aReport>`.
- Produces CLI: `funding-app collect --config config/funding.toml [--duration 60s]`.

- [ ] **Step 1: Write the loopback end-to-end test**

```rust
#[tokio::test]
async fn collects_both_venues_reconnects_and_validates_every_family() {
    let servers = FakeDerivativeVenues::start().await;
    let cfg = servers.config(test_root());
    let shutdown = CancellationToken::new();
    let report = Phase2Collector::new(cfg).unwrap()
        .run(cancel_after(shutdown, Duration::from_secs(1))).await.unwrap();
    assert_eq!(report.common_mainnet_symbols, vec!["BTC/USDT", "ETH/USDT"]);
    assert!(report.reconnects >= 1);
    assert!(report.event_families.contains(&"funding_estimate".into()));
    assert!(report.event_families.contains(&"funding_settlement".into()));
    assert!(report.event_families.contains(&"quote_conversion".into()));
    assert!(md_storage::validate_path(&report.output_root).unwrap().is_valid());
}
```

- [ ] **Step 2: Verify the collector test fails**

Run: `cargo test -p funding-app --test phase2a_e2e`

Expected: FAIL because `Phase2Collector` and the binary crate do not exist.

- [ ] **Step 3: Implement orchestration with bounded channels**

Create separate bounded channels for `NormalizedEvent` and `DerivativeEvent`. Start discovery before subscriptions, including independently requested `USDT/KRW` references on configured domestic venues, and derive side-aware `QuoteConversionSnapshot` values from each fresh reference book. Start storage before adapters; schedule instrument/funding history metadata at 15 minutes, OI at no less than five seconds, trader ratios at five minutes, and current funding/mark over WebSocket when supplied. On shutdown: stop polling/subscriptions, drain channels, flush/finalize both routers, then atomically write `phase2a-report.json`.

- [ ] **Step 4: Implement the exact CLI and report**

```rust
#[derive(clap::Subcommand)]
enum Command {
    Collect {
        #[arg(long, default_value = "config/funding.toml")]
        config: PathBuf,
        #[arg(long, value_parser = humantime::parse_duration)]
        duration: Option<Duration>,
    },
}
```

The report includes requested/eligible/excluded mainnet and testnet symbols, reason codes, per-family event/row counts, reconnects, sequence gaps, parser rejects, stale intervals, rate-limit blocks, and finalized paths. It contains no credentials.

- [ ] **Step 5: Document and run the command surface**

Run: `cargo run -p funding-app -- collect --help`

Expected: help lists only public collection options and contains no order, API-key, paper, testnet-arm, or GUI controls.

- [ ] **Step 6: Run Phase 2A gates**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets --offline -- -D warnings`

Run: `cargo test --workspace --offline`

Run: `cargo build --workspace --release --offline`

Expected: all commands exit 0; the loopback E2E validates Bybit reconstruction and every derivative Arrow family.

- [ ] **Step 7: Commit Phase 2A integration**

```bash
git add Cargo.toml Cargo.lock crates/funding-app README.md docs/data-schema.md
git commit -m "feat: collect derivatives metadata"
```

---

## Phase 2A Completion Gate

- All Phase 1 tests and CLI commands remain green.
- Mainnet intersection preserves configured order; testnet discovery is independent and reports `TESTNET_UNAVAILABLE`.
- Bybit snapshot/delta reconstruction detects gaps, resets on reconnect, and emits sorted validated top-20 books.
- Binance/Bybit funding, mark/index, OI, ratios, instrument rules, and quote conversion are exact normalized events.
- Weighted public polling cannot consume reserved order capacity.
- Every public event family is stored in canonical hourly Arrow streams and passes recursive validation.
- `funding-app collect` is public-data-only and does not load credentials or expose trading controls.
