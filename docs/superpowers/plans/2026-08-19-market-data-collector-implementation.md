# Multi-Exchange Market Data Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-shaped Rust CLI that collects public order-book snapshots and individual trades from Upbit, Bithumb, Binance Spot, and Binance USDⓈ-M Futures into validated hourly Arrow IPC streams.

**Architecture:** Four Tokio-supervised venue adapters timestamp and parse WebSocket frames into shared typed events, send them through a bounded channel, and a single partition router writes hourly Arrow streams. Public REST market discovery prevents invalid subscriptions; structured statistics, explicit gap accounting, recovery, and a validator make data quality observable.

**Tech Stack:** Rust 2024, Tokio, tokio-tungstenite with rustls, reqwest with rustls, simd-json, Apache Arrow Rust crates, clap, serde/TOML, tracing, UUIDv7, hdrhistogram.

**Spec:** `docs/superpowers/specs/2026-08-19-market-data-collector-design.md`

## Global Constraints

- Rust and Tokio asynchronous tasks are required; fixed-core scheduling is excluded.
- JSON market payloads must be parsed through `simd-json`.
- Store Upbit and Bithumb `trade`/`orderbook`, Binance Spot and USDⓈ-M `{symbol}@trade`/`{symbol}@depth20@100ms`.
- Collect BTC, ETH, XRP, SOL, DOGE, ADA, AVAX, LINK, DOT, BCH, LTC, ETC, TRX, XLM, ATOM, NEAR, APT, SUI, ARB, and OP.
- Domestic pairs default to KRW; Binance pairs default to USDT.
- Store source trade time, source event time, and pre-parse local receive time independently as nullable epoch microseconds with explicit source precision.
- Prices and quantities use Arrow `Decimal128(38,18)`; no persisted `f64` price or quantity columns.
- Partition files by exchange, market, symbol, UTC date, and UTC hour.
- The normalized-event channel defaults to 65,536 entries; enqueue timeout defaults to five seconds and may never drop silently.
- Arrow batches flush at 8,192 rows or one second.
- No authenticated APIs, strategies, feature engines, order execution, GUI, Kafka/NATS, or incremental-depth reconstruction.
- Public feeds require no API key.
- Rust 1.85 or newer is required because the workspace uses Rust edition 2024.

---

## File Map

| Path | Responsibility |
|---|---|
| `Cargo.toml`, `rust-toolchain.toml` | Workspace membership, Rust edition, shared package policy |
| `config/default.toml` | All endpoints, assets, queue, flush, retry, and reporting defaults |
| `crates/md-core/src/config.rs` | Strongly typed TOML configuration and validation |
| `crates/md-core/src/model.rs` | Adapter IDs, symbols, timestamps, decimals, books, trades, normalized event enum |
| `crates/md-core/src/decimal.rs` | Exact scale-18 conversion and overflow rejection |
| `crates/md-core/src/validation.rs` | Book ordering, quantity, timestamp, and event validation |
| `crates/md-exchanges/src/domestic.rs` | Shared helpers for Upbit/Bithumb field layouts |
| `crates/md-exchanges/src/upbit.rs` | Upbit discovery, subscription, and parser |
| `crates/md-exchanges/src/bithumb.rs` | Bithumb discovery, subscription, and parser |
| `crates/md-exchanges/src/binance.rs` | Shared Binance combined-stream parsing helpers |
| `crates/md-exchanges/src/binance_spot.rs` | Binance Spot discovery, subscription, parser mapping |
| `crates/md-exchanges/src/binance_usdm.rs` | Binance USDⓈ-M discovery, subscription, parser mapping |
| `crates/md-exchanges/src/discovery.rs` | Active-market intersection and strict-mode result |
| `crates/md-exchanges/src/runtime.rs` | WebSocket session, ping/pong, subscription, supervised reconnect |
| `crates/md-exchanges/src/backoff.rs` | Deterministic capped exponential backoff with jitter injection |
| `crates/md-exchanges/tests/fixtures/` | Eight market-event fixtures plus four discovery fixtures |
| `crates/md-storage/src/schema.rs` | Book/trade Arrow schemas and metadata |
| `crates/md-storage/src/batch.rs` | Normalized-event to Arrow record-batch builders |
| `crates/md-storage/src/partition.rs` | UTC partition keys, router, flush, rotation, finalization |
| `crates/md-storage/src/recovery.rs` | `.partial` valid-prefix recovery and corrupt-tail isolation |
| `crates/md-storage/src/validate.rs` | Arrow dataset validation and validation report |
| `crates/collector/src/stats.rs` | Atomic counters, latency histograms, periodic snapshots |
| `crates/collector/src/report.rs` | Final structured JSON run report and known gaps |
| `crates/collector/src/app.rs` | Adapter orchestration, router lifecycle, cancellation, error policy |
| `crates/collector/src/main.rs` | clap commands: `collect`, `validate`, and `smoke` |
| `crates/collector/tests/e2e.rs` | Fake-WebSocket end-to-end collection and shutdown test |
| `README.md` | Setup, clock synchronization, commands, Arrow queries, limitations |

---

### Task 1: Bootstrap the Workspace and Validated Configuration

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.gitignore`
- Create: `config/default.toml`
- Create: `crates/md-core/Cargo.toml`
- Create: `crates/md-core/src/lib.rs`
- Create: `crates/md-core/src/config.rs`
- Test: `crates/md-core/tests/config.rs`

**Interfaces:**
- Produces: `CollectorConfig::load(path: &Path) -> Result<CollectorConfig, ConfigError>`
- Produces: `CollectorConfig::validate(&self) -> Result<(), ConfigError>`
- Produces: `AdapterId::{UpbitSpot,BithumbSpot,BinanceSpot,BinanceUsdm}` in Task 2; Task 1 represents adapter keys as validated strings until that enum exists.

- [ ] **Step 1: Create the Cargo workspace and core crate manifests**

Use Rust edition 2024 and four workspace members:

```toml
[workspace]
members = ["crates/md-core", "crates/md-exchanges", "crates/md-storage", "crates/collector"]
resolver = "2"

[workspace.package]
edition = "2024"
rust-version = "1.85"
version = "0.1.0"
license = "MIT"
publish = false
```

Create the crates with `cargo new --lib` for the three libraries and `cargo new --bin` for `collector`. Add current compatible releases with Cargo so `Cargo.lock` records exact resolution:

```powershell
cargo add -p md-core serde --features derive
cargo add -p md-core thiserror toml
cargo add -p md-core uuid --features v7,serde
```

Use this toolchain file so formatting and lint components are always present:

```toml
[toolchain]
channel = "stable"
profile = "minimal"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 2: Write the failing configuration test**

```rust
#[test]
fn default_config_has_required_assets_and_limits() {
    let cfg = md_core::config::CollectorConfig::load(
        std::path::Path::new("../../config/default.toml"),
    ).unwrap();
    assert_eq!(cfg.assets.len(), 21);
    assert_eq!(cfg.channel_capacity, 65_536);
    assert_eq!(cfg.batch_rows, 8_192);
    assert_eq!(cfg.flush_interval_ms, 1_000);
    assert_eq!(cfg.enqueue_timeout_ms, 5_000);
    assert_eq!(cfg.assets.first().unwrap(), "BTC");
    assert_eq!(cfg.assets.last().unwrap(), "OP");
    cfg.validate().unwrap();
}
```

- [ ] **Step 3: Run the test and verify the missing type failure**

Run: `cargo test -p md-core --test config`

Expected: compilation fails because `md_core::config::CollectorConfig` does not exist.

- [ ] **Step 4: Implement configuration parsing and semantic validation**

Define these exact fields:

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CollectorConfig {
    pub output_root: std::path::PathBuf,
    pub assets: Vec<String>,
    pub strict_symbols: bool,
    pub channel_capacity: usize,
    pub batch_rows: usize,
    pub flush_interval_ms: u64,
    pub enqueue_timeout_ms: u64,
    pub stats_interval_secs: u64,
    pub retry: RetryConfig,
    pub adapters: std::collections::BTreeMap<String, AdapterConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RetryConfig {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub reset_after_secs: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AdapterConfig {
    pub enabled: bool,
    pub quote: String,
    pub rest_url: String,
    pub websocket_url: String,
    pub proactive_reconnect_secs: Option<u64>,
}
```

`validate` rejects duplicate/non-uppercase assets, empty paths, zero queue/batch/timeout values, retry initial greater than retry maximum, missing adapter entries, non-HTTPS REST URLs, and non-WSS WebSocket URLs. Populate `config/default.toml` with the exact 21 assets and endpoints from the spec. Ignore `data/`, `target/`, `*.arrow`, `*.arrow.partial`, and smoke output in `.gitignore`.

```toml
output_root = "data"
assets = [
  "BTC", "ETH", "XRP", "SOL", "DOGE", "ADA", "AVAX", "LINK", "DOT",
  "BCH", "LTC", "ETC", "TRX", "XLM", "ATOM", "NEAR", "APT", "SUI",
  "ARB", "OP"
]
strict_symbols = false
channel_capacity = 65536
batch_rows = 8192
flush_interval_ms = 1000
enqueue_timeout_ms = 5000
stats_interval_secs = 10

[retry]
initial_ms = 1000
max_ms = 30000
reset_after_secs = 300

[adapters.upbit_spot]
enabled = true
quote = "KRW"
rest_url = "https://api.upbit.com/v1/market/all"
websocket_url = "wss://api.upbit.com/websocket/v1"

[adapters.bithumb_spot]
enabled = true
quote = "KRW"
rest_url = "https://api.bithumb.com/v1/market/all"
websocket_url = "wss://ws-api.bithumb.com/websocket/v1"

[adapters.binance_spot]
enabled = true
quote = "USDT"
rest_url = "https://api.binance.com/api/v3/exchangeInfo"
websocket_url = "wss://stream.binance.com:9443/stream"
proactive_reconnect_secs = 82800

[adapters.binance_usdm]
enabled = true
quote = "USDT"
rest_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
websocket_url = "wss://fstream.binance.com/stream"
proactive_reconnect_secs = 82800
```

- [ ] **Step 5: Run configuration tests and workspace metadata**

Run: `cargo test -p md-core --test config`

Expected: PASS.

Run: `cargo metadata --format-version 1 --no-deps`

Expected: four workspace packages are listed.

- [ ] **Step 6: Commit the configuration foundation**

```powershell
git add Cargo.toml Cargo.lock rust-toolchain.toml .gitignore config crates/md-core crates/md-exchanges/Cargo.toml crates/md-storage/Cargo.toml crates/collector/Cargo.toml
git commit -m "feat: bootstrap market data collector workspace"
```

---

### Task 2: Add Exact Domain Types, Timestamps, Decimals, and Validation

**Files:**
- Create: `crates/md-core/src/model.rs`
- Create: `crates/md-core/src/decimal.rs`
- Create: `crates/md-core/src/validation.rs`
- Modify: `crates/md-core/src/lib.rs`
- Test: `crates/md-core/tests/model.rs`

**Interfaces:**
- Produces: `parse_decimal_18(text: &str) -> Result<i128, DecimalError>`
- Produces: `ms_to_us(value: i64) -> Result<i64, TimestampError>`
- Produces: `validate_event(event: &NormalizedEvent) -> Result<(), ValidationError>`
- Produces: `NormalizedEvent::{Book(BookSnapshot),Trade(TradeTick)}` consumed by every later task.

- [ ] **Step 1: Write failing decimal, timestamp, and book tests**

```rust
#[test]
fn decimal_preserves_scale_and_rejects_excess_precision() {
    assert_eq!(parse_decimal_18("123.45").unwrap(), 123_450_000_000_000_000_000);
    assert_eq!(parse_decimal_18("0.00000001").unwrap(), 10_000_000_000);
    assert!(parse_decimal_18("1.0000000000000000001").is_err());
}

#[test]
fn millisecond_timestamp_retains_declared_precision() {
    assert_eq!(ms_to_us(1_725_929_934_373).unwrap(), 1_725_929_934_373_000);
}

#[test]
fn crossed_or_unsorted_book_is_rejected() {
    let event = fixture_book(vec![(100, 1), (101, 1)], vec![(102, 1)]);
    assert!(validate_event(&NormalizedEvent::Book(event)).is_err());
}
```

- [ ] **Step 2: Run the tests and verify unresolved symbols**

Run: `cargo test -p md-core --test model`

Expected: compilation fails for missing `parse_decimal_18`, `ms_to_us`, and event types.

- [ ] **Step 3: Implement the shared model**

Define the exact public shapes:

```rust
pub const DECIMAL_PRECISION: u8 = 38;
pub const DECIMAL_SCALE: i8 = 18;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, serde::Serialize)]
pub enum AdapterId { UpbitSpot, BithumbSpot, BinanceSpot, BinanceUsdm }

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
pub enum TimestampPrecision { Microsecond, Millisecond, Unavailable }

#[derive(Debug, Clone, Eq, PartialEq, Hash, serde::Serialize)]
pub struct CanonicalSymbol { pub base: String, pub quote: String }

impl CanonicalSymbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self { base: base.into(), quote: quote.into() }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EventMeta {
    pub schema_version: u16,
    pub event_id: uuid::Uuid,
    pub adapter: AdapterId,
    pub symbol: CanonicalSymbol,
    pub source_symbol: String,
    pub source_stream: String,
    pub source_sequence: Option<u64>,
    pub exchange_event_ts_us: Option<i64>,
    pub exchange_trade_ts_us: Option<i64>,
    pub event_ts_precision: TimestampPrecision,
    pub trade_ts_precision: TimestampPrecision,
    pub local_recv_ts_us: i64,
    pub raw_size_bytes: u32,
}

#[derive(Debug, Clone)]
pub struct PriceLevel { pub price: i128, pub quantity: i128 }
#[derive(Debug, Clone)]
pub struct BookSnapshot { pub meta: EventMeta, pub bids: Vec<PriceLevel>, pub asks: Vec<PriceLevel> }
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
pub enum TakerSide { Buy, Sell, Unknown }
#[derive(Debug, Clone)]
pub struct TradeTick { pub meta: EventMeta, pub trade_id: String, pub price: i128, pub quantity: i128, pub taker_side: TakerSide }
#[derive(Debug, Clone)]
pub enum NormalizedEvent { Book(BookSnapshot), Trade(TradeTick) }

impl NormalizedEvent {
    pub fn meta(&self) -> &EventMeta {
        match self { Self::Book(v) => &v.meta, Self::Trade(v) => &v.meta }
    }
}
```

Implement decimal parsing with checked integer arithmetic: split sign, integer, and fraction; reject exponent notation, more than 18 fractional digits, non-digits, and values beyond 38 digits after scale. Implement timestamp multiplication with `checked_mul(1_000)`.

- [ ] **Step 4: Implement event validation**

Require nonempty sides, positive prices/quantities, strictly descending bids, strictly ascending asks, best bid below best ask, positive local timestamp, and source timestamps within seven days before or one day after local receive time. Return typed errors naming the failing invariant and level.

- [ ] **Step 5: Run core tests**

Run: `cargo test -p md-core`

Expected: PASS with decimal boundary, timestamp overflow, valid book, unsorted book, crossed book, and zero-quantity cases covered.

- [ ] **Step 6: Commit the domain model**

```powershell
git add crates/md-core
git commit -m "feat: add normalized market data model"
```

---

### Task 3: Parse Upbit and Bithumb Books and Trades Through simd-json

**Files:**
- Create: `crates/md-exchanges/src/lib.rs`
- Create: `crates/md-exchanges/src/domestic.rs`
- Create: `crates/md-exchanges/src/upbit.rs`
- Create: `crates/md-exchanges/src/bithumb.rs`
- Create: `crates/md-exchanges/tests/domestic_parsers.rs`
- Create: `crates/md-exchanges/tests/fixtures/upbit_book.json`
- Create: `crates/md-exchanges/tests/fixtures/upbit_trade.json`
- Create: `crates/md-exchanges/tests/fixtures/bithumb_book.json`
- Create: `crates/md-exchanges/tests/fixtures/bithumb_trade.json`

**Interfaces:**
- Produces: `upbit::parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError>`
- Produces: `bithumb::parse_frame(frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError>`
- Consumes: Task 2 model, decimal, timestamp, and validation functions.

- [ ] **Step 1: Add parser dependencies**

```powershell
cargo add -p md-exchanges md-core --path crates/md-core
cargo add -p md-exchanges simd-json thiserror ryu
cargo add -p md-exchanges uuid --features v7
```

- [ ] **Step 2: Add official-format fixtures and failing parser assertions**

Each fixture contains one complete, synthetic, non-secret payload matching the documented production shape. Assert exact metadata and values:

```rust
#[test]
fn upbit_trade_maps_both_exchange_times() {
    let mut bytes = include_bytes!("fixtures/upbit_trade.json").to_vec();
    let events = md_exchanges::upbit::parse_frame(&mut bytes, 1_725_929_934_500_000).unwrap();
    let NormalizedEvent::Trade(t) = &events[0] else { panic!("expected trade") };
    assert_eq!(t.meta.exchange_trade_ts_us, Some(1_725_929_934_373_000));
    assert_eq!(t.meta.event_ts_precision, TimestampPrecision::Millisecond);
    assert_eq!(t.taker_side, TakerSide::Buy);
    assert_eq!(t.price, parse_decimal_18("489700").unwrap());
}

#[test]
fn bithumb_book_expands_all_available_levels() {
    let mut bytes = include_bytes!("fixtures/bithumb_book.json").to_vec();
    let events = md_exchanges::bithumb::parse_frame(&mut bytes, 1_725_929_934_500_000).unwrap();
    let NormalizedEvent::Book(book) = &events[0] else { panic!("expected book") };
    assert_eq!(book.bids.len(), 15);
    assert_eq!(book.asks.len(), 15);
    md_core::validation::validate_event(&events[0]).unwrap();
}
```

- [ ] **Step 3: Run and verify missing parser failure**

Run: `cargo test -p md-exchanges --test domestic_parsers`

Expected: compilation fails because the parser modules do not exist.

- [ ] **Step 4: Implement in-place simd-json parsing and decimal extraction**

Use `simd_json::to_borrowed_value(frame)` so the mutable input buffer is reused. `domestic.rs` provides required-field helpers and:

```rust
fn decimal_from_value(value: &simd_json::BorrowedValue<'_>) -> Result<i128, ParseError>
```

String and integer nodes convert directly. Finite floating nodes use `ryu::Buffer::format_finite` before `parse_decimal_18`; nonfinite numbers are rejected. Do not parse a frame a second time with serde_json. Generate one UUIDv7 per source snapshot, not per book level.

- [ ] **Step 5: Cover SIMPLE/DEFAULT aliases and invalid messages**

Map both documented full and abbreviated keys. Recognize `{"status":"UP"}` as a keepalive event returning an empty vector. Reject missing symbols, missing timestamps, negative values, crossed books, unknown event types, and timestamps outside validation bounds with a typed `ParseError`.

- [ ] **Step 6: Run parser tests and commit**

Run: `cargo test -p md-exchanges --test domestic_parsers`

Expected: all Upbit/Bithumb book, trade, keepalive, and invalid-event cases pass.

```powershell
git add crates/md-exchanges
git commit -m "feat: parse upbit and bithumb market data"
```

---

### Task 4: Parse Binance Spot and USDⓈ-M Combined Streams

**Files:**
- Create: `crates/md-exchanges/src/binance.rs`
- Create: `crates/md-exchanges/src/binance_spot.rs`
- Create: `crates/md-exchanges/src/binance_usdm.rs`
- Modify: `crates/md-exchanges/src/lib.rs`
- Create: `crates/md-exchanges/tests/binance_parsers.rs`
- Create: `crates/md-exchanges/tests/fixtures/binance_spot_book.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_spot_trade.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_usdm_book.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_usdm_trade.json`

**Interfaces:**
- Produces: `binance_spot::parse_frame(&mut [u8], i64) -> Result<Vec<NormalizedEvent>, ParseError>`
- Produces: `binance_usdm::parse_frame(&mut [u8], i64) -> Result<Vec<NormalizedEvent>, ParseError>`
- Consumes: Task 2 types and Task 3 decimal extraction behavior.

- [ ] **Step 1: Write failing combined-stream tests**

```rust
#[test]
fn spot_trade_maps_buyer_maker_to_sell_aggressor() {
    let mut bytes = include_bytes!("fixtures/binance_spot_trade.json").to_vec();
    let event = md_exchanges::binance_spot::parse_frame(&mut bytes, 1_672_515_782_200_000).unwrap().remove(0);
    let NormalizedEvent::Trade(t) = event else { panic!("expected trade") };
    assert_eq!(t.trade_id, "12345");
    assert_eq!(t.taker_side, TakerSide::Sell);
    assert_eq!(t.meta.exchange_event_ts_us, Some(1_672_515_782_136_000));
    assert_eq!(t.meta.exchange_trade_ts_us, Some(1_672_515_782_136_000));
}

#[test]
fn spot_partial_book_allows_unavailable_event_time() {
    let mut bytes = include_bytes!("fixtures/binance_spot_book.json").to_vec();
    let event = md_exchanges::binance_spot::parse_frame(&mut bytes, 1_672_515_782_200_000).unwrap().remove(0);
    assert!(event.meta().exchange_event_ts_us.is_none());
}
```

- [ ] **Step 2: Run and confirm missing parser failure**

Run: `cargo test -p md-exchanges --test binance_parsers`

Expected: compilation fails for missing Binance modules.

- [ ] **Step 3: Implement one-pass wrapper and payload parsing**

Parse the combined wrapper shape `{"stream":"btcusdt@trade","data":{"e":"trade"}}` before decoding the complete `data` object. Derive the Spot book symbol from the stream name when absent from payload. For trades use `t`, `p`, `q`, `T`, `E`, and `m`; for books use `lastUpdateId`/`u`, `bids`/`b`, `asks`/`a`, plus `E` and `T` when supplied. Reject `@aggTrade` explicitly so the collector cannot silently collect aggregated trades.

- [ ] **Step 4: Verify all four Binance fixture types**

Run: `cargo test -p md-exchanges --test binance_parsers`

Expected: Spot and Futures book/trade mappings pass, 20 levels are retained, maker-side mapping is correct, and missing Spot book event time remains nullable.

- [ ] **Step 5: Commit Binance parsing**

```powershell
git add crates/md-exchanges
git commit -m "feat: parse binance spot and futures streams"
```

---

### Task 5: Add Market Discovery and Exact Subscription Builders

**Files:**
- Create: `crates/md-exchanges/src/discovery.rs`
- Modify: `crates/md-exchanges/src/upbit.rs`
- Modify: `crates/md-exchanges/src/bithumb.rs`
- Modify: `crates/md-exchanges/src/binance_spot.rs`
- Modify: `crates/md-exchanges/src/binance_usdm.rs`
- Create: `crates/md-exchanges/tests/discovery.rs`
- Create: `crates/md-exchanges/tests/fixtures/upbit_markets.json`
- Create: `crates/md-exchanges/tests/fixtures/bithumb_markets.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_spot_markets.json`
- Create: `crates/md-exchanges/tests/fixtures/binance_usdm_markets.json`

**Interfaces:**
- Produces: `DiscoveryResult { requested: Vec<CanonicalSymbol>, available: Vec<CanonicalSymbol>, missing: Vec<CanonicalSymbol> }`
- Produces: `discover_markets(adapter: AdapterId, client: &reqwest::Client, cfg: &CollectorConfig) -> Result<DiscoveryResult, DiscoveryError>`
- Produces: `build_subscription(adapter: AdapterId, pairs: &[CanonicalSymbol], ticket: uuid::Uuid) -> Result<String, SubscriptionError>`

- [ ] **Step 1: Add HTTPS dependencies and fixture tests**

```powershell
cargo add -p md-exchanges reqwest --no-default-features --features json,rustls-tls
cargo add -p md-exchanges serde --features derive
cargo add -p md-exchanges url
```

Test that inactive Binance symbols are excluded, domestic market codes are uppercase, configured order is stable, and strict validation names every missing pair.

```rust
#[test]
fn binance_subscription_uses_raw_trade_and_depth20() {
    let pairs = vec![CanonicalSymbol::new("BTC", "USDT"), CanonicalSymbol::new("ETH", "USDT")];
    let text = build_subscription(AdapterId::BinanceSpot, &pairs, uuid::Uuid::nil()).unwrap();
    assert!(text.contains("btcusdt@trade"));
    assert!(text.contains("btcusdt@depth20@100ms"));
    assert!(!text.contains("aggTrade"));
}
```

- [ ] **Step 2: Run and verify missing discovery interfaces**

Run: `cargo test -p md-exchanges --test discovery`

Expected: compilation fails for missing `DiscoveryResult` and builders.

- [ ] **Step 3: Implement adapter-specific discovery decoding**

Decode Upbit/Bithumb arrays of market codes and Binance `symbols` entries. Binance accepts only `status == "TRADING"`; Futures also requires the perpetual contract type. Intersect without changing the configured asset order. Return missing pairs instead of logging inside the library.

- [ ] **Step 4: Implement exact subscription JSON**

Upbit/Bithumb requests contain ticket, one `trade` object, one `orderbook` object, and `DEFAULT` format. Upbit requests `.30` depth codes; Bithumb uses its full 15-level response. Binance creates a combined-stream URL query containing lowercase `@trade` and `@depth20@100ms` names. Percent-encode the query with the `url` crate rather than concatenating untrusted strings.

- [ ] **Step 5: Run discovery tests and commit**

Run: `cargo test -p md-exchanges --test discovery`

Expected: all four fixture decoders, unavailable pairs, strict-mode message, and exact subscription snapshots pass.

```powershell
git add crates/md-exchanges
git commit -m "feat: discover markets and build subscriptions"
```

---

### Task 6: Implement WebSocket Sessions and Supervised Reconnection

**Files:**
- Create: `crates/md-exchanges/src/backoff.rs`
- Create: `crates/md-exchanges/src/runtime.rs`
- Modify: `crates/md-exchanges/src/lib.rs`
- Create: `crates/md-exchanges/tests/runtime.rs`

**Interfaces:**
- Produces: `FrameParser` trait with `parse(&self, frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError>`
- Produces: `AdapterRuntime { id, websocket_url, subscription, proactive_reconnect, parser }`
- Produces: `run_supervised(runtime, tx, shutdown, stats) -> Result<(), RuntimeError>`
- Produces: `ReconnectReason::{PeerClosed,IdleTimeout,Protocol,ParseThreshold,ProactiveRotation,Backpressure}`, `RejectReason::{Parse,Validation,Backpressure}`, and `GapReason::{Disconnected,Backpressure}`.
- Produces: `RuntimeStats` trait methods `on_frame(&self, adapter: AdapterId, bytes: u32)`, `on_events(&self, adapter: AdapterId, books: u64, book_rows: u64, trades: u64)`, `on_parse_error(&self, adapter: AdapterId)`, `on_receive_lag_us(&self, adapter: AdapterId, lag_us: u64)`, `on_queue_depth(&self, adapter: AdapterId, depth: usize)`, `on_reconnect(&self, adapter: AdapterId, reason: ReconnectReason)`, `on_rejected_event(&self, adapter: AdapterId, reason: RejectReason)`, `on_backpressure_disconnect(&self, adapter: AdapterId)`, `open_gap(&self, adapter: AdapterId, reason: GapReason)`, and `close_gap(&self, adapter: AdapterId)`.
- Consumes: `tokio::sync::mpsc::Sender<NormalizedEvent>` and Task 5 subscription result.

- [ ] **Step 1: Add async runtime dependencies**

```powershell
cargo add -p md-exchanges async-trait futures-util rand
cargo add -p md-exchanges tokio --features macros,net,rt-multi-thread,sync,time
cargo add -p md-exchanges tokio-util --features rt
cargo add -p md-exchanges tokio-tungstenite --no-default-features --features connect,rustls-tls-webpki-roots
cargo add -p md-exchanges tracing
```

- [ ] **Step 2: Write paused-time backoff tests**

```rust
#[test]
fn capped_backoff_resets_after_healthy_window() {
    let mut b = Backoff::without_jitter(1_000, 30_000, 300_000);
    assert_eq!(b.next_delay_ms(0), 1_000);
    assert_eq!(b.next_delay_ms(0), 2_000);
    assert_eq!(b.next_delay_ms(0), 4_000);
    assert_eq!(b.next_delay_ms(301_000), 1_000);
}
```

- [ ] **Step 3: Write a failing local WebSocket integration test**

Start a `TcpListener` on `127.0.0.1:0`, accept with `tokio_tungstenite::accept_async`, record the subscription, send one fixture frame and a ping, close the connection, accept again, and assert the subscription is resent. Set channel capacity to one and assert a five-second paused-time enqueue timeout increments `backpressure_disconnects` and `rejected_events`.

- [ ] **Step 4: Run and verify missing runtime failure**

Run: `cargo test -p md-exchanges --test runtime`

Expected: compilation fails because `Backoff` and `run_supervised` do not exist.

- [ ] **Step 5: Implement a single WebSocket session**

Capture `SystemTime::now()` immediately after receiving a binary/text frame and before converting it to mutable bytes. Reply to protocol Ping with Pong, ignore Pong, treat Close as reconnectable, send Upbit/Bithumb application `PING` on idle keepalive, and bound every event send with `tokio::time::timeout(Duration::from_secs(5), tx.send(event))`.

- [ ] **Step 6: Implement the supervisor loop**

Apply 1s→2s→4s capped at 30s plus injected jitter, reset after five healthy minutes, and resubscribe every session. Treat cancellation as success. Treat invalid static URL/configuration as fatal. Reconnect after ten consecutive parse errors and before `proactive_reconnect` expires. Emit structured reconnect reasons through a `RuntimeStats` trait so the collector owns metric storage.

- [ ] **Step 7: Run runtime tests and commit**

Run: `cargo test -p md-exchanges --test runtime`

Expected: subscription, ping/pong, close/reconnect, parser threshold, proactive reconnect, backoff reset, queue timeout, and cancellation tests pass.

```powershell
git add crates/md-exchanges
git commit -m "feat: supervise websocket market feeds"
```

---

### Task 7: Build Arrow Schemas, Record Batches, and Round-Trip Writers

**Files:**
- Create: `crates/md-storage/src/lib.rs`
- Create: `crates/md-storage/src/schema.rs`
- Create: `crates/md-storage/src/batch.rs`
- Modify: `crates/md-storage/Cargo.toml`
- Create: `crates/md-storage/tests/roundtrip.rs`

**Interfaces:**
- Produces: `book_schema(meta: &SchemaContext) -> Arc<Schema>` and `trade_schema(meta: &SchemaContext) -> Arc<Schema>`
- Produces: `SchemaContext { adapter: AdapterId, symbol: CanonicalSymbol, utc_hour: chrono::DateTime<chrono::Utc> }`
- Produces: `BookBatchBuilder::push(&BookSnapshot)`, `TradeBatchBuilder::push(&TradeTick)`
- Produces: `finish() -> Result<RecordBatch, StorageError>` and `len() -> usize`

- [ ] **Step 1: Add Arrow dependencies**

```powershell
cargo add -p md-storage md-core --path crates/md-core
cargo add -p md-storage arrow-array arrow-ipc arrow-schema chrono thiserror uuid
```

- [ ] **Step 2: Write failing schema and round-trip tests**

```rust
#[test]
fn book_batch_expands_snapshot_and_keeps_one_event_id() {
    let book = fixture_book_with_two_levels_per_side();
    let mut builder = BookBatchBuilder::new(context());
    builder.push(&book).unwrap();
    let batch = builder.finish().unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.schema().field_with_name("price").unwrap().data_type(),
        &DataType::Decimal128(38, 18));
    assert_one_distinct_event_id(&batch);
}
```

Write the batch with `arrow_ipc::writer::StreamWriter`, reopen it with `StreamReader`, and assert every metadata, decimal, side, level, and nullable timestamp value matches.

- [ ] **Step 3: Run and verify missing storage failure**

Run: `cargo test -p md-storage --test roundtrip`

Expected: compilation fails for missing schemas and builders.

- [ ] **Step 4: Implement self-describing schemas**

Both schemas include all shared metadata. Books add `side: UInt8`, `level: UInt16`, `price`, and `quantity`. Trades add `trade_id: Utf8`, `price`, `quantity`, and `taker_side: UInt8`. Store UUIDv7 as `FixedSizeBinary(16)`. Attach project, schema version, timestamp unit, decimal scale, exchange, market, symbol, and UTC-hour schema metadata.

- [ ] **Step 5: Implement builders with strict type conversion**

Use Decimal128 builders configured with precision 38 and scale 18. Reject a snapshot before adding any rows if validation fails. Map each side in best-to-worst order and duplicate shared metadata across its rows. Reset a builder only after `finish` succeeds.

- [ ] **Step 6: Run Arrow tests and commit**

Run: `cargo test -p md-storage --test roundtrip`

Expected: book and trade round trips pass with null timestamps, Unicode-free path-safe symbols, decimal extremes, and multiple snapshots.

```powershell
git add crates/md-storage
git commit -m "feat: encode market data as arrow batches"
```

---

### Task 8: Add Hourly Partition Routing, Flush, Finalization, and Recovery

**Files:**
- Create: `crates/md-storage/src/partition.rs`
- Create: `crates/md-storage/src/recovery.rs`
- Modify: `crates/md-storage/src/lib.rs`
- Create: `crates/md-storage/tests/partition.rs`
- Create: `crates/md-storage/tests/recovery.rs`

**Interfaces:**
- Produces: `PartitionKey::for_event(&NormalizedEvent) -> Result<PartitionKey, StorageError>`
- Produces: `PartitionRouter::open(config: StorageConfig) -> Result<Self, StorageError>`
- Produces: `StorageConfig { output_root: PathBuf, batch_rows: usize, flush_interval: Duration }`
- Produces: `push(event)`, `flush_due(now)`, and `shutdown()` async methods
- Produces: `recover_partial(path: &Path) -> Result<RecoveryOutcome, RecoveryError>`

- [ ] **Step 1: Write failing path and hour-rotation tests**

```rust
#[test]
fn partition_path_is_utc_and_event_specific() {
    let key = PartitionKey::from_parts(AdapterId::BinanceUsdm, "BTC", "USDT", 1_725_929_934_373_000).unwrap();
    assert_eq!(key.book_path(Path::new("data")),
        PathBuf::from("data/binance/usdm_futures/BTC-USDT/2024-09-10/01/books.arrow.partial"));
}
```

Use a manually advanced clock to send events at `01:59:59.999999` and `02:00:00.000000`; assert the first file finalizes and the second writer opens in the new partition.

- [ ] **Step 2: Write failing truncation-recovery tests**

Create a stream with three complete batches, truncate within the fourth batch, run recovery, then assert the three complete batches remain readable, corrupt trailing bytes move to a `.corrupt` sibling, and the recovery report records kept/rejected bytes.

- [ ] **Step 3: Run and verify missing partition/recovery failure**

Run: `cargo test -p md-storage --test partition --test recovery`

Expected: compilation fails for missing router and recovery functions.

- [ ] **Step 4: Implement safe partition paths and writer lifecycle**

Only enums and validated ASCII symbols may form paths. Maintain separate book/trade writers per key. Flush at 8,192 rows or elapsed one second. On rotation or shutdown, finish the Arrow stream, flush the buffered file, close the handle, and rename `.arrow.partial` to `.arrow`. If the final path already exists, merge readable batches through a new temporary stream before replacing it; never overwrite an unreadable final file.

- [ ] **Step 5: Implement valid-prefix recovery**

Read batches sequentially with `StreamReader`. Accumulate successfully decoded batches. On trailing IPC error or EOF without a stream terminator, write the valid batches to a new sibling temporary stream, preserve rejected bytes in a timestamped `.corrupt` file, then replace the partial only after the clean stream closes. Return counts in:

```rust
pub struct RecoveryOutcome {
    pub batches_kept: usize,
    pub rows_kept: usize,
    pub bytes_kept: u64,
    pub bytes_rejected: u64,
    pub corrupt_path: Option<PathBuf>,
}
```

- [ ] **Step 6: Run partition, recovery, and full storage tests**

Run: `cargo test -p md-storage`

Expected: flush-size, flush-time, UTC rotation, shutdown rename, existing-final merge, recoverable truncation, unrecoverable header, and invalid path tests pass.

- [ ] **Step 7: Commit durable storage**

```powershell
git add crates/md-storage
git commit -m "feat: rotate and recover hourly arrow streams"
```

---

### Task 9: Add Statistics, Gap Accounting, Reports, and Collector Orchestration

**Files:**
- Create: `crates/collector/src/stats.rs`
- Create: `crates/collector/src/report.rs`
- Create: `crates/collector/src/app.rs`
- Create: `crates/collector/src/lib.rs`
- Modify: `crates/collector/Cargo.toml`
- Test: `crates/collector/tests/app.rs`

**Interfaces:**
- Produces: `CollectorApp::new(config) -> Result<Self>` and `CollectorApp::run(shutdown) -> Result<RunReport>`
- Produces: `StatsRegistry` implementing Task 6 `RuntimeStats`
- Produces: `RunReport::write_json(path: &Path) -> Result<()>`
- Consumes: Task 5 discovery, Task 6 supervisors, and Task 8 partition router.

- [ ] **Step 1: Add collector dependencies**

```powershell
cargo add -p collector md-core --path crates/md-core
cargo add -p collector md-exchanges --path crates/md-exchanges
cargo add -p collector md-storage --path crates/md-storage
cargo add -p collector anyhow hdrhistogram serde_json tracing humantime
cargo add -p collector serde --features derive
cargo add -p collector tokio --features macros,rt-multi-thread,signal,sync,time
cargo add -p collector tokio-util --features rt
```

- [ ] **Step 2: Write failing statistics and fatal-storage tests**

Assert an adapter snapshot contains frames, bytes, book events/rows, trades, parse errors, rejected events, reconnect reasons, queue high-water mark, backpressure disconnects, known gap duration, and p50/p95/p99 receive lag. Inject a writer that fails after one batch and assert `CollectorApp::run` cancels all adapters and returns failure instead of continuing.

- [ ] **Step 3: Run and verify missing app interfaces**

Run: `cargo test -p collector --test app`

Expected: compilation fails for missing `CollectorApp` and `StatsRegistry`.

- [ ] **Step 4: Implement lock-light statistics and known gaps**

Use atomics for counters and a short-held mutex per latency histogram. Record receive lag only when `exchange_event_ts_us` exists and lag is nonnegative. Open a gap on backpressure timeout or reconnect; close it after successful resubscription. Serialize exact start/end/reason fields in the final report.

- [ ] **Step 5: Implement orchestration and shutdown ordering**

Discover markets first, fail strict mode before opening writers, recover partial files, create the 65,536-entry channel, spawn the router, then spawn one supervisor per enabled adapter. On Ctrl+C/cancellation stop adapters, drop all senders, drain the receiver, shut down storage, and write `run-report.json`. Any storage failure cancels the shared token and becomes the process error; adapter connection errors stay supervised.

- [ ] **Step 6: Add 10-second structured snapshots**

Emit one compact JSON tracing event per adapter with the fields from the spec. Never include raw market frames. A test with paused Tokio time asserts one snapshot per configured interval and a final snapshot on shutdown.

- [ ] **Step 7: Run collector library tests and commit**

Run: `cargo test -p collector --test app`

Expected: statistics, latency percentiles, missing markets, gap open/close, fatal storage, independent adapter failure, drain order, and final report tests pass.

```powershell
git add crates/collector
git commit -m "feat: orchestrate feeds and report collection health"
```

---

### Task 10: Implement CLI Commands, Dataset Validation, and End-to-End Fake Collection

**Files:**
- Create: `crates/md-storage/src/validate.rs`
- Modify: `crates/md-storage/src/lib.rs`
- Create: `crates/collector/src/main.rs`
- Create: `crates/collector/tests/e2e.rs`
- Create: `crates/md-storage/tests/validate.rs`

**Interfaces:**
- Produces: `validate_path(path: &Path) -> Result<ValidationReport, DatasetError>`
- Produces CLI: `collector collect`, `collector validate`, `collector smoke`
- Consumes: `CollectorApp` and finalized Arrow files.

- [ ] **Step 1: Add clap and CLI-test dependencies**

```powershell
cargo add -p collector clap --features derive
cargo add -p collector tracing-subscriber --features env-filter,json
cargo add -p collector --dev assert_cmd predicates tempfile
cargo add -p md-storage --dev tempfile
```

- [ ] **Step 2: Write failing validator tests**

Create valid files plus files with a wrong schema version, unsorted levels, duplicate level within one side/event, invalid decimal metadata, timestamp outside the partition hour, and unreadable trailing data. Assert the report returns exact error codes and paths, not only free-form messages.

- [ ] **Step 3: Write failing CLI tests**

```rust
#[test]
fn validate_command_returns_nonzero_for_bad_dataset() {
    let bad = create_bad_dataset();
    assert_cmd::Command::cargo_bin("collector").unwrap()
        .args(["validate", "--path", bad.to_str().unwrap()])
        .assert().failure().stderr(predicates::str::contains("UNSORTED_BOOK"));
}
```

- [ ] **Step 4: Run and verify missing CLI/validator failure**

Run: `cargo test -p md-storage --test validate`

Run: `cargo test -p collector --test e2e`

Expected: compilation fails for missing validator and CLI.

- [ ] **Step 5: Implement recursive validation**

For every `.arrow` file, validate schema metadata, field types, event grouping, contiguous zero-based levels, bid/ask order, positive decimals, source precision, and timestamps. Cross-check exchange/market/symbol/hour in schema metadata against the path. Produce `ValidationReport { files, batches, rows, errors: Vec<ValidationIssue> }` and JSON output.

- [ ] **Step 6: Implement exact clap surface**

```text
collector collect --config <path> [--strict-symbols]
collector validate --path <file-or-root> [--json]
collector smoke --config <path> --duration <humantime> [--output <path>]
```

`smoke` forces an isolated output path, cancels after the duration, validates the result, and writes `smoke-report.json`. It fails if there is any parse error, rejected event, backpressure disconnect, invalid file, or no event from an available high-volume BTC pair.

- [ ] **Step 7: Build a full fake-exchange end-to-end test**

Run four local fake WebSocket servers plus local discovery HTTP responses. Feed all eight fixtures, force one reconnect, cancel, then assert finalized book/trade files for all adapters reopen, contain expected rows, pass `validate_path`, and the run report records one intentional reconnect with zero rejected events.

- [ ] **Step 8: Run CLI and end-to-end tests and commit**

Run: `cargo test -p md-storage --test validate`

Run: `cargo test -p collector --test e2e`

Expected: valid/invalid dataset cases and the four-adapter fake collection pass.

```powershell
git add crates/md-storage crates/collector
git commit -m "feat: validate datasets and expose collector cli"
```

---

### Task 11: Document, Quality-Gate, and Run the Live Smoke Test

**Files:**
- Create: `README.md`
- Create: `docs/data-schema.md`
- Create when network succeeds: `outputs/smoke-report.json`
- Create when network succeeds: `outputs/sample-data/`
- Modify: any source file required to remove warnings or correct smoke-test defects

**Interfaces:**
- Produces: user commands and the acceptance evidence required by the spec.
- Consumes: complete workspace from Tasks 1–10.

- [ ] **Step 1: Write README setup and operating instructions**

Document Rust installation through rustup, `cargo build --release`, all three CLI commands, config fields, output layout, public/no-key operation, Windows/macOS/Linux clock-sync checks, Ctrl+C semantics, disk-full behavior, `.partial` recovery, strict symbols, and troubleshooting. State explicitly that microsecond representation does not imply microsecond clock accuracy and that this phase contains no strategy or order execution.

- [ ] **Step 2: Document the Arrow schemas**

List every field, Arrow type, nullability, enum encoding, Decimal128 scale, timestamp precision behavior, path partition, and one Python/PyArrow reading example:

```python
import pyarrow.ipc as ipc
with open("data/binance/spot/BTC-USDT/2026-08-19/12/trades.arrow", "rb") as f:
    table = ipc.open_stream(f).read_all()
print(table.select(["exchange_trade_ts_us", "price", "quantity", "taker_side"]).slice(0, 5))
```

- [ ] **Step 3: Run formatting, linting, and all tests**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Run: `cargo test --workspace`

Expected: all commands exit 0 with no warnings.

- [ ] **Step 4: Build the release binary**

Run: `cargo build --release -p collector`

Expected: `target/release/collector` (or `collector.exe` on Windows) exists.

- [ ] **Step 5: Run the 60-second live smoke test**

Run:

```powershell
target/release/collector.exe smoke --config config/default.toml --duration 60s --output outputs/live-smoke
```

Expected: reachable adapters collect data, active BTC pairs have nonzero books/trades, finalized Arrow files validate, and the smoke report records zero parse errors, rejected events, and backpressure disconnects. Missing pairs are listed without being hidden. If the environment blocks exchange networking, preserve the passing fake end-to-end test and give this exact command to the user; do not claim live validation occurred.

- [ ] **Step 6: Inspect generated evidence**

Run: `target/release/collector.exe validate --path outputs/live-smoke --json`

Expected: exit 0 and a JSON report with zero validation issues. Retain a small BTC sample and report under `outputs/`; keep bulk live data ignored.

- [ ] **Step 7: Commit documentation and verified fixes**

```powershell
git add README.md docs/data-schema.md crates Cargo.toml Cargo.lock config .gitignore
git commit -m "docs: finish market data collector handoff"
```

Do not add bulk market data or credentials to Git.

---

## Plan Self-Review Results

- Every Phase 1 spec requirement maps to Tasks 1–11.
- All later-phase strategy, feature, order, and GUI work remains excluded.
- Shared names are consistent: `NormalizedEvent`, `EventMeta`, `AdapterId`, `PartitionRouter`, `StatsRegistry`, `RunReport`, and `validate_path` are defined before use.
- The plan contains no secret-dependent step; all live inputs are public.
- Live validation is distinguished from deterministic fake-server validation and cannot be reported as passing if network access is unavailable.
