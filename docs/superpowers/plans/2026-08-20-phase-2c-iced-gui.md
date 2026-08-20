# Phase 2C Iced GUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a responsive cross-platform `iced` GUI with all five approved views, read-only Phase 2B data, visible safety/freshness states, and control contracts that cannot bypass risk or place orders before execution engines exist.

**Architecture:** The data plane publishes immutable coalesced `UiSnapshot` values through a bounded latest-value bridge; the `iced` reducer owns only presentation state. Screens render domain view models and emit typed `OperatorCommand` messages to a disabled-by-default command gateway, while all market, storage, and feature processing continues independently of render cadence.

**Tech Stack:** Rust 2024 (Rust 1.85+), iced 0.13.1 with Tokio and Canvas features, Tokio watch channels, exact decimal formatting, serde, tracing, platform CI on Windows/macOS/Linux.

**Spec:** `docs/superpowers/specs/2026-08-20-funding-arbitrage-hft-phase-2-design.md`

## Global Constraints

- Phase 2A and Phase 2B completion gates remain green.
- Pin `iced = "=0.13.1"`: it supports Rust 1.80+, while iced 0.14 requires Rust 1.88 and would violate this workspace's Rust 1.85 floor.
- The GUI is required and must build on Windows, macOS, and Linux; use no platform-specific native widget API.
- Market/order processing must never await rendering. UI updates use a bounded latest-value channel and may drop only superseded UI snapshots.
- The GUI may read immutable snapshots and submit typed operator commands; it may not call venue clients, journals, or executors directly.
- All five views are present: Funding Opportunities, Market Detail, Strategy and Orders, System Health, Risk and Controls.
- Phase 2C is read-only. Strategy/order/risk controls are visibly disabled with reason `EXECUTION_ENGINE_UNAVAILABLE`.
- Risk defaults are labeled “testnet research defaults,” never production recommendations.
- Indicative APR is labeled display-only; stale/invalid inputs cannot be rendered as valid opportunities.
- Secrets, environment-variable values, request signatures, and credential status details are never displayed or serialized into UI snapshots.
- A slow/failed renderer disarms future automated testnet entry once execution exists; Phase 2 does not claim survival of a process-wide crash.

---

## File Map

| Path | Responsibility |
|---|---|
| `crates/funding-app/Cargo.toml` | Add iced Canvas/Tokio dependencies behind the default `gui` feature |
| `crates/funding-app/src/ui/mod.rs` | GUI launch boundary and module exports |
| `crates/funding-app/src/ui/model.rs` | Immutable UI snapshots, rows, charts, health, strategy, and risk view models |
| `crates/funding-app/src/ui/bridge.rs` | Coalesced latest-value publisher/subscriber and health heartbeat |
| `crates/funding-app/src/ui/reducer.rs` | Pure application state/message reducer and selection/sorting/filtering |
| `crates/funding-app/src/ui/theme.rs` | Accessible colors, spacing, typography, status styles |
| `crates/funding-app/src/ui/chart.rs` | Bounded time-series model and iced Canvas rendering |
| `crates/funding-app/src/ui/views/opportunities.rs` | Funding table and exclusions |
| `crates/funding-app/src/ui/views/market.rs` | Books, mid/microprice, basis, OI, ratios, flow, latency |
| `crates/funding-app/src/ui/views/strategy.rs` | State machine, legs, orders, fills, PnL attribution, reconciliation |
| `crates/funding-app/src/ui/views/health.rs` | Throughput, gaps, backpressure, REST budget, storage, connections |
| `crates/funding-app/src/ui/views/risk.rs` | Mode/safety state and typed, confirmed controls |
| `crates/funding-app/src/main.rs` | Add `funding-app gui --config config/funding.toml` |
| `crates/funding-app/tests/ui_reducer.rs` | Pure UI behavior and secret-redaction tests |
| `crates/funding-app/tests/ui_bridge.rs` | Slow-renderer coalescing and failure/disarm tests |
| `.github/workflows/phase2-gui.yml` | Format, lint, tests, and GUI build matrix |

---

### Task 1: Define Immutable UI and Operator-Command Contracts

**Files:**
- Modify: `crates/funding-app/Cargo.toml`
- Create: `crates/funding-app/src/ui/mod.rs`
- Create: `crates/funding-app/src/ui/model.rs`
- Modify: `crates/funding-app/src/lib.rs`
- Create: `crates/funding-app/tests/ui_model.rs`

**Interfaces:**
- Produces: `UiSnapshot`, `OpportunityRow`, `MarketDetailView`, `StrategyOrdersView`, `SystemHealthView`, `RiskControlsView`.
- Produces: `OperatorCommand`, `ControlAvailability`, `ModeLabel`, and `DisarmReason`.
- Later Phase 2E plans consume these command types without changing their names.

- [ ] **Step 1: Write the failing snapshot/safety test**

```rust
#[test]
fn phase2c_snapshot_has_five_views_and_disabled_execution() {
    let snapshot = UiSnapshot::from_engine(engine_snapshot(), system_snapshot());
    assert!(!snapshot.opportunities.rows.is_empty());
    assert!(snapshot.market.selected.is_some());
    assert_eq!(snapshot.strategy.availability, ControlAvailability::Disabled {
        code: "EXECUTION_ENGINE_UNAVAILABLE".into(),
    });
    assert_eq!(snapshot.risk.mode, ModeLabel::Monitor);
    assert!(!snapshot.debug_text().contains("API_SECRET"));
}
```

- [ ] **Step 2: Run the model test and confirm failure**

Run: `cargo test -p funding-app --test ui_model`

Expected: FAIL because the `ui` module and contracts do not exist.

- [ ] **Step 3: Define the exact immutable snapshot**

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct UiSnapshot {
    pub sequence: u64,
    pub generated_at_us: i64,
    pub opportunities: FundingOpportunitiesView,
    pub market: MarketDetailView,
    pub strategy: StrategyOrdersView,
    pub health: SystemHealthView,
    pub risk: RiskControlsView,
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ControlAvailability {
    Enabled,
    Disabled { code: String },
}
```

Rows store exact domain values as `i128` plus presentation labels generated by one decimal formatter. Do not put clients, API keys, environment values, or mutable strategy references in a snapshot.

- [ ] **Step 4: Map Phase 2B snapshots into view models**

Sort opportunity rows by expected net PnL descending, capacity descending, configured symbol order. Preserve raw gap, interval-normalized gap, indicative APR, conservative net, capacity, confidence/sample count, input age, and exclusion codes. Strategy and risk views use explicit unavailable states with stable code `EXECUTION_ENGINE_UNAVAILABLE`, not fabricated zero positions.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test ui_model`

Expected: PASS.

```bash
git add Cargo.lock crates/funding-app
git commit -m "feat: define funding gui view models"
```

---

### Task 2: Build the Latest-Value UI Bridge

**Files:**
- Create: `crates/funding-app/src/ui/bridge.rs`
- Create: `crates/funding-app/tests/ui_bridge.rs`

**Interfaces:**
- Produces: `UiSnapshotPublisher::publish(snapshot)`, `UiSnapshotSubscriber::changed().await`, and `UiHealthSignal`.
- Consumes: immutable `UiSnapshot` from Task 1.

- [ ] **Step 1: Write paused-time coalescing and failure tests**

```rust
#[tokio::test]
async fn slow_subscriber_observes_latest_snapshot_without_blocking_publisher() {
    let (publisher, mut subscriber) = ui_snapshot_channel(UiSnapshot::empty());
    for sequence in 1..=10_000 {
        publisher.publish(UiSnapshot::with_sequence(sequence)).unwrap();
    }
    subscriber.changed().await.unwrap();
    assert_eq!(subscriber.borrow().sequence, 10_000);
    assert_eq!(publisher.superseded_count(), 9_999);
}

#[test]
fn lost_ui_heartbeat_produces_disarm_signal() {
    let signal = ui_health(Duration::from_secs(3), Duration::from_secs(2));
    assert_eq!(signal, UiHealthSignal::Disarm(DisarmReason::UiHeartbeatLost));
}
```

- [ ] **Step 2: Verify missing bridge failure**

Run: `cargo test -p funding-app --test ui_bridge`

Expected: FAIL on unresolved channel and health functions.

- [ ] **Step 3: Implement the bounded bridge**

Wrap `tokio::sync::watch`; publishing calls `send_replace` and compares the previous published sequence with an atomic renderer-acknowledged sequence to count superseded snapshots. `UiSnapshotSubscriber::acknowledge(sequence)` advances that acknowledgement monotonically after rendering. Keep data-plane publishing synchronous and nonblocking. The subscriber exposes cloned immutable snapshots only.

- [ ] **Step 4: Implement heartbeat/disarm semantics**

Track the most recent renderer acknowledgement separately from market freshness. When it exceeds two seconds, emit a `UiHealthSignal::Disarm`; do not cancel data collection, storage, reconciliation, or exposure monitoring. A process restart still requires normal startup reconciliation in Phase 2E.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test ui_bridge`

Expected: PASS in less than one second with paused Tokio time.

```bash
git add crates/funding-app
git commit -m "feat: add nonblocking gui snapshot bridge"
```

---

### Task 3: Implement the Reducer, Navigation, Sorting, and Filtering

**Files:**
- Create: `crates/funding-app/src/ui/reducer.rs`
- Create: `crates/funding-app/src/ui/theme.rs`
- Create: `crates/funding-app/tests/ui_reducer.rs`

**Interfaces:**
- Produces: `FundingGuiState::update(message) -> Option<OperatorCommand>`.
- Produces: `Screen::{Opportunities,Market,Strategy,Health,Risk}` and `SortKey`.

- [ ] **Step 1: Write pure reducer tests**

```rust
#[test]
fn selection_sort_filter_and_confirmation_are_deterministic() {
    let mut state = FundingGuiState::new(snapshot_with_three_rows());
    state.update(Message::SortBy(SortKey::ExpectedNetPnl));
    state.update(Message::FilterTextChanged("BTC".into()));
    state.update(Message::SelectSymbol(symbol("BTC")));
    assert_eq!(state.visible_rows().len(), 1);
    assert_eq!(state.selected_symbol(), Some(&symbol("BTC")));
    assert_eq!(state.update(Message::CancelAllPressed), None);
    assert!(state.confirmation().is_some());
}

#[test]
fn disabled_controls_emit_no_operator_command() {
    let mut state = FundingGuiState::new(phase2c_snapshot());
    assert_eq!(state.update(Message::ArmTestnetPressed), None);
    assert_eq!(state.last_notice_code(), Some("EXECUTION_ENGINE_UNAVAILABLE"));
}
```

- [ ] **Step 2: Confirm reducer test failure**

Run: `cargo test -p funding-app --test ui_reducer`

Expected: FAIL because reducer types do not exist.

- [ ] **Step 3: Implement pure reducer transitions**

Reducer state contains selected screen/symbol, stable sort direction, filter text, latest snapshot, confirmation dialog, and notice queue. A command is returned only after exact confirmation text and `ControlAvailability::Enabled`. `CancelAll` and `RequestClosePositions` have different messages and confirmation phrases.

- [ ] **Step 4: Define an accessible theme**

Use semantic colors with text/icon labels so color is not the only state cue: green `RECEIVE`, red `PAY`/`HALTED`, amber `STALE`/`TESTNET`, gray `UNAVAILABLE`. Keep minimum body text 14 px, controls 36 px high, and table numeric columns right-aligned. Use only system fonts.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test ui_reducer`

Expected: PASS.

```bash
git add crates/funding-app
git commit -m "feat: add funding gui reducer"
```

---

### Task 4: Build the Application Shell and Funding/Market Views

**Files:**
- Create: `crates/funding-app/src/ui/chart.rs`
- Create: `crates/funding-app/src/ui/views/mod.rs`
- Create: `crates/funding-app/src/ui/views/opportunities.rs`
- Create: `crates/funding-app/src/ui/views/market.rs`
- Modify: `crates/funding-app/src/ui/mod.rs`
- Create: `crates/funding-app/tests/ui_presenter.rs`

**Interfaces:**
- Produces: `run_gui(initial, subscriber, command_tx) -> iced::Result`.
- Produces: pure `opportunity_presenter` and `market_presenter` functions testable without a renderer.

- [ ] **Step 1: Write presenter tests**

```rust
#[test]
fn opportunity_and_market_presenters_label_semantics() {
    let row = opportunity_presenter(opportunity_fixture());
    assert_eq!(row.short_label, "SHORT Binance");
    assert_eq!(row.long_label, "LONG Bybit");
    assert!(row.apr_label.ends_with("display only"));
    let market = market_presenter(market_fixture());
    assert!(market.ratio_labels.contains(&"Bybit long/short ratio".into()));
    assert!(market.flow_labels.contains(&"snapshot OFI".into()));
}
```

- [ ] **Step 2: Confirm presenter/UI module failure**

Run: `cargo test -p funding-app --test ui_presenter`

Expected: FAIL on missing presenters.

- [ ] **Step 3: Implement the iced application shell**

Use `iced::application("Funding Arbitrage Monitor", update, view)` with a subscription that receives latest snapshots and emits heartbeat acknowledgements. Navigation renders exactly five persistent tabs and one content pane. Window resize must not drop data or reset selection.

- [ ] **Step 4: Implement opportunities and market detail**

Funding table columns: token, short venue/rate/interval, long venue/rate/interval, settlement countdowns, raw gap, hourly gap, display-only APR, gross funding, conservative net, capacity, confidence, freshness, and exclusion. Market detail renders two top-20 books, bounded mid/microprice Canvas series, named basis values, OI, explicitly named ratio metrics, CVD, tick flow, snapshot OFI, imbalance, depth delta, latency, and freshness.

- [ ] **Step 5: Bound chart memory and test it**

```rust
pub struct TimeSeries { max_points: usize, points: VecDeque<(i64, i128)> }
```

On insert, coalesce the same timestamp and evict oldest points above 3,600. Convert `i128` to screen coordinates only at render time; domain snapshots keep exact decimals.

- [ ] **Step 6: Run and commit**

Run: `cargo test -p funding-app --test ui_presenter`

Expected: PASS.

```bash
git add crates/funding-app
git commit -m "feat: render funding and market views"
```

---

### Task 5: Build Strategy, Health, and Risk Views

**Files:**
- Create: `crates/funding-app/src/ui/views/strategy.rs`
- Create: `crates/funding-app/src/ui/views/health.rs`
- Create: `crates/funding-app/src/ui/views/risk.rs`
- Modify: `crates/funding-app/tests/ui_presenter.rs`

**Interfaces:**
- Consumes: the view models and disabled command gateway from Tasks 1-3.
- Produces: all remaining approved screens with stable empty/unavailable states.

- [ ] **Step 1: Extend presenter tests for safety-critical labels**

```rust
#[test]
fn strategy_health_and_risk_views_never_hide_unknown_state() {
    let strategy = strategy_presenter(unavailable_strategy());
    assert_eq!(strategy.state_label, "UNAVAILABLE");
    assert_eq!(strategy.reason_code, "EXECUTION_ENGINE_UNAVAILABLE");
    let risk = risk_presenter(testnet_defaults());
    assert!(risk.banner.contains("testnet research defaults"));
    assert_ne!(risk.cancel_all_confirmation, risk.close_positions_confirmation);
}
```

- [ ] **Step 2: Run and confirm the new tests fail**

Run: `cargo test -p funding-app --test ui_presenter`

Expected: FAIL because three presenters/views are absent.

- [ ] **Step 3: Implement Strategy and Orders view**

Render state/reason, both legs, orders, partial fills, positions, residual delta, predicted/confirmed funding, fee/slippage/basis/funding/total PnL, and streaming/terminal reconciliation metrics. In Phase 2C each execution field renders `—` with `EXECUTION_ENGINE_UNAVAILABLE`; it never renders numeric zero as a substitute.

- [ ] **Step 4: Implement System Health and Risk views**

Health includes frames/events/features/orders per second, parse/validation errors, reconnects, sequence gaps, backpressure, REST headroom, public/private connections, Arrow, and SQLite status. Before Phase 2E, private connections and SQLite render explicit `NOT_INSTALLED` states rather than numeric zero/healthy values. Risk includes mode, arming, strategy enable, cancel-all, close positions, and kill controls. In Phase 2C, mode is monitor and every mutating control is disabled.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test ui_presenter --test ui_reducer`

Expected: PASS.

```bash
git add crates/funding-app
git commit -m "feat: render strategy health and risk views"
```

---

### Task 6: Wire the GUI Command, Cross-Platform CI, and Shutdown

**Files:**
- Modify: `crates/funding-app/src/main.rs`
- Modify: `crates/funding-app/src/lib.rs`
- Create: `crates/funding-app/tests/gui_cli.rs`
- Create: `.github/workflows/phase2-gui.yml`
- Modify: `README.md`

**Interfaces:**
- Produces CLI: `funding-app gui --config config/funding.toml`.
- Produces a supervised read-only data-plane task, UI heartbeat, and graceful shutdown path.

- [ ] **Step 1: Write CLI and shutdown tests**

```rust
#[test]
fn gui_command_has_no_secret_or_order_flags() {
    let help = command().try_get_matches_from(["funding-app", "gui", "--help"]).unwrap_err().to_string();
    assert!(help.contains("--config"));
    assert!(!help.contains("api-secret"));
    assert!(!help.contains("place-order"));
}
```

- [ ] **Step 2: Verify the CLI test fails**

Run: `cargo test -p funding-app --test gui_cli`

Expected: FAIL because the `gui` subcommand is absent.

- [ ] **Step 3: Add the GUI orchestration boundary**

Start the Phase 2B monitor and snapshot publisher on Tokio. Launch iced with its receiver and a bounded command sender whose Phase 2C implementation always returns `EXECUTION_ENGINE_UNAVAILABLE`. On window close, disarm, cancel the data plane, drain/flush Arrow writers, then write the report. If the data plane fails first, render a fatal health state and keep order controls disabled.

- [ ] **Step 4: Add the cross-platform build matrix**

Create a GitHub Actions matrix over `windows-latest`, `macos-latest`, and `ubuntu-latest`. Install Linux GUI packages needed by iced, then run format check, strict clippy, workspace tests, and `cargo build -p funding-app --release --features gui` on every OS.

- [ ] **Step 5: Run Phase 2C gates**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets --offline -- -D warnings`

Run: `cargo test --workspace --offline`

Run: `cargo build -p funding-app --release --offline --features gui`

Expected: all local gates exit 0; CI YAML contains all three operating systems.

- [ ] **Step 6: Commit Phase 2C**

```bash
git add Cargo.lock crates/funding-app .github/workflows/phase2-gui.yml README.md
git commit -m "feat: add iced funding dashboard"
```

---

## Phase 2C Completion Gate

- All five views render valid, stale, excluded, unavailable, and fatal states without fabricating values.
- The UI bridge coalesces superseded snapshots and never blocks market/storage processing.
- Funding table and market detail expose every approved metric and label venue semantics explicitly.
- Strategy/order/risk controls are present but cannot emit commands in Phase 2C.
- `Cancel all` and `close positions` have distinct confirmations.
- Risk limits are visibly labeled testnet research defaults; APR is visibly display-only.
- No snapshot/log/screenshot model contains credential material.
- Reducer/bridge/presenter tests pass and GUI release builds are configured for Windows, macOS, and Linux.
