# Phase 2D Replay and Paper Trading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic Arrow replay, realistic paper execution/funding settlement, funding-pair and fixed-spread strategy state machines, risk gating, full PnL attribution, and paper-mode GUI state without authenticated venue access.

**Architecture:** `funding-strategy` contains pure state machines that emit order intents but cannot execute them. `risk` evaluates immutable pre-trade contexts, while an `execution` crate implements a deterministic paper executor; `funding-app` supplies either a replay clock or live public-data clock and journals paper results to append-only Arrow reports without introducing the transactional live OMS yet.

**Tech Stack:** Rust 2024 (Rust 1.85+), Tokio paused time, exact scale-18 decimals, Apache Arrow IPC 56, seeded `rand_chacha`, serde/JSON, clap, tracing, proptest.

**Spec:** `docs/superpowers/specs/2026-08-20-funding-arbitrage-hft-phase-2-design.md`

## Global Constraints

- Phase 2A, 2B, and 2C completion gates remain green.
- Replay with identical Arrow input, configuration, and seed must produce identical decisions and canonical reports.
- Strategies emit intents only; every intent passes through the risk gate before an executor receives it.
- Paper fills model depth, fees, slippage, latency, partial fills, cancel/fill races, and discrete funding settlements.
- Funding-pair normal holding waits for both initially identified actual settlements; an earlier documented risk exit is always allowed.
- Both legs target equal base-asset delta after contract multipliers and quantity steps; residual default is `max(1 USDT, 0.5% pair notional)`.
- Entry defaults are 1x leverage, at most 100 USDT per leg, one active pair, 10 bps slippage cap, and 10 bps minimum conservative net opportunity.
- PnL is decomposed into funding, execution, cross-venue basis, fees, slippage, and residual mark-to-market.
- No authenticated endpoints, API keys, SQLite order journal, or testnet orders are added in this phase.
- GUI controls may select monitor/paper and enable a paper strategy; testnet arming remains disabled.

---

## File Map

| Path | Responsibility |
|---|---|
| `Cargo.toml` | Add `funding-strategy`, `risk`, and `execution` crates |
| `crates/funding-core/src/order.rs` | Venue-neutral order intents, commands, fills, positions, and IDs |
| `crates/funding-core/src/strategy.rs` | Funding/fixed-spread states, actions, reasons, and snapshots |
| `crates/risk/src/lib.rs` | Risk config, gates, kill/disarm state, rejection codes |
| `crates/funding-strategy/src/funding_pair.rs` | Pure paired funding state machine |
| `crates/funding-strategy/src/fixed_spread.rs` | Pure post-only fixed-spread state machine |
| `crates/execution/src/paper.rs` | Seeded deterministic order book fill and cancel simulator |
| `crates/execution/src/funding.rs` | Discrete paper funding settlement ledger |
| `crates/funding-app/src/replay.rs` | K-way Arrow merge, deterministic clock, seed, event dispatch |
| `crates/funding-app/src/paper.rs` | Live-public/paper orchestration and reports |
| `crates/md-storage/src/decision_schema.rs` | Decisions, simulated orders/fills/funding/PnL Arrow families |
| `crates/funding-app/tests/phase2d_e2e.rs` | Frozen replay determinism and paper strategy lifecycle |

---

### Task 1: Define Orders, Positions, Strategy States, and Actions

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/funding-core/src/lib.rs`
- Create: `crates/funding-core/src/order.rs`
- Create: `crates/funding-core/src/strategy.rs`
- Create: `crates/funding-strategy/Cargo.toml`
- Create: `crates/funding-strategy/src/lib.rs`
- Create: `crates/funding-strategy/tests/contracts.rs`

**Interfaces:**
- Produces: `OrderIntent`, `OrderSide`, `OrderType`, `TimeInForce`, `OrderUpdate`, `ExecutionFill`, `Position`, `BalanceSnapshot`, `FundingIncome`, and `PrivateEvent`.
- Produces: `FundingPairState`, `FixedSpreadState`, `StrategyAction`, `StrategyReason`, and deterministic `ClientOrderId`.

- [ ] **Step 1: Write the failing contract test**

```rust
#[test]
fn deterministic_client_ids_and_states_are_explicit() {
    let correlation = uuid::Uuid::from_u128(7);
    let id = ClientOrderId::derive(correlation, AdapterId::BinanceUsdm, OrderSide::Sell, 3);
    assert_eq!(id.as_str(), "fa-00000000000000000000000000000007-bu-s-000003");
    assert_eq!(FundingPairState::default(), FundingPairState::Idle);
    assert!(FundingPairState::HedgeRetry.is_failure_substate());
}
```

- [ ] **Step 2: Confirm missing contracts**

Run: `cargo test -p funding-strategy --test contracts`

Expected: FAIL because the new crates and order/strategy types do not exist.

- [ ] **Step 3: Define exact order and account-neutral types**

```rust
pub struct OrderIntent {
    pub correlation_id: Uuid,
    pub client_order_id: ClientOrderId,
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
    pub price: Option<i128>,
    pub quantity: i128,
    pub created_at_us: i64,
}

pub enum OrderSide { Buy, Sell }
pub enum OrderType { Limit, MarketableLimit }
pub enum TimeInForce { PostOnly, Ioc, Gtc }
```

`ExecutionFill` contains venue/client/venue order IDs, fill ID, exact price/quantity/fee, fee asset, liquidity role, and source/local timestamps. `Position` contains signed base quantity, average entry, mark, unrealized PnL, and update time.

```rust
pub struct BalanceSnapshot {
    pub venue: AdapterId,
    pub asset: String,
    pub wallet_balance: i128,
    pub available_balance: i128,
    pub source_ts_us: Option<i64>,
    pub local_recv_ts_us: i64,
}

pub struct FundingIncome {
    pub venue: AdapterId,
    pub symbol: CanonicalSymbol,
    pub income_id: String,
    pub settlement_ts_us: i64,
    pub amount: i128,
    pub asset: String,
    pub source_ts_us: Option<i64>,
    pub local_recv_ts_us: i64,
}

pub enum PrivateEvent {
    Order(OrderUpdate),
    Fill(ExecutionFill),
    Position(Position),
    Balance(BalanceSnapshot),
    FundingIncome(FundingIncome),
}
```

Keep public `FundingSettlement` (the venue's settled rate) distinct from private `FundingIncome` (the account's actual cash flow).

- [ ] **Step 4: Define exact state/action enums**

```rust
pub enum FundingPairState {
    Idle, Candidate, Preflight, EnteringBoth, Hedged, WaitingForSettlements,
    SettlementsConfirmed, ReevaluateOrClose, ClosingBoth, Flat,
    HedgeRetry, EmergencyReduce, Reconcile, Halted,
}

pub enum StrategyAction {
    Submit(OrderIntent),
    Cancel { venue: AdapterId, client_order_id: ClientOrderId },
    Reconcile { venue: AdapterId, symbol: CanonicalSymbol },
    RecordDecision(StrategyDecisionRecord),
    Halt { reason: StrategyReason },
}
```

Make every transition reason explicit and serializable. `ClientOrderId::derive` is length-checked against both venue limits and contains correlation, venue, side, and sequence.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-strategy --test contracts`

Expected: PASS.

```bash
git add Cargo.toml Cargo.lock crates/funding-core crates/funding-strategy
git commit -m "feat: define strategy and order contracts"
```

---

### Task 2: Implement Pre-Trade and Runtime Risk Gates

**Files:**
- Create: `crates/risk/Cargo.toml`
- Create: `crates/risk/src/lib.rs`
- Create: `crates/risk/src/config.rs`
- Create: `crates/risk/src/gate.rs`
- Create: `crates/risk/src/state.rs`
- Create: `crates/risk/tests/gate.rs`

**Interfaces:**
- Produces: `RiskGate::evaluate(&PreTradeContext) -> RiskDecision`.
- Produces: `RiskState::apply(RiskSignal)`, `allows_new_orders()`, and global control state.
- Consumes: opportunity, book/account freshness, residual delta, journal capability flag, connection health, and rate headroom.

- [ ] **Step 1: Write table-driven gate tests**

```rust
#[test]
fn every_unsafe_input_blocks_new_orders_with_one_stable_code() {
    let cases = [
        (context().stale_market(), "STALE_MARKET_DATA"),
        (context().private_disconnected(), "PRIVATE_STREAM_UNHEALTHY"),
        (context().unknown_order(), "UNKNOWN_ORDER_STATE"),
        (context().without_margin(), "MARGIN_UNAVAILABLE"),
        (context().without_rate_headroom(), "RATE_LIMIT_HEADROOM"),
        (context().with_net_bps(scale(9)), "NET_PNL_BELOW_MINIMUM"),
    ];
    for (ctx, code) in cases {
        assert_eq!(RiskGate::test_defaults().evaluate(&ctx).rejection_code(), Some(code));
    }
}

#[test]
fn paper_mode_uses_simulator_health_and_does_not_require_a_private_venue_stream() {
    let ctx = context().with_mode(ExecutionMode::Paper).private_stream_not_required();
    assert!(RiskGate::test_defaults().evaluate(&ctx).is_allowed());
}
```

- [ ] **Step 2: Confirm risk crate failure**

Run: `cargo test -p risk --test gate`

Expected: FAIL because `risk` does not exist.

- [ ] **Step 3: Implement validated research defaults**

```rust
pub struct RiskConfig {
    pub leverage: i128,
    pub max_quote_per_leg: i128,
    pub max_active_pairs: u32,
    pub max_entry_slippage_bps: i128,
    pub min_net_opportunity_bps: i128,
    pub market_stale_after_us: i64,
    pub private_stale_after_us: i64,
    pub hedge_budget_us: i64,
    pub residual_quote_floor: i128,
    pub residual_notional_fraction: i128,
}
```

Set exact defaults from the spec and reject non-positive/inconsistent values. Label serialized config/report fields `testnet_research_defaults`.

- [ ] **Step 4: Implement deterministic gate ordering and global state**

Add `ExecutionMode::{Paper,Testnet}` and a capability requirement for each health input. Paper requires deterministic simulator/account-cache health and does not require a venue private stream or SQLite journal; testnet requires both. Check kill/disarm, required journal capability, unknown state, clock, required connections, freshness, instrument rules, margin/liquidation visibility, leverage/notional/pair count, slippage/depth, net PnL, residual delta, then rate capacity. Return the first stable rejection code in that order. `CancelAllRequested` and `ClosePositionsRequested` remain distinct signals.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p risk --test gate`

Expected: PASS for every rejection and allowed safe context.

```bash
git add Cargo.lock crates/risk
git commit -m "feat: add funding strategy risk gates"
```

---

### Task 3: Build Deterministic Arrow Replay

**Files:**
- Create: `crates/funding-app/src/replay.rs`
- Create: `crates/funding-app/tests/replay.rs`
- Modify: `crates/funding-app/Cargo.toml`

**Interfaces:**
- Produces: `ReplayConfig`, `ReplayClock`, `ReplayEvent`, and `run_replay(config, sink) -> ReplayReport`.
- Consumes: validated Phase 1/2A Arrow datasets and emits events in one canonical order.

- [ ] **Step 1: Write deterministic merge tests**

```rust
#[tokio::test]
async fn replay_order_is_stable_across_filesystem_enumeration() {
    let a = run_replay(fixture_config_with_file_order([2, 0, 1]), collecting_sink()).await.unwrap();
    let b = run_replay(fixture_config_with_file_order([0, 1, 2]), collecting_sink()).await.unwrap();
    assert_eq!(a.event_digest, b.event_digest);
    assert_eq!(a.first_event_ts_us, b.first_event_ts_us);
    assert_eq!(a.last_event_ts_us, b.last_event_ts_us);
}
```

- [ ] **Step 2: Confirm replay interface failure**

Run: `cargo test -p funding-app --test replay`

Expected: FAIL because replay types do not exist.

- [ ] **Step 3: Implement canonical k-way merge**

Validate the input path before opening readers. Sort within each stream by `(effective_ts_us, adapter, market, base, quote, source_sequence_or_max, event_id)` and merge streams with the same key. Reject a per-file timestamp regression. `ReplayClock::now_us()` advances only to the emitted event time and never reads wall time.

- [ ] **Step 4: Implement seed and pacing**

```rust
pub struct ReplayConfig {
    pub input_root: PathBuf,
    pub output_root: PathBuf,
    pub seed: u64,
    pub speed: ReplaySpeed,
}
pub enum ReplaySpeed { AsFastAsPossible, Multiplier(u32) }
```

Use `ChaCha8Rng::seed_from_u64(seed)` for all stochastic latency/fill choices. Pacing changes wall time only; event order and results remain equal.

Derive replay correlation IDs, decision IDs, client order IDs, and simulated fill IDs from `(input event ID, seed, strategy ID, action sequence)` with a stable hash/UUID namespace. Replay output must not call UUIDv7 or wall-clock ID generation; otherwise identical runs cannot have equal canonical digests.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test replay`

Expected: PASS for shuffled files, equal timestamps, nullable source time, and two speeds.

```bash
git add Cargo.lock crates/funding-app
git commit -m "feat: add deterministic arrow replay"
```

---

### Task 4: Implement Paper Orders, Partial Fills, and Funding Settlement

**Files:**
- Create: `crates/execution/Cargo.toml`
- Create: `crates/execution/src/lib.rs`
- Create: `crates/execution/src/paper.rs`
- Create: `crates/execution/src/funding.rs`
- Create: `crates/execution/tests/paper.rs`
- Create: `crates/execution/tests/funding.rs`

**Interfaces:**
- Produces: `PaperExecutor::submit`, `cancel`, `on_book`, `on_trade`, `advance_to`, and `snapshot`.
- Produces: `PaperFundingLedger::settle(slot, positions, marks)`.
- Implements common `Executor` trait for paper only; testnet implementations arrive in Phase 2E.

- [ ] **Step 1: Write fill/cancel/settlement tests**

```rust
#[test]
fn marketable_ioc_consumes_depth_and_partially_fills_at_limit() {
    let mut ex = PaperExecutor::new(PaperConfig::deterministic(7));
    ex.on_book(thin_ask_book());
    let updates = ex.submit(buy_ioc(limit("101"), qty("3"))).unwrap();
    assert_eq!(filled_quantity(&updates), qty("2"));
    assert!(updates.iter().any(|u| u.is_expired_remainder()));
}

#[test]
fn settlement_uses_signed_position_mark_notional() {
    let income = PaperFundingLedger::default().settle(
        settled_slot(rate("0.001")),
        &short_position(qty("1")),
        price("100"),
    ).unwrap();
    assert_eq!(income.amount, money("0.1"));
}
```

- [ ] **Step 2: Confirm execution crate failure**

Run: `cargo test -p execution --test paper --test funding`

Expected: FAIL because `execution` does not exist.

- [ ] **Step 3: Define and implement the paper executor**

```rust
pub trait Executor {
    fn submit(&mut self, intent: OrderIntent) -> Result<Vec<OrderUpdate>, ExecutionError>;
    fn cancel(&mut self, client_order_id: &ClientOrderId) -> Result<Vec<OrderUpdate>, ExecutionError>;
    fn snapshot(&self) -> ExecutionSnapshot;
}
```

Marketable IOC consumes only visible eligible depth up to limit and expires remainder. Post-only rejects crossing prices. Resting orders fill only from subsequent trade/book evidence according to a documented seeded queue-ahead fraction. Model configurable latency before acknowledgement/cancel, partial fills, and cancel/fill races. Deduplicate client IDs and fill IDs.

- [ ] **Step 4: Implement fees, slippage, positions, and funding**

Apply maker/taker fee assumptions with source labels. Record decision-mid, fill price, signed slippage, position average price, realized execution PnL, and fee. Funding settlement requires an actual slot timestamp, signed position, and fresh mark; deduplicate `(venue,symbol,settlement_ts)`.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test paper --test funding`

Expected: PASS for full/partial/no fill, post-only rejection, race, duplicate, long/short, and negative funding.

```bash
git add Cargo.lock crates/execution
git commit -m "feat: add deterministic paper execution"
```

---

### Task 5: Implement Funding-Pair and Fixed-Spread State Machines

**Files:**
- Create: `crates/funding-strategy/src/funding_pair.rs`
- Create: `crates/funding-strategy/src/fixed_spread.rs`
- Create: `crates/funding-strategy/tests/funding_pair.rs`
- Create: `crates/funding-strategy/tests/fixed_spread.rs`

**Interfaces:**
- Produces: `FundingPairMachine::on_event(event, context) -> Result<Vec<StrategyAction>, TransitionError>`.
- Produces: `FixedSpreadMachine::on_event(event, context) -> Result<Vec<StrategyAction>, TransitionError>`.
- Consumes: risk decisions before any `Submit` action is forwarded.

- [ ] **Step 1: Write complete lifecycle and illegal-transition tests**

```rust
#[test]
fn pair_enters_hedges_waits_for_both_initial_settlements_then_closes() {
    let mut s = FundingPairMachine::new(candidate_fixture());
    drive(&mut s, [preflight_ok(), both_entry_fills(), first_settlement(), second_settlement(), close_signal(), both_close_fills()]);
    assert_eq!(s.state(), FundingPairState::Flat);
    assert!(s.snapshot().initial_settlements_confirmed());
}

#[test]
fn one_leg_timeout_retries_then_emergency_reduces() {
    let mut s = entering_machine();
    drive(&mut s, [binance_only_fill(), advance_us(2_000_001), hedge_retry_failed()]);
    assert_eq!(s.state(), FundingPairState::EmergencyReduce);
    assert!(s.actions().iter().any(|a| matches!(a, StrategyAction::Submit(i) if i.reduce_only)));
}
```

- [ ] **Step 2: Confirm state-machine test failure**

Run: `cargo test -p funding-strategy --test funding_pair --test fixed_spread`

Expected: FAIL because machines do not exist.

- [ ] **Step 3: Implement funding-pair transitions**

Use the exact normal and failure states from `funding-core`. Preflight quantizes both legs through contract multiplier/step and rejects residual above `max(1 USDT, 0.5%)`. Submit parallel marketable IOC limits within 10 bps. After one-leg fill: cancel residual, enter `HedgeRetry`, enforce two-second event-clock budget, then emit reduce-only emergency action and halt new entries if unresolved. Close normally only when both initial actual funding records are deduplicated and confirmed; risk exit can transition to close earlier with its reason retained.

- [ ] **Step 4: Implement fixed-spread transitions**

Anchor on valid microprice, otherwise valid mid. Round a post-only bid down and ask up by tick. Cancel/replace on anchor movement, age, inventory, imbalance, stale book/account, disable, or kill. Apply partial fills to inventory before replacement and never cross the current book.

- [ ] **Step 5: Run transition properties and commit**

Add proptests that no emitted buy price exceeds its slippage cap, no post-only quote crosses, and every terminal pair has zero residual position. Run: `cargo test -p funding-strategy`.

Expected: PASS.

```bash
git add crates/funding-strategy
git commit -m "feat: add paper strategy state machines"
```

---

### Task 6: Persist Decisions and Integrate Replay/Paper Modes with GUI

**Files:**
- Create: `crates/md-storage/src/decision_schema.rs`
- Modify: `crates/md-storage/src/lib.rs`
- Modify: `crates/md-storage/src/validate.rs`
- Create: `crates/md-storage/tests/decision_roundtrip.rs`
- Create: `crates/funding-app/src/paper.rs`
- Modify: `crates/funding-app/src/main.rs`
- Modify: `crates/funding-app/src/ui/model.rs`
- Modify: `crates/funding-app/src/ui/reducer.rs`
- Create: `crates/funding-app/tests/phase2d_e2e.rs`
- Modify: `README.md`
- Modify: `docs/data-schema.md`

**Interfaces:**
- Produces CLI: `funding-app replay --input data --output replay-output --seed 7`.
- Produces CLI: `funding-app paper --config config/funding.toml --duration 60s --gui`.
- Produces validated decision, simulated order/fill/funding, transition, and PnL streams plus `paper-report.json`.

- [ ] **Step 1: Write byte-stable replay/paper E2E test**

```rust
#[tokio::test]
async fn same_input_config_and_seed_produce_equivalent_reports() {
    let first = run_frozen_pair_replay(7, test_root()).await.unwrap();
    let second = run_frozen_pair_replay(7, test_root()).await.unwrap();
    assert_eq!(first.canonical_decision_digest, second.canonical_decision_digest);
    assert_eq!(first.pnl, second.pnl);
    assert_eq!(first.final_state, FundingPairState::Flat);
    assert!(first.pnl.total() == first.pnl.component_sum());
}
```

- [ ] **Step 2: Confirm integration test failure**

Run: `cargo test -p funding-app --test phase2d_e2e`

Expected: FAIL on missing paper orchestration and decision storage.

- [ ] **Step 3: Persist every decision and simulated execution fact**

Create Arrow families `strategy_decision`, `strategy_transition`, `paper_order`, `paper_fill`, `paper_funding_income`, and `paper_pnl`. Each row includes correlation/client IDs, exact inputs, risk decision/reason, simulator seed, source event ID, and timestamps. Validator enforces lifecycle ordering, unique fill IDs, PnL component sum, and family/path metadata.

- [ ] **Step 4: Wire replay and live-public paper orchestration**

Run market/derivative events through the feature engine, ranker, strategy, risk gate, and paper executor in that order. In GUI paper mode, enable strategy toggle and paper-only cancel/close controls; keep testnet arm disabled with `TESTNET_EXECUTION_UNAVAILABLE`. Graceful shutdown stops decisions, cancels paper orders, flattens only when configured, flushes streams, and writes the final report.

- [ ] **Step 5: Run Phase 2D gates**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets --offline -- -D warnings`

Run: `cargo test --workspace --offline`

Run: `cargo build --workspace --release --offline`

Expected: all commands exit 0; frozen replay is deterministic and the pair returns flat.

- [ ] **Step 6: Commit Phase 2D**

```bash
git add Cargo.toml Cargo.lock crates/funding-core crates/funding-strategy crates/risk crates/execution crates/md-storage crates/funding-app README.md docs/data-schema.md
git commit -m "feat: add replay and paper strategies"
```

---

## Phase 2D Completion Gate

- Canonically merged replay is deterministic across file enumeration and pacing.
- Paper execution models IOC depth, post-only behavior, latency, fees, slippage, partial fills, and cancel/fill races.
- Paper funding settles once per actual slot using signed mark notional.
- Funding-pair and fixed-spread machines cover every approved normal/failure transition and illegal transition.
- The risk gate blocks every unsafe precondition with stable codes before executor submission.
- Paper PnL exactly equals its funding/execution/basis/fees/slippage/residual components.
- Replay/paper Arrow streams validate; same input/config/seed yields equivalent canonical reports.
- GUI monitor/paper controls work through the command gateway; testnet remains impossible.
