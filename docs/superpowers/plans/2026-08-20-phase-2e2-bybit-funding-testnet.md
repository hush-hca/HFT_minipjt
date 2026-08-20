# Phase 2E-2 Bybit Bilateral Funding Testnet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add authenticated Bybit Linear testnet execution and complete safe Binance–Bybit bilateral funding-pair entry, hedge recovery, settlement confirmation, re-evaluation/close, GUI operation, and exact cross-venue reconciliation.

**Architecture:** A Bybit client implements the same journal-first async executor/reconcile boundaries established in Phase 2E-1. A cross-venue coordinator owns one correlation ID and two venue legs, quantizes equal base delta through contract multipliers, submits concurrently, treats uncertainty as blocking, and prioritizes emergency exposure reduction over expected funding return.

**Tech Stack:** Rust 2024 (Rust 1.85+), Tokio, existing SQLite WAL OMS, reqwest/rustls, tokio-tungstenite, simd-json, HMAC-SHA256, secrecy/zeroize, exact scale-18 decimals, serde, tracing, iced.

**Spec:** `docs/superpowers/specs/2026-08-20-funding-arbitrage-hft-phase-2-design.md`

## Global Constraints

- Phase 2A through Phase 2E-1 completion gates remain green.
- There is no live mode. Authenticated hosts are allowlisted Binance and Bybit testnets or loopback test servers only.
- Mainnet monitor/paper still covers the configured 20-asset common active intersection; testnet execution independently discovers its own common active subset.
- Missing mainnet assets on testnet are reported as `TESTNET_UNAVAILABLE`, not fatal. Smoke defaults to common active BTCUSDT, otherwise the first common active high-liquidity configured asset.
- Both test accounts must be dedicated and clean: one-way position mode, non-portfolio account mode, configured 1x leverage, no pre-existing positions, and no pre-existing open orders.
- Preflight reads and verifies settings; it never silently changes account, position, margin, or leverage mode.
- Contract multiplier, tick, step, min/max quantity, min notional, and price bounds are applied independently per venue before comparing base delta.
- Both entry legs are parallel slippage-capped marketable IOC limits; unknown state blocks, and a one-leg fill follows cancel, two-second hedge retry, then emergency reduce/close.
- Normal close waits for both initially identified actual account funding records. Every observed settlement triggers re-evaluation; risk may close earlier.
- Actual total PnL must be decomposed into funding, execution, basis, fees, slippage, and residual mark-to-market.
- The GUI cannot call venue clients or bypass risk; `Cancel all`, `close positions`, and `kill new order flow` stay distinct.
- No deposits, withdrawals, transfers, collateral automation, or production capital are in scope.

---

## File Map

| Path | Responsibility |
|---|---|
| `crates/execution/src/bybit/auth.rs` | Bybit V5 canonical signing and redaction |
| `crates/execution/src/bybit/rest.rs` | Allowlisted testnet order/account/position/fill/funding reads |
| `crates/execution/src/bybit/private.rs` | Authenticated order/execution/position/wallet streams |
| `crates/execution/src/bybit/preflight.rs` | Account mode, position mode, leverage, orders, positions, clock, fees |
| `crates/execution/src/bybit/executor.rs` | Journal-first submit/cancel/query and reconciliation API |
| `crates/execution/src/quantity.rs` | Cross-venue contract multiplier/step/minimum quantity planner |
| `crates/execution/src/pair.rs` | Concurrent two-leg coordinator and emergency policy |
| `crates/execution/src/settlement.rs` | Expected-to-actual funding identity and confirmation |
| `crates/funding-strategy/src/funding_pair.rs` | Connect real executor outcomes to approved state machine |
| `crates/funding-app/src/testnet.rs` | Add bilateral funding mode and dual preflight/arming |
| `crates/funding-app/src/ui/*` | Enable funding testnet controls and real paired state/PnL views |
| `crates/funding-app/tests/phase2e2_e2e.rs` | Two loopback REST/private WS venues and full lifecycle |
| `docs/testnet-runbook.md` | Bybit setup and bilateral operational/recovery steps |

---

### Task 1: Implement Bybit Signing, Endpoint Safety, and Clean-Account Preflight

**Files:**
- Create: `crates/execution/src/bybit/mod.rs`
- Create: `crates/execution/src/bybit/auth.rs`
- Create: `crates/execution/src/bybit/rest.rs`
- Create: `crates/execution/src/bybit/preflight.rs`
- Modify: `crates/execution/src/lib.rs`
- Create: `crates/execution/tests/bybit_auth.rs`
- Create: `crates/execution/tests/bybit_preflight.rs`
- Create: `crates/execution/tests/fixtures/bybit_testnet_account.json`

**Interfaces:**
- Produces: `BybitSigner`, `BybitTestnetClient`, and `BybitPreflight::run() -> PreflightReport`.
- Implements: `VenueReconcileApi` read methods for orders, fills, positions, balances, and funding income.
- Consumes: Phase 2E-1 `CredentialRef`, REST scheduler, report redaction, and reconciliation contracts.

- [ ] **Step 1: Write fixed-vector, host, and mismatch tests**

```rust
#[test]
fn v5_signer_matches_literal_vector() {
    let signer = BybitSigner::new(SecretString::from("test-secret".to_owned()));
    let signature = signer.sign("1700000000000", "test-key", "5000", "category=linear&symbol=BTCUSDT");
    assert_eq!(signature, "9a7c8cfd6ba1a7c498aa4dd5a7f9cfbba01fcb6eebae734ffe0d775870a1a3fb");
}

#[tokio::test]
async fn preflight_refuses_hedge_or_portfolio_mode_without_mutating_it() {
    let rig = BybitRig::account_modes(PositionMode::Hedge, AccountMode::Portfolio);
    let report = BybitPreflight::new(rig.client()).run().await.unwrap();
    assert!(report.has_code("POSITION_MODE_NOT_ONE_WAY"));
    assert!(report.has_code("PORTFOLIO_MODE_NOT_ALLOWED"));
    assert_eq!(rig.mutating_account_request_count(), 0);
}
```

- [ ] **Step 2: Confirm Bybit module failure**

Run: `cargo test -p execution --test bybit_auth --test bybit_preflight`

Expected: FAIL because Bybit authenticated modules do not exist.

- [ ] **Step 3: Implement V5 signing and allowlisting**

Sign the exact V5 preimage `timestamp + api_key + recv_window + query_or_json_body` with HMAC-SHA256. Canonically sort GET query parameters and serialize POST JSON once before signing/sending. Add required signature headers without logging them. Reject production or arbitrary authenticated hosts; loopback is enabled only by a test constructor.

- [ ] **Step 4: Implement read-only preflight checks**

Read server time, account information/mode, position mode, per-symbol leverage, open orders, positions, wallet/balance, fee rate, and rate-limit status. Require one-way, non-portfolio, exact configured 1x leverage, no nonzero positions, no open orders, fresh clock, recognized USDT linear instrument, nonzero authenticated fee, and private/order budget. Do not implement mode/leverage setters in the preflight client. Cache the fee as `FeeSource::AuthenticatedCommission` and refresh it hourly through the account-data budget; stale/failed refresh blocks new entries.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test bybit_auth --test bybit_preflight`

Expected: PASS for clean account and every isolated mismatch, with zero secret leakage/mutating preflight requests.

```bash
git add Cargo.lock crates/execution
git commit -m "feat: add bybit testnet preflight"
```

---

### Task 2: Parse Bybit Private Streams and Implement Journal-First Execution

**Files:**
- Create: `crates/execution/src/bybit/private.rs`
- Create: `crates/execution/src/bybit/executor.rs`
- Create: `crates/execution/tests/bybit_private.rs`
- Create: `crates/execution/tests/bybit_executor.rs`
- Create: `crates/execution/tests/fixtures/bybit_private_order.json`
- Create: `crates/execution/tests/fixtures/bybit_private_execution.json`
- Create: `crates/execution/tests/fixtures/bybit_private_position.json`
- Create: `crates/execution/tests/fixtures/bybit_private_wallet.json`

**Interfaces:**
- Produces: `BybitPrivateParser::parse(&mut [u8], recv_us) -> Vec<PrivateEvent>`.
- Produces: `BybitTestnetExecutor` implementing `AsyncExecutor` and the remaining `VenueReconcileApi` methods.
- Uses the same `OrderJournal` schema and canonical lifecycle as Binance.

- [ ] **Step 1: Write fill-deduplication and unknown-result tests**

```rust
#[tokio::test]
async fn execution_stream_is_authoritative_and_duplicate_exec_ids_count_once() {
    let mut parser = BybitPrivateParser::default();
    let first = parser.parse(&mut fixture("bybit_private_execution.json"), 1_000_000).unwrap();
    let second = parser.parse(&mut fixture("bybit_private_execution.json"), 1_000_100).unwrap();
    let mut journal = seeded_journal();
    apply_private(&mut journal, first).unwrap();
    apply_private(&mut journal, second).unwrap();
    assert_eq!(journal.snapshot().unwrap().fills.len(), 1);
}

#[tokio::test]
async fn timed_out_create_queries_order_link_id_without_duplicate_post() {
    let rig = BybitRig::timeout_then_order_found();
    let executor = bybit_executor(rig.clone());
    executor.submit(intent_with_link_id("cid-7")).await.unwrap();
    executor.reconcile_unknowns().await.unwrap();
    assert_eq!(rig.create_order_count(), 1);
}
```

- [ ] **Step 2: Confirm parser/executor failure**

Run: `cargo test -p execution --test bybit_private --test bybit_executor`

Expected: FAIL because private and executor modules are absent.

- [ ] **Step 3: Parse private topics with venue semantics**

Authenticate the V5 private socket and subscribe separately to order, execution, position, and wallet topics. Map `orderLinkId` to canonical client ID, `orderId` to venue ID, and each `execId` to a unique fill. Preserve category, side, order status, cumulative/leaves quantity, fee/rate, liquidity role, creation/update/source/local times. A stream close, auth failure, sequence/gap evidence, or stale heartbeat marks private health blocked before reconnect.

- [ ] **Step 4: Implement journal-first Bybit commands**

Apply the same sequence as Binance: risk permit, intent transaction, one create request, acknowledgement or `Reconcile`; query by `orderLinkId` before any retry. Cancel and query are symbol/category-scoped. Normalize REST and WebSocket updates through the same OMS reducer and commit before notifying the strategy.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test bybit_private --test bybit_executor`

Expected: PASS for duplicates, reordering, partial fill/cancel race, disconnect/reconnect, unknown create/cancel, and REST repair.

```bash
git add crates/execution
git commit -m "feat: execute bybit testnet orders safely"
```

---

### Task 3: Quantize Equal Base Delta Across Venue Contracts

**Files:**
- Create: `crates/execution/src/quantity.rs`
- Create: `crates/execution/tests/quantity.rs`

**Interfaces:**
- Produces: `QuantityPlanner::plan(input) -> Result<PairQuantityPlan, QuantityError>`.
- Produces: independent Binance/Bybit venue quantities, base deltas, quote notionals, and residual quote delta.

- [ ] **Step 1: Write multiplier/step/minimum tests**

```rust
#[test]
fn planner_uses_contract_multiplier_before_comparing_delta() {
    let plan = QuantityPlanner::default().plan(PairQuantityInput {
        target_quote_per_leg: money("100"),
        binance_spec: spec_with_multiplier("1", "0.001"),
        bybit_spec: spec_with_multiplier("0.01", "1"),
        binance_price: price("50000"),
        bybit_price: price("50010"),
        residual_limit_quote: money("1"),
    }).unwrap();
    assert!(abs(plan.binance_base_delta - plan.bybit_base_delta) * plan.reference_price <= money("1"));
    assert!(is_multiple(plan.binance_quantity, plan.binance_step));
    assert!(is_multiple(plan.bybit_quantity, plan.bybit_step));
}
```

- [ ] **Step 2: Confirm quantity planner failure**

Run: `cargo test -p execution --test quantity`

Expected: FAIL because planner types do not exist.

- [ ] **Step 3: Implement bounded candidate search**

Convert target quote to raw base at each fresh executable price. Quantize venue quantities toward lower risk, then search the nearest bounded step combinations that satisfy min/max quantity, min notional, price bounds, 100 USDT per-leg default, and residual `max(1 USDT, 0.5% pair notional)`. Compute `base_delta = venue_quantity * contract_multiplier` with checked scale-18 math.

- [ ] **Step 4: Add property tests**

Generate positive multipliers/steps/prices within Decimal128 range. Assert every successful plan satisfies both venue filters, sign symmetry, notional cap, and residual bound; failure returns `NO_COMMON_QUANTITY` without rounding above the configured risk limit.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test quantity`

Expected: PASS.

```bash
git add crates/execution
git commit -m "feat: normalize cross venue quantities"
```

---

### Task 4: Coordinate Parallel Entry and Emergency Hedge/Reduce

**Files:**
- Create: `crates/execution/src/pair.rs`
- Modify: `crates/funding-strategy/src/funding_pair.rs`
- Create: `crates/execution/tests/pair.rs`
- Modify: `crates/funding-strategy/tests/funding_pair.rs`

**Interfaces:**
- Produces: `PairCoordinator::enter(plan, deadline)`, `hedge_or_reduce`, `close`, and `reconcile`.
- Consumes: two `Arc<dyn AsyncExecutor>`, one shared risk state, one correlation ID, and the Phase 2D funding state machine.

- [ ] **Step 1: Write two-leg outcome matrix tests**

```rust
#[tokio::test]
async fn one_leg_fill_then_failed_retry_reduces_filled_venue_and_halts() {
    let rig = PairRig::binance_fills_bybit_rejects_then_hedge_rejects();
    let result = rig.coordinator.enter(rig.plan(), deadline_us(2_000_000)).await.unwrap();
    assert_eq!(result.state, FundingPairState::Halted);
    assert_eq!(rig.binance.reduce_only_order_count(), 1);
    assert_eq!(rig.bybit.hedge_retry_count(), 1);
    assert_eq!(result.residual_delta, 0);
}

#[tokio::test]
async fn unknown_leg_result_reconciles_before_hedge_decision() {
    let rig = PairRig::one_timeout_eventual_fill();
    rig.coordinator.enter(rig.plan(), deadline_us(2_000_000)).await.unwrap();
    assert!(rig.timeline().reconcile_precedes_hedge());
}
```

- [ ] **Step 2: Confirm pair coordinator failure**

Run: `cargo test -p execution --test pair`

Expected: FAIL because coordinator does not exist.

- [ ] **Step 3: Implement concurrent journaled entry**

Create both deterministic intents and commit them atomically with `OrderJournal::record_pair_intents` before starting network futures. Each executor accepts an identical pre-journaled intent idempotently, then dispatch both with `tokio::join!`; never serialize an intentionally parallel pair. Treat rejected, partial, timed-out, or unknown outcomes individually. Cancel all remaining entry quantity once pair target cannot be met.

- [ ] **Step 4: Implement the two-second event-clock emergency policy**

If one side has more base delta, submit one slippage-capped hedge retry on the deficient venue within the remaining two-second budget. If it cannot restore the residual bound, submit reduce-only on the exposed venue; if that remains unknown/unfilled, activate global kill, enter `Halted`, and require operator/reconciliation. Exposure reduction always outranks expected funding PnL.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test pair`

Run: `cargo test -p funding-strategy --test funding_pair`

Expected: PASS for both fills, both rejects, every one-leg direction, partials, unknowns, deadline boundary, and emergency failure.

```bash
git add crates/execution crates/funding-strategy
git commit -m "feat: coordinate bilateral funding entry"
```

---

### Task 5: Confirm Funding Settlements, Re-evaluate, Close, and Attribute PnL

**Files:**
- Create: `crates/execution/src/settlement.rs`
- Create: `crates/execution/tests/settlement.rs`
- Modify: `crates/execution/src/pair.rs`
- Modify: `crates/funding-strategy/src/funding_pair.rs`
- Modify: `crates/funding-core/src/opportunity.rs`

**Interfaces:**
- Produces: `SettlementTracker::expect`, `observe_income`, `status`, and `initial_pair_confirmed`.
- Produces: `PairPnlAttributor::finalize(entry, settlements, exit, marks) -> PnlBreakdown`.

- [ ] **Step 1: Write settlement identity and PnL tests**

```rust
#[test]
fn later_extra_settlement_does_not_replace_missing_initial_confirmation() {
    let mut tracker = tracker_for_initial_times(binance_ts(), bybit_ts());
    tracker.observe_income(bybit_actual()).unwrap();
    tracker.observe_income(bybit_later_actual()).unwrap();
    assert!(!tracker.initial_pair_confirmed());
    tracker.observe_income(binance_actual()).unwrap();
    assert!(tracker.initial_pair_confirmed());
}

#[test]
fn total_pnl_equals_all_named_components() {
    let pnl = PairPnlAttributor::default().finalize(pair_fixture()).unwrap();
    assert_eq!(pnl.total(), pnl.funding_income + pnl.execution_pnl + pnl.basis_pnl + pnl.trading_fees + pnl.slippage + pnl.residual_mark_to_market);
}
```

- [ ] **Step 2: Confirm settlement test failure**

Run: `cargo test -p execution --test settlement`

Expected: FAIL because settlement tracker/attributor do not exist.

- [ ] **Step 3: Implement expected-to-actual settlement matching**

Key expectations by venue, symbol, and initially identified settlement timestamp. Match only account income records with funding type, compatible symbol, and timestamp inside a documented venue tolerance; deduplicate the venue transaction/income ID. Later settlements are stored and trigger re-evaluation but cannot satisfy a missing initial identity.

- [ ] **Step 4: Implement re-evaluation and close**

After every observed actual settlement, refresh public inputs and recompute conservative remaining PnL. When both initial settlements are confirmed, transition to `ReevaluateOrClose`; continue only if a newly constructed opportunity independently passes all gates, otherwise submit parallel reduce-only slippage-capped closes. A stale/unsafe condition initiates early risk close with explicit reason.

- [ ] **Step 5: Implement exact PnL attribution and commit**

Compute funding from actual account income; execution PnL from signed entry/exit fills; basis PnL from cross-venue entry-to-exit basis movement; fees from actual fills; slippage from decision versus fill; residual MTM from any remaining delta. Avoid double-counting by testing a zero-basis/no-fee fixture and one nonzero component at a time.

Run: `cargo test -p execution --test settlement`

Expected: PASS.

```bash
git add crates/funding-core crates/funding-strategy crates/execution
git commit -m "feat: confirm and attribute funding settlements"
```

---

### Task 6: Add Bilateral Testnet Mode and Complete the GUI

**Files:**
- Modify: `crates/funding-app/src/testnet.rs`
- Modify: `crates/funding-app/src/main.rs`
- Modify: `crates/funding-app/src/ui/model.rs`
- Modify: `crates/funding-app/src/ui/reducer.rs`
- Modify: `crates/funding-app/src/ui/views/opportunities.rs`
- Modify: `crates/funding-app/src/ui/views/strategy.rs`
- Modify: `crates/funding-app/src/ui/views/risk.rs`
- Create: `crates/funding-app/tests/phase2e2_e2e.rs`

**Interfaces:**
- Produces CLI: `funding-app testnet funding --config config/funding.toml --symbol auto --max-duration 10h --confirm TESTNET`.
- Enables bilateral strategy controls only after both preflights and exact reconciliations.

- [ ] **Step 1: Write full loopback lifecycle E2E**

```rust
#[tokio::test]
async fn bilateral_testnet_enters_confirms_both_settlements_closes_and_reconciles() {
    let rig = BilateralLoopbackRig::new().with_common_symbols(["BTC", "ETH"]).with_bybit_reconnect();
    let report = run_bilateral_testnet(rig.config(), "BTC", "TESTNET").await.unwrap();
    assert_eq!(report.symbol, "BTC/USDT");
    assert_eq!(report.final_state, FundingPairState::Flat);
    assert_eq!(report.confirmed_initial_settlements, 2);
    assert!(report.terminal_reconciliation_exact);
    assert_eq!(report.residual_delta, 0);
    assert_eq!(report.pnl.total(), report.pnl.component_sum());
    assert!(rig.authenticated_hosts_are_testnet_or_loopback());
}
```

- [ ] **Step 2: Confirm E2E failure**

Run: `cargo test -p funding-app --test phase2e2_e2e`

Expected: FAIL because bilateral mode is not wired.

- [ ] **Step 3: Implement independent testnet discovery and dual arming**

Discover both testnets using Phase 2A rules, intersect in configured order, report each missing mainnet symbol as `TESTNET_UNAVAILABLE`, and select common BTC when active or the first configured high-liquidity common asset. Require successful Binance preflight, Bybit preflight, and exact reconciliations in one arming epoch; any subsequent disconnect/mismatch disarms both.

- [ ] **Step 4: Wire strategy and GUI through the command gateway**

Enable bilateral funding only in armed testnet mode. Funding Opportunities shows execution eligibility separate from mainnet research eligibility. Strategy view displays both legs, fills, residual, expected/actual settlements, state reasons, and complete PnL. Risk controls route arm/enable/cancel-all/close/kill commands to the coordinator/risk gate, with distinct confirmation text and journal records. The command normally ends after both initial settlements are confirmed and the pair is closed; reaching `--max-duration` triggers the documented early risk-close path and reports that the full settlement acceptance run did not complete.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test phase2e2_e2e`

Expected: PASS for normal lifecycle plus reconnect/resubscribe and exact shutdown reconciliation.

```bash
git add crates/funding-app
git commit -m "feat: run bilateral funding testnet"
```

---

### Task 7: Cover Bilateral Failure Matrices and Operational Runbook

**Files:**
- Create: `crates/funding-app/tests/phase2e2_failures.rs`
- Modify: `docs/testnet-runbook.md`
- Modify: `README.md`
- Modify: `docs/data-schema.md`
- Modify: `.github/workflows/phase2-gui.yml`

**Interfaces:**
- Adds deterministic failure-matrix evidence and exact operator procedures.
- Does not add new execution capability.

- [ ] **Step 1: Add the cross-venue failure matrix**

Create separate fake-server cases for Binance reject, Bybit reject, Binance timeout/eventual fill, Bybit timeout/eventual fill, each partial direction, private disconnect on either venue, REST `429`, clock drift, journal failure, stale public data, stale conversion, settlement missing on either venue, duplicate funding income, emergency reduce reject, and process restart with one open leg. Assert the documented state, kill/disarm behavior, network request count, and final reconciliation gate for every case.

- [ ] **Step 2: Add credential/endpoint/report safety tests**

Use recognizable fixture key/secret values and assert they are absent from logs, errors, SQLite text/blob scans, Arrow, JSON reports, and UI snapshots. Assert authenticated request capture contains only allowlisted testnet/loopback hosts and no withdraw/deposit/transfer path.

- [ ] **Step 3: Complete the bilateral runbook**

Document both testnet account prerequisites, environment-variable names, clock checks, independent symbol availability, BTC fallback, `TESTNET_UNAVAILABLE`, two-sided arming, funding calendar interpretation, early risk exit, one-leg emergency behavior, cancel-all versus close, kill switch, restart reconciliation, report/PnL interpretation, and safe credential removal. Explicitly state that testnet funding economics do not prove mainnet profitability.

- [ ] **Step 4: Run Phase 2E-2 and full Phase 2 gates**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets --offline -- -D warnings`

Run: `cargo test --workspace --offline`

Run: `cargo test -p execution --release --test million_order --offline -- --nocapture`

Run: `cargo build --workspace --release --offline`

Expected: all commands exit 0; full failure matrix, bilateral E2E, GUI, Phase 1 regression, and million-order acceptance pass.

- [ ] **Step 5: Commit Phase 2E-2 completion**

```bash
git add crates README.md docs .github/workflows/phase2-gui.yml
git commit -m "docs: complete funding testnet runbook"
```

---

## Phase 2E-2 Completion Gate

- Bybit signing/private streams/executor pass fixed-vector, redaction, allowlist, duplicate, gap, timeout, and repair tests.
- Both accounts must pass clean one-way/non-portfolio/1x/no-position/no-order preflight without automatic setting changes.
- Testnet discovery is independent and reports unsupported configured symbols without failing a valid common subset.
- Quantity plans apply each contract multiplier/filter and meet the residual delta limit before entry.
- Parallel entry handles every fill/reject/timeout/unknown combination; one-leg exposure follows cancel, two-second retry, emergency reduce, then halt.
- Both initially identified actual funding income records are independently confirmed before normal close/re-evaluation.
- Final PnL is exactly decomposed and cannot present funding income as total profitability.
- GUI controls operate only through risk/command gateways and expose all pair, health, reconciliation, and emergency states.
- Normal and failure E2E scenarios end exact/flat or explicitly blocked/halted with documented operator action.
- No live endpoint, transfer capability, or secret persistence exists.
