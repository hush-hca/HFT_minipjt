# Phase 2E-1 OMS and Binance Fixed-Spread Testnet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a restart-safe SQLite OMS, idempotent reconciliation, authenticated Binance USDⓈ-M testnet execution, fixed-spread post-only order/cancel/replace, GUI safety controls, and the deterministic one-million-order reconciliation proof.

**Architecture:** `execution` gains an asynchronous executor boundary, transactional SQLite journal, reconciler, and Binance-specific signed REST/private WebSocket client. Strategy actions are journaled before transmission, private events drive the fast path, REST repairs uncertainty, and the risk gate blocks all new orders whenever account/journal/clock/connection state is not exact.

**Tech Stack:** Rust 2024 (Rust 1.85+), Tokio, rusqlite 0.32.1 with bundled SQLite and WAL, reqwest/rustls, tokio-tungstenite, simd-json, HMAC-SHA256, secrecy/zeroize, UUID, serde, tracing, iced.

**Spec:** `docs/superpowers/specs/2026-08-20-funding-arbitrage-hft-phase-2-design.md`

## Global Constraints

- Phase 2A through Phase 2D completion gates remain green.
- Pin `rusqlite = { version = "=0.32.1", features = ["bundled"] }` for the workspace's Rust 1.85 compatibility and deterministic cross-platform SQLite provisioning; record its transitive SQLite version in the release report.
- There is no live mode. The authenticated endpoint allowlist contains Binance USDⓈ-M testnet only in Phase 2E-1.
- API key/secret values come from configured environment-variable names or an OS credential provider; they never enter TOML, SQLite, Arrow, reports, logs, panic text, screenshots, or UI snapshots.
- The executor requires a dedicated clean test account: one-way mode, non-portfolio/non-multi-asset mode, configured 1x leverage, no existing positions, and no existing open orders.
- Preflight reports mismatches and refuses to arm; it does not silently change account, position, margin, or leverage mode.
- Every order intent is committed before send and uses a deterministic unique client order ID.
- REST acknowledgement is not final execution state; private order/execution events are the fast path and REST order/trade/position queries repair gaps.
- Unknown request results enter blocking reconciliation; they are never retried as new orders.
- A journal failure, stale/disconnected private stream, excessive clock offset, insufficient rate headroom, or unknown state blocks new orders immediately.
- `Cancel all` and `close positions` are separate confirmed commands.
- The one-million-order proof is a deterministic local test: one million canonical orders, at least 10,000 filled orders, both pre-repair attribution metrics at least 99.9%, and exact post-repair state.
- Public testnet validation is rate-limit-safe and never attempts one million external requests.

---

## File Map

| Path | Responsibility |
|---|---|
| `crates/execution/src/oms.rs` | Canonical lifecycle, idempotent transition reducer, terminal rules |
| `crates/execution/src/journal.rs` | SQLite WAL schema, transactions, migrations, intent/update/fill/account persistence |
| `crates/execution/src/reconcile.rs` | Startup, unknown-result, stream-gap, and shutdown reconciliation |
| `crates/execution/src/secrets.rs` | Redacted credential loading and zeroization |
| `crates/execution/src/binance/auth.rs` | Canonical query and HMAC signing with test vectors |
| `crates/execution/src/binance/rest.rs` | Allowlisted USDⓈ-M testnet account/order/trade/position API |
| `crates/execution/src/binance/private.rs` | Listen-key lifecycle and private order/account parser |
| `crates/execution/src/binance/executor.rs` | Journal-first async submit/cancel/query operations |
| `crates/execution/src/binance/preflight.rs` | Account, clock, leverage, position, order, and endpoint checks |
| `crates/execution/src/soak.rs` | Million-order local generator, fault injection, repair, metrics |
| `crates/funding-app/src/testnet.rs` | Arming state and Binance fixed-spread testnet orchestration |
| `crates/funding-app/src/ui/model.rs` | Real OMS/reconciliation/testnet control snapshots |
| `crates/funding-app/tests/phase2e1_e2e.rs` | Loopback signed REST/private WS crash/restart/reconcile scenario |
| `crates/execution/tests/million_order.rs` | Release-profile million-order acceptance test |

---

### Task 1: Implement the Canonical OMS Lifecycle

**Files:**
- Create: `crates/execution/src/oms.rs`
- Modify: `crates/execution/src/lib.rs`
- Create: `crates/execution/tests/oms.rs`

**Interfaces:**
- Produces: `OrderState`, `CanonicalOrder`, `OmsEvent`, `OmsEffect`, and `reduce_order(order, event)`.
- Consumes: `OrderIntent`, `OrderUpdate`, and `ExecutionFill` from `funding-core`.

- [ ] **Step 1: Write lifecycle/idempotence tests**

```rust
#[test]
fn duplicate_and_out_of_order_events_are_idempotent_and_monotonic() {
    let mut order = CanonicalOrder::from_intent(intent(qty("3")));
    apply(&mut order, submitted());
    apply(&mut order, partial_fill("fill-2", qty("2")));
    apply(&mut order, partial_fill("fill-1", qty("1")));
    apply(&mut order, partial_fill("fill-1", qty("1")));
    assert_eq!(order.state, OrderState::Filled);
    assert_eq!(order.cumulative_fill_quantity, qty("3"));
    assert_eq!(order.unique_fill_count, 2);
}

#[test]
fn unknown_submit_result_blocks_until_reconciled() {
    let mut order = CanonicalOrder::from_intent(intent(qty("1")));
    apply(&mut order, submit_timed_out());
    assert_eq!(order.state, OrderState::Reconcile);
    assert!(order.blocks_new_orders());
}
```

- [ ] **Step 2: Confirm reducer tests fail**

Run: `cargo test -p execution --test oms`

Expected: FAIL because OMS types do not exist.

- [ ] **Step 3: Define the exact lifecycle and reducer**

```rust
pub enum OrderState {
    Intent, Submitted, Acknowledged, PartiallyFilled,
    Filled, Canceled, Rejected, Expired, Reconcile,
}

pub struct CanonicalOrder {
    pub intent: OrderIntent,
    pub venue_order_id: Option<String>,
    pub state: OrderState,
    pub cumulative_fill_quantity: i128,
    pub cumulative_fee: i128,
    pub unique_fill_count: u64,
    pub reconciled: bool,
    pub last_source_sequence: Option<u64>,
    pub last_update_ts_us: i64,
}
```

Keep a fill-ID set per order in journal-backed state. Reject cumulative fill regression and quantity above intent; ignore exact duplicates; accept late fill after cancel when venue semantics permit and transition terminal state accordingly. Terminal state is `Filled` when cumulative quantity equals intent, regardless of earlier cancel acknowledgement. `reconciled` is set only after the terminal order, fills, and resulting position agree with venue/account queries; it preserves the terminal outcome instead of replacing it with a lossy generic state.

- [ ] **Step 4: Add property tests for transition safety**

Generate reordered/duplicated events and assert cumulative quantity/fee never regress, a client ID maps to one canonical order, each fill ID counts once, and illegal terminal reversal returns `OmsError` without mutation.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test oms`

Expected: PASS.

```bash
git add crates/execution
git commit -m "feat: add canonical order lifecycle"
```

---

### Task 2: Add the Transactional SQLite WAL Journal

**Files:**
- Modify: `crates/execution/Cargo.toml`
- Create: `crates/execution/src/journal.rs`
- Create: `crates/execution/src/migrations.rs`
- Create: `crates/execution/tests/journal.rs`

**Interfaces:**
- Produces: `OrderJournal::open(path)`, `record_intent`, `record_pair_intents`, `record_submitted`, `apply_venue_update`, `record_operator_command`, `unresolved_orders`, and `snapshot`.
- Every public mutation starts and commits one SQLite transaction or leaves no state change.

- [ ] **Step 1: Write durability/uniqueness/rollback tests**

```rust
#[test]
fn journal_survives_restart_and_enforces_unique_ids() {
    let path = temp_db();
    {
        let mut j = OrderJournal::open(&path).unwrap();
        j.record_intent(&intent(qty("1"))).unwrap();
        assert!(matches!(j.record_intent(&different_intent_same_client_id()), Err(JournalError::Conflict { .. })));
    }
    let j = OrderJournal::open(&path).unwrap();
    assert_eq!(j.unresolved_orders().unwrap().len(), 1);
}

#[test]
fn failed_fill_transaction_leaves_order_and_fill_tables_unchanged() {
    let mut j = seeded_journal();
    let before = j.snapshot().unwrap();
    assert!(j.apply_venue_update(&overfill_event()).is_err());
    assert_eq!(j.snapshot().unwrap(), before);
}
```

- [ ] **Step 2: Confirm journal test failure**

Run: `cargo test -p execution --test journal`

Expected: FAIL because journal APIs are missing.

- [ ] **Step 3: Create the versioned schema**

Use `PRAGMA journal_mode=WAL`, `foreign_keys=ON`, `synchronous=FULL`, and a busy timeout. Create tables `schema_migrations`, `strategy_runs`, `order_intents`, `order_events`, `fills`, `positions`, `balances`, `funding_income`, `reconciliation_runs`, `operator_commands`, and `kill_switch_transitions`. Unique constraints cover `(venue,client_order_id)`, `(venue,venue_order_id)` when present, and `(venue,fill_id)`.

- [ ] **Step 4: Implement journal-first transactional APIs**

Store exact financial values as canonical decimal strings or signed 16-byte big-endian values with round-trip tests; do not use SQLite REAL. Store request parameters and a SHA-256 request hash but not authentication headers/signatures. In `record_intent`, compare an existing hash for idempotence and return conflict when the same client ID has different parameters. `record_pair_intents(first, second)` writes both legs in one transaction or neither, and treats an already-identical pair as idempotent.

- [ ] **Step 5: Run crash/reopen tests and commit**

Run: `cargo test -p execution --test journal`

Expected: PASS including uncheckpointed WAL reopen and migration version rejection.

```bash
git add Cargo.lock crates/execution
git commit -m "feat: add sqlite order journal"
```

---

### Task 3: Implement Reconciliation and Exact Startup Gate

**Files:**
- Create: `crates/execution/src/reconcile.rs`
- Create: `crates/execution/tests/reconcile.rs`

**Interfaces:**
- Produces: object-safe `VenueReconcileApi` and `Reconciler::run(reason) -> ReconciliationReport`.
- Produces: `ReconcileReason::{Startup,UnknownSubmit,PrivateGap,Shutdown}` and exact `ReconciliationGate`.

- [ ] **Step 1: Write unknown-result and restart tests**

```rust
#[tokio::test]
async fn unknown_submit_queries_client_id_before_any_new_submission() {
    let api = FakeVenue::with_order_for_client("cid-1", filled_order());
    let report = Reconciler::new(journal_with_unknown("cid-1"), api)
        .run(ReconcileReason::UnknownSubmit).await.unwrap();
    assert!(report.exact);
    assert_eq!(report.duplicate_orders_created, 0);
    assert_eq!(report.unresolved_orders, 0);
}

#[tokio::test]
async fn startup_blocks_on_position_mismatch_even_when_orders_match() {
    let report = reconciler_with_position_mismatch().run(ReconcileReason::Startup).await.unwrap();
    assert!(!report.exact);
    assert_eq!(report.gate, ReconciliationGate::Blocked("POSITION_MISMATCH".into()));
}
```

- [ ] **Step 2: Confirm reconciliation tests fail**

Run: `cargo test -p execution --test reconcile`

Expected: FAIL because the API and reconciler do not exist.

- [ ] **Step 3: Define the object-safe venue query boundary**

```rust
#[async_trait::async_trait]
pub trait VenueReconcileApi: Send + Sync {
    async fn order_by_client_id(&self, symbol: &CanonicalSymbol, id: &ClientOrderId) -> Result<Option<VenueOrder>, ReconcileError>;
    async fn open_orders(&self) -> Result<Vec<VenueOrder>, ReconcileError>;
    async fn recent_fills(&self, since_us: i64) -> Result<Vec<ExecutionFill>, ReconcileError>;
    async fn positions(&self) -> Result<Vec<Position>, ReconcileError>;
    async fn balances(&self) -> Result<Vec<BalanceSnapshot>, ReconcileError>;
    async fn funding_income(&self, since_us: i64) -> Result<Vec<FundingIncome>, ReconcileError>;
}
```

- [ ] **Step 4: Implement repair order and exact gate**

First repair each `Reconcile`/nonterminal journal order by client ID, then import deduplicated recent fills, compare open orders, positions, balances, and funding income. Persist every query/result and repair source in one reconciliation run. Gate is exact only when no unknown order, duplicate, mismatched fill quantity, unmatched open order, position mismatch, or funding mismatch remains.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test reconcile`

Expected: PASS for timeout/eventual fill, private gap, restart, and exact/blocked gates.

```bash
git add Cargo.lock crates/execution
git commit -m "feat: reconcile orders and account state"
```

---

### Task 4: Build the Binance Testnet Authenticated Client and Preflight

**Files:**
- Create: `crates/execution/src/secrets.rs`
- Create: `crates/execution/src/binance/mod.rs`
- Create: `crates/execution/src/binance/auth.rs`
- Create: `crates/execution/src/binance/rest.rs`
- Create: `crates/execution/src/binance/preflight.rs`
- Create: `crates/execution/tests/binance_auth.rs`
- Create: `crates/execution/tests/binance_preflight.rs`
- Create: `crates/execution/tests/fixtures/binance_testnet_account.json`

**Interfaces:**
- Produces: `CredentialRef::from_env_names(key_name, secret_name)` with redacted `Debug`.
- Produces: `BinanceTestnetClient` implementing order and reconcile APIs.
- Produces: `BinancePreflight::run() -> PreflightReport` and no mutating account-setting methods.

- [ ] **Step 1: Write official-vector and endpoint-safety tests**

```rust
#[test]
fn signer_matches_fixed_hmac_vector_and_never_formats_secret() {
    let signer = BinanceSigner::new(SecretString::from("test-secret".to_owned()));
    assert_eq!(
        signer.sign("symbol=BTCUSDT&timestamp=1700000000000"),
        "4e7e8444963d2d57498c79c818e00d7325c0de1fe36287ea426397a06945cbea",
    );
    assert!(!format!("{signer:?}").contains("test-secret"));
}

#[test]
fn authenticated_client_rejects_mainnet_and_non_allowlisted_hosts() {
    assert!(BinanceTestnetClient::builder().rest_url("https://fapi.binance.com").build().is_err());
    assert!(BinanceTestnetClient::builder().rest_url(loopback_url()).build_for_tests().is_ok());
}
```

The expected signature is a checked-in literal generated independently from the production signer; the test must not derive its expected value with production code.

- [ ] **Step 2: Confirm auth/preflight tests fail**

Run: `cargo test -p execution --test binance_auth --test binance_preflight`

Expected: FAIL because Binance authenticated modules do not exist.

- [ ] **Step 3: Implement redacted credentials and canonical signing**

Load environment values only when testnet arming begins. Wrap secrets in `secrecy::SecretString`; redact `Debug`/errors and zeroize buffers. Sort/percent-encode request parameters once, append timestamp/recvWindow, HMAC the exact query bytes, and send the signature only in the request. Never log query strings for signed account/order calls.

- [ ] **Step 4: Implement allowlisted REST and preflight reads**

Expose only time, account mode/status, commission, leverage/risk, open orders, order query, recent trades, positions, balances, funding income, new order, and cancel order. Preflight verifies endpoint allowlist, clock offset within configured recvWindow tolerance, one-way mode, non-multi-asset/non-portfolio account, exactly 1x symbol leverage, no positions, no open orders, known instrument filters, authenticated commission, and REST/private-stream headroom. It returns mismatch codes and never calls mode/leverage setters. Cache the authenticated fee with `FeeSource::AuthenticatedCommission` and refresh it hourly through the weighted account-data budget; a failed/stale refresh blocks new entries instead of reverting to zero.

- [ ] **Step 5: Run fake-REST tests and commit**

Run: `cargo test -p execution --test binance_auth --test binance_preflight`

Expected: PASS for clean account and each individual mismatch, with no secret in captured logs/errors.

```bash
git add Cargo.lock crates/execution
git commit -m "feat: add binance testnet preflight"
```

---

### Task 5: Add Binance Private Streams and Journal-First Execution

**Files:**
- Create: `crates/execution/src/binance/private.rs`
- Create: `crates/execution/src/binance/executor.rs`
- Create: `crates/execution/tests/binance_private.rs`
- Create: `crates/execution/tests/binance_executor.rs`
- Create: `crates/execution/tests/fixtures/binance_order_update.json`
- Create: `crates/execution/tests/fixtures/binance_account_update.json`

**Interfaces:**
- Produces: `BinancePrivateParser::parse(&mut [u8], recv_us) -> Vec<PrivateEvent>`.
- Produces: `AsyncExecutor::{submit,cancel,query}` implemented by `BinanceTestnetExecutor`.
- Private stream loss emits a blocking signal and reconciliation request.

- [ ] **Step 1: Write parser and unknown-ack tests**

```rust
#[tokio::test]
async fn timeout_after_send_enters_reconcile_without_second_post() {
    let venue = FakeBinance::timeout_then_find_by_client_id();
    let mut executor = test_executor(venue.clone());
    let result = executor.submit(intent(qty("1"))).await.unwrap();
    assert_eq!(result.state, OrderState::Reconcile);
    executor.reconcile_unknowns().await.unwrap();
    assert_eq!(venue.new_order_request_count(), 1);
    assert_eq!(executor.snapshot().orders[0].state, OrderState::Filled);
}
```

- [ ] **Step 2: Confirm missing private/executor interfaces**

Run: `cargo test -p execution --test binance_private --test binance_executor`

Expected: FAIL.

- [ ] **Step 3: Parse and supervise the private stream**

Parse order trade updates, fills, positions, balances, listen-key expiry, and account/config changes with `simd-json`. Deduplicate by venue event/fill IDs and preserve transaction/event/local timestamps. Create/keepalive the listen key within documented limits; reconnect on close/expiry/gap. On disconnect, mark private health stale before reconnect and trigger REST reconciliation.

- [ ] **Step 4: Implement journal-first async execution**

```rust
#[async_trait::async_trait]
pub trait AsyncExecutor: Send + Sync {
    async fn submit(&self, intent: OrderIntent) -> Result<CanonicalOrder, ExecutionError>;
    async fn cancel(&self, symbol: &CanonicalSymbol, id: &ClientOrderId) -> Result<CanonicalOrder, ExecutionError>;
    async fn query(&self, symbol: &CanonicalSymbol, id: &ClientOrderId) -> Result<Option<VenueOrder>, ExecutionError>;
}
```

Within `submit`: risk permit -> journal intent transaction -> signed POST once -> journal acknowledgement or `Reconcile`. Before any retry, query by client ID. `cancel` follows the same command journal and unknown-result rule. Private events update the journal transactionally and notify strategy state only after commit.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p execution --test binance_private --test binance_executor`

Expected: PASS for duplicate/out-of-order fills, listen-key expiry, timeout, 5xx, cancel/fill race, and reconnect repair.

```bash
git add crates/execution
git commit -m "feat: execute binance testnet orders safely"
```

---

### Task 6: Run Fixed-Spread Testnet Mode and Wire GUI Controls

**Files:**
- Create: `crates/funding-app/src/testnet.rs`
- Modify: `crates/funding-app/src/main.rs`
- Modify: `crates/funding-app/src/ui/model.rs`
- Modify: `crates/funding-app/src/ui/reducer.rs`
- Modify: `crates/funding-app/src/ui/views/strategy.rs`
- Modify: `crates/funding-app/src/ui/views/risk.rs`
- Create: `crates/funding-app/tests/testnet_controls.rs`
- Create: `crates/funding-app/tests/phase2e1_e2e.rs`

**Interfaces:**
- Produces CLI: `funding-app testnet fixed-spread --config config/funding.toml --symbol BTC --duration 60s --confirm TESTNET`.
- Produces: `TestnetArmingState` and real command-gateway handling for Binance fixed-spread only.

- [ ] **Step 1: Write arming/control/restart E2E tests**

```rust
#[tokio::test]
async fn testnet_requires_confirmation_clean_preflight_and_exact_reconciliation() {
    let rig = BinanceLoopbackRig::clean();
    let app = testnet_app(rig);
    assert_eq!(app.arm("wrong").await.unwrap_err().code(), "CONFIRMATION_REQUIRED");
    app.arm("TESTNET").await.unwrap();
    app.run_fixed_spread(cancel_after(Duration::from_millis(200))).await.unwrap();
    assert!(app.report().post_only_orders > 0);
    assert!(app.report().cancel_or_replace_commands > 0);
    assert!(app.report().terminal_reconciliation_exact);
}
```

- [ ] **Step 2: Confirm integration failure**

Run: `cargo test -p funding-app --test testnet_controls --test phase2e1_e2e`

Expected: FAIL because the testnet mode is absent.

- [ ] **Step 3: Implement explicit arming and fixed-spread orchestration**

Require `--confirm TESTNET` or the GUI confirmation dialog, load credentials only after confirmation, run discovery/preflight/startup reconciliation, then expose executor capability to the strategy. Default symbol is BTC only when active; otherwise require explicit active symbol. On disable/shutdown: stop new quotes, cancel strategy-owned quotes, reconcile, require flat/known position state, checkpoint WAL, and report.

- [ ] **Step 4: Enable only valid GUI controls**

Show mode banner `TESTNET`; expose arm, fixed-spread enable, cancel-all, close-position request, and kill-new-flow through the command gateway. Keep bilateral funding strategy disabled with `BYBIT_EXECUTION_UNAVAILABLE`. Display orders/fills/positions, unknown states, journal health, private health, and streaming/terminal reconciliation.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-app --test testnet_controls --test phase2e1_e2e`

Expected: PASS with all outbound URLs captured as loopback/testnet allowlisted and exact shutdown reconciliation.

```bash
git add crates/funding-app
git commit -m "feat: run binance fixed spread testnet"
```

---

### Task 7: Prove One-Million-Order Reconciliation

**Files:**
- Create: `crates/execution/src/soak.rs`
- Create: `crates/execution/tests/million_order.rs`
- Create: `crates/funding-app/src/soak.rs`
- Modify: `crates/funding-app/src/main.rs`

**Interfaces:**
- Produces: `run_million_order_soak(config) -> SoakReport`.
- Produces CLI: `funding-app soak --orders 1000000 --filled-orders 10000 --seed 7 --output soak-output`.

- [ ] **Step 1: Write the release-profile acceptance test**

```rust
#[test]
fn one_million_orders_meet_streaming_and_terminal_targets() {
    let report = run_million_order_soak(SoakConfig {
        canonical_orders: 1_000_000,
        filled_orders: 10_000,
        seed: 7,
        ..SoakConfig::acceptance_defaults()
    }).unwrap();
    assert_eq!(report.canonical_orders, 1_000_000);
    assert!(report.filled_orders >= 10_000);
    assert!(report.order_state_attribution_ppm >= 999_000);
    assert!(report.fill_attribution_ppm >= 999_000);
    assert_eq!(report.duplicate_submitted_orders, 0);
    assert_eq!(report.unknown_terminal_orders, 0);
    assert_eq!(report.residual_positions, 0);
    assert_eq!(report.residual_delta, 0);
    assert!(report.post_repair_exact);
}
```

- [ ] **Step 2: Confirm soak interface failure**

Run: `cargo test -p execution --release --test million_order`

Expected: FAIL because the generator/report do not exist.

- [ ] **Step 3: Implement deterministic generation and fault injection**

Generate exactly one million distinct `CanonicalOrder` values. Select at least 10,000 distinct filled orders with the seeded RNG and generate one or more unique fill events each. Inject counted duplicates, reordering, omissions, disconnect boundaries, partial fills, cancel/fill races, and unknown acknowledgements. Retain an authoritative local venue ledger for REST-style repair.

- [ ] **Step 4: Define both accuracy denominators and exact repair**

```text
order_state_attribution_ppm = correctly_attributed_canonical_orders * 1_000_000 / canonical_orders
fill_attribution_ppm = correctly_attributed_fill_events * 1_000_000 / total_fill_events
```

Pre-repair results must be at least 999,000 ppm for both. Repair through the same reconciler interface, then compare every order, fill, and aggregate position to the authoritative ledger. Report canonical order count, lifecycle event count, filled-order count, fill-event count, denominators, injected discrepancy counts, repair paths, elapsed time, and peak memory.

- [ ] **Step 5: Run the soak twice and commit**

Run: `cargo test -p execution --release --test million_order -- --nocapture`

Expected: PASS twice with equal canonical report digest; the test sends zero network requests.

```bash
git add crates/execution crates/funding-app
git commit -m "test: prove million order reconciliation"
```

---

### Task 8: Complete Safety Documentation and Phase 2E-1 Gates

**Files:**
- Modify: `README.md`
- Modify: `docs/data-schema.md`
- Create: `docs/testnet-runbook.md`
- Modify: `.github/workflows/phase2-gui.yml`

**Interfaces:**
- Documents setup and explicit operator actions without storing secrets.
- Adds normal workspace gates plus a scheduled/non-default release soak job.

- [ ] **Step 1: Document the exact operator preconditions**

Document a dedicated Binance USDⓈ-M testnet account, one-way mode, non-multi-asset/non-portfolio mode, 1x leverage, zero positions, zero open orders, environment-variable names, clock synchronization check, testnet endpoint verification, `TESTNET` confirmation, cancel-all versus close-position behavior, restart reconciliation, and emergency halt. State that withdrawal permission is neither needed nor used.

- [ ] **Step 2: Add secret-redaction and endpoint-safety regression scans**

Add tests that serialize every report/UI snapshot/error and assert fixture secret tokens are absent. Traverse captured HTTP/WS requests and assert every authenticated host is allowlisted testnet or loopback. Assert no source module exposes deposit/withdraw/transfer methods.

- [ ] **Step 3: Run Phase 2E-1 gates**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets --offline -- -D warnings`

Run: `cargo test --workspace --offline`

Run: `cargo test -p execution --release --test million_order --offline -- --nocapture`

Run: `cargo build --workspace --release --offline`

Expected: all commands exit 0; one-million-order report meets every threshold and no test uses external network.

- [ ] **Step 4: Commit Phase 2E-1 documentation and gates**

```bash
git add README.md docs/data-schema.md docs/testnet-runbook.md .github/workflows/phase2-gui.yml crates
git commit -m "docs: add binance testnet operations runbook"
```

---

## Phase 2E-1 Completion Gate

- Journal-first OMS survives WAL restart, enforces unique IDs, and applies duplicate/out-of-order events idempotently.
- Unknown submissions never create a second POST and block until client-ID/account reconciliation resolves them.
- Startup/private-gap/shutdown reconciliation is exact across orders, fills, positions, balances, and funding income.
- Binance authenticated access is testnet-only, secret-redacted, and preflight refuses any account mismatch without mutating settings.
- Fixed-spread testnet execution places post-only orders, cancels/replaces, handles partial fills/races, and shuts down reconciled.
- GUI exposes enabled Binance testnet controls only through risk/command gateways; Bybit funding execution remains disabled.
- The local one-million-order run contains at least 10,000 filled orders, reaches both 99.9% pre-repair metrics, and is exactly consistent after repair.
- All tests are deterministic/fake-server-based; the optional real testnet smoke is low-rate and requires the user's explicit credentials and confirmation.
