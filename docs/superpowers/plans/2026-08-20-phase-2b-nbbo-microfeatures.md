# Phase 2B NBBO and Microfeatures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute reproducible quote-aware NBBO, basis, book microfeatures, tick/snapshot order flow, funding calendars, conservative executable opportunities, and validated feature Arrow streams from Phase 1 and Phase 2A events.

**Architecture:** A new `funding-features` crate maintains bounded per-instrument state and exposes pure calculators plus a deterministic streaming engine. Feature and opportunity models live in `funding-core`; the engine emits immutable feature events that a dedicated Arrow router persists and `funding-app monitor` ranks without any order capability.

**Tech Stack:** Rust 2024 (Rust 1.85+), Tokio, exact scale-18 `i128` decimals, Apache Arrow IPC 56, serde, thiserror, proptest, clap, tracing.

**Spec:** `docs/superpowers/specs/2026-08-20-funding-arbitrage-hft-phase-2-design.md`

## Global Constraints

- Phase 2A is complete and its acceptance gate remains green.
- No financial feature, cost, capacity, or PnL value is persisted as binary floating point.
- Books that are empty, crossed, locked, stale, non-positive, or missing required quote conversion produce an explicit invalid result rather than a numeric feature.
- NBBO for a requested quantity uses executable depth-weighted prices, not last trade or top-of-book alone.
- Cross-quote comparison records conversion venue, side, timestamp, and staleness; stale conversion invalidates the comparison.
- Snapshot-derived flow must be named `snapshot_ofi`/`depth_delta`; it must not claim L3 adds or cancels.
- Funding is modeled on venue settlement timestamps and unequal intervals, never by raw-rate subtraction alone.
- Indicative APR is display-only; opportunity ranking uses conservative discrete cash flows and all execution costs.
- Maker/taker fees come from an authenticated source in later phases or explicit nonzero configuration with a source label.
- Phase 2B remains read-only: no paper orders, authenticated clients, testnet calls, or GUI controls.

---

## File Map

| Path | Responsibility |
|---|---|
| `Cargo.toml` | Add `funding-features` workspace member |
| `config/funding.toml` | Explicit fee, cost-buffer, and research-capacity assumptions |
| `crates/funding-core/src/config.rs` | Exact-decimal cost/capacity config validation |
| `crates/funding-core/src/feature.rs` | Feature values, invalid reasons, windows, basis keys, and immutable feature events |
| `crates/funding-core/src/opportunity.rs` | Cost assumptions, capacity, conservative cash-flow legs, opportunity result |
| `crates/funding-core/src/calendar.rs` | Funding slots, estimate observations, and empirical conservative bounds |
| `crates/funding-features/src/book.rs` | Mid, microprice, WAP, depth imbalance, depth deltas, best-level OFI |
| `crates/funding-features/src/flow.rs` | Tick-flow windows, CVD, burst rate, and inter-trade duration |
| `crates/funding-features/src/quote.rs` | Side-aware conversion matrix and freshness checks |
| `crates/funding-features/src/nbbo.rs` | Depth-aware direct and converted NBBO |
| `crates/funding-features/src/basis.rs` | Named ordered-pair spot/perp/mark/index basis |
| `crates/funding-features/src/funding.rs` | Interval normalization and discrete funding calendar valuation |
| `crates/funding-features/src/opportunity.rs` | Capacity, cost, conservative net PnL, rank/exclusion reasons |
| `crates/funding-features/src/engine.rs` | Deterministic state update and feature emission |
| `crates/md-storage/src/feature_schema.rs` | Feature/opportunity Arrow schemas and validation |
| `crates/md-storage/src/feature_partition.rs` | Hourly feature partition router |
| `crates/funding-app/src/monitor.rs` | Read-only feature pipeline and ranked snapshot/report |
| `crates/funding-app/tests/phase2b_e2e.rs` | Frozen event sequence to deterministic persisted opportunities |

---

### Task 1: Define Feature, Calendar, Cost, and Opportunity Contracts

**Files:**
- Modify: `Cargo.toml`
- Modify: `config/funding.toml`
- Modify: `crates/funding-core/src/lib.rs`
- Modify: `crates/funding-core/src/config.rs`
- Create: `crates/funding-core/src/feature.rs`
- Create: `crates/funding-core/src/calendar.rs`
- Create: `crates/funding-core/src/opportunity.rs`
- Create: `crates/funding-features/Cargo.toml`
- Create: `crates/funding-features/src/lib.rs`
- Create: `crates/funding-core/tests/feature_contracts.rs`

**Interfaces:**
- Produces: `FeatureEvent`, `FeatureValidity`, `BookFeatures`, `FlowFeatures`, `BasisFeature`, `FundingCalendar`.
- Produces: `CostModel`, `FeeAssumption`, `Opportunity`, `OpportunityExclusion`, and `PnlBreakdown`.
- All later tasks consume the exact field names introduced here.

- [ ] **Step 1: Write the failing contract test**

```rust
use funding_core::{
    calendar::{FundingCalendar, FundingSlot},
    opportunity::{FeeAssumption, FeeSource, PnlBreakdown},
};

#[test]
fn contracts_keep_discrete_slots_and_attributed_pnl() {
    let calendar = FundingCalendar::new(vec![
        FundingSlot::estimated("binance_usdm", 1_800_000_000_000_000, 100_000_000_000_000),
        FundingSlot::estimated("bybit_linear", 1_800_014_400_000_000, 20_000_000_000_000),
    ]).unwrap();
    assert_eq!(calendar.slots().len(), 2);

    let fees = FeeAssumption::new(400_000_000_000_000, FeeSource::ExplicitConfig).unwrap();
    assert!(fees.rate > 0);
    let pnl = PnlBreakdown::zero();
    assert_eq!(pnl.total(), 0);
}
```

- [ ] **Step 2: Run the test and verify missing modules**

Run: `cargo test -p funding-core --test feature_contracts`

Expected: FAIL with unresolved `calendar`, `feature`, and `opportunity` imports.

- [ ] **Step 3: Define exact feature contracts**

```rust
pub enum FeatureValidity {
    Valid,
    Invalid(FeatureInvalidReason),
}

pub enum FeatureInvalidReason {
    MissingBook,
    EmptyBook,
    CrossedBook,
    LockedBook,
    NonPositiveValue,
    Stale { age_us: i64, limit_us: i64 },
    MissingConversion,
    InsufficientDepth,
    MissingInstrumentRule,
}

pub struct BookFeatures {
    pub mid: Option<i128>,
    pub microprice: Option<i128>,
    pub wap_for_quantity: Option<i128>,
    pub imbalance_1: Option<i128>,
    pub imbalance_5: Option<i128>,
    pub imbalance_10: Option<i128>,
    pub imbalance_20: Option<i128>,
    pub snapshot_ofi: Option<i128>,
    pub depth_delta_bid: Option<i128>,
    pub depth_delta_ask: Option<i128>,
    pub validity: FeatureValidity,
}
```

Define `FlowFeatures` with buy/sell base volume, quote notional, counts, mean size, signed imbalance, CVD, burst count, and mean inter-trade microseconds. Define `BasisFeature` with named reference/comparison venues and price kinds so the sign is explicit.

- [ ] **Step 4: Define fee, opportunity, and PnL contracts**

```rust
pub enum FeeSource { AuthenticatedCommission, ExplicitConfig }

pub struct FeeAssumption { pub rate: i128, pub source: FeeSource }

pub struct PnlBreakdown {
    pub funding_income: i128,
    pub execution_pnl: i128,
    pub basis_pnl: i128,
    pub trading_fees: i128,
    pub slippage: i128,
    pub residual_mark_to_market: i128,
}

pub struct Opportunity {
    pub symbol: CanonicalSymbol,
    pub short_venue: AdapterId,
    pub long_venue: AdapterId,
    pub capacity_base: i128,
    pub capacity_quote: i128,
    pub raw_gap: i128,
    pub hourly_gap: i128,
    pub indicative_apr: i128,
    pub conservative_funding_cashflows: i128,
    pub expected_net_pnl: i128,
    pub expected_net_bps: i128,
    pub decision_ts_us: i64,
}
```

`FeeAssumption::new` rejects zero/negative configured fees. `PnlBreakdown::total` sums all signed components with checked arithmetic.

- [ ] **Step 5: Add exact configured costs and research capacity**

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CostConfig {
    pub binance_taker_rate: ExactDecimal,
    pub bybit_taker_rate: ExactDecimal,
    pub stressed_exit_slippage_bps: ExactDecimal,
    pub book_impact_bps: ExactDecimal,
    pub basis_risk_buffer_bps: ExactDecimal,
    pub funding_error_buffer_bps: ExactDecimal,
    pub leg_risk_buffer_bps: ExactDecimal,
    pub research_quote_per_leg: ExactDecimal,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ExactDecimal(pub i128);
```

Deserialize `ExactDecimal` only from TOML strings through `md_core::decimal`; reject binary floating-point TOML values, zero/negative venue fees, negative buffers, and research capacity above the approved 100 USDT per-leg default. Label the fee source `ExplicitConfig` and the capacity source `ConfiguredResearchLimit`. Phase 2E replaces capacity with the minimum of this limit and authenticated testnet margin, and replaces fees with authenticated commission values.

- [ ] **Step 6: Run contracts and commit**

Run: `cargo test -p funding-core --test feature_contracts`

Expected: PASS.

```bash
git add Cargo.toml Cargo.lock crates/funding-core crates/funding-features
git commit -m "feat: define funding feature contracts"
```

---

### Task 2: Implement Book Microfeatures and Tick/Snapshot Flow

**Files:**
- Create: `crates/funding-features/src/book.rs`
- Create: `crates/funding-features/src/flow.rs`
- Create: `crates/funding-features/tests/book_flow.rs`

**Interfaces:**
- Produces: `compute_book_features(previous, current, requested_qty, now_us, stale_after_us) -> BookFeatures`.
- Produces: `TradeWindow::push(&mut self, trade: &TradeTick)` and `snapshot(&self, now_us) -> FlowFeatures`.

- [ ] **Step 1: Write exact formula and invalid-state tests**

```rust
#[test]
fn mid_microprice_imbalance_and_ofi_are_exact() {
    let previous = book(&[(99, 4)], &[(101, 6)]);
    let current = book(&[(99, 6)], &[(101, 2)]);
    let f = compute_book_features(Some(&previous), &current, scale(5), 1_000_100, 2_000_000);
    assert_eq!(f.mid, Some(scale(100)));
    assert_eq!(f.microprice, Some(scale_decimal("100.5")));
    assert_eq!(f.imbalance_1, Some(scale_decimal("0.5")));
    assert_eq!(f.snapshot_ofi, Some(scale(6)));
}

#[test]
fn crossed_locked_and_stale_books_never_emit_numbers() {
    for invalid in [crossed_book(), locked_book(), stale_book()] {
        let f = compute_book_features(None, &invalid, scale(1), 4_000_000, 2_000_000);
        assert!(f.mid.is_none());
        assert!(matches!(f.validity, FeatureValidity::Invalid(_)));
    }
}
```

- [ ] **Step 2: Confirm the tests fail**

Run: `cargo test -p funding-features --test book_flow`

Expected: FAIL because book/flow calculators do not exist.

- [ ] **Step 3: Implement book formulas with checked decimal math**

Use the spec formulas exactly:

```text
mid = (best_bid + best_ask) / 2
microprice = (best_ask * bid_qty + best_bid * ask_qty) / (bid_qty + ask_qty)
imbalance_N = (bid_qty_N - ask_qty_N) / (bid_qty_N + ask_qty_N)
```

WAP consumes price levels until requested base quantity is filled; otherwise return `InsufficientDepth`. Best-level OFI follows price-change/size-change rules on consecutive snapshots. Price-keyed depth delta sums quantities at the same prices across top 20. Use checked multiply/divide helpers at scale 18.

- [ ] **Step 4: Implement bounded flow windows**

```rust
pub struct TradeWindow {
    horizon_us: i64,
    trades: VecDeque<CompactTrade>,
    cumulative_volume_delta: i128,
}
```

Evict records older than the configured horizon. Compute buy/sell base and quote volumes, signed-volume imbalance, count, exact mean size, CVD, trades in the most recent one-second burst interval, and mean inter-trade duration. `TakerSide::Unknown` increments total count but not signed volume.

- [ ] **Step 5: Run properties and commit**

Add proptests asserting imbalance lies in `[-1,1]`, WAP lies inside consumed prices, and duplicate snapshots produce zero delta. Run: `cargo test -p funding-features --test book_flow`.

Expected: PASS.

```bash
git add crates/funding-features
git commit -m "feat: compute book and flow microfeatures"
```

---

### Task 3: Implement Quote-Aware NBBO and Named Basis

**Files:**
- Create: `crates/funding-features/src/quote.rs`
- Create: `crates/funding-features/src/nbbo.rs`
- Create: `crates/funding-features/src/basis.rs`
- Create: `crates/funding-features/tests/nbbo_basis.rs`

**Interfaces:**
- Produces: `QuoteMatrix::upsert(QuoteConversionSnapshot)` and `convert(value, from, to, side, now_us, max_age_us)`.
- Produces: `compute_nbbo(books, requested_base, target_quote, matrix, now_us, max_age_us) -> NbboResult`.
- Produces: `basis_bps(reference: NamedPrice, compared: NamedPrice) -> Result<BasisFeature, FeatureInvalidReason>`.

- [ ] **Step 1: Write side-aware conversion/NBBO tests**

```rust
#[test]
fn converted_nbbo_uses_executable_side_and_depth() {
    let matrix = usdt_krw_matrix("1399", "1401", 1_000_000);
    let result = compute_nbbo(
        &[upbit_btc_krw_book(), binance_btc_usdt_book()],
        scale_decimal("0.25"),
        "KRW",
        &matrix,
        1_000_100,
        2_000_000,
    ).unwrap();
    assert_eq!(result.requested_base, scale_decimal("0.25"));
    assert_eq!(result.bid.conversion.unwrap().side, ConversionSide::SellBase);
    assert_eq!(result.ask.conversion.unwrap().side, ConversionSide::BuyBase);
}

#[test]
fn stale_conversion_invalidates_cross_quote_nbbo() {
    let error = compute_nbbo(&mixed_books(), scale(1), "KRW", &old_matrix(), 9_000_000, 2_000_000).unwrap_err();
    assert!(matches!(error, FeatureInvalidReason::Stale { .. }));
}
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `cargo test -p funding-features --test nbbo_basis`

Expected: FAIL with unresolved quote/NBBO/basis interfaces.

- [ ] **Step 3: Implement executable conversions and NBBO**

Maintain explicit directed conversion edges. Buying USDT with KRW consumes the conversion ask; selling USDT consumes the conversion bid. Record source venue, side, source timestamp, local timestamp, and age in `AppliedConversion`. For each venue book, compute depth WAP for the requested quantity, convert to target quote, then select highest executable bid and lowest executable ask.

- [ ] **Step 4: Implement named basis**

```rust
pub struct NamedPrice {
    pub venue: AdapterId,
    pub kind: PriceKind,
    pub value: i128,
    pub ts_us: i64,
}

pub enum PriceKind { SpotMid, PerpetualMid, Mark, Index }
```

Compute `(compared - reference) / reference * 10_000` at scale 18, retain both `NamedPrice` identities, and reject non-positive or stale inputs. Tests cover spot-to-perp and mark-to-index sign direction.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-features --test nbbo_basis`

Expected: PASS.

```bash
git add crates/funding-features
git commit -m "feat: add quote aware nbbo and basis"
```

---

### Task 4: Implement Funding Calendars and Conservative Bounds

**Files:**
- Create: `crates/funding-features/src/funding.rs`
- Create: `crates/funding-features/tests/funding_calendar.rs`

**Interfaces:**
- Consumes: `FundingEstimate`, `FundingSettlement`, `FundingCalendar`, and `FundingSlot`.
- Produces: `FundingModel::observe_estimate`, `observe_settlement`, `calendar_for_pair`, and `conservative_cashflow`.

- [ ] **Step 1: Write interval, sign, and empirical-bound tests**

```rust
#[test]
fn unequal_intervals_are_discrete_cashflows_not_raw_subtraction() {
    let model = model_with_no_history();
    let calendar = model.calendar_for_pair(binance_8h_estimate(), bybit_4h_estimate()).unwrap();
    assert_eq!(calendar.slots().len(), 3);
    assert_eq!(calendar.initial_binance_settlement(), ts("2026-08-21T00:00:00Z"));
    assert_eq!(calendar.initial_bybit_settlement(), ts("2026-08-20T20:00:00Z"));
    assert_eq!(model.receipt_credit(scale_decimal("0.001"), 12), scale_decimal("0.0005"));
    assert_eq!(model.payment_reserve(scale_decimal("0.001"), 12), scale_decimal("0.001"));
}

#[test]
fn signed_cashflow_handles_positive_and_negative_rates() {
    assert_eq!(funding_cashflow(PositionSide::Short, scale_decimal("0.001"), scale(100)), scale_decimal("0.1"));
    assert_eq!(funding_cashflow(PositionSide::Long, scale_decimal("-0.001"), scale(100)), scale_decimal("0.1"));
}
```

- [ ] **Step 2: Confirm the tests fail**

Run: `cargo test -p funding-features --test funding_calendar`

Expected: FAIL because the funding model does not exist.

- [ ] **Step 3: Implement calendar construction and signed cash flow**

Build slots from each venue's explicit next timestamp and interval through the later of both initially identified settlements. Mark the two initial settlement identities. Credit the published next receiving estimate at 50% before 30 matched observations; charge payments at 100%. Assign zero benefit to unannounced later receipts and reserve adverse later payments from configured history.

- [ ] **Step 4: Implement empirical error observations**

Store bounded `(settled_rate - prior_estimate)` observations per venue/symbol. At 30+ samples use deterministic nearest-rank 5th percentile for receipt bounds and 95th percentile for payment bounds. Persist sample count, selected quantile, and bound in emitted feature evidence.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-features --test funding_calendar`

Expected: PASS for 1h/4h/8h intervals, negative rates, interval changes, and mismatched settlement times.

```bash
git add crates/funding-features
git commit -m "feat: model conservative funding calendars"
```

---

### Task 5: Rank Conservative Executable Opportunities

**Files:**
- Create: `crates/funding-features/src/opportunity.rs`
- Create: `crates/funding-features/src/engine.rs`
- Create: `crates/funding-features/tests/opportunity.rs`
- Create: `crates/funding-features/tests/engine.rs`

**Interfaces:**
- Produces: `OpportunityRanker::evaluate(&OpportunityInput) -> Result<Opportunity, OpportunityExclusion>`.
- Produces: `FeatureEngine::on_market`, `on_derivative`, and `snapshot(now_us) -> EngineSnapshot`.

- [ ] **Step 1: Write capacity/cost/ranking tests**

```rust
#[test]
fn positive_raw_gap_is_rejected_when_executable_net_is_negative() {
    let input = opportunity_input()
        .with_raw_gap_bps(scale_decimal("12"))
        .with_round_trip_fees_bps(scale_decimal("16"));
    let error = OpportunityRanker::default().evaluate(&input).unwrap_err();
    assert_eq!(error.code(), "NET_PNL_BELOW_MINIMUM");
}

#[test]
fn capacity_is_minimum_of_depth_rules_risk_and_capital() {
    let input = opportunity_input()
        .with_depth_capacity(scale(120))
        .with_rule_capacity(scale(90))
        .with_risk_capacity(scale(100))
        .with_capital_capacity(scale(80));
    assert_eq!(OpportunityRanker::default().evaluate(&input).unwrap().capacity_quote, scale(80));
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p funding-features --test opportunity --test engine`

Expected: FAIL on missing ranker and engine.

- [ ] **Step 3: Implement conservative net PnL**

Compute exactly:

```text
conservative funding cashflows
- entry fees - exit fees
- entry slippage - stressed exit slippage
- book impact - basis risk buffer
- funding error buffer - leg risk buffer
```

Do not include positive basis convergence. Require a nonzero fee source, fresh books/private-independent public inputs, both calendars, instrument rules, enough depth within 10 bps, and a capacity equal to the minimum of book/rule/risk/capital inputs. In monitor/paper the capital input is the labeled configured research limit; in testnet it is additionally capped by authenticated available margin. Rank by expected net PnL, then capacity, then configured symbol order for deterministic ties.

- [ ] **Step 4: Implement deterministic streaming state**

Key latest books, trades, mark/index, funding, OI, ratios, instruments, and conversions by venue/symbol. `snapshot(now_us)` emits feature events in configured symbol order and never depends on hash-map iteration. Bound trade windows and estimate history. Record every exclusion code and freshness age.

- [ ] **Step 5: Run and commit**

Run: `cargo test -p funding-features --test opportunity --test engine`

Expected: PASS, including duplicate-input idempotence and stable ordering.

```bash
git add crates/funding-features
git commit -m "feat: rank executable funding opportunities"
```

---

### Task 6: Persist Features and Add Read-Only Monitor Mode

**Files:**
- Create: `crates/md-storage/src/feature_schema.rs`
- Create: `crates/md-storage/src/feature_partition.rs`
- Modify: `crates/md-storage/src/lib.rs`
- Modify: `crates/md-storage/src/validate.rs`
- Create: `crates/md-storage/tests/feature_roundtrip.rs`
- Create: `crates/funding-app/src/monitor.rs`
- Modify: `crates/funding-app/src/lib.rs`
- Modify: `crates/funding-app/src/main.rs`
- Create: `crates/funding-app/tests/phase2b_e2e.rs`
- Modify: `README.md`
- Modify: `docs/data-schema.md`

**Interfaces:**
- Produces: `FeaturePartitionRouter` and validator support for all feature families.
- Produces CLI: `funding-app monitor --config config/funding.toml [--duration 60s]`.
- Produces: atomic `monitor-report.json` with ranked opportunities and exclusions.

- [ ] **Step 1: Write frozen-sequence E2E test**

```rust
#[tokio::test]
async fn frozen_events_produce_stable_ranked_arrow_output() {
    let root_a = test_root();
    let root_b = test_root();
    let events = frozen_phase2b_events();
    let a = run_monitor(events.clone(), &root_a, 7).await.unwrap();
    let b = run_monitor(events, &root_b, 7).await.unwrap();
    assert_eq!(a.opportunities, b.opportunities);
    assert_eq!(canonical_arrow_digest(&root_a), canonical_arrow_digest(&root_b));
    assert!(md_storage::validate_path(&root_a).unwrap().is_valid());
}
```

- [ ] **Step 2: Confirm storage/monitor interfaces are missing**

Run: `cargo test -p funding-app --test phase2b_e2e`

Expected: FAIL with unresolved monitor and feature-router imports.

- [ ] **Step 3: Define feature Arrow families**

Persist `book_features`, `tick_flow`, `snapshot_flow`, `nbbo`, `basis`, `funding_calendar`, and `opportunity`. Store all validity/exclusion codes and evidence fields. Partition by family, venue or `cross_venue`, symbol, UTC date/hour; validator enforces `Decimal128(38,18)`, enum values, timestamps, path metadata, and valid/ineligible mutual exclusivity.

- [ ] **Step 4: Integrate the monitor pipeline**

Fan Phase 1/2A events into storage and the feature engine through bounded channels. Coalesce read-only snapshots no faster than 100 ms. Write a final report containing ranked opportunities, excluded reasons, data ages, fee-source labels, empirical sample counts, and input gaps. The CLI has no authenticated or order capability.

- [ ] **Step 5: Run Phase 2B gates**

Run: `cargo fmt --all -- --check`

Run: `cargo clippy --workspace --all-targets --offline -- -D warnings`

Run: `cargo test --workspace --offline`

Run: `cargo build --workspace --release --offline`

Expected: all commands exit 0 and the deterministic E2E produces validated feature streams.

- [ ] **Step 6: Commit Phase 2B integration**

```bash
git add Cargo.toml Cargo.lock crates/funding-core crates/funding-features crates/md-storage crates/funding-app README.md docs/data-schema.md
git commit -m "feat: add funding opportunity monitor"
```

---

## Phase 2B Completion Gate

- Mid, microprice, WAP, imbalance 1/5/10/20, tick flow, CVD, snapshot OFI, and depth delta have exact deterministic tests.
- Direct and KRW/USDT converted NBBO use executable sides/depth and reject stale conversion.
- Every basis feature identifies reference/comparison venue and price kind.
- Funding opportunities use discrete settlement calendars, conservative bounds, explicit fee sources, and full cost deductions.
- Capacity is executable and conservative; raw rate gap alone cannot create a candidate.
- Feature/reason/evidence Arrow streams validate and replay identically.
- `funding-app monitor` remains public/read-only and has no order path.
