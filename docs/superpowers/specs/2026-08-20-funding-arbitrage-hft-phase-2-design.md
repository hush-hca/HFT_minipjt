# Funding-Arbitrage HFT Phase 2 Design

**Date:** 2026-08-20  
**Status:** Approved in conversation; pending written-spec review  
**Project:** 손승한 코인 차익·펀비·마이크로피처 기반 HFT 미니프로젝트  
**Depends on:** `2026-08-19-market-data-collector-design.md` (completed Phase 1)

## 1. Summary

Phase 2 extends the completed Rust market-data collector into a research-shaped HFT platform without replacing or narrowing the original project. It adds NBBO and basis calculation, derivatives metadata, funding rates, open interest, top-trader statistics, tick flow, snapshot-derived order flow, microprice features, replay, paper trading, a reliable order-management system, Binance and Bybit testnet execution, and a cross-platform `iced` GUI.

The first complete trading strategy is cross-exchange USDT perpetual funding arbitrage between Binance USDⓈ-M and Bybit Linear: short the venue with the economically higher funding rate and long the venue with the lower rate, then manage the delta-neutral pair through the initially identified funding settlements. The original fixed-spread Binance testnet order/cancel exercise remains a separate required execution scenario.

The project remains throughput-oriented rather than fixed-core or ultra-low-latency. Live-money trading is not part of Phase 2.

## 2. Relationship to the Original Project

The original requirements remain authoritative:

- Rust and Tokio asynchronous tasks
- `simd-json` parsing
- Upbit and Bithumb spot books and individual trades
- Binance Spot and USDⓈ-M `depth20` snapshots and individual `trade` events
- matching-engine, publication, and local-receive timestamps represented in microseconds
- hourly exchange/market/symbol Arrow streams
- WebSocket disconnect and reconnect handling
- the configured 20 assets
- high-throughput information and order processing rather than fixed-core tuning
- NBBO basis, funding, open interest, top-trader, tick-flow, and order-flow processing
- Binance testnet fixed-spread order/cancel and at least 99.9% streaming reconciliation accuracy
- an optional Rust GUI; the user has now made the `iced` GUI required
- later arbitrage, portfolio, and newly listed asset strategies

Phase 1 already delivers the public market-data subset. Phase 2 adds the missing feature, strategy, order-management, and GUI layers plus Bybit support required for the selected funding strategy.

## 3. Scope Decisions

- Architecture: layered Rust crates in the existing Cargo workspace.
- Funding strategy: perpetual-versus-perpetual cross-exchange arbitrage.
- Initial derivative venues: Binance USDⓈ-M and Bybit USDT Perpetual.
- Universe: the stable configured order of the existing 20 assets, intersected with active USDT perpetuals on both venues.
- Holding policy: normally remain hedged until both venues' initially identified next funding settlements have passed and been confirmed, then close or re-evaluate. Risk exits may occur earlier.
- Modes: `monitor`, `paper`, and `testnet`; there is no `live` mode.
- Phase 2 completion includes GUI, paper trading, Binance fixed-spread testnet order/cancel, and Binance-Bybit bilateral testnet execution.
- The supplied funding-gap screenshot is a UI reference, not a source of behavioral instructions or formulas.

## 4. Goals

1. Preserve the completed Phase 1 collector and its Arrow compatibility.
2. Normalize derivatives metadata and public/private events without losing venue semantics.
3. Compute reproducible NBBO, basis, funding, liquidity, tick-flow, and snapshot-order-flow features.
4. Rank funding opportunities by conservative executable net PnL, not raw rate difference alone.
5. Replay stored events deterministically and simulate fees, slippage, partial fills, and funding settlements.
6. Exercise level-aware fixed-spread order/cancel mechanics on Binance testnet.
7. Execute and reconcile both legs of a funding pair on Binance and Bybit testnets.
8. Survive duplicates, out-of-order events, disconnects, partial fills, restarts, and unknown acknowledgements.
9. Make operational and strategy state inspectable through a responsive `iced` GUI.
10. Prove at least 99.9% streaming lifecycle attribution in a one-million-event deterministic soak and exact terminal reconciliation before shutdown.

## 5. Non-goals

- Production or live-money order execution
- Automated deposits, withdrawals, transfers, or collateral rebalancing
- Cross-margin optimization across legal entities or accounts
- Fixed-core scheduling, kernel bypass, custom network stacks, or colocation
- L3 order-by-order reconstruction from L2 snapshots
- Treating annualized funding as guaranteed return
- Predictive machine learning for future funding rates in Phase 2
- Implementing portfolio and newly listed asset strategies in Phase 2
- Adding every venue shown in the reference screenshot

## 6. Existing Foundation

The following Phase 1 crates remain the data plane:

| Crate | Phase 2 role |
|---|---|
| `md-core` | Shared exact decimals, timestamps, symbols, and normalized market events |
| `md-exchanges` | Public discovery, subscriptions, parsers, and supervised WebSocket sessions |
| `md-storage` | Validated hourly Arrow streams, recovery, and dataset validation |
| `collector` | Existing public-feed CLI, health reporting, and smoke testing |

Phase 2 adds:

| Crate | Responsibility |
|---|---|
| `funding-core` | Instruments, funding calendars, features, opportunities, orders, fills, positions, and strategy states |
| `funding-features` | NBBO, quote conversion, basis, microfeatures, flows, funding normalization, capacity, and cost models |
| `funding-strategy` | Funding-pair and fixed-spread strategy state machines |
| `execution` | Paper, Binance testnet, and Bybit testnet executors plus reconciliation |
| `risk` | Pre-trade gates, exposure limits, data freshness, leg-risk handling, and kill switches |
| `funding-app` | Tokio orchestration, CLI modes, reports, and the `iced` GUI |

Crates communicate through typed interfaces. GUI code cannot call venue clients directly and cannot bypass the risk gate.

## 7. Runtime Architecture and Data Flow

```text
Upbit/Bithumb/Binance/Bybit public REST + WebSocket
                         |
                         v
              normalized market/meta events
                  /                   \
                 v                     v
       Arrow storage/validation   feature engine
                                       |
                                       v
                              opportunity ranker
                                       |
                                       v
                              strategy state machines
                                       |
Binance/Bybit private streams -> account cache -> risk gate
                                       |
                                       v
                            paper/testnet executors
                                       |
                                       v
                         order/fill reconciliation
                           /                    \
                          v                      v
                   SQLite WAL journal       UI snapshots
```

Market-data tasks never await GUI rendering. The GUI receives coalesced immutable snapshots every 100-250 ms through a bounded latest-value channel. A slow GUI drops superseded UI snapshots, not market or order events.

## 8. Venues, Instruments, and Discovery

Phase 1 venues remain enabled. Phase 2 adds `BybitLinear` and authenticated testnet clients for `BinanceUsdm` and `BybitLinear`.

The configured asset order remains:

`BTC, ETH, XRP, SOL, DOGE, ADA, AVAX, LINK, DOT, BCH, LTC, ETC, TRX, XLM, ATOM, NEAR, APT, SUI, ARB, OP`.

Funding-strategy eligibility requires, on both derivative venues:

- active/trading instrument status
- linear USDT settlement
- perpetual contract type
- valid tick size, quantity step, minimum quantity, and minimum notional
- known funding interval and next funding time
- public book, mark/index, and funding data freshness
- testnet availability when mode is `testnet`

Discovery preserves configured order and reports every missing or ineligible pair with a stable reason code.

An additional `USDT/KRW` reference market is collected where available to create an explicit quote-conversion matrix. Cross-currency NBBO or basis is invalid when the conversion quote is missing or stale.

## 9. Normalized Derivatives and Account Data

New normalized public event kinds are:

- `InstrumentSpec`
- `MarkIndexSnapshot`
- `FundingEstimate`
- `FundingSettlement`
- `OpenInterestSnapshot`
- `TraderRatioSnapshot`
- `QuoteConversionSnapshot`

New private event kinds are:

- `OrderUpdate`
- `ExecutionFill`
- `PositionSnapshot`
- `BalanceSnapshot`
- `FundingIncome`

All events carry adapter, market, canonical symbol, venue symbol, source timestamps where supplied, local receive time, and a unique event identifier. Venue semantics remain explicit. For example, Binance top-trader account ratio and Bybit long/short ratio are stored with different `metric_kind` values and are never silently treated as equivalent.

Prices, quantities, rates, fees, notionals, and PnL use exact decimal representations. Persisted financial fields do not use binary floating point.

## 10. Storage and Durability

Public market data, derivatives metadata, computed features, opportunity decisions, and strategy reports use hourly Arrow IPC streams partitioned by event kind, venue, market, symbol, UTC date, and hour.

Order intent and mutable execution state use SQLite in WAL mode because they require transactions, uniqueness constraints, and restart-safe state transitions. The journal stores:

- strategy and correlation identifiers
- deterministic client order identifiers
- exact request parameters and request hashes
- venue order identifiers
- every acknowledged order state
- cumulative fill quantity and fees
- reconciliation attempts and sources
- position/funding snapshots used to close a state transition
- operator commands and kill-switch transitions

Secrets are never stored in Arrow, SQLite, reports, logs, screenshots, or configuration files.

## 11. Feature Definitions

### 11.1 Quote conversion and NBBO

NBBO is calculated directly only among books with the same quote. For KRW/USDT comparison, USDT-quoted prices are multiplied by a fresh executable `USDT/KRW` quote. The feature records conversion venue, side, timestamp, and staleness.

For a requested executable quantity, NBBO uses depth-weighted bid and ask prices rather than last trade. The result includes venue, average price, maximum executable quantity, age, and rejection reason.

### 11.2 Mid and microprice

For best bid `b`, best ask `a`, bid quantity `q_b`, and ask quantity `q_a`:

```text
mid = (b + a) / 2
microprice = (a * q_b + b * q_a) / (q_b + q_a)
```

The value is invalid for a crossed, locked, empty, stale, or non-positive book.

### 11.3 Depth imbalance

For the top `N` levels:

```text
imbalance_N = (sum(bid_qty_N) - sum(ask_qty_N))
              / (sum(bid_qty_N) + sum(ask_qty_N))
```

The engine computes configured `N` values such as 1, 5, 10, and 20.

### 11.4 Tick flow

Trade aggressor side comes from venue semantics already normalized by Phase 1 parsers. Per configured window the engine computes:

- buy and sell base volume
- buy and sell quote notional
- trade count and mean size
- signed volume imbalance
- cumulative volume delta (CVD)
- burst rate and inter-trade duration

### 11.5 Snapshot-derived order flow

Because the project intentionally consumes L2 snapshots rather than incremental L3 events, it does not claim exact order add/cancel reconstruction. It computes:

- standard best-level OFI from consecutive best bid/ask price and size changes
- price-keyed quantity deltas across the top `N` levels
- depth pressure and refill/depletion measures

These fields are named `snapshot_ofi` and `depth_delta`, not `order_adds` or `order_cancels`.

### 11.6 Basis

The engine records absolute and basis-point differences among spot, perpetual, mark, and index prices. Positive basis is explicitly defined per ordered pair as:

```text
basis_bps(reference, compared) =
    (compared_price - reference_price) / reference_price * 10_000
```

Every basis value names both legs and price kinds so the sign cannot be misinterpreted.

### 11.7 Funding normalization

The indicative ranking shown by the GUI is:

```text
hourly_gap = short_rate / short_interval_hours
             - long_rate / long_interval_hours

indicative_apr = hourly_gap * 24 * 365
```

APR is display-only. It is not used as guaranteed PnL.

For signed position quantity where long is positive and short is negative, a settlement cash flow received by the account is:

```text
funding_cashflow = -sign(position) * rate * abs(mark_notional)
```

The strategy builds a discrete settlement calendar from venue timestamps. When one venue has additional settlements before the other venue's initially identified settlement, uncertain future receipts are not credited at full value. The conservative model:

- counts the venue-published next estimate with a configurable haircut derived from observed estimate error;
- assigns zero expected benefit to later unannounced receiving settlements;
- reserves an adverse cost from configured historical quantiles for later paying settlements;
- re-evaluates after every observed settlement;
- allows an early risk exit even though normal policy waits for both initially identified settlements.

Until at least 30 estimate-versus-settlement observations exist for a venue and symbol, the default haircut credits only 50% of a predicted receipt and charges 100% of a predicted payment. After that minimum sample, conservative receipt and payment bounds use configurable empirical error quantiles, defaulting to the 5th and 95th percentiles respectively.

### 11.8 Executable funding opportunity

```text
expected_net_pnl =
    conservative_funding_cashflows
  - entry_fees
  - exit_fees
  - entry_slippage
  - stressed_exit_slippage
  - book_impact
  - basis_risk_buffer
  - funding_error_buffer
  - leg_risk_buffer
```

No positive basis convergence is assumed unless a separately tested model supplies it. Capacity is the minimum of book depth, venue quantity/notional limits, configured risk limits, and available testnet margin.

## 12. Strategy State Machines

### 12.1 Funding pair

```text
Idle
 -> Candidate
 -> Preflight
 -> EnteringBoth
 -> Hedged
 -> WaitingForSettlements
 -> SettlementsConfirmed
 -> ReevaluateOrClose
 -> ClosingBoth
 -> Flat
```

Failure substates include `HedgeRetry`, `EmergencyReduce`, `Reconcile`, and `Halted`.

If Binance funding is economically higher, the intended pair is Binance short and Bybit long. If Bybit is higher, the pair is reversed. Negative rates are handled by the same signed-cashflow formula.

Both legs target equal base-asset delta after venue quantity quantization. Residual delta must be below the larger of 1 USDT or 0.5% of pair notional under default testnet settings.

The default entry gate requires:

- fresh public and private data
- known funding calendars and instrument rules
- conservative expected net PnL of at least 10 bps
- sufficient depth within 10 bps maximum entry slippage
- one-times leverage and at most 100 USDT per leg
- at most one active pair
- no unknown orders or unreconciled account state

Orders are parallel marketable IOC limits constrained by the slippage cap. If only one leg fills, the executor cancels residual orders, retries the hedge within a two-second budget, then reduces or closes the filled leg. A residual exposure beyond the emergency limit disables all new strategies.

Closing begins normally only after both actual funding income records are confirmed. A risk exit may close earlier.

### 12.2 Binance fixed-spread order/cancel

The fixed-spread exercise quotes a post-only bid and ask around a configurable anchor, initially microprice when valid and mid otherwise. Prices are rounded away from crossing according to exchange tick rules.

The strategy cancels/replaces when:

- the anchor moves beyond the configured reprice threshold;
- order age exceeds its limit;
- inventory or imbalance breaches a limit;
- the book or account stream becomes stale;
- the strategy is disabled or killed.

Partial fills update inventory before replacement. The exercise shares the same journal, idempotent client IDs, rate limiter, private streams, and reconciler as the funding strategy.

## 13. Order Management and Reconciliation

The canonical lifecycle is:

```text
Intent -> Submitted -> Acknowledged -> PartiallyFilled
       -> Filled | Canceled | Rejected | Expired
       -> Reconciled
```

REST acknowledgement is never considered final execution. Private WebSocket order and execution events drive the fast path; REST order, trade, and position queries repair gaps.

Each command has a deterministic unique client order ID. Retries query by client order ID before creating anything new. Cumulative fill quantity is monotonic, and duplicate or out-of-order updates are idempotent.

An unknown result is not retried as a new order. It enters `Reconcile`, queries both venue order state and account state, and remains trading-blocking until resolved.

At startup the system compares SQLite state with venue open orders, recent fills, positions, balances, and funding income. Testnet mode cannot arm until the comparison is exact. An operator may initiate documented testnet-only recovery actions such as canceling or closing, but cannot override the final exact-consistency gate.

## 14. Risk Model

Safe defaults are:

- startup mode `monitor`
- leverage 1x
- 100 USDT maximum per leg
- one concurrent funding pair
- 10 bps maximum entry slippage
- 10 bps minimum conservative net opportunity
- two-second market-data staleness threshold
- two-second hedge completion budget
- residual delta limit: max(1 USDT, 0.5% pair notional)

The risk gate rejects new orders when any required feed is stale, clock offset exceeds the signed-request tolerance, public or private connections are unhealthy, margin or liquidation distance is unavailable, the journal cannot commit, order state is unknown, rate-limit headroom is inadequate, or a kill switch is active.

Global controls are disable strategy, cancel all open orders, request position close, and kill all new order flow. `Cancel all` and `close positions` are distinct operations and require distinct confirmations.

## 15. GUI Design

The `iced` application supports Windows, macOS, and Linux and contains five views.

### Funding Opportunities

- token and eligibility
- short and long venue
- funding rates and intervals
- time to each settlement
- raw gap, hourly-normalized gap, and indicative APR
- gross and conservative net PnL
- capacity, confidence, freshness, and excluded reason

### Market Detail

- both venue books
- mid and microprice/WAP charts
- spot/perpetual/mark/index basis
- open interest and explicitly labeled trader-ratio metrics
- CVD, tick flow, best-level OFI, depth imbalance, and depth deltas
- publication-to-local latency and feed freshness

### Strategy and Orders

- state-machine state and transition reason
- both legs, orders, partial fills, and positions
- residual delta
- predicted and confirmed funding
- fee, slippage, basis, funding, and total PnL attribution
- streaming and terminal reconciliation metrics

### System Health

- frames, events, features, and orders per second
- parser/validation failures, reconnects, gaps, and backpressure
- REST rate-limit headroom
- public/private connection status
- Arrow and SQLite health

### Risk and Controls

- `monitor`, `paper`, and `testnet` mode status
- explicit testnet arming control
- strategy enable/disable
- cancel-all control
- close-position request
- global kill switch

Secrets are never displayed. Mode and control changes are journaled.

## 16. Modes and Endpoint Safety

- `monitor`: production public market data is allowed; no authenticated order capability exists.
- `paper`: production public data feeds the deterministic simulator; no authenticated order capability exists.
- `testnet`: only allowlisted Binance and Bybit testnet REST, public WebSocket, private WebSocket, and order-entry endpoints are accepted.

Mainnet signals are never translated directly into testnet prices or symbols. Testnet execution uses testnet instruments and public data. Its economic funding results are not presented as evidence of mainnet profitability.

API keys are read from environment variables or an operating-system credential provider. The config contains only environment-variable names, never secret values. Withdrawal permissions are neither needed nor accepted.

## 17. Failure and Recovery Semantics

- Public feed loss invalidates dependent features and candidates but does not corrupt other adapters.
- Private stream loss blocks orders immediately, cancels known open strategy orders when state is confirmed, and triggers REST reconciliation.
- A one-leg fill prioritizes exposure reduction over expected funding profit.
- HTTP timeout or venue 5xx yields unknown state, not assumed failure.
- Rate-limit responses obey venue retry guidance and reduce activity; repeated violations halt the executor.
- Arrow or SQLite failure blocks new order flow. Existing exposure is reconciled before any automated action.
- Clock offset beyond tolerance blocks signed calls and testnet arming.
- GUI command-channel loss does not block the data plane, reconciliation, or exposure monitoring, but it deterministically disarms all new automated testnet entries until the GUI reconnects and the operator arms the mode again.
- Graceful shutdown stops new strategy decisions, cancels open quoting orders, resolves in-flight commands, reconciles positions, flushes Arrow, checkpoints SQLite WAL, and writes a final report.

## 18. Testing Strategy

### Unit and property tests

- exact decimal and quantization behavior
- 1h/4h/8h funding normalization, negative rates, and interval changes
- discrete settlement calendars and conservative future-slot treatment
- quote conversion, NBBO, basis, mid, microprice, imbalance, OFI, CVD, and capacity
- fee, slippage, residual-delta, and conservative PnL formulas
- all legal and illegal state transitions
- idempotence under duplicate and out-of-order order events

### Fixture and fake-server tests

- official-format Binance and Bybit public/private payloads
- paginated instrument discovery and missing symbols
- reconnect and resubscribe
- partial fill/cancel races
- acknowledgement timeout with eventual fill
- unknown order repaired through REST
- private stream gap and startup reconciliation
- funding confirmation and mismatched settlement times

### Replay tests

A frozen Arrow dataset produces byte-stable decisions and equivalent reports given the same config and simulator seed. Replay covers funding rate changes, basis widening, stale conversion quotes, insufficient depth, and early risk exits.

### Million-event reconciliation soak

A deterministic local matching engine emits one million order lifecycle events including 10,000 fills and injected duplicates, reordering, omissions, disconnects, partial fills, and cancel/fill races.

- at least 99.9% of streaming updates must be attributed to the correct canonical order without REST repair;
- after repair, terminal journal, order, fill, and position state must be exactly consistent;
- duplicate orders, unknown terminal orders, residual positions, and residual delta must all be zero;
- every injected discrepancy and repair path must appear in the report.

The one-million-event goal is not implemented by sending one million requests to a public testnet. Testnet validation is rate-limit-safe and reports actual request, order, cancel, fill, reconnect, and reconciliation counts.

### GUI tests

- reducer/state tests independent of rendering
- sorting and filtering for funding opportunities
- stale/error/kill-state presentation
- control confirmation and mode gating
- bounded snapshot behavior under a slow renderer
- build and smoke checks on Windows, macOS, and Linux where CI runners are available

## 19. Phase 2 Delivery Sequence

### Phase 2A: Derivatives data and metadata

Add Bybit public discovery/streams; Binance and Bybit mark/index, funding, OI, ratios, instrument rules, and quote-conversion inputs; add Arrow schemas and validation.

### Phase 2B: NBBO and microfeatures

Add quote-aware NBBO, basis, mid/microprice, depth imbalance, tick flow, snapshot OFI, funding calendars, conservative opportunities, and feature storage.

### Phase 2C: `iced` GUI

Add read-only opportunity, market-detail, and system-health views first; add strategy/order/risk controls only after their engines exist.

### Phase 2D: Replay and paper trading

Add deterministic Arrow replay, fill/funding simulation, cost attribution, fixed-spread simulation, and paired funding strategy state machines.

### Phase 2E: OMS and testnet

Add SQLite journal, private clients, paper/testnet executor interface, Binance fixed-spread execution, bilateral funding execution, risk gates, restart reconciliation, million-event local soak, and rate-limit-safe public testnet validation.

No later subphase may bypass the acceptance gates of an earlier subphase.

## 20. Acceptance Criteria

Phase 2 is complete only when:

1. Existing Phase 1 tests and live-smoke behavior remain valid.
2. All eligible common configured perpetuals are discovered and every exclusion is reported.
3. Funding, OI, ratios, quote conversion, NBBO, basis, and microfeatures persist in validated Arrow schemas.
4. Unequal funding intervals and timestamps are modeled as discrete cash flows rather than raw-rate subtraction.
5. Replay is deterministic and paper PnL is fully attributed.
6. The fixed-spread strategy performs post-only place/cancel/replace and handles partial fills.
7. The funding strategy can enter, hedge, observe settlements, and close on Binance and Bybit testnets without live endpoints.
8. Any unhedged state triggers the documented retry/reduce/halt policy.
9. Streaming reconciliation reaches at least 99.9% in the one-million-event soak and terminal state is exact.
10. Restart reconciliation blocks trading until orders, fills, positions, balances, and funding income agree.
11. The GUI displays all five views, remains responsive under load, and cannot bypass risk.
12. Reports disclose data gaps, prediction error, fees, slippage, basis PnL, funding PnL, unknown states, and repairs.
13. No API secret or withdrawal capability is persisted or exposed.

## 21. Later Phases

- Phase 3: long-running shadow operation and evaluation of forecast error, realized costs, reconciliation, and operational stability.
- Phase 4: additional arbitrage, portfolio, and newly listed asset statistical strategies using the same feature, replay, risk, and OMS interfaces.
- Phase 5: live trading only after a separate design covering credentials, account topology, capital limits, collateral transfers, compliance, monitoring, and incident response receives explicit approval.

## 22. Primary API References

- [Binance USDⓈ-M market data and funding endpoints](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#get-funding-rate-history)
- [Binance USDⓈ-M new order](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#new-order)
- [Binance USDⓈ-M order updates](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams/Event-Order-Update)
- [Bybit instrument information](https://bybit-exchange.github.io/docs/v5/market/instrument)
- [Bybit tickers and current funding fields](https://bybit-exchange.github.io/docs/v5/market/tickers)
- [Bybit order creation](https://bybit-exchange.github.io/docs/v5/order/create-order)
- [Bybit private order stream](https://bybit-exchange.github.io/docs/v5/websocket/private/order)
- [Bybit private execution stream](https://bybit-exchange.github.io/docs/v5/websocket/private/execution)
- [Bybit WebSocket endpoints](https://bybit-exchange.github.io/docs/v5/ws/connect)
