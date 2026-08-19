# Rust Multi-Exchange Market Data Collector Design

**Date:** 2026-08-19  
**Status:** Approved in chat; awaiting written-spec review  
**Project:** 손승한 코인 차익·펀딩비·마이크로피처 기반 HFT 미니프로젝트 — Phase 1

## 1. Summary

Phase 1 builds a headless Rust service that continuously collects public order-book snapshots and individual trade ticks from Upbit, Bithumb, Binance Spot, and Binance USDⓈ-M Futures. It normalizes timestamps and market identifiers, writes lossless numeric values to hourly Apache Arrow IPC stream files, reconnects automatically, and exposes enough operational statistics to prove whether data collection is healthy.

This phase creates trustworthy strategy input. It does not implement, backtest, or execute a trading strategy.

## 2. Goals

- Use Rust and Tokio asynchronous tasks for all live WebSocket feeds.
- Parse JSON with the Rust `simd-json` crate.
- Subscribe to order-book snapshots and non-aggregated trade ticks for 21 requested assets.
- Preserve exchange trade/engine time, exchange event/publication time, and local receive time independently where the source supplies them.
- Store all normalized timestamps as Unix epoch microseconds while retaining the precision of each source field.
- Persist data by exchange, market, symbol, UTC date, and UTC hour in Arrow IPC stream format.
- Handle ping/pong, normal connection expiry, disconnection, resubscription, and bounded retry.
- Avoid silent data loss. Every rejected event or known collection gap must be counted and reported.
- Produce deterministic parsing, storage, reconnection, and shutdown tests plus a live smoke-test report.
- Prioritize sustained throughput, correctness, and observability rather than CPU pinning or minimum single-message latency.

## 3. Non-goals

The following are explicitly deferred:

- NBBO basis, funding-rate, open-interest, top-trader, tick-flow, and order-flow feature engines
- Arbitrage, portfolio, newly listed asset, or any other trading strategy
- Backtesting, signal evaluation, fees, slippage, sizing, inventory, or risk management
- Binance testnet order placement/cancellation and fill reconciliation
- API-key authenticated feeds or private account data
- An iced GUI, Prometheus server, Kafka/NATS, distributed deployment, or fixed-core scheduling
- Binance incremental-depth local-book reconstruction
- Nanosecond latency claims or parsing-stage timestamp instrumentation

## 4. Markets and Symbols

The configured base assets are:

```text
BTC, ETH, XRP, SOL, DOGE, ADA, AVAX, LINK, DOT, BCH, LTC,
ETC, TRX, XLM, ATOM, NEAR, APT, SUI, ARB, OP
```

Default pair mapping:

- Upbit Spot: `KRW-{ASSET}`
- Bithumb Spot: `KRW-{ASSET}`
- Binance Spot: `{ASSET}USDT`
- Binance USDⓈ-M Futures: `{ASSET}USDT`

At startup, each adapter fetches its exchange's public active-market list and intersects it with the configured pairs. Missing, inactive, or unsupported pairs appear in the startup log and final report. Default mode continues with available pairs; `--strict-symbols` fails startup if any configured pair is unavailable.

## 5. Source Protocols

All endpoints and stream names are configuration values with the following production defaults:

| Adapter | Market discovery | WebSocket | Streams |
|---|---|---|---|
| Upbit Spot | `https://api.upbit.com/v1/market/all` | `wss://api.upbit.com/websocket/v1` | `trade`, `orderbook` |
| Bithumb Spot | `https://api.bithumb.com/v1/market/all` | `wss://ws-api.bithumb.com/websocket/v1` | `trade`, `orderbook` |
| Binance Spot | `https://api.binance.com/api/v3/exchangeInfo` | `wss://stream.binance.com:9443/stream` | `{symbol}@trade`, `{symbol}@depth20@100ms` |
| Binance USDⓈ-M | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `wss://fstream.binance.com/stream` | `{symbol}@trade`, `{symbol}@depth20@100ms` |

Upbit and Bithumb receive the source's real-time full snapshot: up to 30 levels per side on Upbit and up to 15 levels per side on Bithumb. Binance uses 20-level partial-book snapshots, not diff-depth reconstruction. The normalized model accepts `0..N` levels and never invents missing levels.

Public feeds require no API keys. Subscription formats, endpoints, and timing rules follow the current official references:

- [Upbit WebSocket usage](https://docs.upbit.com/kr/kr/reference/websocket-guide)
- [Upbit WebSocket best practices](https://docs.upbit.com/kr/docs/websocket-best-practice)
- [Bithumb API overview](https://apidocs.bithumb.com/reference/api-%EB%A0%88%ED%8D%BC%EB%9F%B0%EC%8A%A4)
- [Bithumb order book](https://apidocs.bithumb.com/reference/%ED%98%B8%EA%B0%80-orderbook)
- [Bithumb trades](https://apidocs.bithumb.com/reference/%EC%B2%B4%EA%B2%B0-trade)
- [Bithumb connection management](https://apidocs.bithumb.com/reference/%EC%97%B0%EA%B2%B0-%EA%B4%80%EB%A6%AC)
- [Binance API catalog](https://developers.binance.com/en/docs/catalog)

## 6. Architecture

The repository is one Cargo workspace with four responsibility boundaries:

```text
WebSocket adapters
    -> raw receive + local timestamp
    -> venue parser and normalization
    -> bounded normalized-event channel
    -> partition router
    -> hourly Arrow writers
```

- `md-core`: shared identifiers, normalized events, decimal and timestamp conversion, configuration, and adapter traits
- `md-exchanges`: one adapter module for each of Upbit, Bithumb, Binance Spot, and Binance USDⓈ-M
- `md-storage`: Arrow schemas, record-batch builders, partition routing, hourly rotation, recovery, and reading validation
- `collector`: CLI, task supervision, graceful shutdown, statistics, startup discovery, and final report

Every adapter is independently supervised. One exchange outage must not terminate healthy adapters. A fatal storage error terminates the entire process because continuing would create the false impression that data is being retained.

## 7. Normalized Events

### 7.1 Shared metadata

Each normalized event carries:

- `schema_version`
- `event_id`: UUIDv7 stored as 16-byte fixed binary
- `exchange`: `upbit`, `bithumb`, or `binance`
- `market`: `spot` or `usdm_futures`
- `symbol`: canonical pair such as `BTC/KRW` or `BTC/USDT`
- `source_symbol`: original exchange market code
- `source_stream`
- `source_sequence`: nullable exchange sequence/update/trade identifier
- `exchange_event_ts_us`: nullable publication/event timestamp
- `exchange_trade_ts_us`: nullable matching-engine/trade timestamp
- `local_recv_ts_us`: local wall-clock time captured immediately after the WebSocket frame becomes available and before JSON parsing
- `event_ts_precision` and `trade_ts_precision`: `microsecond`, `millisecond`, or `unavailable`
- `raw_size_bytes`

No timestamp is fabricated. Multiplying a millisecond value by 1,000 changes its storage unit but leaves the precision field as `millisecond`.

### 7.2 Order-book rows

One received snapshot has one `event_id`. It expands into one Arrow row per available price level per side with:

- shared metadata
- `side`: `bid` or `ask`
- `level`: zero-based best-to-worst rank
- `price`: `Decimal128(38,18)`
- `quantity`: `Decimal128(38,18)`

The parser validates that prices and quantities are positive and that bids descend while asks ascend. A malformed snapshot is rejected as one event rather than stored partially.

### 7.3 Trade rows

One exchange trade becomes one row with:

- shared metadata
- `trade_id`: source trade identifier as UTF-8
- `price`: `Decimal128(38,18)`
- `quantity`: `Decimal128(38,18)`
- `taker_side`: `buy`, `sell`, or `unknown`

For Binance, `buyer is maker = true` maps to a sell aggressor; false maps to a buy aggressor. Venue-native direction fields are mapped explicitly in fixture tests.

## 8. Arrow Storage

Data is partitioned by UTC hour:

```text
data/{exchange}/{market}/{symbol}/{YYYY-MM-DD}/{HH}/books.arrow
data/{exchange}/{market}/{symbol}/{YYYY-MM-DD}/{HH}/trades.arrow
```

The active file uses an `.arrow.partial` suffix. Record batches flush at 8,192 rows or after one second, whichever occurs first. At UTC hour rotation or graceful shutdown, the stream closes successfully and is atomically renamed to `.arrow`.

On startup, the storage layer scans only partitions that contain `.partial` files. It reads complete Arrow record batches, discards no valid batch, isolates an unreadable trailing fragment, rewrites a clean active stream, and records recovered and rejected byte counts. If recovery cannot prove the valid prefix, startup fails with the exact affected path.

Files include schema metadata for project name, schema version, timestamp unit, numeric scale, exchange, market, symbol, and UTC hour. All path components are generated from validated enums and symbols; source strings never become unchecked filesystem paths.

## 9. Concurrency and Backpressure

Each adapter owns a WebSocket task and parser. Normalized events enter a bounded channel with default capacity 65,536. The partition router owns writer handles, so WebSocket tasks never write files directly.

There is no silent drop policy. Enqueue waits for capacity. If an adapter cannot enqueue for five seconds, it increments `backpressure_disconnects`, records the beginning of a known gap, and restarts the connection. The event that could not be enqueued is counted as rejected. On successful resubscription, the gap is closed in the operational report.

This policy bounds memory and makes overload visible. It cannot guarantee exchange-level replay because the selected public WebSocket feeds offer no replay cursor. Normal operation is accepted only when the live smoke test reports zero backpressure disconnects and zero rejected events.

## 10. Connection Supervision

- Reply to WebSocket ping frames and send keepalive pings where required.
- Treat idle timeout, peer close, protocol error, subscription error, and parser-error threshold as reconnectable failures.
- Retry with exponential backoff from one second to 30 seconds plus jitter.
- Reset backoff after five continuous healthy minutes.
- Recreate the full subscription after every connection.
- Proactively rotate Binance connections before the documented maximum lifetime, with jitter so Spot and Futures do not reconnect simultaneously.
- Stop retrying only on process shutdown or invalid static configuration.
- Track every connection interval and reconnect reason.

A single malformed payload is rejected and counted. Ten consecutive malformed payloads trigger reconnection because they likely indicate schema drift or a corrupted subscription.

## 11. Shutdown and Failure Semantics

Ctrl+C initiates graceful shutdown:

1. Signal adapters to stop accepting new frames.
2. Close normalized-event senders.
3. Drain queued events.
4. Flush and close all Arrow streams.
5. Rename completed files and write the final JSON report.

Configuration, symbol discovery in strict mode, Arrow schema construction, disk-full, permission, and unrecoverable writer errors are fatal. Individual exchange connection failures are non-fatal and remain under supervision.

## 12. Configuration and CLI

Default configuration is TOML and contains endpoints, assets, quote currencies, output root, channel capacity, flush policy, timeouts, retry policy, and statistics interval. Environment variables may override endpoints and output root but not provide secrets because Phase 1 uses only public data.

Required commands:

```text
collector collect --config config/default.toml
collector collect --config config/default.toml --strict-symbols
collector validate --path <arrow-file-or-data-root>
collector smoke --config config/default.toml --duration 60s
```

`validate` checks schemas, readability, timestamps, decimal ranges, book ordering, and event grouping. `smoke` collects to an isolated output directory, validates it, and writes a machine-readable test report.

## 13. Observability

Every ten seconds, the CLI emits one compact structured statistics record per adapter:

- connection state and uptime
- frames/sec and bytes/sec
- parsed snapshots/sec, book rows/sec, and trades/sec
- parse and validation errors
- reconnects and reconnect reason
- queue capacity and high-water mark
- backpressure disconnects and known gap duration
- receive lag p50/p95/p99 where a source event timestamp exists
- current Arrow partition and rows written

On shutdown, the aggregate report is saved as JSON beside the configured data root. Raw market payloads are not logged by default.

## 14. Clock Accuracy

`local_recv_ts_us` uses the operating system wall clock at microsecond representation. That representation is not proof of microsecond clock accuracy. The README instructs users to enable system time synchronization and verify offset before comparing venues. The collector reports that limitation and does not silently adjust historical timestamps using an inferred offset.

## 15. Test Strategy

### Unit tests

- Market and symbol mapping
- Millisecond-to-microsecond conversion and precision retention
- Exact decimal parsing and scale/overflow rejection
- UTC partition paths and hour boundaries
- Book ordering and side mapping
- Backoff bounds and reset behavior with paused Tokio time

### Fixture tests

Committed, redacted official-format fixtures cover order books and trades for all four adapters: eight source/event combinations. Tests parse mutable byte buffers through `simd-json` and compare complete normalized events.

### Storage tests

- Arrow write/read round trip for books and trades
- Batch-size and timer flush
- UTC hour rotation
- graceful close and final rename
- truncated `.partial` recovery with complete-batch preservation
- disk/writer error propagation

### Connection tests

A local fake WebSocket server verifies subscription payloads, ping/pong, forced close, resubscription, exponential backoff, parser threshold, queue saturation, and graceful shutdown without relying on exchange availability.

### Live smoke test

Collect from every reachable configured market for at least 60 seconds, reopen the resulting Arrow files, and verify schema, nonzero row counts for active high-volume pairs, book ordering, event grouping, timestamp ranges, and finalization after Ctrl+C. The report lists unavailable pairs and asserts zero parse errors, rejected events, and backpressure disconnects under normal test load.

## 16. Acceptance Criteria

1. `cargo fmt --check` passes.
2. `cargo clippy --workspace --all-targets -- -D warnings` passes.
3. `cargo test --workspace` passes.
4. The live smoke test completes when network access is available and its generated Arrow data passes `collector validate`.
5. Every configured missing pair is visible; strict mode fails on any missing pair.
6. Ctrl+C produces readable finalized files and a final JSON report.
7. No parser error, rejected event, or backpressure disconnect occurs during the normal live smoke test.
8. README documents Windows, macOS, and Linux setup; clock synchronization; configuration; operation; Arrow reading examples; exchange timestamp limitations; and troubleshooting.

## 17. Deliverables

- Complete Cargo workspace source
- Default configuration for all 21 assets and four markets
- Parsing fixtures and automated tests
- README and architecture documentation
- A sample live-smoke validation report and small generated Arrow sample when network access permits
- Exact commands for the user to build, test, collect, validate, and inspect data

## 18. Later Phases

Phase 2 can consume this normalized dataset to add NBBO, funding, open interest, top-trader, tick-flow, and order-flow features. Phase 3 requires an explicit strategy specification before backtesting. Phase 4 can add the Binance testnet order/cancel engine and reconciliation target. None of those phases is implied by completion of this design.
