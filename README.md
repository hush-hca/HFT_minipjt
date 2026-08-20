# Multi-Exchange Arrow Market-Data Collector

This repository contains the completed Phase 1 market-data collector and the Phase 2A public derivatives collector. Phase 1 collects public order-book snapshots and individual trades from Upbit Spot, Bithumb Spot, Binance Spot, and Binance USDⓈ-M. Phase 2A adds Binance USDⓈ-M and Bybit Linear instrument rules, mark/index prices, indicative and settled funding, open interest, venue-specific trader ratios, Bybit reconstructed top-20 books, and executable USDT/KRW quote conversions.

This remains data infrastructure only. It contains no strategy decisions, NBBO/microfeature engine, backtest, order placement or cancellation, fill reconciliation, risk engine, paper/testnet execution, or GUI. Do not run a strategy against it as though those components were included.

## Phase 2A public derivatives collection

Build and run a bounded public-only collection:

```powershell
cargo build --release -p funding-app --offline
target\release\funding-app.exe collect --config config/funding.toml --duration 60s
```

Omit `--duration` to run until Ctrl+C. The command surface intentionally contains only `collect`, `--config`, and `--duration`; configuration rejects unknown fields and contains no credentials. Mainnet and testnet instrument discovery are independent. Mainnet public streams are collected, while testnet availability is reported for later execution phases.

The collector starts storage before producers, uses bounded market and derivative channels, then stops producers, drains both channels, finalizes both Arrow routers, recursively validates the dataset, and atomically writes a successful `phase2a-report.json`. Startup, producer, storage, or validation failure triggers the same bounded drain/finalization attempt and may write a failure report whose aggregated `health_errors` explains the incomplete run; that file is operational evidence, not a claim that validation succeeded. Existing reports are replaced directly with the platform atomic-replace primitive: Unix syncs the temporary file, renames over the target, and syncs the parent directory; Windows uses `ReplaceFileW` for an existing report and write-through `MoveFileExW` for first publication. There is no target-away rename window. The report includes requested/eligible/excluded symbols for both environments, stable reason codes, per-family counts, reconnect/sequence-gap/reject/staleness/rate-budget counters, scheduler obligations, measured public-network attempts and credential-policy violations, and finalized paths. `sequence_gaps` counts only order/ticker sequence continuity failures; timestamp regressions remain parser rejects.

Current official Binance top-trader ratio endpoints require an API key. Phase 2A does not call them and reports `BINANCE_TOP_TRADER_REQUIRES_API_KEY`; Bybit's public long/short ratio supplies the collected `trader_ratio` family. No credential support is added as a workaround.

## What it produces

During collection, the service writes one active `*.arrow.partial` stream per event kind and UTC-hour partition. A clean hour rotation or graceful shutdown finalizes it as `books.arrow` or `trades.arrow`. It also prints structured JSON statistics and writes `run-report.json` under the configured output root.

```text
data/
├── upbit/spot/BTC-KRW/2026-08-20/12/
│   ├── books.arrow
│   └── trades.arrow
├── bithumb/spot/BTC-KRW/2026-08-20/12/
├── binance/spot/BTC-USDT/2026-08-20/12/
├── binance/usdm_futures/BTC-USDT/2026-08-20/12/
└── run-report.json
```

Partition hours are UTC and are selected from `local_recv_ts_us`. The complete field and enum contract is in [docs/data-schema.md](docs/data-schema.md).

## Prerequisites and build

Install Rust with [rustup](https://rustup.rs/). The workspace requires Rust 1.85 or newer and pins the stable toolchain in `rust-toolchain.toml`.

Windows (PowerShell):

1. Install **Visual Studio 2022 Build Tools** with **Desktop development with C++** and a Windows SDK.
2. Install Rust with `winget install Rustlang.Rustup`, or run the rustup installer from rustup.rs.
3. Open a new PowerShell window, then build:

```powershell
rustup default stable-x86_64-pc-windows-msvc
cargo build --release -p collector
target\release\collector.exe --help
```

macOS:

```bash
xcode-select --install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
cargo build --release -p collector
./target/release/collector --help
```

Linux (Debian/Ubuntu example):

```bash
sudo apt-get update
sudo apt-get install -y build-essential curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
cargo build --release -p collector
./target/release/collector --help
```

TLS uses bundled Web PKI roots through rustls; OpenSSL development headers are not required.

## Clock synchronization

Enable system time synchronization before comparing prices or receive lag across machines or venues. Check it with the command appropriate for the host:

```powershell
# Windows (run in an elevated PowerShell if access is denied)
w32tm /query /status
```

```bash
# macOS
sudo systemsetup -getusingnetworktime
sudo systemsetup -getnetworktimeserver
```

```bash
# Linux with systemd-timesyncd
timedatectl status
timedatectl timesync-status

# Linux with chrony
chronyc tracking
```

`local_recv_ts_us` is an operating-system wall-clock timestamp stored as an integer number of microseconds. Microsecond representation does **not** imply microsecond clock accuracy, timestamping accuracy, or inter-host synchronization. Millisecond source timestamps are multiplied by 1,000 for storage but remain marked as millisecond precision.

## Configuration

Copy or edit `config/default.toml`. All four adapter tables must be present even when an adapter is disabled. This implementation reads TOML directly; it does not currently apply environment-variable overrides to endpoints or the output root.

| Field | Meaning |
|---|---|
| `output_root` | Dataset and final `run-report.json` directory. |
| `assets` | Unique uppercase ASCII base assets. Defaults to the 20 requested assets. |
| `strict_symbols` | Fail discovery when any requested pair is unavailable; default `false`. |
| `channel_capacity` | Maximum normalized events buffered between adapters and storage; must be greater than zero. |
| `batch_rows` | Flush threshold per Arrow builder; must be greater than zero. A book event expands to multiple rows. |
| `flush_interval_ms` | Maximum interval for flushing a nonempty builder; must be greater than zero. |
| `enqueue_timeout_ms` | Maximum wait for channel capacity before rejecting the event, recording backpressure, and reconnecting; must be greater than zero. |
| `stats_interval_secs` | Structured per-adapter statistics interval; must be greater than zero. |
| `retry.initial_ms` | Initial reconnect delay. |
| `retry.max_ms` | Reconnect delay cap; must be at least `initial_ms`. |
| `retry.reset_after_secs` | Healthy connection duration after which backoff resets. |
| `adapters.<name>.enabled` | Enables discovery and WebSocket collection for the adapter. |
| `adapters.<name>.quote` | Uppercase quote asset (`KRW` or `USDT` in the default file). |
| `adapters.<name>.rest_url` | Public active-market discovery endpoint. HTTPS is required except for loopback tests. |
| `adapters.<name>.websocket_url` | Public WebSocket endpoint. WSS is required except for loopback tests. |
| `adapters.<name>.proactive_reconnect_secs` | Optional positive connection-rotation interval. Used for the Binance adapters by default. |

The recognized adapter table names are `upbit_spot`, `bithumb_spot`, `binance_spot`, and `binance_usdm`. Public market-data collection needs no API key, account credentials, or secret configuration. Never place credentials in this file.

The default assets are BTC, ETH, XRP, SOL, DOGE, ADA, AVAX, LINK, DOT, BCH, LTC, ETC, TRX, XLM, ATOM, NEAR, APT, SUI, ARB, and OP. Each venue's discovered active markets are intersected with those requested pairs.

## Commands

The examples below use the Windows executable. On macOS/Linux, replace `target\release\collector.exe` with `./target/release/collector`.

Collect continuously:

```powershell
target\release\collector.exe collect --config config/default.toml
```

Missing or inactive pairs are retained in the final report while available pairs continue. To fail startup when any requested pair is missing, either set `strict_symbols = true` or use the CLI override:

```powershell
target\release\collector.exe collect --config config/default.toml --strict-symbols
```

The flag only enables strictness; it never disables `strict_symbols = true` from the file. Set `RUST_LOG` to adjust structured logging, for example `$env:RUST_LOG = "collector=debug,md_exchanges=info"` in PowerShell or `export RUST_LOG=collector=debug,md_exchanges=info` in a POSIX shell.

Validate a finalized Arrow file or recursively validate a dataset tree:

```powershell
target\release\collector.exe validate --path data
target\release\collector.exe validate --path data --json
target\release\collector.exe validate --path data\binance\spot\BTC-USDT\2026-08-20\12\trades.arrow
```

Text mode prints totals on success. `--json` prints a `files`, `batches`, `rows`, and `errors` report. A validation issue produces a nonzero exit status. Validation covers finalized `books.arrow` and `trades.arrow` streams, schemas and metadata, path consistency, timestamps and precision flags, decimals, event grouping, book sides/order, and trade identifiers/sides.

Run a bounded live collection into an isolated directory and validate it:

```powershell
target\release\collector.exe smoke --config config/default.toml --duration 60s --output outputs/live-smoke
```

`DURATION` accepts humantime values such as `30s`, `2m`, or `1h` and must be greater than zero. `--output` is optional; without it, a unique child of the configured `output_root` is selected. A supplied output directory must be absent or empty. Smoke writes `smoke-report.json` inside that directory and exits nonzero unless the Arrow data validates, one exchange/market/symbol BTC identity has both finalized book and trade data, and all adapters report zero parse errors, rejected events, and backpressure disconnects. An exchange network restriction or unavailable market can therefore make a live smoke fail even when deterministic tests pass.

## Graceful shutdown and recovery

Press Ctrl+C once and allow the command to return. The collector stops adapter intake, closes senders, drains the bounded event queue, flushes and finishes Arrow streams, renames `*.arrow.partial` to finalized `*.arrow`, and atomically writes `run-report.json`. Do not terminate the process a second time while it drains.

After a crash or power loss, leave `*.arrow.partial` files in place and restart with the same `output_root`. Startup scans partial streams, preserves every decodable complete record batch, rewrites a clean partial stream, and reports `batches_kept`, `rows_kept`, `bytes_kept`, and `bytes_rejected` in `run-report.json`. An unreadable trailing fragment is preserved in a uniquely named sibling ending in `.corrupt`; do not delete it until the recovery report has been reviewed. If the Arrow header itself cannot be proven, startup fails and names the affected path rather than guessing.

Disk-full, permission, schema, and writer failures are fatal. The collector cancels adapters and attempts to close storage and write a failure report, but the same filesystem fault may prevent finalization or report publication. Fix free space or permissions, retain all `.partial`/recovery files, and restart. Never manually rename a partial file to `.arrow`.

## Interpreting health

For a normal smoke run, expect:

- `status: "passed"`, an empty `health_errors` array, and `high_volume_btc_seen: true`;
- validation `errors: []` and nonzero files/batches/rows;
- zero `parse_errors`, `rejected_events`, and `backpressure_disconnects` for each active adapter;
- unavailable configured pairs listed under `run.missing_markets` rather than silently hidden.

For continuous collection, inspect `run-report.json`, especially `status`, `adapters`, `missing_markets`, `recovery`, and `clock_note`. Reconnects and known gap durations are visible; selected public streams provide no replay cursor, so a recorded gap cannot be reconstructed by this service.

## Verified release evidence

The release binary completed a 60-second public-network smoke test on 2026-08-20 KST across all four adapters. The run finalized 156 Arrow streams containing 3,473 record batches and 731,665 rows. Recursive validation returned no issues, BTC book and trade data were present, and every adapter reported zero parse errors, validation errors, rejected events, and backpressure disconnects. Upbit and Bithumb reported `LTC/KRW` unavailable; those missing markets remained visible in the report.

The retained evidence is `outputs/smoke-report.json`, with a small independently validated BTC trade sample under `outputs/sample-data/`. Bulk live data remains ignored. Live venue state can change, so rerun the smoke command in your own environment before relying on a later collection session.

## Troubleshooting

- **`cargo` or `rustup` is not found:** open a new terminal after rustup installation and confirm `$HOME/.cargo/bin` (or `%USERPROFILE%\.cargo\bin`) is on `PATH`.
- **Windows linker errors (`link.exe` missing):** install Visual Studio Build Tools with Desktop development with C++ and a Windows SDK, then reopen the terminal.
- **Discovery/WebSocket TLS or DNS failures:** verify DNS, proxy/firewall policy, system clock, and access to the four public REST/WSS hosts in `config/default.toml`. Corporate TLS interception may require an approved network path; do not disable certificate verification.
- **Strict-symbol startup failure:** review every missing pair, correct the asset/quote if appropriate, or run non-strict mode to collect only available pairs. Missing pairs remain visible in the report.
- **Frequent reconnects or backpressure:** inspect per-adapter `reconnects`, `queue_high_water`, `rejected_*`, and `known_gap_duration_us`. Use a faster disk or cautiously increase `channel_capacity`; do not interpret dropped intervals as continuous data.
- **Disk-full or access-denied error:** stop, free space or grant the service account write permission to `output_root`, preserve partial files, and restart.
- **Validator reports no files:** point it at finalized `books.arrow`/`trades.arrow` data. An active or crash-recovered `.arrow.partial` is not acceptance evidence until finalized.
- **Smoke output is not empty:** choose a new directory or move the old evidence; smoke intentionally refuses to mix runs.

## Developer quality gates

These checks require no live exchange connection once dependencies are cached:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
cargo build --release -p collector --offline
cargo build --workspace --release --offline
```

The deterministic suite includes fixture parsers, fake REST/WebSocket servers, storage rotation/recovery/validation, shutdown, and end-to-end fake collection. A passing deterministic suite is not a claim that a live smoke test ran or passed.
