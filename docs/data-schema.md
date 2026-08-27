# Arrow IPC Data Schema

The collector writes Apache Arrow IPC **stream** files, not Parquet and not Arrow IPC file-format containers. Schema version 1 uses exact `Decimal128(38,18)` prices and quantities and Unix epoch microsecond integers for all timestamps.

## Partition and file contract

```text
{output_root}/{exchange}/{market}/{BASE}-{QUOTE}/{YYYY-MM-DD}/{HH}/books.arrow
{output_root}/{exchange}/{market}/{BASE}-{QUOTE}/{YYYY-MM-DD}/{HH}/trades.arrow
```

`YYYY-MM-DD/HH` is the UTC hour of `local_recv_ts_us`. Valid path values are:

- `exchange`: `upbit`, `bithumb`, or `binance`
- `market`: `spot` or `usdm_futures`
- symbol directory: uppercase validated `BASE-QUOTE`, for example `BTC-KRW` or `BTC-USDT`
- event filename: `books.arrow` or `trades.arrow`

While a writer is active, its name is `books.arrow.partial` or `trades.arrow.partial`. Only the finalized `.arrow` streams form the validation contract.

Every stream schema contains this string metadata:

| Key | Value |
|---|---|
| `project` | `hft-market-data-collector` |
| `schema_version` | `1` |
| `timestamp_unit` | `microsecond` |
| `decimal_scale` | `18` |
| `exchange` | Path-consistent exchange value |
| `market` | Path-consistent market value |
| `symbol` | Canonical `BASE/QUOTE`, for example `BTC/USDT` |
| `utc_hour` | UTC RFC 3339 hour, for example `2026-08-19T12:00:00Z` |

## Common fields

The following fields appear first, in this order, in both book and trade schemas.

| Field | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `schema_version` | `UInt16` | No | Row schema version; currently `1`. |
| `event_id` | `FixedSizeBinary(16)` | No | UUIDv7 bytes. All rows expanded from one book snapshot share one ID; a trade ID identifies exactly one row. |
| `exchange` | `Utf8` | No | `upbit`, `bithumb`, or `binance`. |
| `market` | `Utf8` | No | `spot` or `usdm_futures`. |
| `symbol` | `Utf8` | No | Canonical `BASE/QUOTE`. |
| `source_symbol` | `Utf8` | No | Venue-native market code, such as `KRW-BTC` or `BTCUSDT`. |
| `source_stream` | `Utf8` | No | Venue stream identifier associated with the frame. |
| `source_sequence` | `UInt64` | Yes | Venue update/trade sequence when supplied; otherwise null. |
| `exchange_event_ts_us` | `Int64` | Yes | Venue event/publication timestamp as Unix epoch microseconds; null when unavailable. |
| `exchange_trade_ts_us` | `Int64` | Yes | Venue matching/trade timestamp as Unix epoch microseconds; null when unavailable or inapplicable. |
| `local_recv_ts_us` | `Int64` | No | OS wall-clock Unix epoch microseconds captured after the WebSocket frame becomes available and before parsing. |
| `event_ts_precision` | `UInt8` | No | Encoding of the source precision for `exchange_event_ts_us`. |
| `trade_ts_precision` | `UInt8` | No | Encoding of the source precision for `exchange_trade_ts_us`. |
| `raw_size_bytes` | `UInt32` | No | Raw WebSocket payload length in bytes. |

Timestamp values use microsecond **storage units**, not Arrow `Timestamp` logical types. A millisecond source value is multiplied by 1,000 without claiming additional accuracy. Null source timestamps must use precision `0`; present timestamps use `1` or `2`. `local_recv_ts_us` has microsecond representation but its actual accuracy depends on the host clock and synchronization.

Timestamp precision encoding:

| Value | Name | Meaning |
|---:|---|---|
| `0` | unavailable | The corresponding timestamp is null. |
| `1` | millisecond | Source supplied millisecond precision; stored in microsecond units. |
| `2` | microsecond | Source supplied microsecond precision. |

## Order-book schema

`books.arrow` appends four non-null fields after the common fields. One normalized snapshot becomes one row for every bid and ask level. Shared metadata and `event_id` repeat across those rows.

| Field | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `side` | `UInt8` | No | Book-side enum: `0` bid, `1` ask. |
| `level` | `UInt16` | No | Zero-based best-to-worst rank within the side. |
| `price` | `Decimal128(38,18)` | No | Positive exact price. |
| `quantity` | `Decimal128(38,18)` | No | Positive exact base-asset quantity. |

Book side encoding:

| Value | Name |
|---:|---|
| `0` | bid |
| `1` | ask |

Within each event, both sides are present; levels are contiguous from zero; bid prices strictly descend; ask prices strictly ascend; and best bid is strictly below best ask. The collector rejects a malformed snapshot as an event instead of retaining a partial snapshot.

## Trade schema

`trades.arrow` appends four non-null fields after the common fields. One normalized trade becomes one row.

| Field | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `trade_id` | `Utf8` | No | Nonempty venue trade identifier. |
| `price` | `Decimal128(38,18)` | No | Positive exact execution price. |
| `quantity` | `Decimal128(38,18)` | No | Positive exact base-asset execution quantity. |
| `taker_side` | `UInt8` | No | Aggressor-side enum. |

Taker-side encoding:

| Value | Name | Meaning |
|---:|---|---|
| `0` | unknown | Venue payload did not permit a reliable mapping. |
| `1` | buy | Buyer was the aggressor. |
| `2` | sell | Seller was the aggressor. |

For Binance raw `@trade`, `buyer is maker = true` maps to sell and `false` maps to buy. The collector does not subscribe to `@aggTrade`.

## Decimal representation

`Decimal128(38,18)` permits 38 total decimal digits with exactly 18 fractional scale positions. Arrow stores the scaled value as a signed 128-bit integer: logical `123.45` is represented by integer `123450000000000000000`. Parsing is decimal-string based and rejects nonpositive, over-precision, or overflowed values rather than rounding them.

## Reading with Python and PyArrow

Install PyArrow in the Python environment, then open the stream with `pyarrow.ipc.open_stream`:

```python
from pathlib import Path

import pyarrow.ipc as ipc

path = Path("data/binance/spot/BTC-USDT/2026-08-19/12/trades.arrow")
with path.open("rb") as source:
    table = ipc.open_stream(source).read_all()

print(table.schema.metadata)
print(
    table.select(
        ["exchange_trade_ts_us", "price", "quantity", "taker_side"]
    ).slice(0, 5)
)
```

Convert a stored integer timestamp explicitly as UTC; do not reinterpret it as local time:

```python
import pyarrow as pa
import pyarrow.compute as pc

trade_time = pc.cast(
    table["exchange_trade_ts_us"], pa.timestamp("us", tz="UTC")
)
print(trade_time.slice(0, 5))
```

Some source timestamps are nullable. Filter null values before latency calculations, and use `event_ts_precision`/`trade_ts_precision` so millisecond observations are not treated as genuinely microsecond-precise. Use `event_id` plus `side` and `level` to reconstruct a snapshot; do not treat each book row as an independent source event.

## Phase 2A derivatives streams

Phase 2A adds seven event-family streams under:

```text
{output_root}/derivatives/{event_family}/{venue}/{market}/{BASE}-{QUOTE}/{YYYY-MM-DD}/{HH}/{event_family}.arrow
```

The UTC hour uses a valid source-event timestamp and falls back to local receive time only when the source timestamp is unavailable. Families are `instrument`, `mark_index`, `funding_estimate`, `funding_settlement`, `open_interest`, `trader_ratio`, and `quote_conversion`. Venue/market pairs include `binance/usdm_futures`, `bybit/linear_futures`, and the `upbit/spot` or `bithumb/spot` source of a conversion quote.

Every derivative schema begins with `schema_version`, UUIDv7 `event_id`, `venue`, `market`, `base`, `quote`, `source_symbol`, nullable `exchange_event_ts_us`, `local_recv_ts_us`, and `source_precision`. Financial columns are exact `Decimal128(38,18)`. Funding rows explicitly distinguish `indicative_next` from `settled_actual`, carry `mark_notional` basis, interval seconds and provenance, and the next-funding or settlement timestamp. Trader rows retain their metric kind; Binance top-account/top-position ratios are not collected in Phase 2A because their current official endpoints require an API key, while Bybit public rows use `bybit_long_short_ratio`. Quote conversion emits separate executable best-bid and best-ask rows with available quantity and rejects stale or incomplete books.

A successful `phase2a-report.json` is written only after both market and derivative channels are drained, Arrow streams are finalized, and recursive validation succeeds. On startup, producer, storage, or validation failure the collector still attempts to drain/finalize and may publish a failure report with nonempty aggregated `health_errors`; such a report does not certify the dataset. Report publication syncs a temporary file and atomically replaces the destination without first moving it away; Unix also syncs the parent directory, while Windows uses the native replace/move APIs. Reports are operational metadata rather than Arrow streams and never contain credentials. `sequence_gaps` is limited to sequence/snapshot continuity failures and excludes timestamp regressions.
