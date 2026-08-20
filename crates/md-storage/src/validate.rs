use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::{
    Array, Decimal128Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Schema};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use md_core::model::{DECIMAL_PRECISION, DECIMAL_SCALE};
use serde::Serialize;
use thiserror::Error;

use crate::derivative_schema::{DerivativeEventFamily, derivative_fields};
use crate::{
    BOOK_SIDE_ASK, BOOK_SIDE_BID, PROJECT_NAME, SCHEMA_VERSION, TAKER_SIDE_BUY, TAKER_SIDE_SELL,
    TAKER_SIDE_UNKNOWN, TIMESTAMP_PRECISION_MICROSECOND, TIMESTAMP_PRECISION_MILLISECOND,
    TIMESTAMP_PRECISION_UNAVAILABLE,
};

const MAX_TIMESTAMP_US: i64 = 32_503_680_000_000_000; // 3000-01-01 UTC
const SEVEN_DAYS_US: i64 = 7 * 24 * 60 * 60 * 1_000_000;
const ONE_DAY_US: i64 = 24 * 60 * 60 * 1_000_000;
const MAX_DECIMAL_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ValidationIssue {
    pub code: String,
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<usize>,
    pub message: String,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct ValidationReport {
    pub files: usize,
    pub batches: usize,
    pub rows: usize,
    pub errors: Vec<ValidationIssue>,
}

impl ValidationReport {
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Error)]
pub enum DatasetError {
    #[error("dataset path does not exist: {0}")]
    NotFound(PathBuf),
    #[error("failed to inspect dataset path {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FileKind {
    Books,
    Trades,
}

pub fn validate_path(path: &Path) -> Result<ValidationReport, DatasetError> {
    if !path.exists() {
        return Err(DatasetError::NotFound(path.to_path_buf()));
    }
    let mut files = Vec::new();
    if path.is_file() {
        files.push(absolute(path)?);
    } else {
        collect_arrow_files(path, &mut files)?;
    }
    files.sort();

    let mut report = ValidationReport::default();
    for file in files {
        report.files += 1;
        validate_file(&file, &mut report);
    }
    report.errors.sort_by(|left, right| {
        (
            &left.path,
            left.batch,
            left.row,
            left.code.as_str(),
            left.message.as_str(),
        )
            .cmp(&(
                &right.path,
                right.batch,
                right.row,
                right.code.as_str(),
                right.message.as_str(),
            ))
    });
    Ok(report)
}

fn collect_arrow_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), DatasetError> {
    let entries = std::fs::read_dir(path).map_err(|source| DatasetError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| DatasetError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let entry_path = entry.path();
        let file_type = entry.file_type().map_err(|source| DatasetError::Io {
            path: entry_path.clone(),
            source,
        })?;
        if file_type.is_dir() {
            collect_arrow_files(&entry_path, files)?;
        } else if file_type.is_file()
            && entry_path.extension().and_then(|value| value.to_str()) == Some("arrow")
        {
            files.push(absolute(&entry_path)?);
        }
    }
    Ok(())
}

fn absolute(path: &Path) -> Result<PathBuf, DatasetError> {
    path.canonicalize().map_err(|source| DatasetError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_file(path: &Path, report: &mut ValidationReport) {
    if is_derivative_path(path) {
        validate_derivative_file(path, report);
        return;
    }
    let kind = match path.file_name().and_then(|value| value.to_str()) {
        Some("books.arrow") => FileKind::Books,
        Some("trades.arrow") => FileKind::Trades,
        _ => {
            issue(
                report,
                "UNKNOWN_FILE_KIND",
                path,
                None,
                None,
                "Arrow filename must be books.arrow or trades.arrow",
            );
            return;
        }
    };
    if path_parts(path).is_none() {
        issue(
            report,
            "PATH_LAYOUT",
            path,
            None,
            None,
            "Arrow file must end in exchange/market/BASE-QUOTE/YYYY-MM-DD/HH/books.arrow or trades.arrow",
        );
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) => {
            issue(
                report,
                "UNREADABLE_ARROW",
                path,
                None,
                None,
                error.to_string(),
            );
            return;
        }
    };
    let mut reader = match StreamReader::try_new(file, None) {
        Ok(reader) => reader,
        Err(error) => {
            issue(
                report,
                "UNREADABLE_ARROW",
                path,
                None,
                None,
                error.to_string(),
            );
            return;
        }
    };
    let schema = reader.schema();
    let semantics_ok = validate_schema(path, schema.as_ref(), kind, report);
    let partition_hour = partition_hour(path);
    let mut books = HashMap::<[u8; 16], BookEvent>::new();
    let mut trade_ids = HashSet::<[u8; 16]>::new();

    for (batch_index, batch) in reader.by_ref().enumerate() {
        match batch {
            Ok(batch) => {
                report.batches += 1;
                report.rows += batch.num_rows();
                if semantics_ok {
                    validate_common_rows(path, batch_index, &batch, partition_hour, report);
                    match kind {
                        FileKind::Books => {
                            validate_book_rows(path, batch_index, &batch, &mut books, report)
                        }
                        FileKind::Trades => {
                            validate_trade_rows(path, batch_index, &batch, &mut trade_ids, report)
                        }
                    }
                }
            }
            Err(error) => {
                issue(
                    report,
                    "UNREADABLE_ARROW",
                    path,
                    Some(batch_index),
                    None,
                    error.to_string(),
                );
                break;
            }
        }
    }
    if kind == FileKind::Books && semantics_ok {
        finish_books(path, books, report);
    }
    if !has_stream_terminator(path) {
        issue(
            report,
            "UNREADABLE_ARROW",
            path,
            None,
            None,
            "finalized Arrow stream is missing its end-of-stream marker or has trailing bytes",
        );
    }
}

fn validate_schema(
    path: &Path,
    schema: &Schema,
    kind: FileKind,
    report: &mut ValidationReport,
) -> bool {
    let mut ok = true;
    let expected = expected_fields(kind);
    if schema.fields().len() != expected.len()
        || schema
            .fields()
            .iter()
            .zip(expected.iter())
            .any(|(actual, (name, ty, nullable))| {
                actual.name() != name
                    || actual.data_type() != ty
                    || actual.is_nullable() != *nullable
            })
    {
        issue(
            report,
            "SCHEMA_TYPE",
            path,
            None,
            None,
            "field names, types, order, or nullability do not match the canonical schema",
        );
        ok = false;
    }

    let metadata = schema.metadata();
    for (key, expected) in [
        ("project", PROJECT_NAME.to_owned()),
        ("schema_version", SCHEMA_VERSION.to_string()),
        ("timestamp_unit", "microsecond".to_owned()),
    ] {
        if metadata.get(key) != Some(&expected) {
            issue(
                report,
                if key == "schema_version" {
                    "SCHEMA_VERSION"
                } else {
                    "SCHEMA_METADATA"
                },
                path,
                None,
                None,
                format!("metadata {key:?} must equal {expected:?}"),
            );
            ok = false;
        }
    }
    if metadata.get("decimal_scale") != Some(&DECIMAL_SCALE.to_string()) {
        issue(
            report,
            "DECIMAL_METADATA",
            path,
            None,
            None,
            format!("decimal_scale metadata must equal {DECIMAL_SCALE}"),
        );
        ok = false;
    }
    for key in ["exchange", "market", "symbol", "utc_hour"] {
        if !metadata.contains_key(key) {
            issue(
                report,
                "SCHEMA_METADATA",
                path,
                None,
                None,
                format!("required schema metadata {key:?} is missing"),
            );
            ok = false;
        }
    }
    if metadata
        .get("exchange")
        .is_some_and(|value| !matches!(value.as_str(), "upbit" | "bithumb" | "binance" | "bybit"))
        || metadata.get("market").is_some_and(|value| {
            !matches!(value.as_str(), "spot" | "usdm_futures" | "linear_futures")
        })
        || metadata.get("symbol").is_some_and(|value| {
            value
                .split_once('/')
                .is_none_or(|(base, quote)| !valid_symbol_part(base) || !valid_symbol_part(quote))
        })
        || metadata.get("utc_hour").is_some_and(|value| {
            DateTime::parse_from_rfc3339(value).map_or(true, |hour| {
                hour.offset().local_minus_utc() != 0
                    || hour.minute() != 0
                    || hour.second() != 0
                    || hour.timestamp_subsec_micros() != 0
            })
        })
    {
        issue(
            report,
            "SCHEMA_METADATA",
            path,
            None,
            None,
            "exchange, market, symbol, or utc_hour metadata is malformed",
        );
        ok = false;
    }
    validate_path_metadata(path, metadata, report);
    ok
}

fn expected_fields(kind: FileKind) -> Vec<(&'static str, DataType, bool)> {
    let common = vec![
        ("schema_version", DataType::UInt16, false),
        ("event_id", DataType::FixedSizeBinary(16), false),
        ("exchange", DataType::Utf8, false),
        ("market", DataType::Utf8, false),
        ("symbol", DataType::Utf8, false),
        ("source_symbol", DataType::Utf8, false),
        ("source_stream", DataType::Utf8, false),
        ("source_sequence", DataType::UInt64, true),
        ("exchange_event_ts_us", DataType::Int64, true),
        ("exchange_trade_ts_us", DataType::Int64, true),
        ("local_recv_ts_us", DataType::Int64, false),
        ("event_ts_precision", DataType::UInt8, false),
        ("trade_ts_precision", DataType::UInt8, false),
        ("raw_size_bytes", DataType::UInt32, false),
    ];
    let mut fields = common;
    match kind {
        FileKind::Books => fields.extend([
            ("side", DataType::UInt8, false),
            ("level", DataType::UInt16, false),
            (
                "price",
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                false,
            ),
            (
                "quantity",
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                false,
            ),
        ]),
        FileKind::Trades => fields.extend([
            ("trade_id", DataType::Utf8, false),
            (
                "price",
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                false,
            ),
            (
                "quantity",
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                false,
            ),
            ("taker_side", DataType::UInt8, false),
        ]),
    }
    fields
}

fn validate_path_metadata(
    path: &Path,
    metadata: &HashMap<String, String>,
    report: &mut ValidationReport,
) {
    let Some(parts) = path_parts(path) else {
        return;
    };
    for (key, actual, expected) in [
        ("exchange", metadata.get("exchange"), Some(parts.exchange)),
        ("market", metadata.get("market"), Some(parts.market)),
        (
            "symbol",
            metadata.get("symbol"),
            Some(parts.symbol.replace('-', "/")),
        ),
        ("utc_hour", metadata.get("utc_hour"), parts.utc_hour),
    ] {
        if actual.map(String::as_str) != expected.as_deref() {
            issue(
                report,
                "PATH_METADATA_MISMATCH",
                path,
                None,
                None,
                format!("metadata {key:?} does not match partition path"),
            );
        }
    }
}

struct PathParts {
    exchange: String,
    market: String,
    symbol: String,
    utc_hour: Option<String>,
}

fn path_parts(path: &Path) -> Option<PathParts> {
    let parts = path
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if parts.len() < 6 {
        return None;
    }
    let n = parts.len();
    let exchange = parts[n - 6].as_str();
    let market = parts[n - 5].as_str();
    if !matches!(
        (exchange, market),
        ("upbit", "spot")
            | ("bithumb", "spot")
            | ("binance", "spot")
            | ("binance", "usdm_futures")
            | ("bybit", "linear_futures")
    ) || !parts[n - 4]
        .split_once('-')
        .is_some_and(|(base, quote)| valid_symbol_part(base) && valid_symbol_part(quote))
        || parts[n - 2].len() != 2
    {
        return None;
    }
    let date = NaiveDate::parse_from_str(&parts[n - 3], "%Y-%m-%d").ok()?;
    let hour = parts[n - 2].parse::<u32>().ok()?;
    let utc_hour = date.and_hms_opt(hour, 0, 0).map(|value| {
        DateTime::<Utc>::from_naive_utc_and_offset(value, Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    })?;
    Some(PathParts {
        exchange: exchange.to_owned(),
        market: market.to_owned(),
        symbol: parts[n - 4].clone(),
        utc_hour: Some(utc_hour),
    })
}

fn partition_hour(path: &Path) -> Option<(i64, i64)> {
    let parts = path_parts(path)?;
    let start = DateTime::parse_from_rfc3339(parts.utc_hour.as_deref()?)
        .ok()?
        .timestamp_micros();
    Some((start, start.saturating_add(3_600_000_000)))
}

fn validate_common_rows(
    path: &Path,
    batch_index: usize,
    batch: &RecordBatch,
    partition: Option<(i64, i64)>,
    report: &mut ValidationReport,
) {
    let version = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    let event_ts = batch
        .column(8)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let trade_ts = batch
        .column(9)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let local_ts = batch
        .column(10)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let event_precision = batch
        .column(11)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let trade_precision = batch
        .column(12)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let metadata = batch.schema();
    let exchange = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let market = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let symbol = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        if version.value(row) != SCHEMA_VERSION {
            row_issue(
                report,
                "SCHEMA_VERSION",
                path,
                batch_index,
                row,
                "row schema_version is not supported",
            );
        }
        for (field, actual) in [
            ("exchange", exchange.value(row)),
            ("market", market.value(row)),
            ("symbol", symbol.value(row)),
        ] {
            if metadata.metadata().get(field).map(String::as_str) != Some(actual) {
                row_issue(
                    report,
                    "EVENT_GROUPING",
                    path,
                    batch_index,
                    row,
                    format!("row {field} differs from schema metadata"),
                );
            }
        }
        validate_timestamp(
            path,
            batch_index,
            row,
            "local_recv_ts_us",
            Some(local_ts.value(row)),
            report,
        );
        if let Some((start, end)) = partition
            && !(start..end).contains(&local_ts.value(row))
        {
            row_issue(
                report,
                "TIMESTAMP_PARTITION_MISMATCH",
                path,
                batch_index,
                row,
                "local receive timestamp is outside the path UTC hour",
            );
        }
        validate_timestamp(
            path,
            batch_index,
            row,
            "exchange_event_ts_us",
            (!event_ts.is_null(row)).then(|| event_ts.value(row)),
            report,
        );
        validate_timestamp(
            path,
            batch_index,
            row,
            "exchange_trade_ts_us",
            (!trade_ts.is_null(row)).then(|| trade_ts.value(row)),
            report,
        );
        validate_precision(
            path,
            batch_index,
            row,
            !event_ts.is_null(row),
            event_precision.value(row),
            report,
        );
        validate_precision(
            path,
            batch_index,
            row,
            !trade_ts.is_null(row),
            trade_precision.value(row),
            report,
        );
        for (field, source) in [
            (
                "exchange_event_ts_us",
                (!event_ts.is_null(row)).then(|| event_ts.value(row)),
            ),
            (
                "exchange_trade_ts_us",
                (!trade_ts.is_null(row)).then(|| trade_ts.value(row)),
            ),
        ] {
            let local = local_ts.value(row);
            if source.is_some_and(|source| {
                source < local.saturating_sub(SEVEN_DAYS_US)
                    || source > local.saturating_add(ONE_DAY_US)
            }) {
                row_issue(
                    report,
                    "TIMESTAMP_RANGE",
                    path,
                    batch_index,
                    row,
                    format!("{field} is implausibly far from local_recv_ts_us"),
                );
            }
        }
    }
}

fn validate_timestamp(
    path: &Path,
    batch: usize,
    row: usize,
    field: &str,
    value: Option<i64>,
    report: &mut ValidationReport,
) {
    if value.is_some_and(|value| !(0..MAX_TIMESTAMP_US).contains(&value)) {
        row_issue(
            report,
            "TIMESTAMP_RANGE",
            path,
            batch,
            row,
            format!("{field} is outside the supported Unix microsecond range"),
        );
    }
}

fn validate_precision(
    path: &Path,
    batch: usize,
    row: usize,
    timestamp_present: bool,
    precision: u8,
    report: &mut ValidationReport,
) {
    let valid = if timestamp_present {
        matches!(
            precision,
            TIMESTAMP_PRECISION_MILLISECOND | TIMESTAMP_PRECISION_MICROSECOND
        )
    } else {
        precision == TIMESTAMP_PRECISION_UNAVAILABLE
    };
    if !valid {
        row_issue(
            report,
            "INVALID_PRECISION",
            path,
            batch,
            row,
            "timestamp presence and source precision disagree",
        );
    }
}

#[derive(Default)]
struct BookEvent {
    common: Option<CommonIdentity>,
    sides: BTreeMap<u8, BTreeMap<u16, i128>>,
    duplicate: bool,
}

#[derive(Clone, Eq, PartialEq)]
struct CommonIdentity {
    exchange: String,
    market: String,
    symbol: String,
    source_symbol: String,
    source_stream: String,
    source_sequence: Option<u64>,
    exchange_event_ts_us: Option<i64>,
    exchange_trade_ts_us: Option<i64>,
    local_recv_ts_us: i64,
    event_ts_precision: u8,
    trade_ts_precision: u8,
    raw_size_bytes: u32,
}

fn validate_book_rows(
    path: &Path,
    batch_index: usize,
    batch: &RecordBatch,
    events: &mut HashMap<[u8; 16], BookEvent>,
    report: &mut ValidationReport,
) {
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let side = batch
        .column(14)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let level = batch
        .column(15)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    let price = batch
        .column(16)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let quantity = batch
        .column(17)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let strings = [2, 3, 4, 5, 6].map(|index| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    });
    let sequence = batch
        .column(7)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let event_ts = batch
        .column(8)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let trade_ts = batch
        .column(9)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let local = batch
        .column(10)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let event_precision = batch
        .column(11)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let trade_precision = batch
        .column(12)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let raw_size = batch
        .column(13)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    for row in 0..batch.num_rows() {
        validate_positive_decimal(path, batch_index, row, "price", price.value(row), report);
        validate_positive_decimal(
            path,
            batch_index,
            row,
            "quantity",
            quantity.value(row),
            report,
        );
        let side_value = side.value(row);
        if !matches!(side_value, BOOK_SIDE_BID | BOOK_SIDE_ASK) {
            row_issue(
                report,
                "INVALID_BOOK_SIDE",
                path,
                batch_index,
                row,
                "side must be bid or ask",
            );
            continue;
        }
        let id: [u8; 16] = ids
            .value(row)
            .try_into()
            .expect("schema guarantees 16 bytes");
        let identity = CommonIdentity {
            exchange: strings[0].value(row).to_owned(),
            market: strings[1].value(row).to_owned(),
            symbol: strings[2].value(row).to_owned(),
            source_symbol: strings[3].value(row).to_owned(),
            source_stream: strings[4].value(row).to_owned(),
            source_sequence: (!sequence.is_null(row)).then(|| sequence.value(row)),
            exchange_event_ts_us: (!event_ts.is_null(row)).then(|| event_ts.value(row)),
            exchange_trade_ts_us: (!trade_ts.is_null(row)).then(|| trade_ts.value(row)),
            local_recv_ts_us: local.value(row),
            event_ts_precision: event_precision.value(row),
            trade_ts_precision: trade_precision.value(row),
            raw_size_bytes: raw_size.value(row),
        };
        let event = events.entry(id).or_default();
        if event
            .common
            .as_ref()
            .is_some_and(|common| common != &identity)
        {
            row_issue(
                report,
                "EVENT_GROUPING",
                path,
                batch_index,
                row,
                "rows sharing an event_id have different metadata",
            );
        } else {
            event.common.get_or_insert(identity);
        }
        if event
            .sides
            .entry(side_value)
            .or_default()
            .insert(level.value(row), price.value(row))
            .is_some()
        {
            event.duplicate = true;
        }
    }
}

fn finish_books(path: &Path, events: HashMap<[u8; 16], BookEvent>, report: &mut ValidationReport) {
    for event in events.into_values() {
        if event.duplicate {
            issue(
                report,
                "DUPLICATE_LEVEL",
                path,
                None,
                None,
                "an event repeats a level within one side",
            );
        }
        for side in [BOOK_SIDE_BID, BOOK_SIDE_ASK] {
            let Some(levels) = event.sides.get(&side) else {
                issue(
                    report,
                    "MISSING_BOOK_SIDE",
                    path,
                    None,
                    None,
                    "an event does not contain both bid and ask rows",
                );
                continue;
            };
            if levels
                .keys()
                .copied()
                .ne(0_u16..u16::try_from(levels.len()).unwrap_or(u16::MAX))
            {
                issue(
                    report,
                    "NONCONTIGUOUS_LEVEL",
                    path,
                    None,
                    None,
                    "book levels must be contiguous and zero based",
                );
            }
            let prices = levels.values().copied().collect::<Vec<_>>();
            let sorted = prices.windows(2).all(|pair| {
                if side == BOOK_SIDE_BID {
                    pair[0] > pair[1]
                } else {
                    pair[0] < pair[1]
                }
            });
            if !sorted {
                issue(
                    report,
                    "UNSORTED_BOOK",
                    path,
                    None,
                    None,
                    "book prices are not strictly best-to-worst",
                );
            }
        }
        if let (Some(best_bid), Some(best_ask)) = (
            event
                .sides
                .get(&BOOK_SIDE_BID)
                .and_then(|levels| levels.get(&0)),
            event
                .sides
                .get(&BOOK_SIDE_ASK)
                .and_then(|levels| levels.get(&0)),
        ) && best_bid >= best_ask
        {
            issue(
                report,
                "CROSSED_BOOK",
                path,
                None,
                None,
                "best bid must be strictly below best ask",
            );
        }
    }
}

fn validate_trade_rows(
    path: &Path,
    batch_index: usize,
    batch: &RecordBatch,
    event_ids: &mut HashSet<[u8; 16]>,
    report: &mut ValidationReport,
) {
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let trade_id = batch
        .column(14)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let price = batch
        .column(15)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let quantity = batch
        .column(16)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let side = batch
        .column(17)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    for row in 0..batch.num_rows() {
        validate_positive_decimal(path, batch_index, row, "price", price.value(row), report);
        validate_positive_decimal(
            path,
            batch_index,
            row,
            "quantity",
            quantity.value(row),
            report,
        );
        if trade_id.value(row).is_empty() {
            row_issue(
                report,
                "EMPTY_TRADE_ID",
                path,
                batch_index,
                row,
                "trade_id must not be empty",
            );
        }
        if !matches!(
            side.value(row),
            TAKER_SIDE_UNKNOWN | TAKER_SIDE_BUY | TAKER_SIDE_SELL
        ) {
            row_issue(
                report,
                "INVALID_TAKER_SIDE",
                path,
                batch_index,
                row,
                "taker_side is outside the supported enum",
            );
        }
        let id = ids
            .value(row)
            .try_into()
            .expect("schema guarantees 16 bytes");
        if !event_ids.insert(id) {
            row_issue(
                report,
                "EVENT_GROUPING",
                path,
                batch_index,
                row,
                "trade event_id must identify exactly one row",
            );
        }
    }
}

fn validate_positive_decimal(
    path: &Path,
    batch: usize,
    row: usize,
    field: &str,
    value: i128,
    report: &mut ValidationReport,
) {
    if value <= 0 {
        row_issue(
            report,
            "NON_POSITIVE_DECIMAL",
            path,
            batch,
            row,
            format!("{field} must be positive"),
        );
    } else if value > MAX_DECIMAL_38 {
        row_issue(
            report,
            "DECIMAL_RANGE",
            path,
            batch,
            row,
            format!("{field} exceeds Decimal128 precision 38"),
        );
    }
}

fn valid_symbol_part(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

#[derive(Debug)]
struct DerivativePathParts {
    family: DerivativeEventFamily,
    venue: String,
    market: String,
    base: String,
    quote: String,
    hour_start_us: i64,
}

fn is_derivative_path(path: &Path) -> bool {
    let derivative_filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".arrow"))
        .and_then(DerivativeEventFamily::parse)
        .is_some();
    let parts = path.iter().collect::<Vec<_>>();
    derivative_filename
        || parts
            .len()
            .checked_sub(8)
            .is_some_and(|index| parts[index] == "derivatives")
}

fn derivative_path_parts(path: &Path) -> Option<DerivativePathParts> {
    let parts = path
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if parts.len() < 8 {
        return None;
    }
    let n = parts.len();
    if parts[n - 8] != "derivatives" {
        return None;
    }
    let family = DerivativeEventFamily::parse(&parts[n - 7])?;
    if parts[n - 1] != format!("{}.arrow", family.as_str()) {
        return None;
    }
    let venue = parts[n - 6].clone();
    let market = parts[n - 5].clone();
    if !matches!(
        (venue.as_str(), market.as_str()),
        ("binance", "usdm_futures")
            | ("bybit", "linear_futures")
            | ("binance", "spot")
            | ("upbit", "spot")
            | ("bithumb", "spot")
    ) {
        return None;
    }
    let (base, quote) = parts[n - 4].split_once('-')?;
    if !valid_symbol_part(base) || !valid_symbol_part(quote) {
        return None;
    }
    let date = NaiveDate::parse_from_str(&parts[n - 3], "%Y-%m-%d").ok()?;
    let hour = parts[n - 2].parse::<u32>().ok()?;
    let hour_start_us =
        DateTime::<Utc>::from_naive_utc_and_offset(date.and_hms_opt(hour, 0, 0)?, Utc)
            .timestamp_micros();
    Some(DerivativePathParts {
        family,
        venue,
        market,
        base: base.into(),
        quote: quote.into(),
        hour_start_us,
    })
}

fn validate_derivative_file(path: &Path, report: &mut ValidationReport) {
    let Some(parts) = derivative_path_parts(path) else {
        issue(
            report,
            "PATH_LAYOUT",
            path,
            None,
            None,
            "derivative Arrow file must end in derivatives/family/venue/market/BASE-QUOTE/YYYY-MM-DD/HH/family.arrow",
        );
        return;
    };
    if parts.family != DerivativeEventFamily::QuoteConversion
        && !matches!(
            (parts.venue.as_str(), parts.market.as_str()),
            ("binance", "usdm_futures") | ("bybit", "linear_futures")
        )
    {
        issue(
            report,
            "PATH_LAYOUT",
            path,
            None,
            None,
            "non-conversion derivative families require a derivatives venue",
        );
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) => {
            issue(
                report,
                "UNREADABLE_ARROW",
                path,
                None,
                None,
                error.to_string(),
            );
            return;
        }
    };
    let mut reader = match StreamReader::try_new(file, None) {
        Ok(reader) => reader,
        Err(error) => {
            issue(
                report,
                "UNREADABLE_ARROW",
                path,
                None,
                None,
                error.to_string(),
            );
            return;
        }
    };
    let schema = reader.schema();
    let schema_ok = validate_derivative_schema(path, schema.as_ref(), &parts, report);
    let mut event_ids = HashSet::new();
    for (batch_index, batch) in reader.by_ref().enumerate() {
        match batch {
            Ok(batch) => {
                report.batches += 1;
                report.rows += batch.num_rows();
                if schema_ok {
                    validate_derivative_rows(
                        path,
                        batch_index,
                        &batch,
                        &parts,
                        &mut event_ids,
                        report,
                    );
                }
            }
            Err(error) => {
                issue(
                    report,
                    "UNREADABLE_ARROW",
                    path,
                    Some(batch_index),
                    None,
                    error.to_string(),
                );
                break;
            }
        }
    }
    if !has_stream_terminator(path) {
        issue(
            report,
            "UNREADABLE_ARROW",
            path,
            None,
            None,
            "finalized Arrow stream is missing its end-of-stream marker or has trailing bytes",
        );
    }
}

fn validate_derivative_schema(
    path: &Path,
    schema: &Schema,
    parts: &DerivativePathParts,
    report: &mut ValidationReport,
) -> bool {
    let mut ok = true;
    let expected = derivative_fields(parts.family);
    if schema.fields().len() != expected.len()
        || schema
            .fields()
            .iter()
            .zip(&expected)
            .any(|(actual, expected)| {
                actual.name() != expected.name()
                    || actual.data_type() != expected.data_type()
                    || actual.is_nullable() != expected.is_nullable()
            })
    {
        issue(
            report,
            "SCHEMA_TYPE",
            path,
            None,
            None,
            "field names, types, order, or nullability do not match the canonical derivative schema",
        );
        ok = false;
    }
    let metadata = schema.metadata();
    for (key, expected, code) in [
        ("project", PROJECT_NAME.to_owned(), "SCHEMA_METADATA"),
        (
            "schema_version",
            SCHEMA_VERSION.to_string(),
            "SCHEMA_VERSION",
        ),
        (
            "timestamp_unit",
            "microsecond".to_owned(),
            "SCHEMA_METADATA",
        ),
        (
            "decimal_scale",
            DECIMAL_SCALE.to_string(),
            "DECIMAL_METADATA",
        ),
        (
            "event_family",
            parts.family.as_str().to_owned(),
            "EVENT_FAMILY_METADATA",
        ),
        ("venue", parts.venue.clone(), "PATH_METADATA_MISMATCH"),
        ("market", parts.market.clone(), "PATH_METADATA_MISMATCH"),
        (
            "symbol",
            format!("{}/{}", parts.base, parts.quote),
            "PATH_METADATA_MISMATCH",
        ),
    ] {
        if metadata.get(key) != Some(&expected) {
            issue(
                report,
                code,
                path,
                None,
                None,
                format!("metadata {key:?} must equal {expected:?}"),
            );
            ok = false;
        }
    }
    let expected_hour = DateTime::<Utc>::from_timestamp_micros(parts.hour_start_us)
        .expect("validated partition hour")
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    if metadata.get("utc_hour") != Some(&expected_hour) {
        issue(
            report,
            "PATH_METADATA_MISMATCH",
            path,
            None,
            None,
            "metadata utc_hour does not match partition path",
        );
        ok = false;
    }
    ok
}

#[allow(clippy::too_many_arguments)]
fn validate_derivative_rows(
    path: &Path,
    batch_index: usize,
    batch: &RecordBatch,
    parts: &DerivativePathParts,
    event_ids: &mut HashSet<[u8; 16]>,
    report: &mut ValidationReport,
) {
    let versions = derivative_column::<UInt16Array>(batch, 0);
    let ids = derivative_column::<FixedSizeBinaryArray>(batch, 1);
    let venue = derivative_column::<StringArray>(batch, 2);
    let market = derivative_column::<StringArray>(batch, 3);
    let base = derivative_column::<StringArray>(batch, 4);
    let quote = derivative_column::<StringArray>(batch, 5);
    let source_symbol = derivative_column::<StringArray>(batch, 6);
    let source_ts = derivative_column::<Int64Array>(batch, 7);
    let local_ts = derivative_column::<Int64Array>(batch, 8);
    let precision = derivative_column::<UInt8Array>(batch, 9);
    for row in 0..batch.num_rows() {
        if versions.value(row) != SCHEMA_VERSION {
            row_issue(
                report,
                "SCHEMA_VERSION",
                path,
                batch_index,
                row,
                "row schema version is unsupported",
            );
        }
        let id: [u8; 16] = ids
            .value(row)
            .try_into()
            .expect("canonical fixed-size event id");
        if !event_ids.insert(id) {
            row_issue(
                report,
                "EVENT_GROUPING",
                path,
                batch_index,
                row,
                "derivative event_id must identify exactly one row",
            );
        }
        for (field, actual, expected) in [
            ("venue", venue.value(row), parts.venue.as_str()),
            ("market", market.value(row), parts.market.as_str()),
            ("base", base.value(row), parts.base.as_str()),
            ("quote", quote.value(row), parts.quote.as_str()),
        ] {
            if actual != expected {
                row_issue(
                    report,
                    "EVENT_GROUPING",
                    path,
                    batch_index,
                    row,
                    format!("row {field} differs from partition metadata"),
                );
            }
        }
        if source_symbol.value(row).is_empty() {
            row_issue(
                report,
                "EMPTY_SOURCE_SYMBOL",
                path,
                batch_index,
                row,
                "source_symbol must not be empty",
            );
        }
        let local = local_ts.value(row);
        validate_timestamp(
            path,
            batch_index,
            row,
            "local_recv_ts_us",
            Some(local),
            report,
        );
        let source = (!source_ts.is_null(row)).then(|| source_ts.value(row));
        validate_timestamp(
            path,
            batch_index,
            row,
            "exchange_event_ts_us",
            source,
            report,
        );
        validate_precision(
            path,
            batch_index,
            row,
            source.is_some(),
            precision.value(row),
            report,
        );
        let partition_ts = source.filter(|source| *source > 0).unwrap_or(local);
        if !(parts.hour_start_us..parts.hour_start_us.saturating_add(3_600_000_000))
            .contains(&partition_ts)
        {
            row_issue(
                report,
                "TIMESTAMP_PARTITION_MISMATCH",
                path,
                batch_index,
                row,
                "selected source/local timestamp is outside the path UTC hour",
            );
        }
        validate_derivative_specific(path, batch_index, row, batch, parts.family, report);
    }
}

fn validate_derivative_specific(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    family: DerivativeEventFamily,
    report: &mut ValidationReport,
) {
    match family {
        DerivativeEventFamily::Instrument => {
            for field in [
                "contract_multiplier",
                "tick_size",
                "quantity_step",
                "min_quantity",
                "min_notional",
            ] {
                validate_named_positive_decimal(
                    path,
                    batch_index,
                    row,
                    batch,
                    field,
                    false,
                    report,
                );
            }
            for field in ["max_quantity", "price_lower_bound", "price_upper_bound"] {
                validate_named_positive_decimal(path, batch_index, row, batch, field, true, report);
            }
            for field in ["funding_rate_floor", "funding_rate_cap"] {
                validate_named_decimal(path, batch_index, row, batch, field, report);
            }
            let interval = named_u32(batch, "funding_interval_secs").value(row);
            if interval == 0 {
                row_issue(
                    report,
                    "INVALID_FUNDING_INTERVAL",
                    path,
                    batch_index,
                    row,
                    "funding interval must be positive",
                );
            }
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "contract_kind",
                &["perpetual"],
                report,
            );
            let settlement = named_string(batch, "settlement_asset").value(row);
            if !valid_symbol_part(settlement) {
                row_issue(
                    report,
                    "INVALID_INSTRUMENT_RULE",
                    path,
                    batch_index,
                    row,
                    "settlement_asset must be an uppercase asset code",
                );
            }
            let min_quantity = named_decimal(batch, "min_quantity").value(row);
            let max_quantity = named_decimal(batch, "max_quantity");
            if !max_quantity.is_null(row) && max_quantity.value(row) < min_quantity {
                row_issue(
                    report,
                    "INVALID_INSTRUMENT_RULE",
                    path,
                    batch_index,
                    row,
                    "max_quantity must not be below min_quantity",
                );
            }
            validate_optional_bounds(
                path,
                batch_index,
                row,
                batch,
                "price_lower_bound",
                "price_upper_bound",
                false,
                report,
            );
            validate_optional_bounds(
                path,
                batch_index,
                row,
                batch,
                "funding_rate_floor",
                "funding_rate_cap",
                true,
                report,
            );
            let bounds_provenance =
                named_string(batch, "funding_rate_bounds_provenance").value(row);
            let floor_present = !named_decimal(batch, "funding_rate_floor").is_null(row);
            let cap_present = !named_decimal(batch, "funding_rate_cap").is_null(row);
            let provenance_consistent = match bounds_provenance {
                "venue_funding_info" => floor_present && cap_present,
                "unknown" => !floor_present && !cap_present,
                _ => true,
            };
            if !provenance_consistent {
                row_issue(
                    report,
                    "INVALID_INSTRUMENT_RULE",
                    path,
                    batch_index,
                    row,
                    "funding bounds disagree with their provenance",
                );
            }
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "funding_interval_provenance",
                FUNDING_PROVENANCE,
                report,
            );
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "funding_rate_bounds_provenance",
                &["venue_funding_info", "unknown"],
                report,
            );
            validate_mode_set(
                path,
                batch_index,
                row,
                batch,
                "supported_position_modes",
                &["one_way", "hedge"],
                report,
            );
            validate_mode_set(
                path,
                batch_index,
                row,
                batch,
                "supported_account_modes",
                &["classic", "unified", "portfolio"],
                report,
            );
        }
        DerivativeEventFamily::MarkIndex => {
            for field in ["mark_price", "index_price"] {
                validate_named_positive_decimal(
                    path,
                    batch_index,
                    row,
                    batch,
                    field,
                    false,
                    report,
                );
            }
        }
        DerivativeEventFamily::FundingEstimate => {
            validate_named_decimal(path, batch_index, row, batch, "rate", report);
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "rate_kind",
                &["indicative_next"],
                report,
            );
            validate_funding_row(path, batch_index, row, batch, "next_funding_ts_us", report);
        }
        DerivativeEventFamily::FundingSettlement => {
            validate_named_decimal(path, batch_index, row, batch, "rate", report);
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "rate_kind",
                &["settled_actual"],
                report,
            );
            validate_funding_row(path, batch_index, row, batch, "settlement_ts_us", report);
        }
        DerivativeEventFamily::OpenInterest => {
            validate_named_positive_decimal(
                path,
                batch_index,
                row,
                batch,
                "open_interest",
                false,
                report,
            );
            validate_named_positive_decimal(
                path,
                batch_index,
                row,
                batch,
                "quote_notional",
                true,
                report,
            );
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "open_interest_unit",
                &["contracts", "base_asset"],
                report,
            );
        }
        DerivativeEventFamily::TraderRatio => {
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "metric_kind",
                &[
                    "binance_top_account_ratio",
                    "binance_top_position_ratio",
                    "bybit_long_short_ratio",
                ],
                report,
            );
            for field in ["long_ratio", "long_short_ratio"] {
                validate_named_nonnegative_decimal(path, batch_index, row, batch, field, report);
            }
            validate_named_positive_decimal(
                path,
                batch_index,
                row,
                batch,
                "short_ratio",
                false,
                report,
            );
        }
        DerivativeEventFamily::QuoteConversion => {
            validate_enum(
                path,
                batch_index,
                row,
                batch,
                "side",
                &["bid", "ask"],
                report,
            );
            for field in ["price", "executable_quantity"] {
                validate_named_positive_decimal(
                    path,
                    batch_index,
                    row,
                    batch,
                    field,
                    false,
                    report,
                );
            }
        }
    }
}

fn validate_named_nonnegative_decimal(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    field: &str,
    report: &mut ValidationReport,
) {
    let column = named_decimal(batch, field);
    if column.value(row) < 0 {
        row_issue(
            report,
            "NEGATIVE_DECIMAL",
            path,
            batch_index,
            row,
            format!("{field} must be nonnegative"),
        );
    } else {
        validate_named_decimal(path, batch_index, row, batch, field, report);
    }
}

const FUNDING_PROVENANCE: &[&str] = &["venue_payload", "instrument_rule", "assumed_venue_default"];

fn validate_funding_row(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    timestamp_field: &str,
    report: &mut ValidationReport,
) {
    validate_enum(
        path,
        batch_index,
        row,
        batch,
        "funding_basis",
        &["mark_notional"],
        report,
    );
    validate_enum(
        path,
        batch_index,
        row,
        batch,
        "interval_provenance",
        FUNDING_PROVENANCE,
        report,
    );
    if named_u32(batch, "interval_secs").value(row) == 0 {
        row_issue(
            report,
            "INVALID_FUNDING_INTERVAL",
            path,
            batch_index,
            row,
            "funding interval must be positive",
        );
    }
    validate_timestamp(
        path,
        batch_index,
        row,
        timestamp_field,
        Some(named_i64(batch, timestamp_field).value(row)),
        report,
    );
}

fn validate_enum(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    field: &str,
    allowed: &[&str],
    report: &mut ValidationReport,
) {
    if !allowed.contains(&named_string(batch, field).value(row)) {
        row_issue(
            report,
            "INVALID_ENUM",
            path,
            batch_index,
            row,
            format!("{field} is outside the canonical enum"),
        );
    }
}

fn validate_mode_set(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    field: &str,
    allowed: &[&str],
    report: &mut ValidationReport,
) {
    let value = named_string(batch, field).value(row);
    let modes = value
        .split(',')
        .filter(|mode| !mode.is_empty())
        .collect::<Vec<_>>();
    let unique = modes.iter().copied().collect::<HashSet<_>>();
    if modes.is_empty()
        || unique.len() != modes.len()
        || modes.iter().any(|mode| !allowed.contains(mode))
    {
        row_issue(
            report,
            "INVALID_ENUM",
            path,
            batch_index,
            row,
            format!("{field} contains missing, duplicate, or unknown modes"),
        );
    }
}

fn validate_named_decimal(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    field: &str,
    report: &mut ValidationReport,
) {
    let column = named_decimal(batch, field);
    if !column.is_null(row) && column.value(row).unsigned_abs() > MAX_DECIMAL_38 as u128 {
        row_issue(
            report,
            "DECIMAL_RANGE",
            path,
            batch_index,
            row,
            format!("{field} exceeds Decimal128 precision 38"),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_optional_bounds(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    lower_name: &str,
    upper_name: &str,
    allow_equal: bool,
    report: &mut ValidationReport,
) {
    let lower = named_decimal(batch, lower_name);
    let upper = named_decimal(batch, upper_name);
    if !lower.is_null(row)
        && !upper.is_null(row)
        && if allow_equal {
            lower.value(row) > upper.value(row)
        } else {
            lower.value(row) >= upper.value(row)
        }
    {
        row_issue(
            report,
            "INVALID_INSTRUMENT_RULE",
            path,
            batch_index,
            row,
            format!("{lower_name} must be below {upper_name}"),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_named_positive_decimal(
    path: &Path,
    batch_index: usize,
    row: usize,
    batch: &RecordBatch,
    field: &str,
    nullable: bool,
    report: &mut ValidationReport,
) {
    let column = named_decimal(batch, field);
    if column.is_null(row) {
        if !nullable {
            row_issue(
                report,
                "NON_POSITIVE_DECIMAL",
                path,
                batch_index,
                row,
                format!("{field} must be positive"),
            );
        }
    } else {
        validate_positive_decimal(path, batch_index, row, field, column.value(row), report);
    }
}

fn derivative_column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("canonical derivative schema")
}

fn named_decimal<'a>(batch: &'a RecordBatch, name: &str) -> &'a Decimal128Array {
    batch
        .column_by_name(name)
        .expect("canonical field")
        .as_any()
        .downcast_ref()
        .expect("canonical decimal")
}

fn named_string<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .expect("canonical field")
        .as_any()
        .downcast_ref()
        .expect("canonical string")
}

fn named_u32<'a>(batch: &'a RecordBatch, name: &str) -> &'a UInt32Array {
    batch
        .column_by_name(name)
        .expect("canonical field")
        .as_any()
        .downcast_ref()
        .expect("canonical u32")
}

fn named_i64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .expect("canonical field")
        .as_any()
        .downcast_ref()
        .expect("canonical i64")
}

fn row_issue(
    report: &mut ValidationReport,
    code: &str,
    path: &Path,
    batch: usize,
    row: usize,
    message: impl Into<String>,
) {
    issue(report, code, path, Some(batch), Some(row), message);
}

fn issue(
    report: &mut ValidationReport,
    code: &str,
    path: &Path,
    batch: Option<usize>,
    row: Option<usize>,
    message: impl Into<String>,
) {
    report.errors.push(ValidationIssue {
        code: code.to_owned(),
        path: path.to_path_buf(),
        batch,
        row,
        message: message.into(),
    });
}

fn has_stream_terminator(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let Ok(length) = file.metadata().map(|metadata| metadata.len()) else {
        return false;
    };
    if length < 8 || file.seek(SeekFrom::End(-8)).is_err() {
        return false;
    }
    let mut marker = [0_u8; 8];
    file.read_exact(&mut marker).is_ok() && marker == [255, 255, 255, 255, 0, 0, 0, 0]
}
