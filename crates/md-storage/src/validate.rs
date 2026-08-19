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
        .is_some_and(|value| !matches!(value.as_str(), "upbit" | "bithumb" | "binance"))
        || metadata
            .get("market")
            .is_some_and(|value| !matches!(value.as_str(), "spot" | "usdm_futures"))
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
        ("upbit", "spot") | ("bithumb", "spot") | ("binance", "spot") | ("binance", "usdm_futures")
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
