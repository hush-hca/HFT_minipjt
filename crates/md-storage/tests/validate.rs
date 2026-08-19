use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::{ArrayRef, Decimal128Array, RecordBatch, UInt16Array};
use arrow_ipc::writer::StreamWriter;
use chrono::{TimeZone, Utc};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, PriceLevel, TimestampPrecision,
};
use md_storage::{BookBatchBuilder, SchemaContext, validate_path};
use tempfile::TempDir;
use uuid::Uuid;

const HOUR_US: i64 = 1_725_930_000_000_000;

#[test]
fn recursively_validates_a_well_formed_dataset() {
    let fixture = DatasetFixture::new();
    fixture.write("books.arrow", valid_batch());

    let report = validate_path(fixture.root.path()).unwrap();

    assert_eq!(report.files, 1);
    assert_eq!(report.batches, 1);
    assert_eq!(report.rows, 4);
    assert!(report.errors.is_empty(), "{:#?}", report.errors);
}

#[test]
fn reports_stable_codes_and_paths_for_semantic_failures() {
    let fixture = DatasetFixture::new();

    let mut wrong_version = valid_batch();
    let mut metadata = wrong_version.schema().metadata().clone();
    metadata.insert("schema_version".into(), "999".into());
    wrong_version = RecordBatch::try_new(
        std::sync::Arc::new(
            wrong_version
                .schema()
                .as_ref()
                .clone()
                .with_metadata(metadata),
        ),
        wrong_version.columns().to_vec(),
    )
    .unwrap();
    fixture.write_at("wrong-version", "books.arrow", wrong_version);

    let mut unsorted = valid_batch();
    let mut columns = unsorted.columns().to_vec();
    columns[16] = std::sync::Arc::new(
        Decimal128Array::from(vec![99_i128, 100, 101, 102])
            .with_precision_and_scale(38, 18)
            .unwrap(),
    ) as ArrayRef;
    unsorted = RecordBatch::try_new(unsorted.schema(), columns).unwrap();
    fixture.write_at("unsorted", "books.arrow", unsorted);

    let mut duplicate = valid_batch();
    let mut columns = duplicate.columns().to_vec();
    columns[15] = std::sync::Arc::new(UInt16Array::from(vec![0_u16, 0, 0, 1]));
    duplicate = RecordBatch::try_new(duplicate.schema(), columns).unwrap();
    fixture.write_at("duplicate", "books.arrow", duplicate);

    let mut bad_decimal_meta = valid_batch();
    let mut metadata = bad_decimal_meta.schema().metadata().clone();
    metadata.insert("decimal_scale".into(), "8".into());
    bad_decimal_meta = RecordBatch::try_new(
        std::sync::Arc::new(
            bad_decimal_meta
                .schema()
                .as_ref()
                .clone()
                .with_metadata(metadata),
        ),
        bad_decimal_meta.columns().to_vec(),
    )
    .unwrap();
    fixture.write_at("decimal", "books.arrow", bad_decimal_meta);

    let mut wrong_hour = valid_batch();
    let mut columns = wrong_hour.columns().to_vec();
    columns[10] = std::sync::Arc::new(arrow_array::Int64Array::from(vec![
        HOUR_US + 3_600_000_000;
        4
    ]));
    wrong_hour = RecordBatch::try_new(wrong_hour.schema(), columns).unwrap();
    fixture.write_at("wrong-hour", "books.arrow", wrong_hour);

    let mut crossed = valid_batch();
    let mut columns = crossed.columns().to_vec();
    columns[16] = std::sync::Arc::new(
        Decimal128Array::from(vec![102_i128, 101, 101, 103])
            .with_precision_and_scale(38, 18)
            .unwrap(),
    ) as ArrayRef;
    crossed = RecordBatch::try_new(crossed.schema(), columns).unwrap();
    fixture.write_at("crossed", "books.arrow", crossed);

    fixture.write_at("corrupt", "books.arrow", valid_batch());
    let corrupt = fixture.path_at("corrupt", "books.arrow");
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&corrupt)
        .unwrap()
        .write_all(b"trailing corruption")
        .unwrap();

    let report = validate_path(fixture.root.path()).unwrap();
    let codes = report
        .errors
        .iter()
        .map(|issue| issue.code.as_str())
        .collect::<Vec<_>>();
    for code in [
        "SCHEMA_VERSION",
        "UNSORTED_BOOK",
        "DUPLICATE_LEVEL",
        "DECIMAL_METADATA",
        "TIMESTAMP_PARTITION_MISMATCH",
        "CROSSED_BOOK",
        "UNREADABLE_ARROW",
    ] {
        assert!(
            codes.contains(&code),
            "missing {code}: {:#?}",
            report.errors
        );
    }
    assert!(report.errors.iter().all(|issue| issue.path.is_absolute()));
}

#[test]
fn direct_files_without_a_canonical_partition_layout_report_path_layout() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("books.arrow");
    let batch = valid_batch();
    let writer = File::create(&file).unwrap();
    let mut stream = StreamWriter::try_new(writer, batch.schema().as_ref()).unwrap();
    stream.write(&batch).unwrap();
    stream.finish().unwrap();

    let report = validate_path(&file).unwrap();

    assert_eq!(report.files, 1);
    assert!(
        report
            .errors
            .iter()
            .any(|issue| issue.code == "PATH_LAYOUT"),
        "{:#?}",
        report.errors
    );
}

struct DatasetFixture {
    root: TempDir,
}

impl DatasetFixture {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
        }
    }

    fn write(&self, name: &str, batch: RecordBatch) {
        self.write_at("", name, batch);
    }

    fn write_at(&self, prefix: &str, name: &str, batch: RecordBatch) {
        let path = self.path_at(prefix, name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = File::create(path).unwrap();
        let mut writer = StreamWriter::try_new(file, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    fn path_at(&self, prefix: &str, name: &str) -> PathBuf {
        let base = if prefix.is_empty() {
            self.root.path().to_path_buf()
        } else {
            self.root.path().join(prefix)
        };
        base.join("binance/spot/BTC-USDT/2024-09-10/01").join(name)
    }
}

fn valid_batch() -> RecordBatch {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let context = SchemaContext {
        adapter: AdapterId::BinanceSpot,
        symbol: symbol.clone(),
        utc_hour: Utc.timestamp_micros(HOUR_US).unwrap(),
    };
    let mut builder = BookBatchBuilder::new(context);
    builder
        .push(&BookSnapshot {
            meta: EventMeta {
                schema_version: 1,
                event_id: Uuid::now_v7(),
                adapter: AdapterId::BinanceSpot,
                symbol,
                source_symbol: "BTCUSDT".into(),
                source_stream: "btcusdt@depth20@100ms".into(),
                source_sequence: Some(7),
                exchange_event_ts_us: Some(HOUR_US + 10),
                exchange_trade_ts_us: None,
                event_ts_precision: TimestampPrecision::Millisecond,
                trade_ts_precision: TimestampPrecision::Unavailable,
                local_recv_ts_us: HOUR_US + 20,
                raw_size_bytes: 100,
            },
            bids: vec![level(100), level(99)],
            asks: vec![level(101), level(102)],
        })
        .unwrap();
    builder.finish().unwrap()
}

fn level(price: i128) -> PriceLevel {
    PriceLevel { price, quantity: 1 }
}

#[allow(dead_code)]
fn _assert_path(_: &Path) {}
