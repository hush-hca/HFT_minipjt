use std::fs::{self, File};
use std::io::{BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use chrono::{TimeZone, Utc};
use md_core::model::{
    AdapterId, CanonicalSymbol, EventMeta, TakerSide, TimestampPrecision, TradeTick,
};
use md_storage::{RecoveryError, SchemaContext, TradeBatchBuilder, recover_partial};
use uuid::Uuid;

static TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("md-storage-recovery-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn batch(id: u64) -> arrow_array::RecordBatch {
    let ts = 1_725_930_000_000_000 + id as i64;
    let trade = TradeTick {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter: AdapterId::BinanceSpot,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            source_symbol: "BTCUSDT".to_owned(),
            source_stream: "btcusdt@trade".to_owned(),
            source_sequence: None,
            exchange_event_ts_us: Some(ts),
            exchange_trade_ts_us: Some(ts),
            event_ts_precision: TimestampPrecision::Microsecond,
            trade_ts_precision: TimestampPrecision::Microsecond,
            local_recv_ts_us: ts,
            raw_size_bytes: 100,
        },
        trade_id: id.to_string(),
        price: 60_000_000_000_000_000_000_000,
        quantity: 1_000_000_000_000_000_000,
        taker_side: TakerSide::Sell,
    };
    let context = SchemaContext {
        adapter: AdapterId::BinanceSpot,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        utc_hour: Utc.with_ymd_and_hms(2024, 9, 10, 2, 0, 0).unwrap(),
    };
    let mut builder = TradeBatchBuilder::new(context);
    builder.push(&trade).unwrap();
    builder.finish().unwrap()
}

#[test]
fn truncated_tail_keeps_every_complete_batch_and_isolates_exact_rejected_bytes() {
    let temp = TestDir::new();
    let path = temp.path().join("trades.arrow.partial");
    let batches: Vec<_> = (0..4).map(batch).collect();
    let mut bytes = Vec::new();
    let third_boundary;
    let fourth_boundary;
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batches[0].schema()).unwrap();
        for value in &batches[..3] {
            writer.write(value).unwrap();
        }
        third_boundary = writer.get_mut().len();
        writer.write(&batches[3]).unwrap();
        fourth_boundary = writer.get_mut().len();
        writer.finish().unwrap();
    }
    let truncated_len = third_boundary + (fourth_boundary - third_boundary) / 2;
    bytes.truncate(truncated_len);
    fs::write(&path, &bytes).unwrap();

    let outcome = recover_partial(&path).unwrap();
    assert_eq!(outcome.batches_kept, 3);
    assert_eq!(outcome.rows_kept, 3);
    assert_eq!(outcome.bytes_kept, third_boundary as u64);
    assert_eq!(
        outcome.bytes_rejected,
        (truncated_len - third_boundary) as u64
    );
    let corrupt = outcome.corrupt_path.unwrap();
    assert_eq!(
        corrupt.extension().and_then(|value| value.to_str()),
        Some("corrupt")
    );
    assert_eq!(fs::read(corrupt).unwrap(), bytes[third_boundary..]);

    let decoded: Vec<_> = StreamReader::try_new(BufReader::new(File::open(&path).unwrap()), None)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded.iter().map(|v| v.num_rows()).sum::<usize>(), 3);
}

#[test]
fn complete_batches_without_terminator_are_rewritten_as_a_valid_stream() {
    let temp = TestDir::new();
    let path = temp.path().join("trades.arrow.partial");
    let batches: Vec<_> = (0..2).map(batch).collect();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batches[0].schema()).unwrap();
        writer.write(&batches[0]).unwrap();
        writer.write(&batches[1]).unwrap();
    }
    let original_len = bytes.len();
    fs::write(&path, &bytes).unwrap();

    let outcome = recover_partial(&path).unwrap();
    assert_eq!(outcome.batches_kept, 2);
    assert_eq!(outcome.rows_kept, 2);
    assert_eq!(outcome.bytes_kept, original_len as u64);
    assert_eq!(outcome.bytes_rejected, 0);
    assert!(outcome.corrupt_path.is_none());
    let recovered = fs::read(&path).unwrap();
    assert_eq!(
        &recovered[recovered.len() - 8..],
        &[255, 255, 255, 255, 0, 0, 0, 0]
    );
}

#[test]
fn unprovable_header_fails_without_modifying_source() {
    let temp = TestDir::new();
    let path = temp.path().join("trades.arrow.partial");
    let original = b"broken schema header";
    fs::write(&path, original).unwrap();

    let error = recover_partial(&path).unwrap_err();
    assert!(matches!(error, RecoveryError::UnrecoverableHeader { .. }));
    assert_eq!(fs::read(path).unwrap(), original);
}

#[test]
fn a_clean_finished_stream_reports_no_rejected_bytes() {
    let temp = TestDir::new();
    let path = temp.path().join("trades.arrow.partial");
    let value = batch(0);
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &value.schema()).unwrap();
        writer.write(&value).unwrap();
        writer.finish().unwrap();
    }
    let expected_len = bytes.len();
    fs::write(&path, bytes).unwrap();

    let outcome = recover_partial(&path).unwrap();
    assert_eq!(outcome.batches_kept, 1);
    assert_eq!(outcome.rows_kept, 1);
    assert_eq!(outcome.bytes_kept, expected_len as u64);
    assert_eq!(outcome.bytes_rejected, 0);
    assert!(outcome.corrupt_path.is_none());

    let reader = StreamReader::try_new(Cursor::new(fs::read(path).unwrap()), None).unwrap();
    assert_eq!(reader.map(Result::unwrap).count(), 1);
}
