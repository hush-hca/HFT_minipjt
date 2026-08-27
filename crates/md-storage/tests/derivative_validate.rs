use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use chrono::{TimeZone, Utc};
use funding_core::meta::DerivativeMeta;
use funding_core::public::{
    DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
};
use md_core::model::{AdapterId, CanonicalSymbol, TimestampPrecision};
use md_storage::{
    DerivativeBatchBuilder, DerivativeEventFamily, DerivativeSchemaContext, validate_path,
};
use tempfile::TempDir;
use uuid::Uuid;

const HOUR_US: i64 = 1_725_930_000_000_000;

#[test]
fn reports_structured_derivative_schema_enum_timestamp_and_corruption_issues() {
    let root = TempDir::new().unwrap();

    let mut wrong_family = valid_batch();
    let mut metadata = wrong_family.schema().metadata().clone();
    metadata.insert("event_family".into(), "open_interest".into());
    wrong_family = with_schema_metadata(wrong_family, metadata);
    write_batch(root.path(), "wrong-family", wrong_family, false);

    let mut invalid_enum = valid_batch();
    let mut columns = invalid_enum.columns().to_vec();
    columns[11] = Arc::new(StringArray::from(vec!["settled_actual"])) as ArrayRef;
    invalid_enum = RecordBatch::try_new(invalid_enum.schema(), columns).unwrap();
    write_batch(root.path(), "invalid-enum", invalid_enum, false);

    let mut wrong_type = valid_batch();
    let mut fields = wrong_type
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<Field>>();
    fields[10] = Field::new("rate", DataType::Int64, false);
    let wrong_schema = Arc::new(Schema::new_with_metadata(
        fields,
        wrong_type.schema().metadata().clone(),
    ));
    let mut columns = wrong_type.columns().to_vec();
    columns[10] = Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef;
    wrong_type = RecordBatch::try_new(wrong_schema, columns).unwrap();
    write_batch(root.path(), "wrong-type", wrong_type, false);

    let mut wrong_hour = valid_batch();
    let mut columns = wrong_hour.columns().to_vec();
    columns[7] = Arc::new(Int64Array::from(vec![Some(HOUR_US + 3_600_000_000)])) as ArrayRef;
    wrong_hour = RecordBatch::try_new(wrong_hour.schema(), columns).unwrap();
    write_batch(root.path(), "wrong-hour", wrong_hour, false);

    write_batch(root.path(), "trailing", valid_batch(), true);

    let report = validate_path(root.path()).unwrap();
    let codes = report
        .errors
        .iter()
        .map(|issue| issue.code.as_str())
        .collect::<Vec<_>>();
    for code in [
        "EVENT_FAMILY_METADATA",
        "INVALID_ENUM",
        "SCHEMA_TYPE",
        "TIMESTAMP_PARTITION_MISMATCH",
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

fn valid_batch() -> RecordBatch {
    let context = DerivativeSchemaContext {
        family: DerivativeEventFamily::FundingEstimate,
        venue: AdapterId::BinanceUsdm,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        utc_hour: Utc.timestamp_micros(HOUR_US).unwrap(),
    };
    let mut builder = DerivativeBatchBuilder::new(context);
    builder
        .push(DerivativeEvent::FundingEstimate(FundingEstimate {
            meta: DerivativeMeta {
                schema_version: 1,
                event_id: Uuid::now_v7(),
                venue: AdapterId::BinanceUsdm,
                symbol: CanonicalSymbol::new("BTC", "USDT"),
                venue_symbol: "BTCUSDT".into(),
                source_ts_us: Some(HOUR_US + 1),
                source_ts_precision: TimestampPrecision::Millisecond,
                local_recv_ts_us: HOUR_US + 2,
            },
            rate: 100_000_000_000_000,
            rate_kind: FundingRateKind::IndicativeNext,
            basis: FundingBasis::MarkNotional,
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            next_funding_ts_us: HOUR_US + 28_800_000_000,
        }))
        .unwrap();
    builder.finish().unwrap()
}

fn with_schema_metadata(
    batch: RecordBatch,
    metadata: std::collections::HashMap<String, String>,
) -> RecordBatch {
    let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
    RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap()
}

fn write_batch(root: &std::path::Path, prefix: &str, batch: RecordBatch, trailing: bool) {
    let path = root
        .join(prefix)
        .join("derivatives/funding_estimate/binance/usdm_futures/BTC-USDT/2024-09-10/01/funding_estimate.arrow");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut writer =
        StreamWriter::try_new(File::create(&path).unwrap(), batch.schema().as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    if trailing {
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"trailing corruption")
            .unwrap();
    }
}
