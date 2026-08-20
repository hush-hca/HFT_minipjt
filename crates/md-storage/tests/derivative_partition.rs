use std::time::{Duration, Instant};
use std::{
    fs,
    fs::OpenOptions,
    io::{BufReader, Write},
};

use arrow_ipc::reader::StreamReader;
use funding_core::meta::DerivativeMeta;
use funding_core::public::{DerivativeEvent, MarkIndexSnapshot};
use md_core::model::{AdapterId, CanonicalSymbol, TimestampPrecision};
use md_storage::{
    DerivativePartitionKey, DerivativePartitionRouter, StorageConfig, StorageError, validate_path,
};
use tempfile::tempdir;
use uuid::Uuid;

const HOUR_US: i64 = 1_725_930_000_000_000;

fn mark(source: Option<i64>, local: i64) -> DerivativeEvent {
    mark_symbol("ETH", source, local)
}

fn mark_symbol(base: &str, source: Option<i64>, local: i64) -> DerivativeEvent {
    DerivativeEvent::MarkIndex(MarkIndexSnapshot {
        meta: DerivativeMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            venue: AdapterId::BybitLinear,
            symbol: CanonicalSymbol::new(base, "USDT"),
            venue_symbol: format!("{base}USDT"),
            source_ts_us: source,
            source_ts_precision: source.map_or(TimestampPrecision::Unavailable, |_| {
                TimestampPrecision::Millisecond
            }),
            local_recv_ts_us: local,
        },
        mark_price: 3_000_000_000_000_000_000_000,
        index_price: 2_999_000_000_000_000_000_000,
    })
}

#[test]
fn canonical_path_uses_source_hour_and_family_first() {
    let event = mark(Some(HOUR_US + 1), HOUR_US + 3_600_000_010);
    let key = DerivativePartitionKey::for_event(&event).unwrap();
    assert_eq!(
        key.partial_path(std::path::Path::new("data")),
        std::path::Path::new(
            "data/derivatives/mark_index/bybit/linear_futures/ETH-USDT/2024-09-10/01/mark_index.arrow.partial"
        )
    );
}

#[tokio::test]
async fn flushes_by_time_and_finalizes_on_shutdown() {
    let root = tempdir().unwrap();
    let event = mark(None, HOUR_US + 1);
    let key = DerivativePartitionKey::for_event(&event).unwrap();
    let partial = key.partial_path(root.path());
    let mut router = DerivativePartitionRouter::open(StorageConfig {
        output_root: root.path().into(),
        batch_rows: 100,
        flush_interval: Duration::from_millis(1),
    })
    .unwrap();
    router.push(event).await.unwrap();
    router
        .flush_due(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(partial.exists());
    router.shutdown().await.unwrap();
    assert!(!partial.exists());
    assert!(partial.with_file_name("mark_index.arrow").exists());
}

#[tokio::test]
async fn recovers_an_unfinished_partial_and_merges_all_complete_rows() {
    let root = tempdir().unwrap();
    let event = mark(None, HOUR_US + 1);
    let key = DerivativePartitionKey::for_event(&event).unwrap();
    let partial = key.partial_path(root.path());
    let config = StorageConfig {
        output_root: root.path().into(),
        batch_rows: 1,
        flush_interval: Duration::from_secs(60),
    };

    let mut interrupted = DerivativePartitionRouter::open(config.clone()).unwrap();
    interrupted.push(event).await.unwrap();
    assert!(partial.exists());
    drop(interrupted);

    let mut resumed = DerivativePartitionRouter::open(config).unwrap();
    resumed.push(mark(None, HOUR_US + 2)).await.unwrap();
    resumed.shutdown().await.unwrap();

    let final_path = partial.with_file_name("mark_index.arrow");
    let rows = StreamReader::try_new(BufReader::new(fs::File::open(final_path).unwrap()), None)
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn writer_creation_errors_are_propagated_without_replacing_the_root_file() {
    let root = tempdir().unwrap();
    let root_file = root.path().join("root-is-file");
    fs::write(&root_file, b"keep").unwrap();
    let mut router = DerivativePartitionRouter::open(StorageConfig {
        output_root: root_file.clone(),
        batch_rows: 1,
        flush_interval: Duration::from_secs(1),
    })
    .unwrap();
    assert!(router.push(mark(None, HOUR_US + 1)).await.is_err());
    assert_eq!(fs::read(root_file).unwrap(), b"keep");
}

#[tokio::test]
async fn old_history_uses_positive_source_hour_and_validates() {
    let root = tempdir().unwrap();
    let source = HOUR_US + 1;
    let local = source + 11 * 24 * 3_600_000_000;
    let event = mark(Some(source), local);
    let key = DerivativePartitionKey::for_event(&event).unwrap();
    assert!(key.partial_path(root.path()).ends_with(std::path::Path::new(
        "derivatives/mark_index/bybit/linear_futures/ETH-USDT/2024-09-10/01/mark_index.arrow.partial",
    )));
    let mut router = DerivativePartitionRouter::open(StorageConfig {
        output_root: root.path().into(),
        batch_rows: 10,
        flush_interval: Duration::from_secs(1),
    })
    .unwrap();
    router.push(event).await.unwrap();
    router.shutdown().await.unwrap();
    let report = validate_path(root.path()).unwrap();
    assert!(report.is_valid(), "{:#?}", report.errors);
}

#[tokio::test]
async fn late_and_reopened_hour_events_remain_valid_in_arrival_order() {
    let root = tempdir().unwrap();
    let config = StorageConfig {
        output_root: root.path().into(),
        batch_rows: 1,
        flush_interval: Duration::from_secs(1),
    };
    let mut first = DerivativePartitionRouter::open(config.clone()).unwrap();
    first
        .push(mark(Some(HOUR_US + 200), HOUR_US + 201))
        .await
        .unwrap();
    first
        .push(mark(Some(HOUR_US + 100), HOUR_US + 202))
        .await
        .unwrap();
    first.shutdown().await.unwrap();

    let mut reopened = DerivativePartitionRouter::open(config).unwrap();
    reopened
        .push(mark(Some(HOUR_US + 50), HOUR_US + 203))
        .await
        .unwrap();
    reopened.shutdown().await.unwrap();
    let report = validate_path(root.path()).unwrap();
    assert!(report.is_valid(), "{:#?}", report.errors);
}

#[tokio::test]
async fn corrupt_existing_final_is_rejected_and_preserved_during_merge() {
    let root = tempdir().unwrap();
    let config = StorageConfig {
        output_root: root.path().into(),
        batch_rows: 1,
        flush_interval: Duration::from_secs(1),
    };
    let event = mark(None, HOUR_US + 1);
    let final_path = DerivativePartitionKey::for_event(&event)
        .unwrap()
        .partial_path(root.path())
        .with_file_name("mark_index.arrow");
    let mut first = DerivativePartitionRouter::open(config.clone()).unwrap();
    first.push(event).await.unwrap();
    first.shutdown().await.unwrap();
    OpenOptions::new()
        .append(true)
        .open(&final_path)
        .unwrap()
        .write_all(b"trailing")
        .unwrap();
    let corrupt = fs::read(&final_path).unwrap();

    let mut reopened = DerivativePartitionRouter::open(config).unwrap();
    reopened.push(mark(None, HOUR_US + 2)).await.unwrap();
    assert!(matches!(
        reopened.shutdown().await,
        Err(StorageError::UnreadableFinal { .. })
    ));
    assert_eq!(fs::read(final_path).unwrap(), corrupt);
}

#[tokio::test]
async fn missing_eos_existing_final_is_rejected_and_preserved_during_merge() {
    let root = tempdir().unwrap();
    let config = StorageConfig {
        output_root: root.path().into(),
        batch_rows: 1,
        flush_interval: Duration::from_secs(1),
    };
    let event = mark(None, HOUR_US + 1);
    let final_path = DerivativePartitionKey::for_event(&event)
        .unwrap()
        .partial_path(root.path())
        .with_file_name("mark_index.arrow");
    let mut first = DerivativePartitionRouter::open(config.clone()).unwrap();
    first.push(event).await.unwrap();
    first.shutdown().await.unwrap();
    let shortened_len = fs::metadata(&final_path).unwrap().len() - 8;
    OpenOptions::new()
        .write(true)
        .open(&final_path)
        .unwrap()
        .set_len(shortened_len)
        .unwrap();
    let corrupt = fs::read(&final_path).unwrap();

    let mut reopened = DerivativePartitionRouter::open(config).unwrap();
    reopened.push(mark(None, HOUR_US + 2)).await.unwrap();
    assert!(matches!(
        reopened.shutdown().await,
        Err(StorageError::UnreadableFinal { .. })
    ));
    assert_eq!(fs::read(final_path).unwrap(), corrupt);
}

#[tokio::test]
async fn shutdown_attempts_every_writer_after_one_partition_fails() {
    let root = tempdir().unwrap();
    let config = StorageConfig {
        output_root: root.path().into(),
        batch_rows: 1,
        flush_interval: Duration::from_secs(1),
    };
    let failing = mark_symbol("ETH", None, HOUR_US + 1);
    let failing_final = DerivativePartitionKey::for_event(&failing)
        .unwrap()
        .partial_path(root.path())
        .with_file_name("mark_index.arrow");
    let mut seed = DerivativePartitionRouter::open(config.clone()).unwrap();
    seed.push(failing).await.unwrap();
    seed.shutdown().await.unwrap();
    OpenOptions::new()
        .append(true)
        .open(&failing_final)
        .unwrap()
        .write_all(b"trailing")
        .unwrap();

    let failing_new = mark_symbol("ETH", None, HOUR_US + 2);
    let healthy = mark_symbol("BTC", None, HOUR_US + 3);
    let healthy_final = DerivativePartitionKey::for_event(&healthy)
        .unwrap()
        .partial_path(root.path())
        .with_file_name("mark_index.arrow");
    let mut router = DerivativePartitionRouter::open(config).unwrap();
    router.push(failing_new).await.unwrap();
    router.push(healthy).await.unwrap();
    assert!(router.shutdown().await.is_err());
    assert!(healthy_final.exists(), "healthy writer was not finalized");
    assert!(!healthy_final.with_extension("arrow.partial").exists());
}
