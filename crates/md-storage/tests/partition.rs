use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow_ipc::reader::StreamReader;
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel, TakerSide,
    TimestampPrecision, TradeTick,
};
use md_storage::{PartitionKey, PartitionRouter, StorageConfig};
use uuid::Uuid;

const HOUR_01_END: i64 = 1_725_930_000_000_000 - 1;
const HOUR_02_START: i64 = 1_725_930_000_000_000;
static TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("md-storage-partition-{}-{id}", std::process::id()));
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

fn trade(ts_us: i64, trade_id: &str) -> NormalizedEvent {
    NormalizedEvent::Trade(TradeTick {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter: AdapterId::BinanceUsdm,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            source_symbol: "BTCUSDT".to_owned(),
            source_stream: "btcusdt@trade".to_owned(),
            source_sequence: None,
            exchange_event_ts_us: Some(ts_us),
            exchange_trade_ts_us: Some(ts_us),
            event_ts_precision: TimestampPrecision::Microsecond,
            trade_ts_precision: TimestampPrecision::Microsecond,
            local_recv_ts_us: ts_us,
            raw_size_bytes: 100,
        },
        trade_id: trade_id.to_owned(),
        price: 60_000_000_000_000_000_000_000,
        quantity: 1_000_000_000_000_000_000,
        taker_side: TakerSide::Buy,
    })
}

fn book(ts_us: i64) -> NormalizedEvent {
    let meta = match trade(ts_us, "meta") {
        NormalizedEvent::Trade(value) => value.meta,
        NormalizedEvent::Book(_) => unreachable!(),
    };
    NormalizedEvent::Book(BookSnapshot {
        meta,
        bids: vec![PriceLevel {
            price: 59_999_000_000_000_000_000_000,
            quantity: 1_000_000_000_000_000_000,
        }],
        asks: vec![PriceLevel {
            price: 60_001_000_000_000_000_000_000,
            quantity: 2_000_000_000_000_000_000,
        }],
    })
}

fn config(root: &Path, batch_rows: usize, flush_interval: Duration) -> StorageConfig {
    StorageConfig {
        output_root: root.to_owned(),
        batch_rows,
        flush_interval,
    }
}

fn count_rows(path: &Path) -> usize {
    StreamReader::try_new(BufReader::new(File::open(path).unwrap()), None)
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum()
}

#[test]
fn partition_path_is_utc_and_event_specific() {
    let key =
        PartitionKey::from_parts(AdapterId::BinanceUsdm, "BTC", "USDT", 1_725_930_034_373_000)
            .unwrap();
    assert_eq!(
        key.book_path(Path::new("data")),
        Path::new("data/binance/usdm_futures/BTC-USDT/2024-09-10/01/books.arrow.partial")
    );
    assert_eq!(
        key.trade_path(Path::new("data")),
        Path::new("data/binance/usdm_futures/BTC-USDT/2024-09-10/01/trades.arrow.partial")
    );
}

#[test]
fn bybit_linear_uses_its_own_storage_namespace() {
    let key =
        PartitionKey::from_parts(AdapterId::BybitLinear, "BTC", "USDT", HOUR_02_START).unwrap();
    assert_eq!(
        key.book_path(Path::new("data")),
        Path::new("data/bybit/linear_futures/BTC-USDT/2024-09-10/01/books.arrow.partial")
    );
}

#[test]
fn unsafe_symbols_are_rejected_before_forming_paths() {
    for bad in ["", "../BTC", "BTC/USD", "BT C", "ÉTH", "btc"] {
        assert!(
            PartitionKey::from_parts(AdapterId::BinanceSpot, bad, "USDT", HOUR_02_START).is_err(),
            "accepted unsafe component {bad:?}"
        );
    }
}

#[test]
fn invalid_flush_configuration_is_rejected_at_open() {
    let root = Path::new("unused");
    assert!(PartitionRouter::open(config(root, 0, Duration::from_secs(1))).is_err());
    assert!(PartitionRouter::open(config(root, 1, Duration::ZERO)).is_err());
}

#[tokio::test]
async fn books_and_trades_use_separate_active_and_final_streams() {
    let temp = TestDir::new();
    let mut router =
        PartitionRouter::open(config(temp.path(), 100, Duration::from_secs(60))).unwrap();
    let book_event = book(HOUR_02_START);
    let trade_event = trade(HOUR_02_START + 1, "one");
    let key = PartitionKey::for_event(&book_event).unwrap();
    router.push(book_event).await.unwrap();
    router.push(trade_event).await.unwrap();
    assert!(key.book_path(temp.path()).exists());
    assert!(key.trade_path(temp.path()).exists());

    router.shutdown().await.unwrap();
    let book_final = key.book_path(temp.path()).with_file_name("books.arrow");
    let trade_final = key.trade_path(temp.path()).with_file_name("trades.arrow");
    assert_eq!(count_rows(&book_final), 2);
    assert_eq!(count_rows(&trade_final), 1);
}

#[tokio::test]
async fn utc_hour_rotation_finalizes_old_partition_and_shutdown_finalizes_new_one() {
    let temp = TestDir::new();
    let mut router =
        PartitionRouter::open(config(temp.path(), 100, Duration::from_secs(1))).unwrap();

    let first_key = PartitionKey::for_event(&trade(HOUR_01_END, "one")).unwrap();
    let second_key = PartitionKey::for_event(&trade(HOUR_02_START, "two")).unwrap();
    router.push(trade(HOUR_01_END, "one")).await.unwrap();
    router.push(trade(HOUR_02_START, "two")).await.unwrap();

    let first_partial = first_key.trade_path(temp.path());
    let first_final = first_partial.with_file_name("trades.arrow");
    assert!(!first_partial.exists());
    assert!(first_final.exists());
    assert_eq!(count_rows(&first_final), 1);

    router.shutdown().await.unwrap();
    let second_partial = second_key.trade_path(temp.path());
    let second_final = second_partial.with_file_name("trades.arrow");
    assert!(!second_partial.exists());
    assert_eq!(count_rows(&second_final), 1);
}

#[tokio::test]
async fn row_and_time_thresholds_flush_complete_batches() {
    let by_size = TestDir::new();
    let event = trade(HOUR_02_START, "one");
    let key = PartitionKey::for_event(&event).unwrap();
    let path = key.trade_path(by_size.path());
    let mut router =
        PartitionRouter::open(config(by_size.path(), 2, Duration::from_secs(60))).unwrap();
    router.push(event).await.unwrap();
    let before = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    router.push(trade(HOUR_02_START + 1, "two")).await.unwrap();
    let after = fs::metadata(&path).unwrap().len();
    assert!(after > before);
    router.shutdown().await.unwrap();

    let by_time = TestDir::new();
    let event = trade(HOUR_02_START, "timer");
    let key = PartitionKey::for_event(&event).unwrap();
    let path = key.trade_path(by_time.path());
    let mut router =
        PartitionRouter::open(config(by_time.path(), 100, Duration::from_secs(1))).unwrap();
    router.push(event).await.unwrap();
    let before = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    router
        .flush_due(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(fs::metadata(&path).unwrap().len() > before);
    router.shutdown().await.unwrap();
}

#[tokio::test]
async fn existing_readable_final_is_merged_instead_of_overwritten() {
    let temp = TestDir::new();
    for id in ["one", "two"] {
        let mut router =
            PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(1))).unwrap();
        router.push(trade(HOUR_02_START, id)).await.unwrap();
        router.shutdown().await.unwrap();
    }

    let key = PartitionKey::for_event(&trade(HOUR_02_START, "unused")).unwrap();
    let final_path = key.trade_path(temp.path()).with_file_name("trades.arrow");
    assert_eq!(count_rows(&final_path), 2);
}

#[tokio::test]
async fn reopen_does_not_replay_partial_proven_consumed_by_merge_witness() {
    let temp = TestDir::new();
    for id in ["one", "two"] {
        let mut router =
            PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(1))).unwrap();
        router.push(trade(HOUR_02_START, id)).await.unwrap();
        router.shutdown().await.unwrap();
    }

    let key = PartitionKey::for_event(&trade(HOUR_02_START, "unused")).unwrap();
    let partial = key.trade_path(temp.path());
    let final_path = partial.with_file_name("trades.arrow");
    fs::write(&partial, b"already consumed before interruption").unwrap();
    let witness = final_path.with_file_name("trades.arrow.merge-witness.interrupted.candidate");
    let source = final_path.with_file_name("trades.arrow.merge-witness.interrupted.source");
    fs::copy(&final_path, &witness).unwrap();
    fs::copy(&partial, &source).unwrap();

    let mut resumed =
        PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(1))).unwrap();
    resumed.push(trade(HOUR_02_START, "three")).await.unwrap();
    resumed.shutdown().await.unwrap();

    assert_eq!(count_rows(&final_path), 3);
    assert!(!partial.exists());
    assert!(!witness.exists());
    assert!(!source.exists());
}

#[tokio::test]
async fn stale_witness_never_consumes_a_new_unrelated_partial() {
    let temp = TestDir::new();
    for id in ["one", "two"] {
        let mut router =
            PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(1))).unwrap();
        router.push(trade(HOUR_02_START, id)).await.unwrap();
        router.shutdown().await.unwrap();
    }
    let key = PartitionKey::for_event(&trade(HOUR_02_START, "unused")).unwrap();
    let partial = key.trade_path(temp.path());
    let final_path = partial.with_file_name("trades.arrow");

    let mut interrupted =
        PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(60))).unwrap();
    interrupted
        .push(trade(HOUR_02_START, "three"))
        .await
        .unwrap();
    drop(interrupted);

    let witness = final_path.with_file_name("trades.arrow.merge-witness.stale.candidate");
    let source = final_path.with_file_name("trades.arrow.merge-witness.stale.source");
    fs::copy(&final_path, &witness).unwrap();
    fs::write(&source, b"different previously consumed partial").unwrap();

    let mut resumed =
        PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(1))).unwrap();
    resumed.push(trade(HOUR_02_START, "four")).await.unwrap();
    resumed.shutdown().await.unwrap();

    assert_eq!(count_rows(&final_path), 4);
    assert!(!witness.exists());
    assert!(!source.exists());
}

#[tokio::test]
async fn unreadable_existing_final_is_never_overwritten() {
    let temp = TestDir::new();
    let event = trade(HOUR_02_START, "one");
    let key = PartitionKey::for_event(&event).unwrap();
    let final_path = key.trade_path(temp.path()).with_file_name("trades.arrow");
    fs::create_dir_all(final_path.parent().unwrap()).unwrap();
    fs::write(&final_path, b"not arrow").unwrap();

    let mut router = PartitionRouter::open(config(temp.path(), 1, Duration::from_secs(1))).unwrap();
    router.push(event).await.unwrap();
    assert!(router.shutdown().await.is_err());
    assert_eq!(fs::read(final_path).unwrap(), b"not arrow");
}

#[tokio::test]
async fn writer_creation_errors_are_propagated() {
    let temp = TestDir::new();
    let root_file = temp.path().join("root-is-file");
    fs::write(&root_file, b"x").unwrap();
    let mut router = PartitionRouter::open(config(&root_file, 1, Duration::from_secs(1))).unwrap();
    assert!(router.push(trade(HOUR_02_START, "one")).await.is_err());
}
