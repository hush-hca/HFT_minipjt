use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use chrono::{DateTime, Datelike, Timelike, Utc};
use md_core::model::{AdapterId, CanonicalSymbol, NormalizedEvent};

use crate::{BookBatchBuilder, SchemaContext, StorageError, TradeBatchBuilder, recover_partial};

static FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub output_root: PathBuf,
    pub batch_rows: usize,
    pub flush_interval: Duration,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PartitionKey {
    adapter: AdapterId,
    symbol: CanonicalSymbol,
    utc_hour: DateTime<Utc>,
}

impl PartitionKey {
    pub fn for_event(event: &NormalizedEvent) -> Result<Self, StorageError> {
        let meta = event.meta();
        Self::from_parts(
            meta.adapter,
            &meta.symbol.base,
            &meta.symbol.quote,
            meta.local_recv_ts_us,
        )
    }

    pub fn from_parts(
        adapter: AdapterId,
        base: &str,
        quote: &str,
        timestamp_us: i64,
    ) -> Result<Self, StorageError> {
        validate_component(base)?;
        validate_component(quote)?;
        let timestamp = DateTime::<Utc>::from_timestamp_micros(timestamp_us).ok_or(
            StorageError::InvalidPartitionTimestamp {
                value: timestamp_us,
            },
        )?;
        let utc_hour = timestamp
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .ok_or(StorageError::InvalidPartitionTimestamp {
                value: timestamp_us,
            })?;
        Ok(Self {
            adapter,
            symbol: CanonicalSymbol::new(base, quote),
            utc_hour,
        })
    }

    pub fn book_path(&self, root: &Path) -> PathBuf {
        self.partition_dir(root).join("books.arrow.partial")
    }

    pub fn trade_path(&self, root: &Path) -> PathBuf {
        self.partition_dir(root).join("trades.arrow.partial")
    }

    fn partition_dir(&self, root: &Path) -> PathBuf {
        let (exchange, market) = adapter_path(self.adapter);
        root.join(exchange)
            .join(market)
            .join(format!("{}-{}", self.symbol.base, self.symbol.quote))
            .join(format!(
                "{:04}-{:02}-{:02}",
                self.utc_hour.year(),
                self.utc_hour.month(),
                self.utc_hour.day()
            ))
            .join(format!("{:02}", self.utc_hour.hour()))
    }

    fn context(&self) -> SchemaContext {
        SchemaContext {
            adapter: self.adapter,
            symbol: self.symbol.clone(),
            utc_hour: self.utc_hour,
        }
    }

    fn same_feed(&self, other: &Self) -> bool {
        self.adapter == other.adapter && self.symbol == other.symbol
    }
}

pub struct PartitionRouter {
    config: StorageConfig,
    writers: HashMap<PartitionKey, PartitionWriters>,
}

impl PartitionRouter {
    pub fn open(config: StorageConfig) -> Result<Self, StorageError> {
        if config.batch_rows == 0 {
            return Err(StorageError::InvalidBatchRows);
        }
        if config.flush_interval.is_zero() {
            return Err(StorageError::InvalidFlushInterval);
        }
        Ok(Self {
            config,
            writers: HashMap::new(),
        })
    }

    pub async fn push(&mut self, event: NormalizedEvent) -> Result<(), StorageError> {
        let key = PartitionKey::for_event(&event)?;
        self.rotate_conflicts(&key)?;
        let root = self.config.output_root.clone();
        let batch_rows = self.config.batch_rows;
        let writers = self
            .writers
            .entry(key.clone())
            .or_insert_with(|| PartitionWriters::new(key));

        match event {
            NormalizedEvent::Book(book) => {
                if writers.books.is_none() {
                    writers.books = Some(BookWriter::open(&writers.key, &root)?);
                }
                let writer = writers.books.as_mut().expect("book writer was inserted");
                writer.builder.push(&book)?;
                if writer.builder.len() >= batch_rows {
                    writer.flush(Instant::now())?;
                }
            }
            NormalizedEvent::Trade(trade) => {
                if writers.trades.is_none() {
                    writers.trades = Some(TradeWriter::open(&writers.key, &root)?);
                }
                let writer = writers.trades.as_mut().expect("trade writer was inserted");
                writer.builder.push(&trade)?;
                if writer.builder.len() >= batch_rows {
                    writer.flush(Instant::now())?;
                }
            }
        }
        Ok(())
    }

    pub async fn flush_due(&mut self, now: Instant) -> Result<(), StorageError> {
        for writers in self.writers.values_mut() {
            if let Some(writer) = &mut writers.books
                && !writer.builder.is_empty()
                && now.saturating_duration_since(writer.last_flush) >= self.config.flush_interval
            {
                writer.flush(now)?;
            }
            if let Some(writer) = &mut writers.trades
                && !writer.builder.is_empty()
                && now.saturating_duration_since(writer.last_flush) >= self.config.flush_interval
            {
                writer.flush(now)?;
            }
        }
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), StorageError> {
        let writers = std::mem::take(&mut self.writers);
        for (_, writer) in writers {
            writer.finalize()?;
        }
        Ok(())
    }

    fn rotate_conflicts(&mut self, incoming: &PartitionKey) -> Result<(), StorageError> {
        let old_keys: Vec<_> = self
            .writers
            .keys()
            .filter(|key| key.same_feed(incoming) && *key != incoming)
            .cloned()
            .collect();
        for key in old_keys {
            if let Some(writer) = self.writers.remove(&key) {
                writer.finalize()?;
            }
        }
        Ok(())
    }
}

struct PartitionWriters {
    key: PartitionKey,
    books: Option<BookWriter>,
    trades: Option<TradeWriter>,
}

impl PartitionWriters {
    fn new(key: PartitionKey) -> Self {
        Self {
            key,
            books: None,
            trades: None,
        }
    }

    fn finalize(self) -> Result<(), StorageError> {
        if let Some(writer) = self.books {
            writer.finalize()?;
        }
        if let Some(writer) = self.trades {
            writer.finalize()?;
        }
        Ok(())
    }
}

struct BookWriter {
    builder: BookBatchBuilder,
    writer: StreamWriter<BufWriter<File>>,
    partial_path: PathBuf,
    last_flush: Instant,
}

impl BookWriter {
    fn open(key: &PartitionKey, root: &Path) -> Result<Self, StorageError> {
        let partial_path = key.book_path(root);
        prepare_partial(&partial_path)?;
        let writer = open_stream(&partial_path, &crate::book_schema(&key.context()))?;
        Ok(Self {
            builder: BookBatchBuilder::new(key.context()),
            writer,
            partial_path,
            last_flush: Instant::now(),
        })
    }

    fn flush(&mut self, now: Instant) -> Result<(), StorageError> {
        if !self.builder.is_empty() {
            self.writer.write(&self.builder.finish()?)?;
            self.writer.get_mut().flush()?;
        }
        self.last_flush = now;
        Ok(())
    }

    fn finalize(mut self) -> Result<(), StorageError> {
        self.flush(Instant::now())?;
        self.writer.finish()?;
        self.writer.get_mut().flush()?;
        drop(self.writer);
        finalize_partial(&self.partial_path)
    }
}

struct TradeWriter {
    builder: TradeBatchBuilder,
    writer: StreamWriter<BufWriter<File>>,
    partial_path: PathBuf,
    last_flush: Instant,
}

impl TradeWriter {
    fn open(key: &PartitionKey, root: &Path) -> Result<Self, StorageError> {
        let partial_path = key.trade_path(root);
        prepare_partial(&partial_path)?;
        let writer = open_stream(&partial_path, &crate::trade_schema(&key.context()))?;
        Ok(Self {
            builder: TradeBatchBuilder::new(key.context()),
            writer,
            partial_path,
            last_flush: Instant::now(),
        })
    }

    fn flush(&mut self, now: Instant) -> Result<(), StorageError> {
        if !self.builder.is_empty() {
            self.writer.write(&self.builder.finish()?)?;
            self.writer.get_mut().flush()?;
        }
        self.last_flush = now;
        Ok(())
    }

    fn finalize(mut self) -> Result<(), StorageError> {
        self.flush(Instant::now())?;
        self.writer.finish()?;
        self.writer.get_mut().flush()?;
        drop(self.writer);
        finalize_partial(&self.partial_path)
    }
}

fn open_stream(
    partial_path: &Path,
    schema: &SchemaRef,
) -> Result<StreamWriter<BufWriter<File>>, StorageError> {
    if let Some(parent) = partial_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(partial_path)?;
    let mut writer = StreamWriter::try_new_buffered(file, schema)?;
    writer.get_mut().flush()?;
    Ok(writer)
}

fn prepare_partial(partial_path: &Path) -> Result<(), StorageError> {
    if partial_path.exists() {
        recover_partial(partial_path)?;
        finalize_partial(partial_path)?;
    }
    Ok(())
}

fn finalize_partial(partial_path: &Path) -> Result<(), StorageError> {
    let final_path = final_path(partial_path)?;
    if !final_path.exists() {
        fs::rename(partial_path, final_path)?;
        return Ok(());
    }

    let (final_schema, mut batches) =
        read_stream(&final_path).map_err(|error| StorageError::UnreadableFinal {
            path: final_path.clone(),
            message: error.to_string(),
        })?;
    let (partial_schema, partial_batches) = read_stream(partial_path)?;
    if final_schema != partial_schema {
        return Err(StorageError::MergeSchemaMismatch { path: final_path });
    }
    batches.extend(partial_batches);

    let replacement = unique_sibling(&final_path, "merge");
    write_stream(&replacement, &final_schema, &batches)?;
    if let Err(error) = replace_file(&replacement, &final_path) {
        let _ = fs::remove_file(&replacement);
        return Err(error.into());
    }
    fs::remove_file(partial_path)?;
    Ok(())
}

fn read_stream(path: &Path) -> Result<(SchemaRef, Vec<RecordBatch>), StorageError> {
    let file = File::open(path)?;
    let reader = StreamReader::try_new(BufReader::new(file), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

fn write_stream(
    path: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(), StorageError> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = StreamWriter::try_new_buffered(file, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    writer.get_mut().flush()?;
    Ok(())
}

fn replace_file(replacement: &Path, target: &Path) -> std::io::Result<()> {
    let backup = unique_sibling(target, "merge-backup");
    fs::rename(target, &backup)?;
    if let Err(error) = fs::rename(replacement, target) {
        let _ = fs::rename(&backup, target);
        return Err(error);
    }
    fs::remove_file(backup)
}

fn final_path(partial_path: &Path) -> Result<PathBuf, StorageError> {
    let name = partial_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| StorageError::InvalidPartitionComponent {
            component: partial_path.display().to_string(),
        })?;
    let final_name =
        name.strip_suffix(".partial")
            .ok_or_else(|| StorageError::InvalidPartitionComponent {
                component: name.to_owned(),
            })?;
    Ok(partial_path.with_file_name(final_name))
}

fn unique_sibling(path: &Path, label: &str) -> PathBuf {
    let id = FILE_ID.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("arrow");
    path.with_file_name(format!("{name}.{label}.{}.{}", std::process::id(), id))
}

fn validate_component(value: &str) -> Result<(), StorageError> {
    let valid = !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidPartitionComponent {
            component: value.to_owned(),
        })
    }
}

fn adapter_path(adapter: AdapterId) -> (&'static str, &'static str) {
    match adapter {
        AdapterId::UpbitSpot => ("upbit", "spot"),
        AdapterId::BithumbSpot => ("bithumb", "spot"),
        AdapterId::BinanceSpot => ("binance", "spot"),
        AdapterId::BinanceUsdm => ("binance", "usdm_futures"),
        AdapterId::BybitLinear => ("bybit", "linear_futures"),
    }
}
