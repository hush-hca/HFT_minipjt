use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use chrono::{DateTime, Datelike, Timelike, Utc};
use funding_core::public::DerivativeEvent;
use md_core::model::{AdapterId, CanonicalSymbol};

use crate::derivative_batch::{DerivativeBatchBuilder, family_of};
use crate::derivative_schema::{
    DerivativeEventFamily, DerivativeSchemaContext, derivative_schema, derivative_venue_path,
};
use crate::{StorageConfig, StorageError, recover_partial};

static DERIVATIVE_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DerivativePartitionKey {
    family: DerivativeEventFamily,
    venue: AdapterId,
    symbol: CanonicalSymbol,
    utc_hour: DateTime<Utc>,
}

impl DerivativePartitionKey {
    pub fn for_event(event: &DerivativeEvent) -> Result<Self, StorageError> {
        let meta = event.meta();
        validate_component(&meta.symbol.base)?;
        validate_component(&meta.symbol.quote)?;
        let timestamp_us = event.partition_ts_us();
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
            family: family_of(event),
            venue: meta.venue,
            symbol: meta.symbol.clone(),
            utc_hour,
        })
    }

    pub fn partial_path(&self, root: &Path) -> PathBuf {
        let (venue, market) = derivative_venue_path(self.venue);
        let family = self.family.as_str();
        root.join("derivatives")
            .join(family)
            .join(venue)
            .join(market)
            .join(format!("{}-{}", self.symbol.base, self.symbol.quote))
            .join(format!(
                "{:04}-{:02}-{:02}",
                self.utc_hour.year(),
                self.utc_hour.month(),
                self.utc_hour.day()
            ))
            .join(format!("{:02}", self.utc_hour.hour()))
            .join(format!("{family}.arrow.partial"))
    }

    fn context(&self) -> DerivativeSchemaContext {
        DerivativeSchemaContext {
            family: self.family,
            venue: self.venue,
            symbol: self.symbol.clone(),
            utc_hour: self.utc_hour,
        }
    }

    fn same_feed(&self, other: &Self) -> bool {
        self.family == other.family && self.venue == other.venue && self.symbol == other.symbol
    }
}

pub struct DerivativePartitionRouter {
    config: StorageConfig,
    writers: HashMap<DerivativePartitionKey, DerivativeWriter>,
}

impl DerivativePartitionRouter {
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

    pub async fn push(&mut self, event: DerivativeEvent) -> Result<(), StorageError> {
        let key = DerivativePartitionKey::for_event(&event)?;
        self.rotate_conflicts(&key)?;
        if let Some(writer) = self.writers.get_mut(&key) {
            writer.ensure_healthy()?;
            writer.builder.push(event)?;
            if writer.builder.len() >= self.config.batch_rows {
                writer.flush(Instant::now())?;
            }
            return Ok(());
        }

        let writer =
            DerivativeWriter::open_with_event(key.clone(), &self.config.output_root, event)?;
        self.writers.insert(key.clone(), writer);
        if self
            .writers
            .get(&key)
            .is_some_and(|writer| writer.builder.len() >= self.config.batch_rows)
        {
            self.writers
                .get_mut(&key)
                .expect("writer was inserted")
                .flush(Instant::now())?;
        }
        Ok(())
    }

    pub async fn flush_due(&mut self, now: Instant) -> Result<(), StorageError> {
        let mut first_error = None;
        for writer in self.writers.values_mut() {
            if !writer.builder.is_empty()
                && now.saturating_duration_since(writer.last_flush) >= self.config.flush_interval
                && let Err(error) = writer.flush(now)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn shutdown(mut self) -> Result<(), StorageError> {
        let mut first_error = None;
        for (_, writer) in std::mem::take(&mut self.writers) {
            if let Err(error) = writer.finalize()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn rotate_conflicts(&mut self, incoming: &DerivativePartitionKey) -> Result<(), StorageError> {
        let old = self
            .writers
            .keys()
            .filter(|key| key.same_feed(incoming) && *key != incoming)
            .cloned()
            .collect::<Vec<_>>();
        let mut first_error = None;
        for key in old {
            if let Some(writer) = self.writers.remove(&key) {
                if let Err(error) = writer.finalize()
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

struct DerivativeWriter {
    builder: DerivativeBatchBuilder,
    sink: Box<dyn DerivativeSink>,
    partial_path: PathBuf,
    last_flush: Instant,
    poisoned: bool,
}

impl DerivativeWriter {
    fn open_with_event(
        key: DerivativePartitionKey,
        root: &Path,
        event: DerivativeEvent,
    ) -> Result<Self, StorageError> {
        let context = key.context();
        let mut builder = DerivativeBatchBuilder::new(context.clone());
        builder.push(event)?;
        let partial_path = key.partial_path(root);
        prepare_partial(&partial_path)?;
        let sink = Box::new(IpcDerivativeSink::open(
            &partial_path,
            &derivative_schema(&context),
        )?);
        Ok(Self {
            builder,
            sink,
            partial_path,
            last_flush: Instant::now(),
            poisoned: false,
        })
    }

    fn flush(&mut self, now: Instant) -> Result<(), StorageError> {
        self.ensure_healthy()?;
        if !self.builder.is_empty() {
            let batch = self.builder.build()?;
            if let Err(error) = self
                .sink
                .write_batch(&batch)
                .and_then(|()| self.sink.flush())
            {
                self.poisoned = true;
                return Err(error);
            }
            self.builder.commit();
        }
        self.last_flush = now;
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), StorageError> {
        if self.poisoned {
            Err(StorageError::PoisonedDerivativeWriter {
                path: self.partial_path.clone(),
            })
        } else {
            Ok(())
        }
    }

    fn finalize(mut self) -> Result<(), StorageError> {
        self.flush(Instant::now())?;
        if let Err(error) = self.sink.finish().and_then(|()| self.sink.flush()) {
            self.poisoned = true;
            return Err(error);
        }
        drop(self.sink);
        finalize_partial(&self.partial_path)
    }
}

trait DerivativeSink: Send {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), StorageError>;
    fn flush(&mut self) -> Result<(), StorageError>;
    fn finish(&mut self) -> Result<(), StorageError>;
}

struct IpcDerivativeSink {
    writer: StreamWriter<BufWriter<File>>,
}

impl IpcDerivativeSink {
    fn open(path: &Path, schema: &SchemaRef) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = StreamWriter::try_new_buffered(file, schema)?;
        writer.get_mut().flush()?;
        Ok(Self { writer })
    }
}

impl DerivativeSink for IpcDerivativeSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), StorageError> {
        self.writer.write(batch)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.writer.get_mut().flush()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), StorageError> {
        self.writer.finish()?;
        Ok(())
    }
}

fn prepare_partial(path: &Path) -> Result<(), StorageError> {
    if path.exists() {
        recover_partial(path)?;
        finalize_partial(path)?;
    }
    Ok(())
}

fn finalize_partial(partial: &Path) -> Result<(), StorageError> {
    let final_path = partial.with_extension("");
    if !final_path.exists() {
        fs::rename(partial, final_path)?;
        return Ok(());
    }
    let mut final_reader = strict_reader(&final_path, true)?;
    let final_schema = final_reader.schema();
    let mut partial_reader = strict_reader(partial, false)?;
    let partial_schema = partial_reader.schema();
    if final_schema != partial_schema {
        return Err(StorageError::MergeSchemaMismatch { path: final_path });
    }
    let replacement = unique_sibling(&final_path, "merge");
    let merge_result = (|| -> Result<(), StorageError> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&replacement)?;
        let mut writer = StreamWriter::try_new_buffered(file, &final_schema)?;
        copy_stream(&mut final_reader, &mut writer, &final_path, true)?;
        copy_stream(&mut partial_reader, &mut writer, partial, false)?;
        writer.finish()?;
        writer.get_mut().flush()?;
        Ok(())
    })();
    if let Err(error) = merge_result {
        let _ = fs::remove_file(&replacement);
        return Err(error);
    }
    if let Err(error) = replace_file(&replacement, &final_path) {
        let _ = fs::remove_file(&replacement);
        return Err(error.into());
    }
    fs::remove_file(partial)?;
    Ok(())
}

fn strict_reader(
    path: &Path,
    existing_final: bool,
) -> Result<StreamReader<BufReader<File>>, StorageError> {
    if !has_exact_stream_terminator(path)? {
        return Err(stream_read_error(
            path,
            existing_final,
            "missing Arrow end-of-stream marker or trailing bytes",
        ));
    }
    StreamReader::try_new(BufReader::new(File::open(path)?), None)
        .map_err(|error| stream_read_error(path, existing_final, error.to_string()))
}

fn copy_stream(
    reader: &mut StreamReader<BufReader<File>>,
    writer: &mut StreamWriter<BufWriter<File>>,
    path: &Path,
    existing_final: bool,
) -> Result<(), StorageError> {
    for batch in reader.by_ref() {
        let batch =
            batch.map_err(|error| stream_read_error(path, existing_final, error.to_string()))?;
        writer.write(&batch)?;
    }
    let logical_position = reader.get_mut().stream_position()?;
    let file_length = fs::metadata(path)?.len();
    if logical_position != file_length {
        return Err(stream_read_error(
            path,
            existing_final,
            "Arrow stream contains bytes after its end-of-stream marker",
        ));
    }
    Ok(())
}

fn has_exact_stream_terminator(path: &Path) -> Result<bool, StorageError> {
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length < 8 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-8))?;
    let mut marker = [0_u8; 8];
    file.read_exact(&mut marker)?;
    Ok(marker == [255, 255, 255, 255, 0, 0, 0, 0])
}

fn stream_read_error(
    path: &Path,
    existing_final: bool,
    message: impl Into<String>,
) -> StorageError {
    let message = message.into();
    if existing_final {
        StorageError::UnreadableFinal {
            path: path.to_owned(),
            message,
        }
    } else {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        ))
    }
}

fn replace_file(replacement: &Path, target: &Path) -> std::io::Result<()> {
    let backup = unique_sibling(target, "merge-backup");
    fs::rename(target, &backup)?;
    if let Err(error) = fs::rename(replacement, target) {
        let _ = fs::rename(&backup, target);
        return Err(error);
    }
    let _ = fs::remove_file(backup);
    Ok(())
}

fn unique_sibling(path: &Path, label: &str) -> PathBuf {
    let id = DERIVATIVE_FILE_ID.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().and_then(|v| v.to_str()).unwrap_or("arrow");
    path.with_file_name(format!("{name}.{label}.{}.{}", std::process::id(), id))
}

fn validate_component(value: &str) -> Result<(), StorageError> {
    if !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(StorageError::InvalidPartitionComponent {
            component: value.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use funding_core::meta::DerivativeMeta;
    use funding_core::public::{DerivativeEvent, MarkIndexSnapshot};
    use md_core::model::{AdapterId, CanonicalSymbol, TimestampPrecision};
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct SinkState {
        writes: usize,
        flushes: usize,
        finishes: usize,
    }

    struct FailingSink {
        state: Arc<Mutex<SinkState>>,
        fail_write: bool,
        fail_flush: bool,
    }

    struct FailFlushIpcSink {
        inner: IpcDerivativeSink,
    }

    impl DerivativeSink for FailFlushIpcSink {
        fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), StorageError> {
            self.inner.write_batch(batch)
        }

        fn flush(&mut self) -> Result<(), StorageError> {
            Err(injected_error("disk flush"))
        }

        fn finish(&mut self) -> Result<(), StorageError> {
            self.inner.finish()
        }
    }

    impl DerivativeSink for FailingSink {
        fn write_batch(&mut self, _: &RecordBatch) -> Result<(), StorageError> {
            self.state.lock().unwrap().writes += 1;
            if self.fail_write {
                Err(injected_error("write"))
            } else {
                Ok(())
            }
        }

        fn flush(&mut self) -> Result<(), StorageError> {
            self.state.lock().unwrap().flushes += 1;
            if self.fail_flush {
                Err(injected_error("flush"))
            } else {
                Ok(())
            }
        }

        fn finish(&mut self) -> Result<(), StorageError> {
            self.state.lock().unwrap().finishes += 1;
            Ok(())
        }
    }

    #[test]
    fn failed_write_or_flush_retains_pending_rows_and_poisons_retry() {
        for (fail_write, fail_flush) in [(true, false), (false, true)] {
            let state = Arc::new(Mutex::new(SinkState::default()));
            let mut builder = DerivativeBatchBuilder::new(test_context());
            builder.push(test_event()).unwrap();
            let mut writer = DerivativeWriter {
                builder,
                sink: Box::new(FailingSink {
                    state: Arc::clone(&state),
                    fail_write,
                    fail_flush,
                }),
                partial_path: PathBuf::from("injected.arrow.partial"),
                last_flush: Instant::now(),
                poisoned: false,
            };

            assert!(writer.flush(Instant::now()).is_err());
            assert_eq!(writer.builder.len(), 1, "accepted row was cleared");
            assert!(writer.poisoned);
            assert!(matches!(
                writer.flush(Instant::now()),
                Err(StorageError::PoisonedDerivativeWriter { .. })
            ));
            assert_eq!(state.lock().unwrap().writes, 1, "poisoned writer retried");
            assert!(writer.finalize().is_err());
            assert_eq!(
                state.lock().unwrap().finishes,
                0,
                "poisoned writer finalized"
            );
        }
    }

    #[test]
    fn poisoned_ipc_writer_keeps_its_partial_for_recovery() {
        let root = tempfile::tempdir().unwrap();
        let partial_path = root.path().join("mark_index.arrow.partial");
        let context = test_context();
        let mut builder = DerivativeBatchBuilder::new(context.clone());
        builder.push(test_event()).unwrap();
        let sink = IpcDerivativeSink::open(&partial_path, &derivative_schema(&context)).unwrap();
        let mut writer = DerivativeWriter {
            builder,
            sink: Box::new(FailFlushIpcSink { inner: sink }),
            partial_path: partial_path.clone(),
            last_flush: Instant::now(),
            poisoned: false,
        };

        assert!(writer.flush(Instant::now()).is_err());
        assert_eq!(writer.builder.len(), 1);
        assert!(writer.finalize().is_err());
        assert!(partial_path.exists());
        let outcome = recover_partial(&partial_path).unwrap();
        assert_eq!(outcome.rows_kept, 1);
    }

    #[tokio::test]
    async fn flush_due_attempts_every_writer_after_the_first_due_writer_fails() {
        let now = Instant::now();
        let first_key = test_key("BTC");
        let second_key = test_key("ETH");
        let mut writers = HashMap::from([
            (first_key, test_writer("BTC", now)),
            (second_key, test_writer("ETH", now)),
        ]);
        let failing_key = writers.keys().next().expect("two writers").clone();
        let healthy_key = writers
            .keys()
            .find(|key| **key != failing_key)
            .expect("second writer")
            .clone();
        let failing_state = Arc::new(Mutex::new(SinkState::default()));
        let healthy_state = Arc::new(Mutex::new(SinkState::default()));
        writers.get_mut(&failing_key).unwrap().sink = Box::new(FailingSink {
            state: Arc::clone(&failing_state),
            fail_write: false,
            fail_flush: true,
        });
        writers.get_mut(&healthy_key).unwrap().sink = Box::new(FailingSink {
            state: Arc::clone(&healthy_state),
            fail_write: false,
            fail_flush: false,
        });
        let mut router = DerivativePartitionRouter {
            config: StorageConfig {
                output_root: PathBuf::from("unused"),
                batch_rows: 100,
                flush_interval: std::time::Duration::from_secs(1),
            },
            writers,
        };

        assert!(
            router
                .flush_due(now + std::time::Duration::from_secs(2))
                .await
                .is_err()
        );
        assert_eq!(failing_state.lock().unwrap().flushes, 1);
        assert_eq!(healthy_state.lock().unwrap().flushes, 1);
        assert!(router.writers.get(&failing_key).unwrap().poisoned);
        assert!(router.writers.get(&healthy_key).unwrap().builder.is_empty());
    }

    fn test_context() -> DerivativeSchemaContext {
        test_context_for("BTC")
    }

    fn test_context_for(base: &str) -> DerivativeSchemaContext {
        DerivativeSchemaContext {
            family: DerivativeEventFamily::MarkIndex,
            venue: AdapterId::BybitLinear,
            symbol: CanonicalSymbol::new(base, "USDT"),
            utc_hour: DateTime::<Utc>::from_timestamp_micros(1_725_930_000_000_000).unwrap(),
        }
    }

    fn test_event() -> DerivativeEvent {
        test_event_for("BTC")
    }

    fn test_event_for(base: &str) -> DerivativeEvent {
        DerivativeEvent::MarkIndex(MarkIndexSnapshot {
            meta: DerivativeMeta {
                schema_version: 1,
                event_id: Uuid::now_v7(),
                venue: AdapterId::BybitLinear,
                symbol: CanonicalSymbol::new(base, "USDT"),
                venue_symbol: format!("{base}USDT"),
                source_ts_us: Some(1_725_930_000_000_001),
                source_ts_precision: TimestampPrecision::Microsecond,
                local_recv_ts_us: 1_725_930_000_000_002,
            },
            mark_price: 1,
            index_price: 1,
        })
    }

    fn test_key(base: &str) -> DerivativePartitionKey {
        DerivativePartitionKey {
            family: DerivativeEventFamily::MarkIndex,
            venue: AdapterId::BybitLinear,
            symbol: CanonicalSymbol::new(base, "USDT"),
            utc_hour: DateTime::<Utc>::from_timestamp_micros(1_725_930_000_000_000).unwrap(),
        }
    }

    fn test_writer(base: &str, now: Instant) -> DerivativeWriter {
        let mut builder = DerivativeBatchBuilder::new(test_context_for(base));
        builder.push(test_event_for(base)).unwrap();
        DerivativeWriter {
            builder,
            sink: Box::new(FailingSink {
                state: Arc::new(Mutex::new(SinkState::default())),
                fail_write: false,
                fail_flush: false,
            }),
            partial_path: PathBuf::from(format!("{base}.arrow.partial")),
            last_flush: now,
            poisoned: false,
        }
    }

    fn injected_error(operation: &str) -> StorageError {
        StorageError::Io(std::io::Error::other(format!(
            "injected {operation} failure"
        )))
    }
}
