use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, SchemaRef};
use thiserror::Error;

static RECOVERY_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecoveryOutcome {
    pub batches_kept: usize,
    pub rows_kept: usize,
    pub bytes_kept: u64,
    pub bytes_rejected: u64,
    pub corrupt_path: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("failed to read partial Arrow stream {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot prove the Arrow IPC header in {path}: {source}")]
    UnrecoverableHeader {
        path: PathBuf,
        #[source]
        source: ArrowError,
    },
    #[error("failed while decoding the valid Arrow prefix in {path}: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: ArrowError,
    },
    #[error("failed to rewrite recovered Arrow stream {path}: {source}")]
    Rewrite {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub fn recover_partial(path: &Path) -> Result<RecoveryOutcome, RecoveryError> {
    let bytes = fs::read(path).map_err(|source| RecoveryError::Read {
        path: path.to_owned(),
        source,
    })?;
    let mut reader =
        StreamReader::try_new(Cursor::new(bytes.as_slice()), None).map_err(|source| {
            RecoveryError::UnrecoverableHeader {
                path: path.to_owned(),
                source,
            }
        })?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    let mut last_valid = reader.get_ref().position() as usize;

    loop {
        match reader.next() {
            Some(Ok(batch)) => {
                last_valid = reader.get_ref().position() as usize;
                batches.push(batch);
            }
            Some(Err(_)) => break,
            None => {
                last_valid = reader.get_ref().position() as usize;
                break;
            }
        }
    }

    if last_valid > bytes.len() {
        return Err(RecoveryError::Decode {
            path: path.to_owned(),
            source: ArrowError::ParseError("reader advanced past the input length".to_owned()),
        });
    }

    let bytes_rejected = bytes.len() - last_valid;
    let corrupt_path = if bytes_rejected == 0 {
        None
    } else {
        let corrupt_path = unique_sibling(path, "corrupt");
        write_new_file(&corrupt_path, &bytes[last_valid..]).map_err(|source| {
            RecoveryError::Rewrite {
                path: path.to_owned(),
                source: Box::new(source),
            }
        })?;
        Some(corrupt_path)
    };

    let replacement = unique_sibling(path, "recovering");
    if let Err(source) = write_stream(&replacement, &schema, &batches) {
        if let Some(corrupt) = &corrupt_path {
            let _ = fs::remove_file(corrupt);
        }
        return Err(RecoveryError::Rewrite {
            path: path.to_owned(),
            source,
        });
    }
    if let Err(source) = replace_file(&replacement, path) {
        let _ = fs::remove_file(&replacement);
        if let Some(corrupt) = &corrupt_path {
            let _ = fs::remove_file(corrupt);
        }
        return Err(RecoveryError::Rewrite {
            path: path.to_owned(),
            source: Box::new(source),
        });
    }

    Ok(RecoveryOutcome {
        batches_kept: batches.len(),
        rows_kept: batches.iter().map(RecordBatch::num_rows).sum(),
        bytes_kept: last_valid as u64,
        bytes_rejected: bytes_rejected as u64,
        corrupt_path,
    })
}

fn write_stream(
    path: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = StreamWriter::try_new_buffered(file, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    writer.get_mut().flush()?;
    Ok(())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(bytes)?;
    writer.flush()
}

fn replace_file(replacement: &Path, target: &Path) -> std::io::Result<()> {
    let backup = unique_sibling(target, "recovery-backup");
    fs::rename(target, &backup)?;
    if let Err(error) = fs::rename(replacement, target) {
        let _ = fs::rename(&backup, target);
        return Err(error);
    }
    fs::remove_file(backup)
}

fn unique_sibling(path: &Path, label: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let id = RECOVERY_ID.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("arrow.partial");
    path.with_file_name(format!("{name}.{stamp}.{id}.{label}"))
}
