use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::AdapterSnapshot;

static REPORT_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct MissingMarkets {
    pub adapter: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct RecoveryRecord {
    pub path: PathBuf,
    pub batches_kept: usize,
    pub rows_kept: usize,
    pub bytes_kept: u64,
    pub bytes_rejected: u64,
    pub corrupt_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunReport {
    pub status: String,
    pub started_at_us: i64,
    pub ended_at_us: i64,
    pub duration_us: u64,
    pub adapters: Vec<AdapterSnapshot>,
    pub missing_markets: Vec<MissingMarkets>,
    pub recovery: Vec<RecoveryRecord>,
    pub clock_note: String,
}

impl RunReport {
    pub fn empty(started_at_us: i64, ended_at_us: i64) -> Self {
        Self {
            status: "completed".to_owned(),
            started_at_us,
            ended_at_us,
            duration_us: ended_at_us.saturating_sub(started_at_us).max(0) as u64,
            adapters: Vec::new(),
            missing_markets: Vec::new(),
            recovery: Vec::new(),
            clock_note: "timestamps use the operating-system wall clock at microsecond representation; this does not imply microsecond clock accuracy".to_owned(),
        }
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
        let id = REPORT_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("run-report.json");
        let temporary =
            path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), id));
        let result = (|| -> Result<()> {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .with_context(|| format!("failed to create report {}", temporary.display()))?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer_pretty(&mut writer, self)
                .context("failed to encode run report")?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
            publish_report(&temporary, path, id)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

fn publish_report(temporary: &Path, target: &Path, id: u64) -> Result<()> {
    if !target.exists() {
        return fs::rename(temporary, target).with_context(|| {
            format!(
                "failed to atomically publish report {} as {}",
                temporary.display(),
                target.display()
            )
        });
    }

    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("run-report.json");
    let backup = target.with_file_name(format!(
        ".{file_name}.{}.{}.previous",
        std::process::id(),
        id
    ));
    fs::rename(target, &backup)
        .with_context(|| format!("failed to stage existing report {}", target.display()))?;
    if let Err(error) = fs::rename(temporary, target) {
        let _ = fs::rename(&backup, target);
        return Err(error).with_context(|| {
            format!(
                "failed to replace report {} with {}",
                target.display(),
                temporary.display()
            )
        });
    }
    fs::remove_file(&backup)
        .with_context(|| format!("failed to remove old report backup {}", backup.display()))?;
    Ok(())
}
