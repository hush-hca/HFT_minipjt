use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
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
    #[error("failed to resolve an interrupted Arrow publication for {path}: {source}")]
    Publication {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
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
    recover_orphaned_publication(path).map_err(|source| RecoveryError::Publication {
        path: path.to_owned(),
        source,
    })?;
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
    if let Err(source) = publish_arrow(&replacement, path) {
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

/// Publishes a complete Arrow stream while keeping the previous canonical
/// stream recoverable through every modeled process-interruption state.
///
/// Windows cannot atomically rename over an existing file with `std::fs`, so
/// the old target is first preserved as a synced hard link (or a copied
/// fallback). If the process exits after removing the target but before the
/// replacement rename, startup restores the verified backup.
/// This is a process-crash protocol; it does not claim survival of every
/// hardware, controller, or power-loss failure mode on every filesystem.
pub(crate) fn publish_arrow(replacement: &Path, target: &Path) -> std::io::Result<()> {
    verify_complete_arrow(replacement)
        .map_err(|error| io_context("verify replacement", replacement, error))?;
    sync_file(replacement).map_err(|error| io_context("sync replacement", replacement, error))?;
    recover_orphaned_publication_except(target, Some(replacement))
        .map_err(|error| io_context("resolve prior publication", target, error))?;

    if !target.exists() {
        fs::rename(replacement, target)?;
        sync_file(target)?;
        sync_parent(target)?;
        return Ok(());
    }

    // A backup left by an earlier committed generation is harmless while the
    // canonical target exists, but it would make the next remove+rename crash
    // ambiguous. Strictly retire it before entering another destructive gap.
    retire_publication_backups(target)?;

    let backup = publication_backup(target);
    link_or_copy(target, &backup)
        .map_err(|error| io_context("preserve canonical backup", target, error))?;
    sync_file(&backup).map_err(|error| io_context("sync canonical backup", &backup, error))?;
    sync_parent(target).map_err(|error| io_context("sync backup directory", target, error))?;

    if let Err(error) = fs::remove_file(target) {
        let _ = fs::remove_file(&backup);
        return Err(error);
    }
    sync_parent(target)?;

    if let Err(error) = fs::rename(replacement, target) {
        let restore_error = restore_from_backup(&backup, target).err();
        return match restore_error {
            Some(restore_error) => Err(std::io::Error::new(
                error.kind(),
                format!(
                    "failed to publish replacement ({error}); backup restoration also failed ({restore_error})"
                ),
            )),
            None => Err(error),
        };
    }

    if let Err(error) = sync_file(target)
        .and_then(|()| verify_complete_arrow(target))
        .and_then(|()| sync_parent(target))
    {
        // Roll back to the old canonical stream before reporting failure so
        // the caller can safely retain and retry its unconsumed partial.
        let _ = fs::remove_file(target);
        let restore_error = restore_from_backup(&backup, target).err();
        return match restore_error {
            Some(restore_error) => Err(std::io::Error::new(
                error.kind(),
                format!(
                    "new canonical stream could not be verified and synced ({error}); backup restoration also failed ({restore_error})"
                ),
            )),
            None => Err(error),
        };
    }

    // Backup cleanup is idempotent startup housekeeping. Once the verified
    // new target has been verified and synced, a cleanup failure must not
    // make the caller replay the already-published partial a second time.
    let _ = fs::remove_file(&backup);
    let _ = sync_parent(target);
    Ok(())
}

/// Resolves artifacts left by an interrupted publication. A valid canonical
/// stream wins. Otherwise one unambiguous verified backup wins; a verified
/// recovery staging stream is used only when no verified backup exists.
pub(crate) fn recover_orphaned_publication(target: &Path) -> std::io::Result<()> {
    recover_orphaned_publication_except(target, None)
}

fn recover_orphaned_publication_except(
    target: &Path,
    preserve_staging: Option<&Path>,
) -> std::io::Result<()> {
    let backups = publication_backups(target)?;
    let staging = publication_staging(target)?;
    if backups.is_empty() && staging.is_empty() {
        return Ok(());
    }

    if target.exists() && verify_complete_arrow(target).is_ok() {
        remove_best_effort(&backups, None);
        remove_strict_except(&staging, preserve_staging)?;
        sync_parent(target)?;
        return Ok(());
    }

    if backups.is_empty()
        && staging
            .iter()
            .all(|path| preserve_staging == Some(path.as_path()))
    {
        // The caller's freshly verified replacement is the only staging
        // stream. It is about to supersede this incomplete recovery target.
        return Ok(());
    }

    let valid_backups = backups
        .iter()
        .filter(|path| verify_complete_arrow(path).is_ok())
        .cloned()
        .collect::<Vec<_>>();
    if valid_backups.len() > 1 {
        return Err(ambiguous_generation(target, "publication backup"));
    }
    let valid_recovery_staging = staging
        .iter()
        .filter(|path| preserve_staging != Some(path.as_path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".recovering"))
        })
        .filter(|path| verify_complete_arrow(path).is_ok())
        .cloned()
        .collect::<Vec<_>>();
    let recovery_source = if let Some(backup) = valid_backups.first() {
        Some(backup.clone())
    } else {
        if valid_recovery_staging.len() > 1 {
            return Err(ambiguous_generation(target, "recovery staging"));
        }
        valid_recovery_staging.first().cloned()
    };
    let Some(recovery_source) = recovery_source else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "no verified Arrow publication backup or staging stream exists for {}",
                target.display()
            ),
        ));
    };

    if target.exists() {
        fs::remove_file(target)?;
    }
    restore_from_backup(&recovery_source, target)?;
    verify_complete_arrow(target)?;
    sync_file(target)?;
    remove_best_effort(&backups, None);
    remove_strict_except(&staging, preserve_staging)?;
    sync_parent(target)?;
    Ok(())
}

fn ambiguous_generation(target: &Path, artifact: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "multiple verified {artifact} generations exist for {}; refusing mtime-based selection",
            target.display()
        ),
    )
}

/// Creates a verified witness of the exact merged candidate before it is
/// published. The witness lets startup distinguish "candidate not committed"
/// from "candidate committed but source partial not yet deleted".
#[derive(Debug)]
pub(crate) struct MergeWitness {
    candidate: PathBuf,
    source_partial: PathBuf,
}

pub(crate) fn create_merge_witness(
    replacement: &Path,
    target: &Path,
    partial: &Path,
) -> std::io::Result<MergeWitness> {
    verify_complete_arrow(replacement)?;
    let witness = merge_witness(target);
    link_or_copy(replacement, &witness.candidate)?;
    if let Err(error) = link_or_copy(partial, &witness.source_partial) {
        let _ = fs::remove_file(&witness.candidate);
        return Err(error);
    }
    sync_file(&witness.candidate)?;
    sync_file(&witness.source_partial)?;
    sync_parent(target)?;
    Ok(witness)
}

/// Resolves an interrupted merge. A partial is consumed only when a verified
/// candidate matches the canonical stream and its paired source snapshot is
/// byte-identical to the current partial.
pub(crate) fn resolve_merge_witness(target: &Path, partial: &Path) -> std::io::Result<bool> {
    let artifacts = merge_witness_artifacts(target)?;
    if artifacts.is_empty() {
        return Ok(false);
    }

    let mut committed = false;
    if target.exists() && verify_complete_arrow(target).is_ok() {
        for witness in merge_witness_pairs(&artifacts) {
            let source_matches = if partial.exists() {
                witness.source_partial.exists() && files_equal(partial, &witness.source_partial)?
            } else {
                true
            };
            if source_matches
                && verify_complete_arrow(&witness.candidate).is_ok()
                && files_equal(target, &witness.candidate)?
            {
                committed = true;
                break;
            }
        }
    }
    if committed {
        if partial.exists() {
            fs::remove_file(partial)?;
            sync_parent(partial)?;
        }
        remove_strict(&artifacts)?;
        sync_parent(target)?;
        return Ok(true);
    }

    // No candidate+source pair matches, so no current partial was proven
    // consumed. Strictly retire every stale/legacy witness before allowing a
    // new writer to open; otherwise it could affect a later generation.
    remove_strict(&artifacts)?;
    sync_parent(target)?;
    Ok(false)
}

/// Completes the normal post-publication path. The witness is deliberately
/// removed only after the partial deletion has been synced where the platform
/// supports directory syncing; interruption at
/// either point remains resolvable by `resolve_merge_witness`.
pub(crate) fn complete_merge_witness(
    witness: &MergeWitness,
    partial: &Path,
) -> std::io::Result<()> {
    if partial.exists() {
        fs::remove_file(partial)?;
        sync_parent(partial)?;
    }
    remove_strict(&[witness.candidate.clone(), witness.source_partial.clone()])?;
    sync_parent(&witness.candidate)?;
    Ok(())
}

fn restore_from_backup(backup: &Path, target: &Path) -> std::io::Result<()> {
    link_or_copy(backup, target)?;
    sync_file(target)?;
    sync_parent(target)
}

fn link_or_copy(source: &Path, destination: &Path) -> std::io::Result<()> {
    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(_) => {
            let mut source_file = File::open(source)?;
            let mut destination_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)?;
            if let Err(error) = std::io::copy(&mut source_file, &mut destination_file)
                .and_then(|_| destination_file.sync_all())
            {
                drop(destination_file);
                let _ = fs::remove_file(destination);
                return Err(error);
            }
            Ok(())
        }
    }
}

fn publication_backups(target: &Path) -> std::io::Result<Vec<PathBuf>> {
    matching_siblings(target, |name, target_name| {
        matches_labeled_numeric(name, target_name, "publish-backup", 3)
            || matches_labeled_numeric(name, target_name, "merge-backup", 2)
            || matches_numeric_suffix(name, target_name, "recovery-backup", 2)
    })
}

fn publication_staging(target: &Path) -> std::io::Result<Vec<PathBuf>> {
    matching_siblings(target, |name, target_name| {
        matches_labeled_numeric(name, target_name, "merge", 2)
            || matches_numeric_suffix(name, target_name, "recovering", 2)
    })
}

fn matches_labeled_numeric(
    name: &str,
    target_name: &str,
    label: &str,
    component_count: usize,
) -> bool {
    let prefix = format!("{target_name}.{label}.");
    name.strip_prefix(&prefix)
        .is_some_and(|rest| exact_numeric_components(rest, component_count))
}

fn matches_numeric_suffix(
    name: &str,
    target_name: &str,
    suffix: &str,
    component_count: usize,
) -> bool {
    let prefix = format!("{target_name}.");
    let suffix = format!(".{suffix}");
    name.strip_prefix(&prefix)
        .and_then(|rest| rest.strip_suffix(&suffix))
        .is_some_and(|middle| exact_numeric_components(middle, component_count))
}

fn exact_numeric_components(value: &str, expected: usize) -> bool {
    let mut components = value.split('.');
    (0..expected).all(|_| {
        components.next().is_some_and(|component| {
            !component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit())
        })
    }) && components.next().is_none()
}

fn retire_publication_backups(target: &Path) -> std::io::Result<()> {
    let backups = publication_backups(target)?;
    remove_strict(&backups)?;
    sync_parent(target)
}

fn merge_witness_artifacts(target: &Path) -> std::io::Result<Vec<PathBuf>> {
    matching_siblings(target, |name, target_name| {
        name.starts_with(&format!("{target_name}.merge-witness."))
    })
}

fn merge_witness_pairs(artifacts: &[PathBuf]) -> Vec<MergeWitness> {
    artifacts
        .iter()
        .filter_map(|candidate| {
            let name = candidate.file_name()?.to_str()?;
            let stem = name.strip_suffix(".candidate")?;
            Some(MergeWitness {
                candidate: candidate.clone(),
                source_partial: candidate.with_file_name(format!("{stem}.source")),
            })
        })
        .collect()
}

fn matching_siblings(
    target: &Path,
    matches: impl Fn(&str, &str) -> bool,
) -> std::io::Result<Vec<PathBuf>> {
    let Some(parent) = target.parent() else {
        return Ok(Vec::new());
    };
    if !parent.exists() {
        return Ok(Vec::new());
    }
    let Some(target_name) = target.file_name().and_then(|name| name.to_str()) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Arrow target has no UTF-8 file name: {}", target.display()),
        ));
    };
    let mut paths = Vec::new();
    for entry in fs::read_dir(parent)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if matches(name, target_name) {
            paths.push(path);
        }
    }
    // Ordering is deterministic only; publication authority is established by
    // exact artifact binding and verification, never by filesystem timestamps.
    paths.sort();
    Ok(paths)
}

fn remove_best_effort(paths: &[PathBuf], preserve: Option<&Path>) {
    for path in paths {
        if preserve != Some(path.as_path()) {
            let _ = fs::remove_file(path);
        }
    }
}

fn remove_strict(paths: &[PathBuf]) -> std::io::Result<()> {
    for path in paths {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn remove_strict_except(paths: &[PathBuf], preserve: Option<&Path>) -> std::io::Result<()> {
    for path in paths {
        if preserve != Some(path.as_path()) {
            remove_strict(std::slice::from_ref(path))?;
        }
    }
    Ok(())
}

fn files_equal(left: &Path, right: &Path) -> std::io::Result<bool> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = BufReader::new(File::open(left)?);
    let mut right = BufReader::new(File::open(right)?);
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_read = left.read(&mut left_buffer)?;
        let right_read = right.read(&mut right_buffer)?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn verify_complete_arrow(path: &Path) -> std::io::Result<()> {
    let mut tail = File::open(path)?;
    if tail.metadata()?.len() < 8 {
        return Err(invalid_arrow(path, "stream is shorter than its terminator"));
    }
    tail.seek(SeekFrom::End(-8))?;
    let mut marker = [0_u8; 8];
    tail.read_exact(&mut marker)?;
    if marker != [255, 255, 255, 255, 0, 0, 0, 0] {
        return Err(invalid_arrow(
            path,
            "stream has no exact end-of-stream marker",
        ));
    }

    let file = File::open(path)?;
    let length = file.metadata()?.len();
    let mut reader = StreamReader::try_new(BufReader::new(file), None)
        .map_err(|error| invalid_arrow(path, error))?;
    for batch in reader.by_ref() {
        batch.map_err(|error| invalid_arrow(path, error))?;
    }
    let position = reader.get_mut().stream_position()?;
    if position != length {
        return Err(invalid_arrow(path, "stream contains trailing bytes"));
    }
    Ok(())
}

fn invalid_arrow(path: &Path, error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("invalid Arrow stream {}: {error}", path.display()),
    )
}

fn io_context(action: &str, path: &Path, error: std::io::Error) -> std::io::Error {
    std::io::Error::new(
        error.kind(),
        format!("{action} for {} failed: {error}", path.display()),
    )
}

fn sync_file(path: &Path) -> std::io::Result<()> {
    OpenOptions::new().write(true).open(path)?.sync_all()
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(parent) => File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

#[cfg(not(unix))]
fn sync_parent(_: &Path) -> std::io::Result<()> {
    Ok(())
}

fn publication_backup(path: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = RECOVERY_ID.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("arrow");
    path.with_file_name(format!(
        "{name}.publish-backup.{stamp}.{}.{}",
        std::process::id(),
        id
    ))
}

fn merge_witness(path: &Path) -> MergeWitness {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = RECOVERY_ID.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("arrow");
    let stem = format!("{name}.merge-witness.{stamp}.{}.{}", std::process::id(), id);
    MergeWitness {
        candidate: path.with_file_name(format!("{stem}.candidate")),
        source_partial: path.with_file_name(format!("{stem}.source")),
    }
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

#[cfg(test)]
mod publication_tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn restores_verified_backup_when_canonical_name_was_removed() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        write_test_stream(&target, 7);
        let backup = publication_backup(&target);
        fs::hard_link(&target, &backup).unwrap();
        fs::remove_file(&target).unwrap();

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 7);
        assert!(!backup.exists());
    }

    #[test]
    fn valid_new_canonical_wins_over_orphaned_old_backup() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        write_test_stream(&target, 7);
        let backup = publication_backup(&target);
        fs::hard_link(&target, &backup).unwrap();
        fs::remove_file(&target).unwrap();
        write_test_stream(&target, 11);

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 11);
        assert!(!backup.exists());
    }

    #[test]
    fn invalid_new_canonical_is_replaced_by_verified_old_backup() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        write_test_stream(&target, 7);
        let backup = publication_backup(&target);
        fs::hard_link(&target, &backup).unwrap();
        fs::remove_file(&target).unwrap();
        fs::write(&target, b"interrupted replacement").unwrap();

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 7);
        assert!(!backup.exists());
    }

    #[test]
    fn missing_canonical_with_invalid_backup_fails_without_deleting_evidence() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        let backup = publication_backup(&target);
        fs::write(&backup, b"not arrow").unwrap();

        let error = recover_orphaned_publication(&target).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(!target.exists());
        assert!(backup.exists());
    }

    #[test]
    fn publish_keeps_new_stream_and_removes_backup_after_verification() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        let replacement = root.path().join("trades.arrow.merge");
        write_test_stream(&target, 7);
        write_test_stream(&replacement, 11);

        publish_arrow(&replacement, &target).unwrap();

        assert_eq!(read_test_value(&target), 11);
        assert!(!replacement.exists());
        assert!(publication_backups(&target).unwrap().is_empty());
    }

    #[test]
    fn committed_merge_witness_prevents_partial_replay_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        let partial = root.path().join("trades.arrow.partial");
        let replacement = root.path().join("trades.arrow.merge.1");
        write_test_stream(&target, 7);
        write_test_stream(&partial, 11);
        write_test_stream(&replacement, 18);
        let witness = create_merge_witness(&replacement, &target, &partial).unwrap();
        publish_arrow(&replacement, &target).unwrap();

        assert!(resolve_merge_witness(&target, &partial).unwrap());

        assert_eq!(read_test_value(&target), 18);
        assert!(!partial.exists());
        assert!(!witness.candidate.exists());
        assert!(!witness.source_partial.exists());
    }

    #[test]
    fn uncommitted_merge_witness_keeps_partial_for_retry() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        let partial = root.path().join("trades.arrow.partial");
        let replacement = root.path().join("trades.arrow.merge.1");
        write_test_stream(&target, 7);
        write_test_stream(&partial, 11);
        write_test_stream(&replacement, 18);
        let witness = create_merge_witness(&replacement, &target, &partial).unwrap();

        assert!(!resolve_merge_witness(&target, &partial).unwrap());

        assert_eq!(read_test_value(&target), 7);
        assert!(partial.exists());
        assert!(!witness.candidate.exists());
        assert!(!witness.source_partial.exists());
    }

    #[test]
    fn committed_witness_never_consumes_a_different_partial() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        let partial = root.path().join("trades.arrow.partial");
        let replacement = root.path().join("trades.arrow.merge.1");
        write_test_stream(&target, 7);
        write_test_stream(&partial, 11);
        write_test_stream(&replacement, 18);
        let witness = create_merge_witness(&replacement, &target, &partial).unwrap();
        publish_arrow(&replacement, &target).unwrap();
        fs::remove_file(&partial).unwrap();
        write_test_stream(&partial, 23);

        assert!(!resolve_merge_witness(&target, &partial).unwrap());

        assert_eq!(read_test_value(&partial), 23);
        assert!(!witness.candidate.exists());
        assert!(!witness.source_partial.exists());
    }

    #[test]
    fn valid_recovery_staging_wins_when_only_backup_is_incomplete() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow.partial");
        fs::write(&target, b"incomplete original").unwrap();
        let backup = publication_backup(&target);
        fs::hard_link(&target, &backup).unwrap();
        fs::remove_file(&target).unwrap();
        let staging = unique_sibling(&target, "recovering");
        write_test_stream(&staging, 23);

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 23);
        assert!(!backup.exists());
        assert!(!staging.exists());
    }

    #[test]
    fn verified_backup_wins_over_unbound_newer_recovery_staging() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow.partial");
        write_test_stream(&target, 7);
        let backup = publication_backup(&target);
        fs::hard_link(&target, &backup).unwrap();
        fs::remove_file(&target).unwrap();
        let staging = unique_sibling(&target, "recovering");
        write_test_stream(&staging, 23);

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 7);
        assert!(!backup.exists());
        assert!(!staging.exists());
    }

    #[test]
    fn one_verified_backup_wins_over_multiple_recovery_stages() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow.partial");
        write_test_stream(&target, 7);
        let backup = publication_backup(&target);
        fs::hard_link(&target, &backup).unwrap();
        fs::remove_file(&target).unwrap();
        let first_staging = unique_sibling(&target, "recovering");
        let second_staging = unique_sibling(&target, "recovering");
        write_test_stream(&first_staging, 23);
        write_test_stream(&second_staging, 29);

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 7);
        assert!(!backup.exists());
        assert!(!first_staging.exists());
        assert!(!second_staging.exists());
    }

    #[test]
    fn stale_backup_is_strictly_retired_before_the_next_crash_window() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        write_test_stream(&target, 7);
        let stale_backup = publication_backup(&target);
        write_test_stream(&stale_backup, 3);

        retire_publication_backups(&target).unwrap();
        let current_backup = publication_backup(&target);
        fs::hard_link(&target, &current_backup).unwrap();
        fs::remove_file(&target).unwrap();

        recover_orphaned_publication(&target).unwrap();

        assert_eq!(read_test_value(&target), 7);
        assert!(!stale_backup.exists());
        assert!(!current_backup.exists());
    }

    #[test]
    fn multiple_verified_backup_generations_are_preserved_as_ambiguous() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("trades.arrow");
        write_test_stream(&target, 7);
        let first = publication_backup(&target);
        let second = publication_backup(&target);
        fs::hard_link(&target, &first).unwrap();
        fs::hard_link(&target, &second).unwrap();
        fs::remove_file(&target).unwrap();

        let error = recover_orphaned_publication(&target).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("refusing mtime-based selection"));
        assert!(first.exists());
        assert!(second.exists());
        assert!(!target.exists());
    }

    #[test]
    fn missing_final_never_claims_its_partial_targets_recovery_stage() {
        for (final_name, partial_name) in [
            ("trades.arrow", "trades.arrow.partial"),
            ("mark_index.arrow", "mark_index.arrow.partial"),
        ] {
            let root = tempfile::tempdir().unwrap();
            let final_path = root.path().join(final_name);
            let partial = root.path().join(partial_name);
            let partial_staging = unique_sibling(&partial, "recovering");
            write_test_stream(&partial_staging, 31);

            recover_orphaned_publication(&final_path).unwrap();

            assert!(!final_path.exists());
            assert!(partial_staging.exists());
            recover_orphaned_publication(&partial).unwrap();
            assert_eq!(read_test_value(&partial), 31);
        }
    }

    #[test]
    fn valid_final_never_deletes_its_partial_targets_recovery_stage() {
        for (final_name, partial_name) in [
            ("trades.arrow", "trades.arrow.partial"),
            ("mark_index.arrow", "mark_index.arrow.partial"),
        ] {
            let root = tempfile::tempdir().unwrap();
            let final_path = root.path().join(final_name);
            let partial = root.path().join(partial_name);
            write_test_stream(&final_path, 7);
            let partial_staging = unique_sibling(&partial, "recovering");
            write_test_stream(&partial_staging, 31);

            recover_orphaned_publication(&final_path).unwrap();

            assert_eq!(read_test_value(&final_path), 7);
            assert!(partial_staging.exists());
            recover_orphaned_publication(&partial).unwrap();
            assert_eq!(read_test_value(&partial), 31);
        }
    }

    fn write_test_stream(path: &Path, value: i64) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        let mut writer = StreamWriter::try_new_buffered(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        writer.get_mut().flush().unwrap();
        writer.get_mut().get_ref().sync_all().unwrap();
    }

    fn read_test_value(path: &Path) -> i64 {
        let mut reader =
            StreamReader::try_new(BufReader::new(File::open(path).unwrap()), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }
}
