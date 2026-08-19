use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use collector::{CollectorApp, RunReport};
use md_core::config::CollectorConfig;
use md_storage::{ValidationReport, validate_path};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "collector",
    version,
    about = "Multi-exchange Arrow market-data collector"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Collect continuously until Ctrl+C.
    Collect(CollectArgs),
    /// Validate one finalized Arrow stream or a dataset tree.
    Validate(ValidateArgs),
    /// Run a bounded collection into an isolated directory and validate it.
    Smoke(SmokeArgs),
}

#[derive(Debug, Args)]
struct CollectArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long)]
    strict_symbols: bool,
}

#[derive(Debug, Args)]
struct ValidateArgs {
    #[arg(long, value_name = "FILE_OR_ROOT")]
    path: PathBuf,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct SmokeArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    duration: Duration,
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct SmokeReport {
    status: &'static str,
    requested_duration_ms: u64,
    output_root: PathBuf,
    health_errors: Vec<String>,
    high_volume_btc_seen: bool,
    run: RunReport,
    validation: ValidationReport,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    match Cli::parse().command {
        Command::Collect(args) => collect(args).await,
        Command::Validate(args) => validate(args),
        Command::Smoke(args) => smoke(args).await,
    }
}

async fn collect(args: CollectArgs) -> Result<()> {
    let mut config = CollectorConfig::load(&args.config)?;
    config.strict_symbols |= args.strict_symbols;
    let shutdown = CancellationToken::new();
    let ctrl_c_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            ctrl_c_shutdown.cancel();
        }
    });
    let report = CollectorApp::new(config)?.run(shutdown).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn validate(args: ValidateArgs) -> Result<()> {
    let report = validate_path(&args.path)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if report.errors.is_empty() {
        println!(
            "validated {} files, {} batches, {} rows",
            report.files, report.batches, report.rows
        );
    } else {
        for error in &report.errors {
            eprintln!("{} {}: {}", error.code, error.path.display(), error.message);
        }
    }
    if !report.is_valid() {
        bail!(
            "dataset validation failed with {} issue(s)",
            report.errors.len()
        );
    }
    Ok(())
}

async fn smoke(args: SmokeArgs) -> Result<()> {
    let mut config = CollectorConfig::load(&args.config)?;
    let output_root = args
        .output
        .unwrap_or_else(|| default_smoke_root(&config.output_root));
    ensure_isolated(&output_root)?;
    config.output_root = output_root.clone();

    let shutdown = CancellationToken::new();
    let app = CollectorApp::new(config)?;
    let running = tokio::spawn(app.run(shutdown.clone()));
    tokio::time::sleep(args.duration).await;
    shutdown.cancel();
    let run = running.await.context("collector smoke task panicked")??;
    let validation = validate_path(&output_root)?;
    let high_volume_btc_seen = btc_has_books_and_trades(&output_root);
    let mut health_errors = Vec::new();
    for adapter in &run.adapters {
        if adapter.parse_errors != 0 {
            health_errors.push(format!(
                "{} reported {} parse errors",
                adapter.adapter, adapter.parse_errors
            ));
        }
        if adapter.rejected_events != 0 {
            health_errors.push(format!(
                "{} rejected {} events",
                adapter.adapter, adapter.rejected_events
            ));
        }
        if adapter.backpressure_disconnects != 0 {
            health_errors.push(format!(
                "{} had {} backpressure disconnects",
                adapter.adapter, adapter.backpressure_disconnects
            ));
        }
    }
    if !validation.is_valid() {
        health_errors.push(format!(
            "dataset validation found {} issue(s)",
            validation.errors.len()
        ));
    }
    if !high_volume_btc_seen {
        health_errors.push("no finalized BTC book/trade pair was collected".to_owned());
    }

    let report = SmokeReport {
        status: if health_errors.is_empty() {
            "passed"
        } else {
            "failed"
        },
        requested_duration_ms: u64::try_from(args.duration.as_millis()).unwrap_or(u64::MAX),
        output_root: output_root.clone(),
        health_errors,
        high_volume_btc_seen,
        run,
        validation,
    };
    write_json(&output_root.join("smoke-report.json"), &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.status != "passed" {
        bail!("smoke test failed: {}", report.health_errors.join("; "));
    }
    Ok(())
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(duration)
}

fn default_smoke_root(configured: &Path) -> PathBuf {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_micros())
        .unwrap_or(0);
    configured.join(format!("smoke-{}-{micros}", std::process::id()))
}

fn ensure_isolated(path: &Path) -> Result<()> {
    if path.exists() {
        let mut entries = std::fs::read_dir(path)
            .with_context(|| format!("failed to inspect smoke output {}", path.display()))?;
        if entries.next().transpose()?.is_some() {
            bail!("smoke output must be empty: {}", path.display());
        }
    }
    Ok(())
}

fn btc_has_books_and_trades(root: &Path) -> bool {
    let mut books = std::collections::HashSet::new();
    let mut trades = std::collections::HashSet::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if std::fs::metadata(&path).map_or(true, |meta| meta.len() == 0) {
                continue;
            }
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let components = relative
                .iter()
                .map(|part| part.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            if components.len() < 6 {
                continue;
            }
            let n = components.len();
            let symbol = &components[n - 4];
            if !symbol
                .strip_prefix("BTC-")
                .is_some_and(|quote| !quote.is_empty())
            {
                continue;
            }
            let identity = (
                components[n - 6].clone(),
                components[n - 5].clone(),
                symbol.clone(),
            );
            match components[n - 1].as_str() {
                "books.arrow" => {
                    books.insert(identity);
                }
                "trades.arrow" => {
                    trades.insert(identity);
                }
                _ => {}
            }
        }
    }
    books.iter().any(|identity| trades.contains(identity))
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec_pretty(value)?;
    std::fs::write(path, bytes)
        .with_context(|| format!("failed to write smoke report {}", path.display()))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btc_evidence_must_share_one_exchange_market_symbol_identity() {
        let root = tempfile::tempdir().unwrap();
        touch(root.path(), "upbit/spot/BTC-KRW/2026-08-20/01/books.arrow");
        touch(
            root.path(),
            "binance/spot/BTC-USDT/2026-08-20/01/trades.arrow",
        );
        assert!(!btc_has_books_and_trades(root.path()));

        touch(root.path(), "upbit/spot/BTC-KRW/2026-08-20/02/trades.arrow");
        assert!(btc_has_books_and_trades(root.path()));
    }

    fn touch(root: &Path, relative: &str) {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"nonempty").unwrap();
    }
}
