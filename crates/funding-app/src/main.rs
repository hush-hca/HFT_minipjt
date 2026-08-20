use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use funding_app::Phase2Collector;
use funding_core::config::FundingConfig;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "funding-app",
    version,
    about = "Public derivatives metadata collector (no trading)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Collect public market and derivatives data until Ctrl+C or the optional duration.
    Collect {
        #[arg(long, default_value = "config/funding.toml", value_name = "PATH")]
        config: PathBuf,
        #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
        duration: Option<Duration>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .try_init();
    match Cli::parse().command {
        Command::Collect { config, duration } => {
            let config = FundingConfig::load(&config)?;
            let shutdown = CancellationToken::new();
            let signal = shutdown.clone();
            tokio::spawn(async move {
                match duration {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                }
                signal.cancel();
            });
            let report = Phase2Collector::new(config)?.run(shutdown).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        Err("duration must be greater than zero".to_owned())
    } else {
        Ok(duration)
    }
}
