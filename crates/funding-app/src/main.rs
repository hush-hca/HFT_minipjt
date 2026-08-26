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
    about = "Public HFT market-data, funding analytics and read-only monitoring"
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
    /// Open the read-only HFT and funding monitor. This command cannot place orders.
    #[cfg(feature = "gui")]
    Gui {
        #[arg(long, default_value = "config/funding.toml", value_name = "PATH")]
        config: PathBuf,
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
        #[cfg(feature = "gui")]
        Command::Gui { config } => {
            let config = FundingConfig::load(&config)?;
            let initial = funding_app::ui::model::UiSnapshot::demo();
            let (publisher, subscriber) =
                funding_app::ui::bridge::ui_snapshot_channel(initial.clone());
            let shutdown = CancellationToken::new();
            let collector =
                Phase2Collector::new(config)?.with_ui_publisher(publisher, initial.clone());
            let collector_shutdown = shutdown.clone();
            let collector_task =
                tokio::spawn(async move { collector.run(collector_shutdown).await });
            let ui_result = funding_app::ui::run_live_gui(initial, subscriber);
            shutdown.cancel();
            match collector_task.await {
                Ok(Ok(report)) => tracing::info!(
                    status = ?report.status,
                    "collector stopped after GUI shutdown"
                ),
                Ok(Err(error)) => tracing::error!(%error, "collector stopped with an error"),
                Err(error) => tracing::error!(%error, "collector task panicked"),
            }
            ui_result?;
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
