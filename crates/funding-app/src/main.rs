#[cfg(feature = "gui")]
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
#[cfg(feature = "gui")]
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
#[cfg(feature = "gui")]
use collector::CollectorApp;
use funding_app::Phase2Collector;
use funding_core::config::FundingConfig;
#[cfg(feature = "gui")]
use futures_util::FutureExt;
#[cfg(feature = "gui")]
use md_core::config::CollectorConfig;
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
        #[arg(long, default_value = "config/default.toml", value_name = "PATH")]
        market_config: PathBuf,
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
        Command::Gui {
            config,
            market_config,
        } => {
            let config = FundingConfig::load(&config)?;
            let market_config = CollectorConfig::load(&market_config)?;
            let initial = funding_app::ui::model::UiSnapshot::starting();
            let (publisher, subscriber) =
                funding_app::ui::bridge::ui_snapshot_channel(initial.clone());
            let selection =
                funding_app::ui::live::MarketSelection::new("Binance USD-M", "BTC/USDT");
            let ui_state = Arc::new(Mutex::new(funding_app::ui::live::LiveUiState::new(
                publisher,
                initial.clone(),
                selection.clone(),
                config.cost.clone(),
            )));
            let shutdown = CancellationToken::new();
            let funding_status_state = ui_state.clone();
            let market_status_state = ui_state.clone();
            let funding_collector = Phase2Collector::new(config)?.with_ui_state(ui_state.clone());
            let market_collector = CollectorApp::new(market_config)?.with_event_observer(Arc::new(
                funding_app::ui::live::SharedLiveUiObserver::new(ui_state),
            ));
            let funding_shutdown = shutdown.clone();
            let funding_task = tokio::spawn(async move {
                let result = AssertUnwindSafe(funding_collector.run(funding_shutdown))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("funding collector task panicked")));
                set_data_plane_result(
                    &funding_status_state,
                    funding_app::ui::live::DataPlane::Funding,
                    result.is_ok(),
                );
                result
            });
            let market_shutdown = shutdown.clone();
            let market_task = tokio::spawn(async move {
                let result = AssertUnwindSafe(market_collector.run(market_shutdown))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!("core market collector task panicked"))
                    });
                set_data_plane_result(
                    &market_status_state,
                    funding_app::ui::live::DataPlane::CoreMarket,
                    result.is_ok(),
                );
                result
            });
            let ui_result = funding_app::ui::run_live_gui(initial, subscriber, selection);
            shutdown.cancel();
            match funding_task.await {
                Ok(Ok(report)) => tracing::info!(
                    status = ?report.status,
                    "funding collector stopped after GUI shutdown"
                ),
                Ok(Err(error)) => {
                    tracing::error!(%error, "funding collector stopped with an error")
                }
                Err(error) => tracing::error!(%error, "funding collector task panicked"),
            }
            match market_task.await {
                Ok(Ok(report)) => tracing::info!(
                    status = %report.status,
                    "core market collector stopped after GUI shutdown"
                ),
                Ok(Err(error)) => {
                    tracing::error!(%error, "core market collector stopped with an error")
                }
                Err(error) => tracing::error!(%error, "core market collector task panicked"),
            }
            ui_result?;
        }
    }
    Ok(())
}

#[cfg(feature = "gui")]
fn set_data_plane_result(
    state: &Arc<Mutex<funding_app::ui::live::LiveUiState>>,
    plane: funding_app::ui::live::DataPlane,
    succeeded: bool,
) {
    let status = if succeeded {
        funding_app::ui::live::DataPlaneStatus::Stopped
    } else {
        funding_app::ui::live::DataPlaneStatus::Failed
    };
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_data_plane_status(plane, status);
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        Err("duration must be greater than zero".to_owned())
    } else {
        Ok(duration)
    }
}
