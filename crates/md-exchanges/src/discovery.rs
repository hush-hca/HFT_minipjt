use std::collections::HashSet;

use md_core::{
    config::{AdapterConfig, CollectorConfig},
    model::{AdapterId, CanonicalSymbol},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveryResult {
    pub requested: Vec<CanonicalSymbol>,
    pub available: Vec<CanonicalSymbol>,
    pub missing: Vec<CanonicalSymbol>,
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("adapter {adapter:?} is missing configuration key {key:?}")]
    MissingAdapterConfig {
        adapter: AdapterId,
        key: &'static str,
    },
    #[error("market discovery request failed for {adapter:?}: {source}")]
    Request {
        adapter: AdapterId,
        #[source]
        source: reqwest::Error,
    },
    #[error("market discovery response for {adapter:?} was not successful: HTTP {status}")]
    HttpStatus {
        adapter: AdapterId,
        status: reqwest::StatusCode,
    },
    #[error("invalid market discovery payload for {adapter:?}: {message}")]
    Decode { adapter: AdapterId, message: String },
    #[error("invalid market code {market:?} in {adapter:?} discovery response")]
    InvalidMarket { adapter: AdapterId, market: String },
    #[error("strict symbol discovery failed for {adapter:?}; missing: {names}")]
    MissingSymbols {
        adapter: AdapterId,
        missing: Vec<CanonicalSymbol>,
        names: String,
    },
}

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("at least one market pair is required")]
    EmptyPairs,
    #[error(
        "invalid canonical symbol {base:?}/{quote:?}; components must be uppercase ASCII letters or digits"
    )]
    InvalidSymbol { base: String, quote: String },
    #[error("invalid combined-stream base URL {url:?}: {message}")]
    InvalidUrl { url: String, message: String },
    #[error("failed to serialize subscription: {0}")]
    Serialize(String),
}

#[derive(Debug, Deserialize)]
struct DomesticMarket {
    market: String,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceMarket {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default)]
    contract_type: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum DomesticSubscription<'a> {
    Ticket {
        ticket: String,
    },
    Channel {
        r#type: &'static str,
        codes: &'a [String],
    },
    Format {
        format: &'static str,
    },
}

pub async fn discover_markets(
    adapter: AdapterId,
    client: &reqwest::Client,
    cfg: &CollectorConfig,
) -> Result<DiscoveryResult, DiscoveryError> {
    let adapter_cfg = adapter_config(adapter, cfg)?;
    let response = client
        .get(&adapter_cfg.rest_url)
        .send()
        .await
        .map_err(|source| DiscoveryError::Request { adapter, source })?;

    if !response.status().is_success() {
        return Err(DiscoveryError::HttpStatus {
            adapter,
            status: response.status(),
        });
    }

    let mut payload = response
        .bytes()
        .await
        .map_err(|source| DiscoveryError::Request { adapter, source })?
        .to_vec();
    discovery_from_payload(adapter, cfg, &mut payload)
}

pub fn discovery_from_payload(
    adapter: AdapterId,
    cfg: &CollectorConfig,
    payload: &mut [u8],
) -> Result<DiscoveryResult, DiscoveryError> {
    let active = match adapter {
        AdapterId::UpbitSpot => crate::upbit::parse_active_markets(payload)?,
        AdapterId::BithumbSpot => crate::bithumb::parse_active_markets(payload)?,
        AdapterId::BinanceSpot => crate::binance_spot::parse_active_markets(payload)?,
        AdapterId::BinanceUsdm => crate::binance_usdm::parse_active_markets(payload)?,
    };
    ordered_intersection(adapter, cfg, &active)
}

pub fn build_subscription(
    adapter: AdapterId,
    pairs: &[CanonicalSymbol],
    ticket: Uuid,
) -> Result<String, SubscriptionError> {
    match adapter {
        AdapterId::UpbitSpot => crate::upbit::build_subscription(pairs, ticket),
        AdapterId::BithumbSpot => crate::bithumb::build_subscription(pairs, ticket),
        AdapterId::BinanceSpot => crate::binance_spot::build_subscription(pairs),
        AdapterId::BinanceUsdm => crate::binance_usdm::build_subscription(pairs),
    }
}

pub fn build_combined_stream_url(
    base_url: &str,
    pairs: &[CanonicalSymbol],
) -> Result<Url, SubscriptionError> {
    let streams = binance_stream_names(pairs)?;
    let mut url = Url::parse(base_url).map_err(|error| SubscriptionError::InvalidUrl {
        url: base_url.to_owned(),
        message: error.to_string(),
    })?;
    if url.scheme() != "wss" || url.cannot_be_a_base() || url.fragment().is_some() {
        return Err(SubscriptionError::InvalidUrl {
            url: base_url.to_owned(),
            message: "expected a hierarchical wss URL without a fragment".to_owned(),
        });
    }
    url.set_query(None);
    url.query_pairs_mut()
        .append_pair("streams", &streams.join("/"));
    Ok(url)
}

pub(crate) fn parse_domestic_active_markets(
    adapter: AdapterId,
    payload: &mut [u8],
) -> Result<HashSet<CanonicalSymbol>, DiscoveryError> {
    let markets: Vec<DomesticMarket> =
        simd_json::serde::from_slice(payload).map_err(|error| DiscoveryError::Decode {
            adapter,
            message: error.to_string(),
        })?;

    markets
        .into_iter()
        .map(|entry| {
            let (quote, base) =
                entry
                    .market
                    .split_once('-')
                    .ok_or_else(|| DiscoveryError::InvalidMarket {
                        adapter,
                        market: entry.market.clone(),
                    })?;
            if !valid_symbol_component(base) || !valid_symbol_component(quote) {
                return Err(DiscoveryError::InvalidMarket {
                    adapter,
                    market: entry.market,
                });
            }
            Ok(CanonicalSymbol::new(base, quote))
        })
        .collect()
}

pub(crate) fn parse_binance_active_markets(
    adapter: AdapterId,
    payload: &mut [u8],
    require_perpetual: bool,
) -> Result<HashSet<CanonicalSymbol>, DiscoveryError> {
    let response: BinanceExchangeInfo =
        simd_json::serde::from_slice(payload).map_err(|error| DiscoveryError::Decode {
            adapter,
            message: error.to_string(),
        })?;

    response
        .symbols
        .into_iter()
        .filter(|market| {
            market.status == "TRADING"
                && (!require_perpetual || market.contract_type.as_deref() == Some("PERPETUAL"))
        })
        .map(|market| {
            if !valid_symbol_component(&market.base_asset)
                || !valid_symbol_component(&market.quote_asset)
                || market.symbol != format!("{}{}", market.base_asset, market.quote_asset)
            {
                return Err(DiscoveryError::InvalidMarket {
                    adapter,
                    market: market.symbol,
                });
            }
            Ok(CanonicalSymbol::new(market.base_asset, market.quote_asset))
        })
        .collect()
}

pub(crate) fn build_domestic_subscription(
    pairs: &[CanonicalSymbol],
    ticket: Uuid,
    upbit_depth: bool,
) -> Result<String, SubscriptionError> {
    validate_pairs(pairs)?;
    let trade_codes = pairs.iter().map(domestic_source_symbol).collect::<Vec<_>>();
    let book_codes = trade_codes
        .iter()
        .map(|code| {
            if upbit_depth {
                format!("{code}.30")
            } else {
                code.clone()
            }
        })
        .collect::<Vec<_>>();
    let subscription = [
        DomesticSubscription::Ticket {
            ticket: ticket.to_string(),
        },
        DomesticSubscription::Channel {
            r#type: "trade",
            codes: &trade_codes,
        },
        DomesticSubscription::Channel {
            r#type: "orderbook",
            codes: &book_codes,
        },
        DomesticSubscription::Format { format: "DEFAULT" },
    ];

    simd_json::serde::to_string(&subscription)
        .map_err(|error| SubscriptionError::Serialize(error.to_string()))
}

pub(crate) fn build_binance_subscription_query(
    pairs: &[CanonicalSymbol],
) -> Result<String, SubscriptionError> {
    let streams = binance_stream_names(pairs)?;
    Ok(url::form_urlencoded::Serializer::new(String::new())
        .append_pair("streams", &streams.join("/"))
        .finish())
}

fn ordered_intersection(
    adapter: AdapterId,
    cfg: &CollectorConfig,
    active: &HashSet<CanonicalSymbol>,
) -> Result<DiscoveryResult, DiscoveryError> {
    let adapter_cfg = adapter_config(adapter, cfg)?;
    let requested = cfg
        .assets
        .iter()
        .map(|base| CanonicalSymbol::new(base, &adapter_cfg.quote))
        .collect::<Vec<_>>();
    let (available, missing): (Vec<_>, Vec<_>) = requested
        .iter()
        .cloned()
        .partition(|symbol| active.contains(symbol));

    if cfg.strict_symbols && !missing.is_empty() {
        let names = missing
            .iter()
            .map(|symbol| format!("{}/{}", symbol.base, symbol.quote))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(DiscoveryError::MissingSymbols {
            adapter,
            missing,
            names,
        });
    }

    Ok(DiscoveryResult {
        requested,
        available,
        missing,
    })
}

fn adapter_config(
    adapter: AdapterId,
    cfg: &CollectorConfig,
) -> Result<&AdapterConfig, DiscoveryError> {
    let key = match adapter {
        AdapterId::UpbitSpot => "upbit_spot",
        AdapterId::BithumbSpot => "bithumb_spot",
        AdapterId::BinanceSpot => "binance_spot",
        AdapterId::BinanceUsdm => "binance_usdm",
    };
    cfg.adapters
        .get(key)
        .ok_or(DiscoveryError::MissingAdapterConfig { adapter, key })
}

fn domestic_source_symbol(symbol: &CanonicalSymbol) -> String {
    format!("{}-{}", symbol.quote, symbol.base)
}

fn binance_stream_names(pairs: &[CanonicalSymbol]) -> Result<Vec<String>, SubscriptionError> {
    validate_pairs(pairs)?;
    Ok(pairs
        .iter()
        .flat_map(|symbol| {
            let source = format!("{}{}", symbol.base, symbol.quote).to_ascii_lowercase();
            [format!("{source}@trade"), format!("{source}@depth20@100ms")]
        })
        .collect())
}

fn validate_pairs(pairs: &[CanonicalSymbol]) -> Result<(), SubscriptionError> {
    if pairs.is_empty() {
        return Err(SubscriptionError::EmptyPairs);
    }
    for symbol in pairs {
        if !valid_symbol_component(&symbol.base) || !valid_symbol_component(&symbol.quote) {
            return Err(SubscriptionError::InvalidSymbol {
                base: symbol.base.clone(),
                quote: symbol.quote.clone(),
            });
        }
    }
    Ok(())
}

fn valid_symbol_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}
