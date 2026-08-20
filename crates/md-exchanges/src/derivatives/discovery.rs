use std::collections::{HashMap, HashSet};

use funding_core::{
    config::{EndpointSet, FundingConfig},
    instrument::{
        AccountMode, ContractKind, EligibilityReason, FundingRateBoundsProvenance, InstrumentSpec,
        PositionMode,
    },
    meta::DerivativeMeta,
    public::FundingIntervalProvenance,
};
use md_core::{
    decimal::{DecimalError, parse_decimal_18},
    model::{AdapterId, CanonicalSymbol, TimestampPrecision, ms_to_us},
};
use reqwest::{StatusCode, header::HeaderMap};
use serde::Deserialize;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const USDT: &str = "USDT";
const ONE: i128 = 1_000_000_000_000_000_000;
const BINANCE_DEFAULT_FUNDING_INTERVAL_HOURS: u32 = 8;
const MAX_BYBIT_DISCOVERY_PAGES: usize = 100;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Environment {
    Mainnet,
    Testnet,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DerivativeDiscovery {
    pub eligible: Vec<CommonInstrument>,
    pub excluded: Vec<IneligibleInstrument>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommonInstrument {
    pub symbol: CanonicalSymbol,
    pub binance: InstrumentSpec,
    pub bybit: InstrumentSpec,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IneligibleInstrument {
    pub symbol: CanonicalSymbol,
    pub venue: Option<AdapterId>,
    pub reason: EligibilityReason,
    pub code: &'static str,
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub struct VenueInstruments {
    specs: HashMap<CanonicalSymbol, InstrumentSpec>,
    invalid: HashMap<CanonicalSymbol, String>,
    seen: HashSet<CanonicalSymbol>,
}

impl VenueInstruments {
    pub fn from_specs(specs: Vec<InstrumentSpec>) -> Self {
        let mut result = Self::default();
        for spec in specs {
            let symbol = spec.meta.symbol.clone();
            result.insert_result(symbol, Ok(spec));
        }
        result
    }

    fn merge(&mut self, mut other: Self) {
        for symbol in other.seen {
            if self.seen.contains(&symbol) {
                self.mark_duplicate(symbol);
            } else {
                self.seen.insert(symbol.clone());
                if let Some(spec) = other.specs.remove(&symbol) {
                    self.specs.insert(symbol, spec);
                } else if let Some(detail) = other.invalid.remove(&symbol) {
                    self.invalid.insert(symbol, detail);
                }
            }
        }
    }

    fn insert_result(&mut self, symbol: CanonicalSymbol, value: Result<InstrumentSpec, String>) {
        if !self.seen.insert(symbol.clone()) {
            self.mark_duplicate(symbol);
            return;
        }
        match value {
            Ok(spec) => {
                self.specs.insert(symbol, spec);
            }
            Err(detail) => {
                self.invalid.insert(symbol, detail);
            }
        }
    }

    fn mark_duplicate(&mut self, symbol: CanonicalSymbol) {
        self.seen.insert(symbol.clone());
        self.specs.remove(&symbol);
        self.invalid.insert(
            symbol.clone(),
            format!(
                "duplicate canonical instrument {}/{} in venue discovery payload",
                symbol.base, symbol.quote
            ),
        );
    }
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("funding configuration is missing venue {venue:?}")]
    MissingVenueConfig { venue: &'static str },
    #[error("invalid derivative discovery URL for {adapter:?}: {message}")]
    InvalidUrl { adapter: AdapterId, message: String },
    #[error("derivative discovery request failed for {adapter:?}: {source}")]
    Request {
        adapter: AdapterId,
        #[source]
        source: reqwest::Error,
    },
    #[error("derivative discovery response for {adapter:?} returned HTTP {status}")]
    HttpStatus {
        adapter: AdapterId,
        status: reqwest::StatusCode,
    },
    #[error("invalid derivative discovery payload for {adapter:?}: {message}")]
    Decode { adapter: AdapterId, message: String },
    #[error("pagination cursor cycle for {adapter:?}: {cursor:?}")]
    PaginationCycle { adapter: AdapterId, cursor: String },
    #[error("pagination for {adapter:?} exceeded the {max_pages}-page safety limit")]
    PaginationLimitExceeded {
        adapter: AdapterId,
        max_pages: usize,
    },
    #[error("derivative discovery request policy failed for {adapter:?}: {message}")]
    RequestPolicy { adapter: AdapterId, message: String },
}

/// Lifecycle callbacks around each physical discovery HTTP request.
pub trait DiscoveryRequestObserver: Send + Sync {
    fn before_request(&self, _adapter: AdapterId, _url: &Url) -> Result<(), String> {
        Ok(())
    }

    fn complete_request(
        &self,
        _adapter: AdapterId,
        _url: &Url,
        _headers: &HeaderMap,
        _status: StatusCode,
        _bybit_ret_code: Option<i64>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn abandon_request(&self, _adapter: AdapterId, _url: &Url) -> Result<(), String> {
        Ok(())
    }
}

struct NoopDiscoveryObserver;

impl DiscoveryRequestObserver for NoopDiscoveryObserver {}

pub async fn discover_derivatives(
    client: &reqwest::Client,
    config: &FundingConfig,
    environment: Environment,
) -> Result<DerivativeDiscovery, DiscoveryError> {
    discover_derivatives_observed(client, config, environment, &NoopDiscoveryObserver).await
}

pub async fn discover_derivatives_observed(
    client: &reqwest::Client,
    config: &FundingConfig,
    environment: Environment,
    observer: &dyn DiscoveryRequestObserver,
) -> Result<DerivativeDiscovery, DiscoveryError> {
    let binance_endpoints = endpoints(config, "binance_usdm", environment)?;
    let bybit_endpoints = endpoints(config, "bybit_linear", environment)?;

    let binance = fetch_binance(
        client,
        &binance_endpoints.rest_url,
        &config.assets,
        observer,
    )
    .await?;
    let bybit = fetch_bybit(client, &bybit_endpoints.rest_url, &config.assets, observer).await?;

    Ok(intersect_active(
        &config.assets,
        &binance,
        &bybit,
        environment,
    ))
}

pub fn parse_binance_instruments(
    payload: &mut [u8],
    requested: &[String],
    local_recv_ts_us: i64,
) -> Result<VenueInstruments, DiscoveryError> {
    let response: BinanceExchangeInfo =
        simd_json::serde::from_slice(payload).map_err(|error| DiscoveryError::Decode {
            adapter: AdapterId::BinanceUsdm,
            message: error.to_string(),
        })?;
    let requested = requested.iter().map(String::as_str).collect::<HashSet<_>>();
    let source_ts_us = timestamp_ms(response.server_time, AdapterId::BinanceUsdm)?;
    let mut result = VenueInstruments::default();

    for market in response.symbols {
        if !is_component(&market.base_asset)
            || !is_component(&market.quote_asset)
            || !requested.contains(market.base_asset.as_str())
            || market.quote_asset != USDT
        {
            continue;
        }
        let symbol = CanonicalSymbol::new(&market.base_asset, USDT);
        if market.status != "TRADING"
            || market.contract_type != "PERPETUAL"
            || market.margin_asset != USDT
        {
            continue;
        }
        if market.symbol != format!("{}{}", market.base_asset, market.quote_asset) {
            result.insert_result(
                symbol,
                Err(format!(
                    "venue symbol {:?} does not match base and quote",
                    market.symbol
                )),
            );
            continue;
        }

        let parsed = binance_spec(market, symbol.clone(), source_ts_us, local_recv_ts_us);
        result.insert_result(symbol, parsed);
    }

    Ok(result)
}

pub fn parse_bybit_instruments(
    payload: &mut [u8],
    requested: &[String],
    local_recv_ts_us: i64,
) -> Result<VenueInstruments, DiscoveryError> {
    let (instruments, _) = parse_bybit_page(payload, requested, local_recv_ts_us)?;
    Ok(instruments)
}

pub fn intersect_active(
    requested: &[String],
    binance: &VenueInstruments,
    bybit: &VenueInstruments,
    environment: Environment,
) -> DerivativeDiscovery {
    let mut eligible = Vec::with_capacity(requested.len());
    let mut excluded = Vec::new();

    for base in requested {
        let symbol = CanonicalSymbol::new(base, USDT);
        if let Some(detail) = binance.invalid.get(&symbol) {
            excluded.push(exclusion(
                symbol,
                Some(AdapterId::BinanceUsdm),
                EligibilityReason::RuleInvalid,
                detail.clone(),
            ));
            continue;
        }
        if let Some(detail) = bybit.invalid.get(&symbol) {
            excluded.push(exclusion(
                symbol,
                Some(AdapterId::BybitLinear),
                EligibilityReason::RuleInvalid,
                detail.clone(),
            ));
            continue;
        }

        match (binance.specs.get(&symbol), bybit.specs.get(&symbol)) {
            (Some(binance), Some(bybit)) => eligible.push(CommonInstrument {
                symbol,
                binance: binance.clone(),
                bybit: bybit.clone(),
            }),
            (binance_spec, bybit_spec) => {
                let (venue, reason, detail) = if environment == Environment::Testnet {
                    (
                        None,
                        EligibilityReason::TestnetUnavailable,
                        testnet_detail(binance_spec.is_some(), bybit_spec.is_some()),
                    )
                } else if binance_spec.is_none() {
                    (
                        Some(AdapterId::BinanceUsdm),
                        EligibilityReason::BinanceUnavailable,
                        "active Binance USD-M USDT perpetual is unavailable".to_owned(),
                    )
                } else {
                    (
                        Some(AdapterId::BybitLinear),
                        EligibilityReason::BybitUnavailable,
                        "active Bybit linear USDT perpetual is unavailable".to_owned(),
                    )
                };
                excluded.push(exclusion(symbol, venue, reason, detail));
            }
        }
    }

    DerivativeDiscovery { eligible, excluded }
}

async fn fetch_binance(
    client: &reqwest::Client,
    base_url: &str,
    requested: &[String],
    observer: &dyn DiscoveryRequestObserver,
) -> Result<VenueInstruments, DiscoveryError> {
    let url = endpoint_url(base_url, "/fapi/v1/exchangeInfo", AdapterId::BinanceUsdm)?;
    let mut fetched = fetch(client, url, AdapterId::BinanceUsdm, observer).await?;
    let parsed = parse_binance_instruments(&mut fetched.payload, requested, fetched.recv_us);
    let mut instruments = finish_decoded(fetched, parsed, observer, None)?;
    let funding_url = endpoint_url(base_url, "/fapi/v1/fundingInfo", AdapterId::BinanceUsdm)?;
    let mut fetched = fetch(client, funding_url, AdapterId::BinanceUsdm, observer).await?;
    let parsed = apply_binance_funding_info(&mut fetched.payload, &mut instruments);
    finish_decoded(fetched, parsed, observer, None)?;
    Ok(instruments)
}

async fn fetch_bybit(
    client: &reqwest::Client,
    base_url: &str,
    requested: &[String],
    observer: &dyn DiscoveryRequestObserver,
) -> Result<VenueInstruments, DiscoveryError> {
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut page_count = 0_usize;
    let mut result = VenueInstruments::default();

    loop {
        if page_count >= MAX_BYBIT_DISCOVERY_PAGES {
            return Err(DiscoveryError::PaginationLimitExceeded {
                adapter: AdapterId::BybitLinear,
                max_pages: MAX_BYBIT_DISCOVERY_PAGES,
            });
        }
        let mut url = endpoint_url(
            base_url,
            "/v5/market/instruments-info",
            AdapterId::BybitLinear,
        )?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("category", "linear");
            query.append_pair("limit", "1000");
            if let Some(cursor) = cursor.as_deref() {
                query.append_pair("cursor", cursor);
            }
        }
        let mut fetched = fetch(client, url, AdapterId::BybitLinear, observer).await?;
        page_count += 1;
        let decoded = decode_bybit_response(&mut fetched.payload);
        let response = match decoded {
            Ok(response) => response,
            Err(error) => {
                abandon(&fetched, observer)?;
                return Err(error);
            }
        };
        let ret_code = response.ret_code;
        if ret_code != 0 {
            complete(&fetched, observer, Some(ret_code))?;
            return Err(DiscoveryError::Decode {
                adapter: AdapterId::BybitLinear,
                message: format!("retCode {}: {}", response.ret_code, response.ret_msg),
            });
        }
        let parsed = convert_bybit_page(response, requested, fetched.recv_us);
        let (page, next_cursor) = finish_decoded(fetched, parsed, observer, Some(0))?;
        result.merge(page);
        match next_cursor {
            Some(next) if seen_cursors.insert(next.clone()) => cursor = Some(next),
            Some(cursor) => {
                return Err(DiscoveryError::PaginationCycle {
                    adapter: AdapterId::BybitLinear,
                    cursor,
                });
            }
            None => break,
        }
    }

    Ok(result)
}

async fn fetch(
    client: &reqwest::Client,
    url: Url,
    adapter: AdapterId,
    observer: &dyn DiscoveryRequestObserver,
) -> Result<FetchedDiscoveryResponse, DiscoveryError> {
    observer
        .before_request(adapter, &url)
        .map_err(|message| DiscoveryError::RequestPolicy { adapter, message })?;
    let response = client.get(url.clone()).send().await.map_err(|source| {
        let _ = observer.abandon_request(adapter, &url);
        DiscoveryError::Request { adapter, source }
    })?;
    let status = response.status();
    let headers = response.headers().clone();
    if !status.is_success() {
        observer
            .complete_request(adapter, &url, &headers, status, None)
            .map_err(|message| DiscoveryError::RequestPolicy { adapter, message })?;
        return Err(DiscoveryError::HttpStatus { adapter, status });
    }
    let payload = response
        .bytes()
        .await
        .map_err(|source| {
            let _ = observer.abandon_request(adapter, &url);
            DiscoveryError::Request { adapter, source }
        })?
        .to_vec();
    let local_recv_ts_us = unix_time_us();
    Ok(FetchedDiscoveryResponse {
        adapter,
        url,
        headers,
        status,
        payload,
        recv_us: local_recv_ts_us,
    })
}

struct FetchedDiscoveryResponse {
    adapter: AdapterId,
    url: Url,
    headers: HeaderMap,
    status: StatusCode,
    payload: Vec<u8>,
    recv_us: i64,
}

fn finish_decoded<T>(
    fetched: FetchedDiscoveryResponse,
    decoded: Result<T, DiscoveryError>,
    observer: &dyn DiscoveryRequestObserver,
    bybit_ret_code: Option<i64>,
) -> Result<T, DiscoveryError> {
    match decoded {
        Ok(value) => {
            complete(&fetched, observer, bybit_ret_code)?;
            Ok(value)
        }
        Err(error) => {
            abandon(&fetched, observer)?;
            Err(error)
        }
    }
}

fn complete(
    fetched: &FetchedDiscoveryResponse,
    observer: &dyn DiscoveryRequestObserver,
    bybit_ret_code: Option<i64>,
) -> Result<(), DiscoveryError> {
    observer
        .complete_request(
            fetched.adapter,
            &fetched.url,
            &fetched.headers,
            fetched.status,
            bybit_ret_code,
        )
        .map_err(|message| DiscoveryError::RequestPolicy {
            adapter: fetched.adapter,
            message,
        })
}

fn abandon(
    fetched: &FetchedDiscoveryResponse,
    observer: &dyn DiscoveryRequestObserver,
) -> Result<(), DiscoveryError> {
    observer
        .abandon_request(fetched.adapter, &fetched.url)
        .map_err(|message| DiscoveryError::RequestPolicy {
            adapter: fetched.adapter,
            message,
        })
}

fn apply_binance_funding_info(
    payload: &mut [u8],
    instruments: &mut VenueInstruments,
) -> Result<(), DiscoveryError> {
    let entries: Vec<BinanceFundingInfo> =
        simd_json::serde::from_slice(payload).map_err(|error| DiscoveryError::Decode {
            adapter: AdapterId::BinanceUsdm,
            message: error.to_string(),
        })?;
    for entry in entries {
        let Some(base) = entry.symbol.strip_suffix(USDT) else {
            continue;
        };
        let symbol = CanonicalSymbol::new(base, USDT);
        let interval = positive_u32("funding_interval_hours", Some(entry.funding_interval_hours))
            .and_then(|hours| {
                hours
                    .checked_mul(3_600)
                    .ok_or_else(|| "funding_interval_hours overflows seconds".to_owned())
            });
        let bounds = (|| {
            let floor = parse_decimal_18(&entry.adjusted_funding_rate_floor)
                .map_err(|error| format!("adjusted_funding_rate_floor: {error}"))?;
            let cap = parse_decimal_18(&entry.adjusted_funding_rate_cap)
                .map_err(|error| format!("adjusted_funding_rate_cap: {error}"))?;
            if floor >= cap {
                return Err("adjusted funding rate floor must be less than cap".to_owned());
            }
            Ok((floor, cap))
        })();
        match (instruments.specs.get_mut(&symbol), interval, bounds) {
            (Some(spec), Ok(interval), Ok((floor, cap))) => {
                spec.funding_interval_secs = interval;
                spec.funding_interval_provenance = FundingIntervalProvenance::VenuePayload;
                spec.funding_rate_floor = Some(floor);
                spec.funding_rate_cap = Some(cap);
                spec.funding_rate_bounds_provenance = FundingRateBoundsProvenance::VenueFundingInfo;
            }
            (Some(_), Err(detail), _) | (Some(_), _, Err(detail)) => {
                instruments.specs.remove(&symbol);
                instruments.invalid.insert(symbol, detail);
            }
            (None, _, _) => {}
        }
    }
    Ok(())
}

fn parse_bybit_page(
    payload: &mut [u8],
    requested: &[String],
    local_recv_ts_us: i64,
) -> Result<(VenueInstruments, Option<String>), DiscoveryError> {
    let response = decode_bybit_response(payload)?;
    convert_bybit_page(response, requested, local_recv_ts_us)
}

fn decode_bybit_response(payload: &mut [u8]) -> Result<BybitResponse, DiscoveryError> {
    simd_json::serde::from_slice(payload).map_err(|error| DiscoveryError::Decode {
        adapter: AdapterId::BybitLinear,
        message: error.to_string(),
    })
}

fn convert_bybit_page(
    response: BybitResponse,
    requested: &[String],
    local_recv_ts_us: i64,
) -> Result<(VenueInstruments, Option<String>), DiscoveryError> {
    if response.ret_code != 0 {
        return Err(DiscoveryError::Decode {
            adapter: AdapterId::BybitLinear,
            message: format!("retCode {}: {}", response.ret_code, response.ret_msg),
        });
    }
    if response.result.category != "linear" {
        return Err(DiscoveryError::Decode {
            adapter: AdapterId::BybitLinear,
            message: format!("unexpected category {:?}", response.result.category),
        });
    }

    let requested = requested.iter().map(String::as_str).collect::<HashSet<_>>();
    let source_ts_us = timestamp_ms(Some(response.time), AdapterId::BybitLinear)?;
    let mut result = VenueInstruments::default();
    for market in response.result.list {
        if !is_component(&market.base_coin)
            || !is_component(&market.quote_coin)
            || !requested.contains(market.base_coin.as_str())
            || market.quote_coin != USDT
        {
            continue;
        }
        let symbol = CanonicalSymbol::new(&market.base_coin, USDT);
        if market.status != "Trading"
            || market.contract_type != "LinearPerpetual"
            || market.settle_coin != USDT
        {
            continue;
        }
        if market.symbol != format!("{}{}", market.base_coin, market.quote_coin) {
            result.insert_result(
                symbol,
                Err(format!(
                    "venue symbol {:?} does not match base and quote",
                    market.symbol
                )),
            );
            continue;
        }
        let parsed = bybit_spec(market, symbol.clone(), source_ts_us, local_recv_ts_us);
        result.insert_result(symbol, parsed);
    }

    let cursor = response
        .result
        .next_page_cursor
        .filter(|cursor| !cursor.is_empty());
    Ok((result, cursor))
}

fn binance_spec(
    market: BinanceMarket,
    symbol: CanonicalSymbol,
    source_ts_us: Option<i64>,
    local_recv_ts_us: i64,
) -> Result<InstrumentSpec, String> {
    let price = market
        .filters
        .iter()
        .find(|filter| filter.filter_type == "PRICE_FILTER")
        .ok_or_else(|| "price filter is missing".to_owned())?;
    let lot = market
        .filters
        .iter()
        .find(|filter| filter.filter_type == "LOT_SIZE")
        .ok_or_else(|| "lot-size filter is missing".to_owned())?;
    let notional = market
        .filters
        .iter()
        .find(|filter| filter.filter_type == "MIN_NOTIONAL")
        .ok_or_else(|| "minimum-notional filter is missing".to_owned())?;

    let funding_interval_secs = BINANCE_DEFAULT_FUNDING_INTERVAL_HOURS
        .checked_mul(3_600)
        .expect("the Binance default funding interval fits u32 seconds");
    Ok(InstrumentSpec {
        meta: derivative_meta(
            AdapterId::BinanceUsdm,
            symbol,
            market.symbol,
            source_ts_us,
            local_recv_ts_us,
        ),
        contract_kind: ContractKind::Perpetual,
        settlement_asset: USDT.to_owned(),
        contract_multiplier: ONE,
        tick_size: positive_decimal("tick_size", price.tick_size.as_deref())?,
        quantity_step: positive_decimal("quantity_step", lot.step_size.as_deref())?,
        min_quantity: positive_decimal("min_quantity", lot.min_qty.as_deref())?,
        max_quantity: Some(positive_decimal("max_quantity", lot.max_qty.as_deref())?),
        min_notional: positive_decimal(
            "min_notional",
            notional
                .notional
                .as_deref()
                .or(notional.min_notional.as_deref()),
        )?,
        funding_interval_secs,
        funding_interval_provenance: FundingIntervalProvenance::AssumedVenueDefault,
        funding_rate_floor: None,
        funding_rate_cap: None,
        funding_rate_bounds_provenance: FundingRateBoundsProvenance::Unknown,
        price_lower_bound: disabled_zero_bound("price_lower_bound", price.min_price.as_deref())?,
        price_upper_bound: disabled_zero_bound("price_upper_bound", price.max_price.as_deref())?,
        supported_position_modes: vec![PositionMode::OneWay, PositionMode::Hedge],
        supported_account_modes: vec![AccountMode::Classic, AccountMode::Portfolio],
    })
}

fn bybit_spec(
    market: BybitMarket,
    symbol: CanonicalSymbol,
    source_ts_us: Option<i64>,
    local_recv_ts_us: i64,
) -> Result<InstrumentSpec, String> {
    let funding_interval_minutes =
        positive_u32("funding_interval_minutes", market.funding_interval)?;
    let funding_interval_secs = funding_interval_minutes
        .checked_mul(60)
        .ok_or_else(|| "funding_interval_minutes overflows seconds".to_owned())?;
    Ok(InstrumentSpec {
        meta: derivative_meta(
            AdapterId::BybitLinear,
            symbol,
            market.symbol,
            source_ts_us,
            local_recv_ts_us,
        ),
        contract_kind: ContractKind::Perpetual,
        settlement_asset: USDT.to_owned(),
        contract_multiplier: ONE,
        tick_size: positive_decimal("tick_size", market.price_filter.tick_size.as_deref())?,
        quantity_step: positive_decimal(
            "quantity_step",
            market.lot_size_filter.qty_step.as_deref(),
        )?,
        min_quantity: positive_decimal(
            "min_quantity",
            market.lot_size_filter.min_order_qty.as_deref(),
        )?,
        max_quantity: Some(positive_decimal(
            "max_quantity",
            market.lot_size_filter.max_order_qty.as_deref(),
        )?),
        min_notional: positive_decimal(
            "min_notional",
            market.lot_size_filter.min_notional_value.as_deref(),
        )?,
        funding_interval_secs,
        funding_interval_provenance: FundingIntervalProvenance::VenuePayload,
        funding_rate_floor: None,
        funding_rate_cap: None,
        funding_rate_bounds_provenance: FundingRateBoundsProvenance::Unknown,
        price_lower_bound: Some(positive_decimal(
            "price_lower_bound",
            market.price_filter.min_price.as_deref(),
        )?),
        price_upper_bound: Some(positive_decimal(
            "price_upper_bound",
            market.price_filter.max_price.as_deref(),
        )?),
        supported_position_modes: vec![PositionMode::OneWay, PositionMode::Hedge],
        supported_account_modes: bybit_account_modes(market.unified_margin_trade),
    })
}

fn endpoints<'a>(
    config: &'a FundingConfig,
    venue: &'static str,
    environment: Environment,
) -> Result<&'a EndpointSet, DiscoveryError> {
    let venue_config = config
        .venues
        .get(venue)
        .ok_or(DiscoveryError::MissingVenueConfig { venue })?;
    Ok(match environment {
        Environment::Mainnet => &venue_config.mainnet,
        Environment::Testnet => &venue_config.testnet,
    })
}

fn endpoint_url(base_url: &str, path: &str, adapter: AdapterId) -> Result<Url, DiscoveryError> {
    let mut url = Url::parse(base_url).map_err(|error| DiscoveryError::InvalidUrl {
        adapter,
        message: error.to_string(),
    })?;
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn positive_decimal(field: &str, value: Option<&str>) -> Result<i128, String> {
    let value = value.ok_or_else(|| format!("{field} is missing"))?;
    let parsed =
        parse_decimal_18(value).map_err(|error: DecimalError| format!("{field}: {error}"))?;
    if parsed <= 0 {
        Err(format!("{field} must be positive"))
    } else {
        Ok(parsed)
    }
}

fn disabled_zero_bound(field: &str, value: Option<&str>) -> Result<Option<i128>, String> {
    let value = value.ok_or_else(|| format!("{field} is missing"))?;
    let parsed =
        parse_decimal_18(value).map_err(|error: DecimalError| format!("{field}: {error}"))?;
    if parsed < 0 {
        Err(format!("{field} must not be negative"))
    } else if parsed == 0 {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

fn positive_u32(field: &str, value: Option<u32>) -> Result<u32, String> {
    value
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{field} must be present and positive"))
}

fn bybit_account_modes(unified_margin_trade: bool) -> Vec<AccountMode> {
    let mut modes = vec![AccountMode::Classic];
    if unified_margin_trade {
        modes.extend([AccountMode::Unified, AccountMode::Portfolio]);
    }
    modes
}

fn timestamp_ms(value: Option<i64>, adapter: AdapterId) -> Result<Option<i64>, DiscoveryError> {
    value
        .filter(|value| *value > 0)
        .map(ms_to_us)
        .transpose()
        .map_err(|error| DiscoveryError::Decode {
            adapter,
            message: error.to_string(),
        })
}

fn derivative_meta(
    venue: AdapterId,
    symbol: CanonicalSymbol,
    venue_symbol: String,
    source_ts_us: Option<i64>,
    local_recv_ts_us: i64,
) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol,
        venue_symbol,
        source_ts_us,
        source_ts_precision: if source_ts_us.is_some() {
            TimestampPrecision::Millisecond
        } else {
            TimestampPrecision::Unavailable
        },
        local_recv_ts_us,
    }
}

fn exclusion(
    symbol: CanonicalSymbol,
    venue: Option<AdapterId>,
    reason: EligibilityReason,
    detail: String,
) -> IneligibleInstrument {
    IneligibleInstrument {
        symbol,
        venue,
        reason,
        code: reason_code(reason),
        detail,
    }
}

fn reason_code(reason: EligibilityReason) -> &'static str {
    match reason {
        EligibilityReason::BinanceUnavailable => "BINANCE_UNAVAILABLE",
        EligibilityReason::BybitUnavailable => "BYBIT_UNAVAILABLE",
        EligibilityReason::TestnetUnavailable => "TESTNET_UNAVAILABLE",
        EligibilityReason::Inactive => "INACTIVE",
        EligibilityReason::UnsupportedContract => "UNSUPPORTED_CONTRACT",
        EligibilityReason::UnsupportedSettlement => "UNSUPPORTED_SETTLEMENT",
        EligibilityReason::RuleInvalid => "RULE_INVALID",
        EligibilityReason::FundingScheduleUnknown => "FUNDING_SCHEDULE_UNKNOWN",
    }
}

fn testnet_detail(binance: bool, bybit: bool) -> String {
    match (binance, bybit) {
        (false, false) => "symbol is unavailable on both derivative testnets",
        (false, true) => "symbol is unavailable on Binance USD-M testnet",
        (true, false) => "symbol is unavailable on Bybit linear testnet",
        (true, true) => "symbol is unavailable on the derivative testnet intersection",
    }
    .to_owned()
}

fn is_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn unix_time_us() -> i64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    i64::try_from(micros).unwrap_or(i64::MAX)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceExchangeInfo {
    #[serde(default)]
    server_time: Option<i64>,
    symbols: Vec<BinanceMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceMarket {
    symbol: String,
    status: String,
    contract_type: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default)]
    margin_asset: String,
    #[serde(default)]
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFundingInfo {
    symbol: String,
    adjusted_funding_rate_cap: String,
    adjusted_funding_rate_floor: String,
    funding_interval_hours: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFilter {
    filter_type: String,
    #[serde(default)]
    min_price: Option<String>,
    #[serde(default)]
    max_price: Option<String>,
    #[serde(default)]
    tick_size: Option<String>,
    #[serde(default)]
    min_qty: Option<String>,
    #[serde(default)]
    max_qty: Option<String>,
    #[serde(default)]
    step_size: Option<String>,
    #[serde(default)]
    notional: Option<String>,
    #[serde(default)]
    min_notional: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitResponse {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    time: i64,
    result: BybitResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitResult {
    category: String,
    #[serde(default)]
    next_page_cursor: Option<String>,
    list: Vec<BybitMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitMarket {
    symbol: String,
    contract_type: String,
    status: String,
    base_coin: String,
    quote_coin: String,
    settle_coin: String,
    #[serde(default)]
    funding_interval: Option<u32>,
    #[serde(default)]
    unified_margin_trade: bool,
    #[serde(default)]
    price_filter: BybitPriceFilter,
    #[serde(default)]
    lot_size_filter: BybitLotSizeFilter,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitPriceFilter {
    #[serde(default)]
    min_price: Option<String>,
    #[serde(default)]
    max_price: Option<String>,
    #[serde(default)]
    tick_size: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLotSizeFilter {
    #[serde(default)]
    min_order_qty: Option<String>,
    #[serde(default)]
    max_order_qty: Option<String>,
    #[serde(default)]
    qty_step: Option<String>,
    #[serde(default)]
    min_notional_value: Option<String>,
}
