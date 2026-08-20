use funding_core::config::FundingConfig;
use funding_core::{instrument::FundingRateBoundsProvenance, public::FundingIntervalProvenance};
use md_core::{decimal::parse_decimal_18, model::AdapterId};
use md_exchanges::derivatives::discovery::{
    DiscoveryError, DiscoveryRequestObserver, Environment, VenueInstruments, discover_derivatives,
    discover_derivatives_observed, intersect_active, parse_binance_instruments,
    parse_bybit_instruments,
};
use reqwest::{StatusCode, header::HeaderMap};
use std::sync::{Arc, Mutex};
use url::Url;

#[derive(Default)]
struct RecordingObserver {
    started: Mutex<Vec<String>>,
    completed: Mutex<Vec<String>>,
    abandoned: Mutex<Vec<String>>,
}

impl DiscoveryRequestObserver for RecordingObserver {
    fn before_request(&self, _adapter: AdapterId, url: &Url) -> Result<(), String> {
        self.started.lock().unwrap().push(url.as_str().to_owned());
        Ok(())
    }

    fn complete_request(
        &self,
        _adapter: AdapterId,
        url: &Url,
        _headers: &HeaderMap,
        _status: StatusCode,
        _bybit_ret_code: Option<i64>,
    ) -> Result<(), String> {
        self.completed.lock().unwrap().push(url.as_str().to_owned());
        Ok(())
    }

    fn abandon_request(&self, _adapter: AdapterId, url: &Url) -> Result<(), String> {
        self.abandoned.lock().unwrap().push(url.as_str().to_owned());
        Ok(())
    }
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("tests/fixtures/{name}")).unwrap()
}

fn bases(symbols: &[md_exchanges::derivatives::discovery::CommonInstrument]) -> Vec<&str> {
    symbols
        .iter()
        .map(|item| item.symbol.base.as_str())
        .collect()
}

#[test]
fn mainnet_and_testnet_discovery_are_independent_and_stable() {
    let requested = vec!["BTC".into(), "ETH".into(), "OP".into()];
    let mut binance_payload = fixture("binance_usdm_instruments_phase2.json");
    let mut bybit_payload = fixture("bybit_linear_instruments.json");
    let binance =
        parse_binance_instruments(&mut binance_payload, &requested, 1_800_000_000_200_000).unwrap();
    let bybit =
        parse_bybit_instruments(&mut bybit_payload, &requested, 1_800_000_000_300_000).unwrap();

    let mainnet = intersect_active(&requested, &binance, &bybit, Environment::Mainnet);
    assert_eq!(bases(&mainnet.eligible), ["BTC", "ETH"]);
    assert_eq!(mainnet.excluded.len(), 1);
    assert_eq!(mainnet.excluded[0].code, "BYBIT_UNAVAILABLE");

    let test_binance = VenueInstruments::from_specs(vec![mainnet.eligible[0].binance.clone()]);
    let test_bybit = VenueInstruments::from_specs(vec![mainnet.eligible[0].bybit.clone()]);
    let testnet = intersect_active(&requested, &test_binance, &test_bybit, Environment::Testnet);
    assert_eq!(bases(&testnet.eligible), ["BTC"]);
    assert_eq!(testnet.excluded.len(), 2);
    assert!(
        testnet
            .excluded
            .iter()
            .all(|excluded| excluded.code == "TESTNET_UNAVAILABLE")
    );
}

#[test]
fn venue_rules_are_exact_and_unrelated_unicode_does_not_disturb_order() {
    let requested = vec!["BTC".into(), "ETH".into()];
    let mut binance_payload = fixture("binance_usdm_instruments_phase2.json");
    let mut bybit_payload = fixture("bybit_linear_instruments.json");
    let binance = parse_binance_instruments(&mut binance_payload, &requested, 10_000_000).unwrap();
    let bybit = parse_bybit_instruments(&mut bybit_payload, &requested, 10_000_100).unwrap();
    let result = intersect_active(&requested, &binance, &bybit, Environment::Mainnet);

    assert_eq!(bases(&result.eligible), ["BTC", "ETH"]);
    let btc = &result.eligible[0];
    assert_eq!(btc.binance.tick_size, parse_decimal_18("0.10").unwrap());
    assert_eq!(
        btc.binance.quantity_step,
        parse_decimal_18("0.001").unwrap()
    );
    assert_eq!(btc.binance.min_notional, parse_decimal_18("5").unwrap());
    assert_eq!(btc.binance.funding_interval_secs, 28_800);
    assert_eq!(
        btc.binance.funding_interval_provenance,
        FundingIntervalProvenance::AssumedVenueDefault
    );
    assert_eq!(btc.binance.funding_rate_floor, None);
    assert_eq!(
        btc.binance.funding_rate_bounds_provenance,
        FundingRateBoundsProvenance::Unknown
    );
    assert_eq!(
        btc.bybit.price_upper_bound,
        Some(parse_decimal_18("1000000.00").unwrap())
    );
    assert_eq!(
        btc.bybit.contract_multiplier,
        parse_decimal_18("1").unwrap()
    );
}

#[test]
fn malformed_representable_configured_instrument_is_rule_invalid() {
    let requested = vec!["BTC".into()];
    let mut malformed = br#"{
      "serverTime": 1800000000000,
      "symbols": [{
        "symbol":"BTCUSDT", "status":"TRADING", "contractType":"PERPETUAL",
        "baseAsset":"BTC", "quoteAsset":"USDT", "marginAsset":"USDT",
        "fundingIntervalHours":8,
        "filters":[
          {"filterType":"PRICE_FILTER","minPrice":"100","maxPrice":"1000000","tickSize":"0"},
          {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
          {"filterType":"MIN_NOTIONAL","notional":"5"}
        ]
      }]
    }"#
    .to_vec();
    let mut bybit_payload = fixture("bybit_linear_instruments.json");
    let binance = parse_binance_instruments(&mut malformed, &requested, 10_000_000).unwrap();
    let bybit = parse_bybit_instruments(&mut bybit_payload, &requested, 10_000_100).unwrap();
    let result = intersect_active(&requested, &binance, &bybit, Environment::Mainnet);

    assert!(result.eligible.is_empty());
    assert_eq!(result.excluded[0].code, "RULE_INVALID");
    assert_eq!(result.excluded[0].venue, Some(AdapterId::BinanceUsdm));
    assert!(result.excluded[0].detail.contains("tick_size"));

    let mut binance_payload = fixture("binance_usdm_instruments_phase2.json");
    let mut malformed_bybit = br#"{
      "retCode":0, "retMsg":"OK", "time":1800000000000,
      "result":{"category":"linear","nextPageCursor":"","list":[{
        "symbol":"BTCUSDT", "contractType":"LinearPerpetual", "status":"Trading",
        "baseCoin":"BTC", "quoteCoin":"USDT", "settleCoin":"USDT",
        "fundingInterval":480,
        "priceFilter":{"minPrice":"100","maxPrice":"1000000","tickSize":"0.1"},
        "lotSizeFilter":{"minOrderQty":"0.001","maxOrderQty":"1000","qtyStep":"0","minNotionalValue":"5"}
      }]}
    }"#
    .to_vec();
    let binance = parse_binance_instruments(&mut binance_payload, &requested, 10_000_200).unwrap();
    let bybit = parse_bybit_instruments(&mut malformed_bybit, &requested, 10_000_300).unwrap();
    let result = intersect_active(&requested, &binance, &bybit, Environment::Mainnet);
    assert_eq!(result.excluded[0].code, "RULE_INVALID");
    assert_eq!(result.excluded[0].venue, Some(AdapterId::BybitLinear));
    assert!(result.excluded[0].detail.contains("quantity_step"));
}

#[tokio::test]
async fn network_discovery_uses_the_selected_environment_only() {
    let binance = fixture("binance_usdm_instruments_phase2.json");
    let bybit = fixture("bybit_linear_instruments.json");
    let funding_info = br#"[{"symbol":"BTCUSDT","adjustedFundingRateCap":"0.005","adjustedFundingRateFloor":"-0.004","fundingIntervalHours":4}]"#.to_vec();
    let (binance_url, binance_task, _) = serve_responses(vec![binance, funding_info]).await;
    let (bybit_url, bybit_task, _) = serve_responses(vec![bybit]).await;
    let mut cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    cfg.assets = vec!["BTC".into(), "ETH".into(), "OP".into()];
    cfg.venues.get_mut("binance_usdm").unwrap().testnet.rest_url = binance_url;
    cfg.venues.get_mut("bybit_linear").unwrap().testnet.rest_url = bybit_url;

    let observer = RecordingObserver::default();
    let result = discover_derivatives_observed(
        &reqwest::Client::new(),
        &cfg,
        Environment::Testnet,
        &observer,
    )
    .await
    .unwrap();
    assert_eq!(bases(&result.eligible), ["BTC", "ETH"]);
    assert_eq!(result.eligible[0].binance.funding_interval_secs, 14_400);
    assert_eq!(result.eligible[1].binance.funding_interval_secs, 28_800);
    assert_eq!(
        result.eligible[0].binance.funding_rate_cap,
        Some(parse_decimal_18("0.005").unwrap())
    );
    assert_eq!(
        result.eligible[0].binance.funding_interval_provenance,
        FundingIntervalProvenance::VenuePayload
    );
    assert!(
        md_exchanges::derivatives::binance::FundingRules::from_instrument(
            &result.eligible[0].binance
        )
        .is_ok()
    );
    assert!(matches!(
        md_exchanges::derivatives::binance::FundingRules::from_instrument(
            &result.eligible[1].binance
        ),
        Err(md_exchanges::derivatives::binance::DerivativeParseError::MissingRateBounds)
    ));
    assert_eq!(result.excluded[0].code, "TESTNET_UNAVAILABLE");
    binance_task.await.unwrap();
    bybit_task.await.unwrap();
}

#[tokio::test]
async fn bybit_pagination_forwards_cursor_merges_pages_and_preserves_configured_order() {
    let binance = fixture("binance_usdm_instruments_phase2.json");
    let (binance_url, binance_task, _) = serve_responses(vec![binance, br#"[]"#.to_vec()]).await;
    let (bybit_url, bybit_task, requests) = serve_responses(vec![
        bybit_page("ETH", "0.01", "cursor-A"),
        bybit_page("BTC", "0.10", ""),
    ])
    .await;
    let mut cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    cfg.assets = vec!["BTC".into(), "ETH".into()];
    cfg.venues.get_mut("binance_usdm").unwrap().testnet.rest_url = binance_url;
    cfg.venues.get_mut("bybit_linear").unwrap().testnet.rest_url = bybit_url;

    let observer = RecordingObserver::default();
    let result = discover_derivatives_observed(
        &reqwest::Client::new(),
        &cfg,
        Environment::Testnet,
        &observer,
    )
    .await
    .unwrap();
    assert_eq!(bases(&result.eligible), ["BTC", "ETH"]);
    {
        let requests = requests.lock().unwrap();
        assert!(requests[0].contains("category=linear"));
        assert!(!requests[0].contains("cursor="));
        assert!(requests[1].contains("cursor=cursor-A"));
    }
    let started = observer.started.lock().unwrap().clone();
    let completed = observer.completed.lock().unwrap().clone();
    assert_eq!(
        started.len(),
        4,
        "two Binance requests plus two Bybit pages"
    );
    assert_eq!(completed, started);
    assert!(observer.abandoned.lock().unwrap().is_empty());
    binance_task.await.unwrap();
    bybit_task.await.unwrap();
}

#[tokio::test]
async fn bybit_pagination_rejects_non_adjacent_cursor_cycles() {
    let binance = fixture("binance_usdm_instruments_phase2.json");
    let (binance_url, binance_task, _) = serve_responses(vec![binance, br#"[]"#.to_vec()]).await;
    let (bybit_url, bybit_task, _) = serve_responses(vec![
        bybit_page("BTC", "0.10", "A"),
        bybit_page("ETH", "0.01", "B"),
        empty_bybit_page("A"),
    ])
    .await;
    let mut cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    cfg.assets = vec!["BTC".into(), "ETH".into()];
    cfg.venues.get_mut("binance_usdm").unwrap().testnet.rest_url = binance_url;
    cfg.venues.get_mut("bybit_linear").unwrap().testnet.rest_url = bybit_url;

    let error = discover_derivatives(&reqwest::Client::new(), &cfg, Environment::Testnet)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DiscoveryError::PaginationCycle {
            adapter: AdapterId::BybitLinear,
            cursor
        } if cursor == "A"
    ));
    binance_task.await.unwrap();
    bybit_task.await.unwrap();
}

#[test]
fn duplicate_same_page_and_from_specs_are_rule_invalid() {
    let requested = vec!["BTC".into()];
    let mut duplicate_binance = duplicate_binance_page();
    let mut bybit_payload = fixture("bybit_linear_instruments.json");
    let binance =
        parse_binance_instruments(&mut duplicate_binance, &requested, 10_001_000).unwrap();
    let bybit = parse_bybit_instruments(&mut bybit_payload, &requested, 10_001_100).unwrap();
    let result = intersect_active(&requested, &binance, &bybit, Environment::Mainnet);
    assert_eq!(result.excluded[0].code, "RULE_INVALID");
    assert!(
        result.excluded[0]
            .detail
            .contains("duplicate canonical instrument BTC/USDT")
    );

    let valid = parse_binance_instruments(
        &mut fixture("binance_usdm_instruments_phase2.json"),
        &requested,
        10_001_200,
    )
    .unwrap();
    let spec = intersect_active(&requested, &valid, &bybit, Environment::Mainnet).eligible[0]
        .binance
        .clone();
    let duplicate_from_specs = VenueInstruments::from_specs(vec![spec.clone(), spec]);
    let result = intersect_active(
        &requested,
        &duplicate_from_specs,
        &bybit,
        Environment::Mainnet,
    );
    assert_eq!(result.excluded[0].code, "RULE_INVALID");
    assert!(
        result.excluded[0]
            .detail
            .contains("duplicate canonical instrument BTC/USDT")
    );
}

#[tokio::test]
async fn duplicate_bybit_instrument_across_pages_is_rule_invalid() {
    let binance = fixture("binance_usdm_instruments_phase2.json");
    let (binance_url, binance_task, _) = serve_responses(vec![binance, br#"[]"#.to_vec()]).await;
    let (bybit_url, bybit_task, _) = serve_responses(vec![
        bybit_page("BTC", "0.10", "next"),
        bybit_page("BTC", "0.20", ""),
    ])
    .await;
    let mut cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    cfg.assets = vec!["BTC".into()];
    cfg.venues.get_mut("binance_usdm").unwrap().testnet.rest_url = binance_url;
    cfg.venues.get_mut("bybit_linear").unwrap().testnet.rest_url = bybit_url;

    let result = discover_derivatives(&reqwest::Client::new(), &cfg, Environment::Testnet)
        .await
        .unwrap();
    assert!(result.eligible.is_empty());
    assert_eq!(result.excluded[0].code, "RULE_INVALID");
    assert_eq!(result.excluded[0].venue, Some(AdapterId::BybitLinear));
    assert!(
        result.excluded[0]
            .detail
            .contains("duplicate canonical instrument BTC/USDT")
    );
    binance_task.await.unwrap();
    bybit_task.await.unwrap();
}

#[test]
fn binance_zero_price_bounds_are_disabled_but_negative_or_missing_bounds_are_invalid() {
    let requested = vec!["BTC".into()];
    let mut zero_bounds = binance_bounds_page(Some("0"), Some("0"), "0.1");
    let parsed = parse_binance_instruments(&mut zero_bounds, &requested, 11_000_000).unwrap();
    let mut bybit_payload = fixture("bybit_linear_instruments.json");
    let bybit = parse_bybit_instruments(&mut bybit_payload, &requested, 11_000_100).unwrap();
    let result = intersect_active(&requested, &parsed, &bybit, Environment::Mainnet);
    assert_eq!(result.eligible[0].binance.price_lower_bound, None);
    assert_eq!(result.eligible[0].binance.price_upper_bound, None);

    for mut invalid in [
        binance_bounds_page(Some("-1"), Some("1000000"), "0.1"),
        binance_bounds_page(None, Some("1000000"), "0.1"),
    ] {
        let parsed = parse_binance_instruments(&mut invalid, &requested, 11_000_200).unwrap();
        let result = intersect_active(&requested, &parsed, &bybit, Environment::Mainnet);
        assert_eq!(result.excluded[0].code, "RULE_INVALID");
        assert!(result.excluded[0].detail.contains("price_lower_bound"));
    }

    let mut zero_tick = binance_bounds_page(Some("0"), Some("0"), "0");
    let parsed = parse_binance_instruments(&mut zero_tick, &requested, 11_000_300).unwrap();
    let result = intersect_active(&requested, &parsed, &bybit, Environment::Mainnet);
    assert_eq!(result.excluded[0].code, "RULE_INVALID");
    assert!(result.excluded[0].detail.contains("tick_size"));
}

async fn serve_responses(
    bodies: Vec<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<()>, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let task = tokio::spawn(async move {
        for body in bodies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).await.unwrap();
            let first_line = String::from_utf8_lossy(&request[..read])
                .lines()
                .next()
                .unwrap_or_default()
                .to_owned();
            captured.lock().unwrap().push(first_line);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        }
    });
    (format!("http://{address}"), task, requests)
}

fn bybit_page(base: &str, tick_size: &str, cursor: &str) -> Vec<u8> {
    format!(
        r#"{{"retCode":0,"retMsg":"OK","time":1800000000100,"result":{{"category":"linear","nextPageCursor":"{cursor}","list":[{{"symbol":"{base}USDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"{base}","quoteCoin":"USDT","settleCoin":"USDT","fundingInterval":480,"unifiedMarginTrade":true,"priceFilter":{{"minPrice":"0.01","maxPrice":"1000000","tickSize":"{tick_size}"}},"lotSizeFilter":{{"minOrderQty":"0.001","maxOrderQty":"1000","qtyStep":"0.001","minNotionalValue":"5"}}}}]}}}}"#
    )
    .into_bytes()
}

fn empty_bybit_page(cursor: &str) -> Vec<u8> {
    format!(
        r#"{{"retCode":0,"retMsg":"OK","time":1800000000100,"result":{{"category":"linear","nextPageCursor":"{cursor}","list":[]}}}}"#
    )
    .into_bytes()
}

fn duplicate_binance_page() -> Vec<u8> {
    let first = String::from_utf8(binance_bounds_page(Some("0"), Some("0"), "0.1")).unwrap();
    let symbol = first
        .split_once("\"symbols\":[")
        .unwrap()
        .1
        .strip_suffix("]}")
        .unwrap();
    format!(r#"{{"serverTime":1800000000000,"symbols":[{symbol},{symbol}]}}"#).into_bytes()
}

fn binance_bounds_page(min_price: Option<&str>, max_price: Option<&str>, tick: &str) -> Vec<u8> {
    let min = min_price
        .map(|value| format!(r#","minPrice":"{value}""#))
        .unwrap_or_default();
    let max = max_price
        .map(|value| format!(r#","maxPrice":"{value}""#))
        .unwrap_or_default();
    format!(
        r#"{{"serverTime":1800000000000,"symbols":[{{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","filters":[{{"filterType":"PRICE_FILTER","tickSize":"{tick}"{min}{max}}},{{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"}},{{"filterType":"MIN_NOTIONAL","notional":"5"}}]}}]}}"#
    )
    .into_bytes()
}
