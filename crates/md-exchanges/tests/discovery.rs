use std::{collections::BTreeMap, path::PathBuf};

use md_core::{
    config::{AdapterConfig, CollectorConfig, RetryConfig},
    model::{AdapterId, CanonicalSymbol},
};
use md_exchanges::{
    DiscoveryError, build_combined_stream_url, build_subscription, discovery_from_payload,
};
use uuid::Uuid;

fn config(strict_symbols: bool) -> CollectorConfig {
    let adapters = [
        ("upbit_spot", "KRW"),
        ("bithumb_spot", "KRW"),
        ("binance_spot", "USDT"),
        ("binance_usdm", "USDT"),
    ]
    .into_iter()
    .map(|(name, quote)| {
        (
            name.to_owned(),
            AdapterConfig {
                enabled: true,
                quote: quote.to_owned(),
                rest_url: "https://example.com/markets".to_owned(),
                websocket_url: "wss://example.com/stream".to_owned(),
                proactive_reconnect_secs: None,
            },
        )
    })
    .collect::<BTreeMap<_, _>>();

    CollectorConfig {
        output_root: PathBuf::from("data"),
        assets: vec!["ETH".into(), "BTC".into(), "XRP".into(), "SOL".into()],
        strict_symbols,
        channel_capacity: 65_536,
        batch_rows: 8_192,
        flush_interval_ms: 1_000,
        enqueue_timeout_ms: 5_000,
        stats_interval_secs: 10,
        retry: RetryConfig {
            initial_ms: 1_000,
            max_ms: 30_000,
            reset_after_secs: 300,
        },
        adapters,
    }
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("tests/fixtures/{name}")).unwrap()
}

#[test]
fn upbit_and_bithumb_discovery_preserve_configured_order() {
    for (adapter, name) in [
        (AdapterId::UpbitSpot, "upbit_markets.json"),
        (AdapterId::BithumbSpot, "bithumb_markets.json"),
    ] {
        let mut payload = fixture(name);
        let result = discovery_from_payload(adapter, &config(false), &mut payload).unwrap();

        assert_eq!(
            result.requested,
            ["ETH", "BTC", "XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "KRW"))
        );
        assert_eq!(
            result.available,
            ["ETH", "BTC"].map(|base| CanonicalSymbol::new(base, "KRW"))
        );
        assert_eq!(
            result.missing,
            ["XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "KRW"))
        );
    }
}

#[test]
fn binance_discovery_excludes_inactive_and_non_perpetual_symbols() {
    let mut spot = fixture("binance_spot_markets.json");
    let spot = discovery_from_payload(AdapterId::BinanceSpot, &config(false), &mut spot).unwrap();
    assert_eq!(
        spot.available,
        ["ETH", "BTC"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );
    assert_eq!(
        spot.missing,
        ["XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );

    let mut usdm = fixture("binance_usdm_markets.json");
    let usdm = discovery_from_payload(AdapterId::BinanceUsdm, &config(false), &mut usdm).unwrap();
    assert_eq!(
        usdm.available,
        ["ETH", "BTC"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );
    assert_eq!(
        usdm.missing,
        ["XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );
}

#[test]
fn binance_discovery_ignores_unrepresentable_unrelated_markets() {
    let mut spot = r#"{
        "symbols": [
            {
                "symbol": "币安人生USDT",
                "status": "TRADING",
                "baseAsset": "币安人生",
                "quoteAsset": "USDT"
            },
            {
                "symbol": "BAD/USDT",
                "status": "TRADING",
                "baseAsset": "BAD/",
                "quoteAsset": "USDT"
            },
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT"
            }
        ]
    }"#
    .as_bytes()
    .to_vec();

    let result = discovery_from_payload(AdapterId::BinanceSpot, &config(false), &mut spot).unwrap();
    assert_eq!(result.available, [CanonicalSymbol::new("BTC", "USDT")]);
    assert_eq!(
        result.missing,
        ["ETH", "XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );

    let mut usdm = r#"{
        "symbols": [
            {
                "symbol": "币安人生USDT",
                "status": "TRADING",
                "baseAsset": "币安人生",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL"
            },
            {
                "symbol": "BTCUSDT_250926",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "contractType": "CURRENT_QUARTER"
            },
            {
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL"
            }
        ]
    }"#
    .as_bytes()
    .to_vec();

    let result = discovery_from_payload(AdapterId::BinanceUsdm, &config(false), &mut usdm).unwrap();
    assert_eq!(result.available, [CanonicalSymbol::new("ETH", "USDT")]);
    assert_eq!(
        result.missing,
        ["BTC", "XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );
}

#[test]
fn binance_discovery_still_rejects_inconsistent_representable_markets() {
    let mut payload = br#"{
        "symbols": [{
            "symbol": "BROKEN",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT"
        }]
    }"#
    .to_vec();

    let error =
        discovery_from_payload(AdapterId::BinanceSpot, &config(false), &mut payload).unwrap_err();
    assert!(matches!(
        error,
        DiscoveryError::InvalidMarket {
            adapter: AdapterId::BinanceSpot,
            market
        } if market == "BROKEN"
    ));
}

#[test]
fn strict_mode_error_names_every_missing_pair() {
    let mut payload = fixture("binance_spot_markets.json");
    let error =
        discovery_from_payload(AdapterId::BinanceSpot, &config(true), &mut payload).unwrap_err();
    let message = error.to_string();
    let DiscoveryError::MissingSymbols { missing, .. } = error else {
        panic!("expected strict-symbol error");
    };

    assert_eq!(
        missing,
        ["XRP", "SOL"].map(|base| CanonicalSymbol::new(base, "USDT"))
    );
    assert!(message.contains("XRP/USDT"));
    assert!(message.contains("SOL/USDT"));
}

#[test]
fn public_parsers_decode_all_four_fixture_formats() {
    let mut upbit = fixture("upbit_markets.json");
    let mut bithumb = fixture("bithumb_markets.json");
    let mut spot = fixture("binance_spot_markets.json");
    let mut usdm = fixture("binance_usdm_markets.json");

    assert_eq!(
        md_exchanges::upbit::parse_active_markets(&mut upbit)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        md_exchanges::bithumb::parse_active_markets(&mut bithumb)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        md_exchanges::binance_spot::parse_active_markets(&mut spot)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        md_exchanges::binance_usdm::parse_active_markets(&mut usdm)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn domestic_discovery_rejects_non_uppercase_market_codes() {
    let mut payload = br#"[{"market":"KRW-btc"}]"#.to_vec();
    let error = md_exchanges::upbit::parse_active_markets(&mut payload).unwrap_err();
    assert!(matches!(error, DiscoveryError::InvalidMarket { .. }));
}

#[test]
fn domestic_subscriptions_are_exact_and_use_uppercase_codes() {
    let pairs = vec![
        CanonicalSymbol::new("BTC", "KRW"),
        CanonicalSymbol::new("ETH", "KRW"),
    ];
    let ticket = Uuid::nil();

    assert_eq!(
        build_subscription(AdapterId::UpbitSpot, &pairs, ticket).unwrap(),
        r#"[{"ticket":"00000000-0000-0000-0000-000000000000"},{"type":"trade","codes":["KRW-BTC","KRW-ETH"]},{"type":"orderbook","codes":["KRW-BTC.30","KRW-ETH.30"]},{"format":"DEFAULT"}]"#
    );
    assert_eq!(
        build_subscription(AdapterId::BithumbSpot, &pairs, ticket).unwrap(),
        r#"[{"ticket":"00000000-0000-0000-0000-000000000000"},{"type":"trade","codes":["KRW-BTC","KRW-ETH"]},{"type":"orderbook","codes":["KRW-BTC","KRW-ETH"]},{"format":"DEFAULT"}]"#
    );
}

#[test]
fn binance_subscription_uses_encoded_raw_trade_and_depth20_query() {
    let pairs = vec![
        CanonicalSymbol::new("BTC", "USDT"),
        CanonicalSymbol::new("ETH", "USDT"),
    ];
    let query = build_subscription(AdapterId::BinanceSpot, &pairs, Uuid::nil()).unwrap();
    assert_eq!(
        query,
        "streams=btcusdt%40trade%2Fbtcusdt%40depth20%40100ms%2Fethusdt%40trade%2Fethusdt%40depth20%40100ms"
    );
    assert!(!query.contains("aggTrade"));

    let url = build_combined_stream_url("wss://stream.binance.com:9443/stream", &pairs).unwrap();
    assert_eq!(url.query().unwrap(), query);
    assert_eq!(
        url.query_pairs()
            .find(|(key, _)| key == "streams")
            .unwrap()
            .1,
        "btcusdt@trade/btcusdt@depth20@100ms/ethusdt@trade/ethusdt@depth20@100ms"
    );
}

#[test]
fn subscriptions_reject_empty_or_unsafe_symbols_and_invalid_base_urls() {
    assert!(build_subscription(AdapterId::BinanceSpot, &[], Uuid::nil()).is_err());
    assert!(
        build_subscription(
            AdapterId::BinanceSpot,
            &[CanonicalSymbol::new("BTC/ETH", "USDT")],
            Uuid::nil(),
        )
        .is_err()
    );
    assert!(
        build_combined_stream_url("not a URL", &[CanonicalSymbol::new("BTC", "USDT")],).is_err()
    );
}
