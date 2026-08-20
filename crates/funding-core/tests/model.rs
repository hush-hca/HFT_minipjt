use funding_core::{
    config::FundingConfig,
    instrument::{AccountMode, ContractKind, InstrumentSpec, PositionMode},
    meta::DerivativeMeta,
    public::{DerivativeEvent, FundingBasis, FundingEstimate, FundingRateKind},
};
use md_core::model::{AdapterId, CanonicalSymbol, DECIMAL_SCALE, TimestampPrecision};
use uuid::Uuid;

#[test]
fn funding_config_and_types_preserve_venue_semantics() {
    let cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    assert_eq!(cfg.assets.len(), 20);
    assert_eq!(cfg.assets.first().unwrap(), "BTC");
    assert_eq!(cfg.assets.last().unwrap(), "OP");
    assert_eq!(
        cfg.venues.keys().cloned().collect::<Vec<_>>(),
        ["binance_usdm", "bybit_linear"]
    );
    assert_eq!(cfg.quote_conversions[0].base, "USDT");
    assert_eq!(cfg.quote_conversions[0].quote, "KRW");
    assert_eq!(
        cfg.quote_conversions[0].venues,
        ["upbit_spot", "bithumb_spot"]
    );
    assert_eq!(DECIMAL_SCALE, 18);

    let spec = test_usdt_perpetual(AdapterId::BybitLinear, CanonicalSymbol::new("BTC", "USDT"));
    assert_eq!(spec.contract_kind, ContractKind::Perpetual);
    assert_eq!(spec.contract_multiplier, 1_000_000_000_000_000_000);

    let event = DerivativeEvent::FundingEstimate(FundingEstimate {
        meta: derivative_meta(
            AdapterId::BinanceUsdm,
            CanonicalSymbol::new("BTC", "USDT"),
            "BTCUSDT",
        ),
        rate: 100_000_000_000_000,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        next_funding_ts_us: 1_800_000_000_000_000,
    });
    assert!(matches!(event, DerivativeEvent::FundingEstimate(_)));
    assert_eq!(event.meta().schema_version, 1);
    assert_eq!(
        event.meta().source_ts_precision,
        TimestampPrecision::Millisecond
    );
}

#[test]
fn funding_config_rejects_duplicate_assets_zero_limits_and_insecure_remote_urls() {
    let cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();

    let mut duplicate = cfg.clone();
    duplicate.assets.push("BTC".into());
    assert!(duplicate.validate().is_err());

    let mut zero_capacity = cfg.clone();
    zero_capacity.channel_capacity = 0;
    assert!(zero_capacity.validate().is_err());

    let mut insecure = cfg;
    insecure
        .venues
        .get_mut("binance_usdm")
        .unwrap()
        .mainnet
        .rest_url = "http://example.com".into();
    assert!(insecure.validate().is_err());
}

#[test]
fn funding_config_rejects_query_secrets_and_unknown_credential_keys() {
    let mut cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    cfg.venues.get_mut("binance_usdm").unwrap().mainnet.rest_url =
        "https://example.com/api?api_key=secret".into();
    assert!(cfg.validate().is_err());

    let source = std::fs::read_to_string("../../config/funding.toml").unwrap();
    let path = std::env::temp_dir().join(format!(
        "funding-config-unknown-secret-{}.toml",
        std::process::id()
    ));
    std::fs::write(&path, format!("{source}\napi_key = \"secret\"\n")).unwrap();
    let result = FundingConfig::load(&path);
    let _ = std::fs::remove_file(path);
    assert!(result.is_err());
}

#[test]
fn funding_config_validates_quote_conversions_and_polling_floors() {
    let cfg = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();

    let mut same_asset = cfg.clone();
    same_asset.quote_conversions[0].quote = "USDT".into();
    assert!(same_asset.validate().is_err());

    let mut duplicate_pair = cfg.clone();
    duplicate_pair
        .quote_conversions
        .push(duplicate_pair.quote_conversions[0].clone());
    assert!(duplicate_pair.validate().is_err());

    let mut duplicate_venue = cfg.clone();
    duplicate_venue.quote_conversions[0]
        .venues
        .push("upbit_spot".into());
    assert!(duplicate_venue.validate().is_err());

    let mut unsupported_venue = cfg.clone();
    unsupported_venue.quote_conversions[0].venues = vec!["binance_spot".into()];
    assert!(unsupported_venue.validate().is_err());

    let mut too_fast = cfg;
    too_fast.poll.instrument_secs = 899;
    assert!(too_fast.validate().is_err());
    too_fast.poll.instrument_secs = 900;
    too_fast.poll.funding_metadata_secs = 899;
    assert!(too_fast.validate().is_err());
    too_fast.poll.funding_metadata_secs = 900;
    too_fast.poll.open_interest_secs = 4;
    assert!(too_fast.validate().is_err());
    too_fast.poll.open_interest_secs = 5;
    too_fast.poll.trader_ratio_secs = 299;
    assert!(too_fast.validate().is_err());
}

#[test]
fn partition_timestamp_prefers_a_positive_source_timestamp() {
    let event = DerivativeEvent::FundingEstimate(FundingEstimate {
        meta: derivative_meta(
            AdapterId::BinanceUsdm,
            CanonicalSymbol::new("ETH", "USDT"),
            "ETHUSDT",
        ),
        rate: 0,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        next_funding_ts_us: 1_800_000_000_000_000,
    });
    assert_eq!(event.partition_ts_us(), 1_799_999_999_000_000);

    let mut missing_source = match event {
        DerivativeEvent::FundingEstimate(value) => value,
        _ => unreachable!(),
    };
    missing_source.meta.source_ts_us = Some(1);
    assert_eq!(
        DerivativeEvent::FundingEstimate(missing_source.clone()).partition_ts_us(),
        1_799_999_999_100_000
    );

    missing_source.meta.source_ts_us = Some(1_799_999_999_000_000);
    missing_source.meta.local_recv_ts_us = 0;
    assert_eq!(
        DerivativeEvent::FundingEstimate(missing_source).partition_ts_us(),
        0
    );
}

fn test_usdt_perpetual(venue: AdapterId, symbol: CanonicalSymbol) -> InstrumentSpec {
    InstrumentSpec {
        meta: derivative_meta(venue, symbol, "BTCUSDT"),
        contract_kind: ContractKind::Perpetual,
        settlement_asset: "USDT".into(),
        contract_multiplier: 1_000_000_000_000_000_000,
        tick_size: 100_000_000_000_000,
        quantity_step: 1_000_000_000_000_000,
        min_quantity: 1_000_000_000_000_000,
        max_quantity: Some(1_000_000_000_000_000_000_000),
        min_notional: 5_000_000_000_000_000_000,
        funding_interval_secs: 28_800,
        price_lower_bound: None,
        price_upper_bound: None,
        supported_position_modes: vec![PositionMode::OneWay, PositionMode::Hedge],
        supported_account_modes: vec![AccountMode::Classic, AccountMode::Unified],
    }
}

fn derivative_meta(
    venue: AdapterId,
    symbol: CanonicalSymbol,
    venue_symbol: &str,
) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol,
        venue_symbol: venue_symbol.into(),
        source_ts_us: Some(1_799_999_999_000_000),
        source_ts_precision: TimestampPrecision::Millisecond,
        local_recv_ts_us: 1_799_999_999_100_000,
    }
}
