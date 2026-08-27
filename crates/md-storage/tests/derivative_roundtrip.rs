use std::fs::File;
use std::io::BufReader;
use std::time::Duration;

use arrow_array::{Array, Decimal128Array, StringArray};
use arrow_ipc::reader::StreamReader;
use funding_core::instrument::{
    AccountMode, ContractKind, FundingRateBoundsProvenance, InstrumentSpec, PositionMode,
};
use funding_core::meta::DerivativeMeta;
use funding_core::public::{
    DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
    FundingSettlement, MarkIndexSnapshot, OpenInterestSnapshot, OpenInterestUnit,
    QuoteConversionSnapshot, QuoteSide, TraderMetricKind, TraderRatioSnapshot,
};
use md_core::model::{AdapterId, CanonicalSymbol, TimestampPrecision};
use md_storage::{DerivativePartitionRouter, StorageConfig, validate_path};
use tempfile::tempdir;
use uuid::Uuid;

const HOUR_US: i64 = 1_725_930_000_000_000;

fn meta(source_ts_us: Option<i64>) -> DerivativeMeta {
    meta_for(
        AdapterId::BinanceUsdm,
        "BTC",
        "USDT",
        "BTCUSDT",
        source_ts_us,
    )
}

fn meta_for(
    venue: AdapterId,
    base: &str,
    quote: &str,
    venue_symbol: &str,
    source_ts_us: Option<i64>,
) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol: CanonicalSymbol::new(base, quote),
        venue_symbol: venue_symbol.into(),
        source_ts_us,
        source_ts_precision: source_ts_us.map_or(TimestampPrecision::Unavailable, |_| {
            TimestampPrecision::Millisecond
        }),
        local_recv_ts_us: HOUR_US + 10,
    }
}

#[tokio::test]
async fn derivative_events_round_trip_by_family_and_utc_hour() {
    let root = tempdir().unwrap();
    let mut router = DerivativePartitionRouter::open(StorageConfig {
        output_root: root.path().into(),
        batch_rows: 100,
        flush_interval: Duration::from_secs(60),
    })
    .unwrap();
    router
        .push(DerivativeEvent::FundingEstimate(FundingEstimate {
            meta: meta(Some(HOUR_US + 1)),
            rate: 100_000_000_000_000,
            rate_kind: FundingRateKind::IndicativeNext,
            basis: FundingBasis::MarkNotional,
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            next_funding_ts_us: HOUR_US + 28_800_000_000,
        }))
        .await
        .unwrap();
    router
        .push(DerivativeEvent::OpenInterest(OpenInterestSnapshot {
            meta: meta(Some(HOUR_US + 2)),
            open_interest: 42_000_000_000_000_000_000,
            unit: OpenInterestUnit::Contracts,
            quote_notional: Some(2_500_000_000_000_000_000_000_000),
        }))
        .await
        .unwrap();
    router
        .push(DerivativeEvent::Instrument(Box::new(InstrumentSpec {
            meta: meta(Some(HOUR_US + 3)),
            contract_kind: ContractKind::Perpetual,
            settlement_asset: "USDT".into(),
            contract_multiplier: 1_000_000_000_000_000_000,
            tick_size: 100_000_000_000_000,
            quantity_step: 1_000_000_000_000_000,
            min_quantity: 1_000_000_000_000_000,
            max_quantity: Some(10_000_000_000_000_000_000),
            min_notional: 5_000_000_000_000_000_000,
            funding_interval_secs: 28_800,
            funding_interval_provenance: FundingIntervalProvenance::InstrumentRule,
            funding_rate_floor: Some(-30_000_000_000_000_000),
            funding_rate_cap: Some(30_000_000_000_000_000),
            funding_rate_bounds_provenance: FundingRateBoundsProvenance::VenueFundingInfo,
            price_lower_bound: Some(1_000_000_000_000_000_000),
            price_upper_bound: Some(1_000_000_000_000_000_000_000_000),
            supported_position_modes: vec![PositionMode::OneWay, PositionMode::Hedge],
            supported_account_modes: vec![AccountMode::Classic, AccountMode::Unified],
        })))
        .await
        .unwrap();
    router
        .push(DerivativeEvent::MarkIndex(MarkIndexSnapshot {
            meta: meta(Some(HOUR_US + 4)),
            mark_price: 60_000_000_000_000_000_000_000,
            index_price: 59_999_000_000_000_000_000_000,
        }))
        .await
        .unwrap();
    router
        .push(DerivativeEvent::FundingSettlement(FundingSettlement {
            meta: meta(Some(HOUR_US + 5)),
            rate: -50_000_000_000_000,
            rate_kind: FundingRateKind::SettledActual,
            basis: FundingBasis::MarkNotional,
            interval_secs: 28_800,
            interval_provenance: FundingIntervalProvenance::VenuePayload,
            settlement_ts_us: HOUR_US + 5,
        }))
        .await
        .unwrap();
    router
        .push(DerivativeEvent::TraderRatio(TraderRatioSnapshot {
            meta: meta(Some(HOUR_US + 6)),
            metric_kind: TraderMetricKind::BinanceTopAccountRatio,
            long_ratio: 550_000_000_000_000_000,
            short_ratio: 450_000_000_000_000_000,
            long_short_ratio: 1_222_222_222_222_222_222,
        }))
        .await
        .unwrap();
    router
        .push(DerivativeEvent::QuoteConversion(QuoteConversionSnapshot {
            meta: meta_for(
                AdapterId::UpbitSpot,
                "USDT",
                "KRW",
                "KRW-USDT",
                Some(HOUR_US + 7),
            ),
            side: QuoteSide::Bid,
            price: 1_350_000_000_000_000_000_000,
            executable_quantity: 10_000_000_000_000_000_000,
        }))
        .await
        .unwrap();
    router.shutdown().await.unwrap();

    let report = validate_path(root.path()).unwrap();
    assert!(report.is_valid(), "{:#?}", report.errors);
    assert_eq!(report.files, 7);
    let funding = root.path().join(
        "derivatives/funding_estimate/binance/usdm_futures/BTC-USDT/2024-09-10/01/funding_estimate.arrow",
    );
    let oi = root.path().join(
        "derivatives/open_interest/binance/usdm_futures/BTC-USDT/2024-09-10/01/open_interest.arrow",
    );
    assert!(funding.exists());
    assert!(oi.exists());

    let mut funding_reader =
        StreamReader::try_new(BufReader::new(File::open(funding).unwrap()), None).unwrap();
    let batch = funding_reader.next().unwrap().unwrap();
    assert_eq!(
        batch.schema().metadata().get("event_family").unwrap(),
        "funding_estimate"
    );
    let rate = batch
        .column_by_name("rate")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(rate.value(0), 100_000_000_000_000);
    let provenance = batch
        .column_by_name("interval_provenance")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(provenance.value(0), "venue_payload");

    let mut oi_reader =
        StreamReader::try_new(BufReader::new(File::open(oi).unwrap()), None).unwrap();
    let batch = oi_reader.next().unwrap().unwrap();
    let notional = batch
        .column_by_name("quote_notional")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(!notional.is_null(0));
    assert_eq!(notional.value(0), 2_500_000_000_000_000_000_000_000);
}

#[tokio::test]
async fn invalid_event_does_not_mutate_the_pending_batch() {
    let root = tempdir().unwrap();
    let mut router = DerivativePartitionRouter::open(StorageConfig {
        output_root: root.path().into(),
        batch_rows: 100,
        flush_interval: Duration::from_secs(60),
    })
    .unwrap();
    let invalid = DerivativeEvent::FundingEstimate(FundingEstimate {
        meta: meta(Some(HOUR_US + 1)),
        rate: 1,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 0,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        next_funding_ts_us: HOUR_US + 1,
    });
    assert!(router.push(invalid).await.is_err());
    router.shutdown().await.unwrap();
    assert_eq!(validate_path(root.path()).unwrap().files, 0);
}

#[tokio::test]
async fn zero_long_and_long_short_ratios_round_trip_and_validate() {
    let root = tempdir().unwrap();
    let mut router = DerivativePartitionRouter::open(StorageConfig {
        output_root: root.path().into(),
        batch_rows: 10,
        flush_interval: Duration::from_secs(1),
    })
    .unwrap();
    router
        .push(DerivativeEvent::TraderRatio(TraderRatioSnapshot {
            meta: meta(Some(HOUR_US + 1)),
            metric_kind: TraderMetricKind::BinanceTopAccountRatio,
            long_ratio: 0,
            short_ratio: 1_000_000_000_000_000_000,
            long_short_ratio: 0,
        }))
        .await
        .unwrap();
    router.shutdown().await.unwrap();
    let report = validate_path(root.path()).unwrap();
    assert!(report.is_valid(), "{:#?}", report.errors);
}
