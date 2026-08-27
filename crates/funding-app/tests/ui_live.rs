#![cfg(feature = "gui")]

use collector::MarketEventObserver;
use funding_app::ui::bridge::ui_snapshot_channel;
use funding_app::ui::live::{LiveUiState, MarketSelection, SharedLiveUiObserver};
use funding_app::ui::model::UiSnapshot;
use funding_core::{
    config::{ExactDecimal, FundingConfig},
    meta::DerivativeMeta,
    public::{
        DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
        MarkIndexSnapshot,
    },
};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
    TimestampPrecision,
};
use uuid::Uuid;

#[test]
fn observer_does_not_wait_when_the_ui_aggregator_is_busy() {
    let initial = UiSnapshot::demo();
    let (publisher, subscriber) = ui_snapshot_channel(initial.clone());
    let selection = MarketSelection::new("Binance USD-M", "BTC/USDT");
    let shared = std::sync::Arc::new(std::sync::Mutex::new(LiveUiState::new(
        publisher,
        initial,
        selection,
        cost(),
    )));
    let observer = SharedLiveUiObserver::new(shared.clone());
    let busy = shared.lock().unwrap();

    observer.observe(&book(AdapterId::BinanceUsdm, "USDT"));
    drop(busy);
    shared
        .lock()
        .unwrap()
        .market(&book(AdapterId::BybitLinear, "USDT"));

    assert_eq!(subscriber.borrow().health.ui_input_drops, 1);
}

#[test]
fn core_venues_are_listed_and_selection_switches_the_detailed_market() {
    let initial = UiSnapshot::demo();
    let (publisher, subscriber) = ui_snapshot_channel(initial.clone());
    let selection = MarketSelection::new("Binance USD-M", "BTC/USDT");
    let mut state = LiveUiState::new(publisher, initial, selection.clone(), cost());

    for (adapter, quote) in [
        (AdapterId::UpbitSpot, "KRW"),
        (AdapterId::BithumbSpot, "KRW"),
        (AdapterId::BinanceSpot, "USDT"),
        (AdapterId::BinanceUsdm, "USDT"),
    ] {
        state.market(&book(adapter, quote));
    }

    let snapshot = subscriber.borrow();
    assert_eq!(snapshot.markets.len(), 4);
    assert!(snapshot.health.ui_snapshots_superseded > 0);
    assert_eq!(snapshot.market.venue, "Binance USD-M");
    assert_eq!(snapshot.market.symbol, "BTC/USDT");
    assert_eq!(snapshot.market.mid_history.len(), 1);

    selection.select("Upbit Spot", "BTC/KRW");
    state.market(&book(AdapterId::UpbitSpot, "KRW"));

    let snapshot = subscriber.borrow();
    assert_eq!(snapshot.market.venue, "Upbit Spot");
    assert_eq!(snapshot.market.symbol, "BTC/KRW");
    assert_eq!(snapshot.market.bids.len(), 1);
    assert_eq!(snapshot.market.asks.len(), 1);
}

#[test]
fn live_snapshot_contains_costed_funding_opportunity() {
    let initial = UiSnapshot::starting();
    let (publisher, subscriber) = ui_snapshot_channel(initial.clone());
    let selection = MarketSelection::new("Binance USD-M", "BTC/USDT");
    let mut state = LiveUiState::new(publisher, initial, selection, cost());
    let timestamp_us = now_us();
    let symbol = CanonicalSymbol::new("BTC", "USDT");

    for event in [
        funding(
            AdapterId::BinanceUsdm,
            &symbol,
            ExactDecimal::SCALE / 100,
            timestamp_us,
        ),
        funding(AdapterId::BybitLinear, &symbol, 0, timestamp_us),
        mark(AdapterId::BinanceUsdm, &symbol, 101, timestamp_us),
        mark(AdapterId::BybitLinear, &symbol, 100, timestamp_us),
    ] {
        state.derivative(&event);
    }
    state.market(&priced_book(
        AdapterId::BinanceUsdm,
        &symbol,
        101,
        102,
        timestamp_us,
    ));
    state.market(&priced_book(
        AdapterId::BybitLinear,
        &symbol,
        99,
        100,
        timestamp_us,
    ));

    let snapshot = subscriber.borrow();
    let row = snapshot.opportunities.first().unwrap();
    assert_eq!(row.symbol, "BTC/USDT");
    assert_eq!(row.exclusion, None);
    assert!(
        row.conservative_net_usd_micros
            .is_some_and(|value| value > 0)
    );
    assert!(row.capacity_usd_micros.is_some());
}

fn cost() -> funding_core::config::CostConfig {
    FundingConfig::load(std::path::Path::new("../../config/funding.toml"))
        .unwrap()
        .cost
}

fn book(adapter: AdapterId, quote: &str) -> NormalizedEvent {
    let now_us = now_us();
    NormalizedEvent::Book(BookSnapshot {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter,
            symbol: CanonicalSymbol::new("BTC", quote),
            source_symbol: format!("BTC{quote}"),
            source_stream: "depth20".into(),
            source_sequence: None,
            exchange_event_ts_us: Some(now_us - 1_000),
            exchange_trade_ts_us: None,
            event_ts_precision: TimestampPrecision::Microsecond,
            trade_ts_precision: TimestampPrecision::Unavailable,
            local_recv_ts_us: now_us,
            raw_size_bytes: 100,
        },
        bids: vec![PriceLevel {
            price: 100_000_000_000_000_000_000,
            quantity: 1_000_000_000_000_000_000,
        }],
        asks: vec![PriceLevel {
            price: 101_000_000_000_000_000_000,
            quantity: 2_000_000_000_000_000_000,
        }],
    })
}

fn funding(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    rate: i128,
    timestamp_us: i64,
) -> DerivativeEvent {
    DerivativeEvent::FundingEstimate(FundingEstimate {
        meta: derivative_meta(venue, symbol, timestamp_us),
        rate,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        next_funding_ts_us: timestamp_us + 1_000_000,
    })
}

fn mark(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    price: i128,
    timestamp_us: i64,
) -> DerivativeEvent {
    DerivativeEvent::MarkIndex(MarkIndexSnapshot {
        meta: derivative_meta(venue, symbol, timestamp_us),
        mark_price: price * ExactDecimal::SCALE,
        index_price: 100 * ExactDecimal::SCALE,
    })
}

fn derivative_meta(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    timestamp_us: i64,
) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol: symbol.clone(),
        venue_symbol: format!("{}{}", symbol.base, symbol.quote),
        source_ts_us: Some(timestamp_us - 100),
        source_ts_precision: TimestampPrecision::Microsecond,
        local_recv_ts_us: timestamp_us - 50,
    }
}

fn priced_book(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    bid: i128,
    ask: i128,
    timestamp_us: i64,
) -> NormalizedEvent {
    NormalizedEvent::Book(BookSnapshot {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter: venue,
            symbol: symbol.clone(),
            source_symbol: format!("{}{}", symbol.base, symbol.quote),
            source_stream: "depth20".into(),
            source_sequence: None,
            exchange_event_ts_us: Some(timestamp_us - 100),
            exchange_trade_ts_us: None,
            event_ts_precision: TimestampPrecision::Microsecond,
            trade_ts_precision: TimestampPrecision::Unavailable,
            local_recv_ts_us: timestamp_us - 50,
            raw_size_bytes: 100,
        },
        bids: vec![PriceLevel {
            price: bid * ExactDecimal::SCALE,
            quantity: 2 * ExactDecimal::SCALE,
        }],
        asks: vec![PriceLevel {
            price: ask * ExactDecimal::SCALE,
            quantity: 2 * ExactDecimal::SCALE,
        }],
    })
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap())
        .unwrap()
}
