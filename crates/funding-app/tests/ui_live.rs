#![cfg(feature = "gui")]

use collector::MarketEventObserver;
use funding_app::ui::bridge::ui_snapshot_channel;
use funding_app::ui::live::{LiveUiState, MarketSelection, SharedLiveUiObserver};
use funding_app::ui::model::UiSnapshot;
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, NormalizedEvent, PriceLevel,
    TimestampPrecision,
};
use uuid::Uuid;

#[test]
fn observer_does_not_wait_when_the_ui_aggregator_is_busy() {
    let initial = UiSnapshot::demo();
    let (publisher, _subscriber) = ui_snapshot_channel(initial.clone());
    let selection = MarketSelection::new("Binance USD-M", "BTC/USDT");
    let shared = std::sync::Arc::new(std::sync::Mutex::new(LiveUiState::new(
        publisher, initial, selection,
    )));
    let observer = SharedLiveUiObserver::new(shared.clone());
    let _busy = shared.lock().unwrap();

    observer.observe(&book(AdapterId::BinanceUsdm, "USDT"));
}

#[test]
fn core_venues_are_listed_and_selection_switches_the_detailed_market() {
    let initial = UiSnapshot::demo();
    let (publisher, subscriber) = ui_snapshot_channel(initial.clone());
    let selection = MarketSelection::new("Binance USD-M", "BTC/USDT");
    let mut state = LiveUiState::new(publisher, initial, selection.clone());

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

fn book(adapter: AdapterId, quote: &str) -> NormalizedEvent {
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap())
        .unwrap();
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
