#![cfg(feature = "gui")]

use funding_app::ui::opportunity::LiveOpportunityEngine;
use funding_core::{
    config::{ExactDecimal, FundingConfig},
    meta::DerivativeMeta,
    public::{
        DerivativeEvent, FundingBasis, FundingEstimate, FundingIntervalProvenance, FundingRateKind,
        MarkIndexSnapshot,
    },
};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, PriceLevel, TimestampPrecision,
};
use uuid::Uuid;

const ONE: i128 = ExactDecimal::SCALE;
const DECISION_US: i64 = 1_800_000_000_000_000;

#[test]
fn live_engine_uses_exact_cost_and_capacity_evaluation() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let mut engine = engine();
    for event in evidence(&symbol) {
        engine.observe(&event);
    }

    let binance = book(AdapterId::BinanceUsdm, &symbol, 101, 102);
    let bybit = book(AdapterId::BybitLinear, &symbol, 99, 100);
    let row = engine.evaluate(&symbol, Some(&binance), Some(&bybit), DECISION_US);

    assert_eq!(row.short_venue, "Binance USD-M");
    assert_eq!(row.long_venue, "Bybit Linear");
    assert_eq!(row.exclusion, None);
    assert!(
        row.conservative_net_usd_micros
            .is_some_and(|value| value > 0)
    );
    assert!(
        row.capacity_usd_micros
            .is_some_and(|value| (99_000_000..=100_000_000).contains(&value))
    );
}

#[test]
fn live_engine_rejects_missing_and_stale_evidence_without_inventing_values() {
    let symbol = CanonicalSymbol::new("ETH", "USDT");
    let mut engine = engine();
    let missing = engine.evaluate(&symbol, None, None, DECISION_US);
    assert_eq!(missing.exclusion.as_deref(), Some("MISSING_FUNDING"));
    assert_eq!(missing.short_venue, "UNAVAILABLE");
    assert_eq!(missing.long_venue, "UNAVAILABLE");
    assert_eq!(missing.conservative_net_usd_micros, None);

    for event in evidence(&symbol) {
        engine.observe(&event);
    }
    let binance = book(AdapterId::BinanceUsdm, &symbol, 101, 102);
    let bybit = book(AdapterId::BybitLinear, &symbol, 99, 100);
    let stale = engine.evaluate(
        &symbol,
        Some(&binance),
        Some(&bybit),
        DECISION_US + 2_000_001,
    );
    assert_eq!(stale.exclusion.as_deref(), Some("STALE_FUNDING"));
    assert_eq!(stale.conservative_net_usd_micros, None);
    assert_eq!(stale.capacity_usd_micros, None);
}

fn engine() -> LiveOpportunityEngine {
    let config = FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    LiveOpportunityEngine::new(config.cost)
}

fn evidence(symbol: &CanonicalSymbol) -> Vec<DerivativeEvent> {
    vec![
        funding(AdapterId::BinanceUsdm, symbol, ONE / 100),
        funding(AdapterId::BybitLinear, symbol, 0),
        mark(AdapterId::BinanceUsdm, symbol, 101),
        mark(AdapterId::BybitLinear, symbol, 100),
    ]
}

fn funding(venue: AdapterId, symbol: &CanonicalSymbol, rate: i128) -> DerivativeEvent {
    DerivativeEvent::FundingEstimate(FundingEstimate {
        meta: derivative_meta(venue, symbol),
        rate,
        rate_kind: FundingRateKind::IndicativeNext,
        basis: FundingBasis::MarkNotional,
        interval_secs: 28_800,
        interval_provenance: FundingIntervalProvenance::VenuePayload,
        next_funding_ts_us: DECISION_US + 1_000_000,
    })
}

fn mark(venue: AdapterId, symbol: &CanonicalSymbol, price: i128) -> DerivativeEvent {
    DerivativeEvent::MarkIndex(MarkIndexSnapshot {
        meta: derivative_meta(venue, symbol),
        mark_price: price * ONE,
        index_price: 100 * ONE,
    })
}

fn derivative_meta(venue: AdapterId, symbol: &CanonicalSymbol) -> DerivativeMeta {
    DerivativeMeta {
        schema_version: 1,
        event_id: Uuid::now_v7(),
        venue,
        symbol: symbol.clone(),
        venue_symbol: format!("{}{}", symbol.base, symbol.quote),
        source_ts_us: Some(DECISION_US - 100),
        source_ts_precision: TimestampPrecision::Microsecond,
        local_recv_ts_us: DECISION_US - 50,
    }
}

fn book(venue: AdapterId, symbol: &CanonicalSymbol, bid: i128, ask: i128) -> BookSnapshot {
    BookSnapshot {
        meta: EventMeta {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            adapter: venue,
            symbol: symbol.clone(),
            source_symbol: format!("{}{}", symbol.base, symbol.quote),
            source_stream: "depth20".into(),
            source_sequence: None,
            exchange_event_ts_us: Some(DECISION_US - 100),
            exchange_trade_ts_us: None,
            event_ts_precision: TimestampPrecision::Microsecond,
            trade_ts_precision: TimestampPrecision::Unavailable,
            local_recv_ts_us: DECISION_US - 50,
            raw_size_bytes: 100,
        },
        bids: vec![PriceLevel {
            price: bid * ONE,
            quantity: 2 * ONE,
        }],
        asks: vec![PriceLevel {
            price: ask * ONE,
            quantity: 2 * ONE,
        }],
    }
}
