use std::collections::BTreeMap;
use std::time::Instant;

use funding_core::config::ExactDecimal;
use funding_core::public::DerivativeEvent;
use funding_features::{book::compute_book_features, flow::TradeWindow};
use md_core::model::{AdapterId, BookSnapshot, NormalizedEvent};

use super::bridge::UiSnapshotPublisher;
use super::model::{BookLevel, OpportunityRow, UiSnapshot};

pub struct LiveUiState {
    snapshot: UiSnapshot,
    publisher: UiSnapshotPublisher,
    funding: BTreeMap<(String, u8), (i128, u32, u64)>,
    started: Instant,
    market_events: u64,
    derivative_events: u64,
    previous_book: Option<BookSnapshot>,
    trade_window: TradeWindow,
}

impl LiveUiState {
    pub fn new(publisher: UiSnapshotPublisher, initial: UiSnapshot) -> Self {
        Self {
            snapshot: initial,
            publisher,
            funding: BTreeMap::new(),
            started: Instant::now(),
            market_events: 0,
            derivative_events: 0,
            previous_book: None,
            trade_window: TradeWindow::new(5_000_000).expect("positive flow horizon"),
        }
    }

    pub fn market(&mut self, event: &NormalizedEvent) {
        self.market_events = self.market_events.saturating_add(1);
        match event {
            NormalizedEvent::Book(book)
                if book.meta.symbol.base == "BTC"
                    && book.meta.symbol.quote == "USDT"
                    && book.meta.adapter == AdapterId::BinanceUsdm =>
            {
                self.update_book(book);
            }
            NormalizedEvent::Trade(trade)
                if trade.meta.symbol.base == "BTC"
                    && trade.meta.symbol.quote == "USDT"
                    && trade.meta.adapter == AdapterId::BinanceUsdm
                    && self.trade_window.push(trade).is_ok() =>
            {
                let flow = self.trade_window.snapshot(trade.meta.local_recv_ts_us);
                self.snapshot.market.cvd = Some(flow.cumulative_volume_delta.scaled());
            }
            _ => {}
        }
        self.publish();
    }

    pub fn derivative(&mut self, event: &DerivativeEvent) {
        self.derivative_events = self.derivative_events.saturating_add(1);
        let meta = event.meta();
        let symbol = format!("{}/{}", meta.symbol.base, meta.symbol.quote);
        match event {
            DerivativeEvent::FundingEstimate(value) => {
                self.funding.insert(
                    (symbol, venue_key(meta.venue)),
                    (
                        value.rate,
                        value.interval_secs,
                        age_ms(meta.local_recv_ts_us),
                    ),
                );
                self.rebuild_opportunities();
            }
            DerivativeEvent::MarkIndex(value)
                if meta.symbol.base == "BTC" && meta.symbol.quote == "USDT" =>
            {
                self.snapshot.market.basis_bps = value
                    .mark_price
                    .checked_sub(value.index_price)
                    .and_then(|delta| delta.checked_mul(10_000))
                    .and_then(|scaled| scaled.checked_div(value.index_price));
            }
            DerivativeEvent::OpenInterest(value)
                if meta.symbol.base == "BTC" && meta.symbol.quote == "USDT" =>
            {
                self.snapshot.market.open_interest = Some(value.open_interest);
            }
            DerivativeEvent::TraderRatio(value)
                if meta.symbol.base == "BTC" && meta.symbol.quote == "USDT" =>
            {
                self.snapshot.market.top_trader_ratio_ppm =
                    value.long_short_ratio.checked_div(1_000_000_000_000);
            }
            _ => {}
        }
        self.publish();
    }

    fn update_book(&mut self, book: &BookSnapshot) {
        self.snapshot.market.symbol =
            format!("{}/{}", book.meta.symbol.base, book.meta.symbol.quote);
        self.snapshot.market.venue = format!("{:?}", book.meta.adapter);
        self.snapshot.market.bids = book
            .bids
            .iter()
            .take(20)
            .map(|level| BookLevel {
                price: level.price,
                quantity: level.quantity,
            })
            .collect();
        self.snapshot.market.asks = book
            .asks
            .iter()
            .take(20)
            .map(|level| BookLevel {
                price: level.price,
                quantity: level.quantity,
            })
            .collect();
        let features = compute_book_features(
            self.previous_book.as_ref(),
            book,
            ExactDecimal::from_scaled(ExactDecimal::SCALE).expect("one base unit is representable"),
            book.meta.local_recv_ts_us,
            5_000_000,
        );
        let mid = features.mid.map(ExactDecimal::scaled);
        let micro = features.microprice.map(ExactDecimal::scaled);
        self.snapshot.market.mid_price = mid;
        self.snapshot.market.micro_price = micro;
        self.snapshot.market.order_flow_imbalance_ppm =
            features.snapshot_ofi.map(ExactDecimal::scaled);
        if let Some(value) = mid {
            append_point(
                &mut self.snapshot.market.mid_history,
                book.meta.local_recv_ts_us,
                value,
            );
        }
        if let Some(value) = micro {
            append_point(
                &mut self.snapshot.market.micro_history,
                book.meta.local_recv_ts_us,
                value,
            );
        }
        self.snapshot.market.latency_us = book
            .meta
            .exchange_event_ts_us
            .map(|source| book.meta.local_recv_ts_us.saturating_sub(source));
        self.snapshot.market.freshness_ms = age_ms(book.meta.local_recv_ts_us);
        self.previous_book = Some(book.clone());
    }

    fn rebuild_opportunities(&mut self) {
        let mut by_symbol = BTreeMap::<String, Vec<(u8, i128, u32, u64)>>::new();
        for ((symbol, venue), (rate, interval, age)) in &self.funding {
            by_symbol
                .entry(symbol.clone())
                .or_default()
                .push((*venue, *rate, *interval, *age));
        }
        let mut rows = Vec::new();
        for (symbol, mut values) in by_symbol {
            if values.len() < 2 {
                continue;
            }
            values.sort_by_key(|(_, rate, _, _)| *rate);
            let long = values.first().copied().expect("two funding values");
            let short = values.last().copied().expect("two funding values");
            let gap = short.1.saturating_sub(long.1);
            let gap_ppm = gap / 1_000_000_000_000;
            let settlements_per_year = 31_536_000_i128 / i128::from(short.2.max(long.2));
            let apr_bps = gap_ppm.saturating_mul(settlements_per_year) / 100;
            rows.push(OpportunityRow {
                symbol,
                short_venue: venue_name(short.0).into(),
                short_rate_ppm: short.1 / 1_000_000_000_000,
                short_interval_secs: short.2,
                long_venue: venue_name(long.0).into(),
                long_rate_ppm: long.1 / 1_000_000_000_000,
                long_interval_secs: long.2,
                raw_gap_ppm: gap_ppm,
                indicative_apr_bps: apr_bps,
                conservative_net_usd_micros: 0,
                capacity_usd_micros: 0,
                freshness_ms: short.3.max(long.3),
                exclusion: Some("COST_MODEL_PENDING".into()),
            });
        }
        rows.sort_by_key(|row| std::cmp::Reverse(row.raw_gap_ppm));
        self.snapshot.opportunities = rows;
    }

    fn publish(&mut self) {
        self.snapshot.sequence = self.snapshot.sequence.saturating_add(1);
        self.snapshot.generated_at_us = now_us();
        let elapsed = self.started.elapsed().as_secs().max(1);
        self.snapshot.health.frames_per_second = self.market_events / elapsed;
        self.snapshot.health.events_per_second =
            self.market_events.saturating_add(self.derivative_events) / elapsed;
        self.snapshot.health.features_per_second = self.derivative_events / elapsed;
        self.snapshot.health.public_connections = "RECEIVING".into();
        self.snapshot.health.arrow_status = "WRITING".into();
        self.publisher.publish(self.snapshot.clone());
    }
}

fn append_point(points: &mut Vec<(i64, i128)>, timestamp: i64, value: i128) {
    if points.last().is_some_and(|(last, _)| *last == timestamp) {
        if let Some(last) = points.last_mut() {
            *last = (timestamp, value);
        }
    } else {
        points.push((timestamp, value));
    }
    if points.len() > 3_600 {
        points.drain(..points.len() - 3_600);
    }
}

fn venue_key(venue: AdapterId) -> u8 {
    match venue {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}

fn venue_name(venue: u8) -> &'static str {
    match venue {
        3 => "Binance USD-M",
        4 => "Bybit Linear",
        _ => "Unsupported venue",
    }
}

fn age_ms(timestamp_us: i64) -> u64 {
    u64::try_from(now_us().saturating_sub(timestamp_us).max(0) / 1_000).unwrap_or(u64::MAX)
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| i64::try_from(value.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}
