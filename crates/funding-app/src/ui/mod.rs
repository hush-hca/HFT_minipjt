pub mod bridge;
pub mod live;
pub mod model;
pub mod reducer;

use iced::widget::{button, column, container, row, scrollable, text, text_input};
use iced::{Element, Length, Subscription, Task, Theme};

use bridge::UiSnapshotSubscriber;
use model::{ControlAvailability, UiSnapshot, decimal};
use reducer::{FundingGuiState, Message, Screen};

pub fn run_gui(initial: UiSnapshot) -> iced::Result {
    run_gui_with_subscriber(initial, None)
}

pub fn run_live_gui(initial: UiSnapshot, subscriber: UiSnapshotSubscriber) -> iced::Result {
    run_gui_with_subscriber(initial, Some(subscriber))
}

fn run_gui_with_subscriber(
    initial: UiSnapshot,
    subscriber: Option<UiSnapshotSubscriber>,
) -> iced::Result {
    iced::application("HFT Market & Funding Monitor", App::update, App::view)
        .theme(|_| Theme::Dark)
        .window_size((1440.0, 900.0))
        .subscription(App::subscription)
        .run_with(|| (App::new(initial, subscriber), Task::none()))
}

struct App {
    state: FundingGuiState,
    subscriber: Option<UiSnapshotSubscriber>,
}

impl App {
    fn new(snapshot: UiSnapshot, subscriber: Option<UiSnapshotSubscriber>) -> Self {
        Self {
            state: FundingGuiState::new(snapshot),
            subscriber,
        }
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        if matches!(message, Message::PollSnapshot) {
            if let Some(subscriber) = &self.subscriber {
                let snapshot = subscriber.borrow();
                subscriber.acknowledge(snapshot.sequence);
                let _ = self.state.update(Message::Snapshot(Box::new(snapshot)));
            }
        } else {
            let _ = self.state.update(message);
        }
        Task::none()
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(std::time::Duration::from_millis(250)).map(|_| Message::PollSnapshot)
    }

    fn view(&self) -> Element<'_, Message> {
        let tabs = row![
            tab(
                "Funding Opportunities",
                Screen::Opportunities,
                self.state.screen
            ),
            tab("Market Detail", Screen::Market, self.state.screen),
            tab("Strategy & Orders", Screen::Strategy, self.state.screen),
            tab("System Health", Screen::Health, self.state.screen),
            tab("Risk & Controls", Screen::Risk, self.state.screen),
        ]
        .spacing(8);
        let header = column![
            text("손승한 · 코인 차익 · 펀비 · 마이크로피쳐 HFT").size(24),
            text("READ-ONLY MONITOR · execution remains disabled until OMS acceptance").size(14),
            tabs,
        ]
        .spacing(8);
        let content = match self.state.screen {
            Screen::Opportunities => self.opportunities(),
            Screen::Market => self.market(),
            Screen::Strategy => self.strategy(),
            Screen::Health => self.health(),
            Screen::Risk => self.risk(),
        };
        container(column![header, content].spacing(16).padding(18))
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    }

    fn opportunities(&self) -> Element<'_, Message> {
        let filter = text_input("Filter symbol (BTC, ETH, XRP…)", &self.state.filter)
            .on_input(Message::FilterChanged)
            .padding(10);
        let mut rows = column![
            row![
                fixed("TOKEN", 120),
                fixed("SHORT (high)", 220),
                fixed("LONG (low)", 220),
                fixed("GAP", 110),
                fixed("APR edge*", 130),
                fixed("NET / $1k", 140),
                fixed("FRESH", 100),
            ]
            .spacing(8)
        ]
        .spacing(6);
        for item in self.state.visible_opportunities() {
            let symbol = item.symbol.clone();
            let select = button(text(item.symbol.clone()))
                .on_press(Message::SelectSymbol(symbol))
                .width(120);
            let short = format!(
                "{}  {:+.4}% / {}h",
                item.short_venue,
                item.short_rate_ppm as f64 / 10_000.0,
                item.short_interval_secs / 3600
            );
            let long = format!(
                "{}  {:+.4}% / {}h",
                item.long_venue,
                item.long_rate_ppm as f64 / 10_000.0,
                item.long_interval_secs / 3600
            );
            let exclusion = item.exclusion.as_deref().unwrap_or("eligible");
            rows = rows.push(
                row![
                    select,
                    fixed(short, 220),
                    fixed(long, 220),
                    fixed(format!("{:+.4}%", item.raw_gap_ppm as f64 / 10_000.0), 110),
                    fixed(
                        format!(
                            "{:+.2}% display only",
                            item.indicative_apr_bps as f64 / 100.0
                        ),
                        130
                    ),
                    fixed(
                        decimal(Some(item.conservative_net_usd_micros), 1_000_000, " USD"),
                        140
                    ),
                    fixed(format!("{} ms · {exclusion}", item.freshness_ms), 180),
                ]
                .spacing(8),
            );
        }
        column![
            text("Funding-rate opportunities").size(22),
            text("* Indicative APR is display-only. Fees, slippage, basis risk and freshness gates determine eligibility."),
            filter,
            scrollable(rows).height(Length::Fill),
        ]
        .spacing(12)
        .into()
    }

    fn market(&self) -> Element<'_, Message> {
        const SCALE: i128 = 1_000_000_000_000_000_000;
        let market = &self.state.snapshot.market;
        let mut book = column![row![
            fixed("BID PRICE", 140),
            fixed("BID QTY", 120),
            fixed("ASK PRICE", 140),
            fixed("ASK QTY", 120),
        ]]
        .spacing(4);
        for index in 0..market.bids.len().max(market.asks.len()) {
            let bid = market.bids.get(index);
            let ask = market.asks.get(index);
            book = book.push(
                row![
                    fixed(decimal(bid.map(|v| v.price), SCALE, ""), 140),
                    fixed(decimal(bid.map(|v| v.quantity), SCALE, ""), 120),
                    fixed(decimal(ask.map(|v| v.price), SCALE, ""), 140),
                    fixed(decimal(ask.map(|v| v.quantity), SCALE, ""), 120),
                ]
                .spacing(8),
            );
        }
        let metrics = column![
            text(format!("{} · {}", market.symbol, market.venue)).size(22),
            metric("Mid price", decimal(market.mid_price, SCALE, "")),
            metric("Micro/WAP price", decimal(market.micro_price, SCALE, "")),
            metric("Basis", decimal(market.basis_bps, 100, "%")),
            metric("Open interest", decimal(market.open_interest, SCALE, "")),
            metric(
                "Top trader ratio",
                decimal(market.top_trader_ratio_ppm, 1_000_000, "")
            ),
            metric("CVD / tick flow", decimal(market.cvd, SCALE, "")),
            metric(
                "Snapshot OFI",
                decimal(market.order_flow_imbalance_ppm, SCALE, "")
            ),
            metric(
                "Publication → local",
                market.latency_us.map_or("—".into(), |v| format!("{v} µs"))
            ),
            metric("Freshness", format!("{} ms", market.freshness_ms)),
            text("Mid / microprice chart (latest 120 samples)").size(16),
            text(sparkline(&market.mid_history)),
            text(sparkline(&market.micro_history)),
        ]
        .spacing(8)
        .width(Length::FillPortion(2));
        row![scrollable(book).width(Length::FillPortion(3)), metrics]
            .spacing(20)
            .height(Length::Fill)
            .into()
    }

    fn strategy(&self) -> Element<'_, Message> {
        let strategy = &self.state.snapshot.strategy;
        column![
            text("Strategy & Orders").size(22),
            text(format!("State: {}", strategy.state)),
            text(format!("Reason: {}", strategy.transition_reason)),
            text("Legs / orders / partial fills / positions: —"),
            text("Predicted / confirmed funding: —"),
            text("Fee / slippage / basis / funding / total PnL: —"),
            text(format!("Reconciliation: {}", strategy.reconciliation)),
            text("EXECUTION_ENGINE_UNAVAILABLE — no order can be emitted from this GUI"),
        ]
        .spacing(12)
        .into()
    }

    fn health(&self) -> Element<'_, Message> {
        let health = &self.state.snapshot.health;
        column![
            text("System Health").size(22),
            metric("Frames / sec", health.frames_per_second.to_string()),
            metric("Events / sec", health.events_per_second.to_string()),
            metric("Features / sec", health.features_per_second.to_string()),
            metric("Orders / sec", health.orders_per_second.to_string()),
            metric("Parser failures", health.parse_failures.to_string()),
            metric(
                "Validation failures",
                health.validation_failures.to_string()
            ),
            metric("Reconnects", health.reconnects.to_string()),
            metric("Sequence gaps", health.sequence_gaps.to_string()),
            metric("Backpressure drops", health.backpressure_drops.to_string()),
            metric("Public connections", health.public_connections.clone()),
            metric("Private connections", health.private_connections.clone()),
            metric("Arrow", health.arrow_status.clone()),
            metric("SQLite", health.sqlite_status.clone()),
        ]
        .spacing(10)
        .into()
    }

    fn risk(&self) -> Element<'_, Message> {
        let risk = &self.state.snapshot.risk;
        let disabled_code = match &risk.availability {
            ControlAvailability::Enabled => "ENABLED",
            ControlAvailability::Disabled { code } => code,
        };
        column![
            text("Risk & Controls").size(22),
            text(&risk.banner),
            text(format!("Mode: {:?}", risk.mode)),
            text(format!("Control availability: {disabled_code}")),
            button("Arm testnet").on_press(Message::ArmPressed),
            button("Cancel all orders").on_press(Message::CancelAllPressed),
            button("Request close positions").on_press(Message::ClosePositionsPressed),
            button("Global kill switch").on_press(Message::KillPressed),
            text(
                self.state
                    .last_notice
                    .as_deref()
                    .unwrap_or("All controls are disabled in read-only Phase 2C.")
            ),
        ]
        .spacing(12)
        .into()
    }
}

fn tab(
    label: &'static str,
    screen: Screen,
    selected: Screen,
) -> iced::widget::Button<'static, Message> {
    let label = if screen == selected {
        format!("● {label}")
    } else {
        label.into()
    };
    button(text(label)).on_press(Message::Navigate(screen))
}

fn fixed<'a>(value: impl ToString, width: u16) -> Element<'a, Message> {
    container(text(value.to_string())).width(width).into()
}

fn metric<'a>(label: &'a str, value: String) -> Element<'a, Message> {
    row![fixed(label, 190), text(value)].spacing(12).into()
}

fn sparkline(points: &[(i64, i128)]) -> String {
    const BLOCKS: &[char] = &['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let Some(min) = points.iter().map(|(_, value)| *value).min() else {
        return "—".into();
    };
    let max = points.iter().map(|(_, value)| *value).max().unwrap_or(min);
    let span = (max - min).max(1);
    points
        .iter()
        .step_by((points.len() / 80).max(1))
        .map(|(_, value)| {
            let index = ((*value - min) * 7 / span) as usize;
            BLOCKS[index]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_five_safe_views_and_chart_data() {
        let snapshot = UiSnapshot::demo();
        assert!(!snapshot.opportunities.is_empty());
        assert_eq!(snapshot.market.bids.len(), 20);
        assert_eq!(snapshot.market.asks.len(), 20);
        assert!(!sparkline(&snapshot.market.mid_history).is_empty());
        assert!(matches!(
            snapshot.risk.availability,
            ControlAvailability::Disabled { .. }
        ));
        assert!(!snapshot.debug_text().contains("SECRET"));
    }
}
