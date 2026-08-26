#![cfg(feature = "gui")]

use funding_app::ui::model::UiSnapshot;
use funding_app::ui::reducer::{FundingGuiState, Message, Screen};

#[test]
fn navigation_filter_selection_and_disabled_controls_are_deterministic() {
    let mut state = FundingGuiState::new(UiSnapshot::demo());
    state.update(Message::FilterChanged("BTC".into()));
    assert_eq!(state.visible_opportunities().count(), 1);
    state.update(Message::SelectMarket {
        symbol: "BTC/USDT".into(),
        venue: "Binance USD-M".into(),
    });
    assert_eq!(state.screen, Screen::Market);
    assert_eq!(state.selected_symbol.as_deref(), Some("BTC/USDT"));
    assert_eq!(state.selected_venue.as_deref(), Some("Binance USD-M"));
    assert!(state.update(Message::CancelAllPressed).is_none());
    assert_eq!(
        state.last_notice.as_deref(),
        Some("EXECUTION_ENGINE_UNAVAILABLE")
    );
}
