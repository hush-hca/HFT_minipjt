#![cfg(feature = "gui")]

use funding_app::ui::model::{ControlAvailability, ModeLabel, UiSnapshot, decimal};

#[test]
fn snapshot_has_all_read_only_views_without_secrets() {
    let snapshot = UiSnapshot::demo();
    assert!(!snapshot.opportunities.is_empty());
    assert!(!snapshot.markets.is_empty());
    assert_eq!(snapshot.market.bids.len(), 20);
    assert_eq!(snapshot.market.asks.len(), 20);
    assert_eq!(snapshot.risk.mode, ModeLabel::Monitor);
    assert!(matches!(
        snapshot.strategy.availability,
        ControlAvailability::Disabled { ref code }
            if code == "EXECUTION_ENGINE_UNAVAILABLE"
    ));
    assert!(!snapshot.debug_text().contains("API_SECRET"));
    assert_eq!(decimal(None, 1_000_000, " USD"), "—");
}
