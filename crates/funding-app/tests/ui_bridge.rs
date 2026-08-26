#![cfg(feature = "gui")]

use std::time::Duration;

use funding_app::ui::bridge::{UiHealthSignal, ui_health, ui_snapshot_channel};
use funding_app::ui::model::UiSnapshot;

#[tokio::test]
async fn slow_renderer_receives_latest_without_blocking_publisher() {
    let (publisher, mut subscriber) = ui_snapshot_channel(UiSnapshot::demo());
    for sequence in 1..=10_000 {
        let mut snapshot = UiSnapshot::demo();
        snapshot.sequence = sequence;
        publisher.publish(snapshot);
    }
    subscriber.changed().await.unwrap();
    assert_eq!(subscriber.borrow().sequence, 10_000);
    assert_eq!(publisher.superseded_count(), 9_999);
    subscriber.acknowledge(10_000);
}

#[test]
fn lost_heartbeat_disarms_future_execution_only() {
    assert_eq!(
        ui_health(Duration::from_secs(3), Duration::from_secs(2)),
        UiHealthSignal::DisarmUiHeartbeatLost
    );
}
