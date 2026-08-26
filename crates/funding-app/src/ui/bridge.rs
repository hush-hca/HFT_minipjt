use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use tokio::sync::watch;

use super::model::UiSnapshot;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum UiHealthSignal {
    Healthy,
    DisarmUiHeartbeatLost,
}

#[derive(Clone)]
pub struct UiSnapshotPublisher {
    tx: watch::Sender<UiSnapshot>,
    acknowledged: Arc<AtomicU64>,
    superseded: Arc<AtomicU64>,
}

pub struct UiSnapshotSubscriber {
    rx: watch::Receiver<UiSnapshot>,
    acknowledged: Arc<AtomicU64>,
}

pub fn ui_snapshot_channel(initial: UiSnapshot) -> (UiSnapshotPublisher, UiSnapshotSubscriber) {
    let (tx, rx) = watch::channel(initial);
    let acknowledged = Arc::new(AtomicU64::new(0));
    (
        UiSnapshotPublisher {
            tx,
            acknowledged: Arc::clone(&acknowledged),
            superseded: Arc::new(AtomicU64::new(0)),
        },
        UiSnapshotSubscriber { rx, acknowledged },
    )
}

impl UiSnapshotPublisher {
    pub fn publish(&self, snapshot: UiSnapshot) {
        let previous = self.tx.borrow().sequence;
        if previous > self.acknowledged.load(Ordering::Acquire) {
            self.superseded.fetch_add(1, Ordering::Relaxed);
        }
        self.tx.send_replace(snapshot);
    }

    pub fn superseded_count(&self) -> u64 {
        self.superseded.load(Ordering::Relaxed)
    }
}

impl UiSnapshotSubscriber {
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.rx.changed().await
    }

    pub fn borrow(&self) -> UiSnapshot {
        self.rx.borrow().clone()
    }

    pub fn acknowledge(&self, sequence: u64) {
        self.acknowledged.fetch_max(sequence, Ordering::Release);
    }
}

pub fn ui_health(age: Duration, threshold: Duration) -> UiHealthSignal {
    if age > threshold {
        UiHealthSignal::DisarmUiHeartbeatLost
    } else {
        UiHealthSignal::Healthy
    }
}
