mod common;
use execution::{
    journal::{JournalError, OrderJournal},
    oms::{OmsEvent, OrderState},
};

#[test]
fn wal_restart_is_durable_and_conflicts_roll_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oms.db");
    {
        let mut j = OrderJournal::open(&path).unwrap();
        let i = common::intent("c1", 3);
        j.record_intent(&i).unwrap();
        j.apply_event(
            i.venue,
            "submitted",
            &i.client_order_id,
            &OmsEvent::Submitted,
        )
        .unwrap();
        j.apply_event(i.venue, "e1", &i.client_order_id, &OmsEvent::UnknownSubmit)
            .unwrap();
    }
    let mut j = OrderJournal::open(&path).unwrap();
    let i = common::intent("c1", 3);
    assert_eq!(
        j.order(i.venue, &i.client_order_id).unwrap().unwrap().state,
        OrderState::Reconcile
    );
    let before = j.snapshot().unwrap();
    assert!(matches!(
        j.record_intent(&common::intent("c1", 4)),
        Err(JournalError::Conflict(_))
    ));
    assert_eq!(j.snapshot().unwrap(), before);
}

#[test]
fn full_u64_sequence_and_i128_values_survive_wal_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wide.db");
    let mut i = common::intent("wide", i128::MAX);
    i.limit_price = Some(i128::MAX);
    {
        let mut j = OrderJournal::open(&path).unwrap();
        j.record_intent(&i).unwrap();
        j.apply_event(
            i.venue,
            "max-seq",
            &i.client_order_id,
            &OmsEvent::Status {
                state: OrderState::Acknowledged,
                cumulative_quantity: 0,
                source_sequence: Some(u64::MAX),
                venue_order_id: None,
            },
        )
        .unwrap();
    }
    let j = OrderJournal::open(&path).unwrap();
    let got = j.order(i.venue, &i.client_order_id).unwrap().unwrap();
    assert_eq!(got.intent.quantity, i128::MAX);
    assert_eq!(got.intent.limit_price, Some(i128::MAX));
    assert_eq!(got.last_source_sequence, Some(u64::MAX));
}
