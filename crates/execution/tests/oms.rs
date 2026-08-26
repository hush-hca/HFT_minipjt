mod common;
use execution::oms::{CanonicalOrder, OmsError, OmsEvent, OrderState, reduce_order};
use funding_core::execution::{ExecutionFill, FillId};

fn fill(id: &str, qty: i128) -> ExecutionFill {
    ExecutionFill {
        venue: md_core::model::AdapterId::BinanceUsdm,
        client_order_id: funding_core::execution::ClientOrderId("c1".into()),
        venue_order_id: None,
        fill_id: FillId(id.into()),
        price: 100,
        quantity: qty,
        fee: 1,
        fee_asset: "USDT".into(),
        source_ts_us: 2,
    }
}

#[test]
fn duplicate_fill_is_idempotent_conflict_and_overfill_are_atomic() {
    let order = CanonicalOrder::new(common::intent("c1", 3)).unwrap();
    let once = reduce_order(&order, &OmsEvent::Fill(fill("f1", 2))).unwrap();
    assert_eq!(
        reduce_order(&once, &OmsEvent::Fill(fill("f1", 2))).unwrap(),
        once
    );
    let mut conflict = fill("f1", 2);
    conflict.fee = 2;
    assert!(matches!(
        reduce_order(&once, &OmsEvent::Fill(conflict)),
        Err(OmsError::FillConflict)
    ));
    assert!(matches!(
        reduce_order(&once, &OmsEvent::Fill(fill("f2", 2))),
        Err(OmsError::Overfill)
    ));
    assert_eq!(once.attributed_fill_quantity, 2);
}

#[test]
fn watermark_does_not_double_count_and_late_fill_can_complete_cancel() {
    let order = CanonicalOrder::new(common::intent("c1", 3)).unwrap();
    let order = reduce_order(
        &order,
        &OmsEvent::Status {
            state: OrderState::Canceled,
            cumulative_quantity: 2,
            source_sequence: Some(5),
            venue_order_id: None,
        },
    )
    .unwrap();
    assert_eq!(order.attributed_fill_quantity, 0);
    let order = reduce_order(&order, &OmsEvent::Fill(fill("f1", 1))).unwrap();
    assert_eq!(order.state, OrderState::Canceled);
    let order = reduce_order(&order, &OmsEvent::Fill(fill("f2", 2))).unwrap();
    assert_eq!(order.state, OrderState::Filled);
}

#[test]
fn unknown_submit_blocks_until_reconciled() {
    let order = CanonicalOrder::new(common::intent("c1", 1)).unwrap();
    assert!(matches!(
        reduce_order(&order, &OmsEvent::UnknownSubmit),
        Err(OmsError::InvalidTransition)
    ));
    let order = reduce_order(&order, &OmsEvent::Submitted).unwrap();
    let order = reduce_order(&order, &OmsEvent::UnknownSubmit).unwrap();
    assert_eq!(order.state, OrderState::Reconcile);
    assert!(order.blocks_new_orders());
}

#[test]
fn stale_and_duplicate_status_are_noops_but_equal_sequence_conflict_is_rejected() {
    let mut order = CanonicalOrder::new(common::intent("c1", 3)).unwrap();
    order.reconciled = true;
    let status = OmsEvent::Status {
        state: OrderState::Acknowledged,
        cumulative_quantity: 0,
        source_sequence: Some(5),
        venue_order_id: None,
    };
    let current = reduce_order(&order, &status).unwrap();
    let mut current = current;
    current.reconciled = true;
    assert_eq!(reduce_order(&current, &status).unwrap(), current);
    let stale = OmsEvent::Status {
        state: OrderState::Acknowledged,
        cumulative_quantity: 0,
        source_sequence: Some(4),
        venue_order_id: None,
    };
    assert_eq!(reduce_order(&current, &stale).unwrap(), current);
    let conflict = OmsEvent::Status {
        state: OrderState::PartiallyFilled,
        cumulative_quantity: 1,
        source_sequence: Some(5),
        venue_order_id: None,
    };
    assert!(matches!(
        reduce_order(&current, &conflict),
        Err(OmsError::InvalidTransition)
    ));
}

#[test]
fn rejected_order_cannot_receive_fill_and_bad_fill_identity_is_atomic() {
    let order = CanonicalOrder::new(common::intent("c1", 1)).unwrap();
    let order = reduce_order(
        &order,
        &OmsEvent::Status {
            state: OrderState::Rejected,
            cumulative_quantity: 0,
            source_sequence: Some(1),
            venue_order_id: None,
        },
    )
    .unwrap();
    assert!(matches!(
        reduce_order(&order, &OmsEvent::Fill(fill("f", 1))),
        Err(OmsError::InvalidTransition)
    ));
    let mut bad = fill("", 1);
    bad.source_ts_us = 0;
    assert!(
        reduce_order(
            &CanonicalOrder::new(common::intent("c1", 1)).unwrap(),
            &OmsEvent::Fill(bad)
        )
        .is_err()
    );
}

#[test]
fn venue_status_cannot_publish_internal_states_or_invalid_terminal_quantities() {
    let order = CanonicalOrder::new(common::intent("c1", 2)).unwrap();
    for state in [
        OrderState::Intent,
        OrderState::Submitted,
        OrderState::Reconcile,
    ] {
        assert!(
            reduce_order(
                &order,
                &OmsEvent::Status {
                    state,
                    cumulative_quantity: 0,
                    source_sequence: Some(1),
                    venue_order_id: None
                }
            )
            .is_err()
        );
    }
    assert!(
        reduce_order(
            &order,
            &OmsEvent::Status {
                state: OrderState::Filled,
                cumulative_quantity: 1,
                source_sequence: Some(1),
                venue_order_id: None
            }
        )
        .is_err()
    );
    let rejected = reduce_order(
        &order,
        &OmsEvent::Status {
            state: OrderState::Rejected,
            cumulative_quantity: 0,
            source_sequence: Some(1),
            venue_order_id: None,
        },
    )
    .unwrap();
    assert!(
        reduce_order(
            &rejected,
            &OmsEvent::Status {
                state: OrderState::Filled,
                cumulative_quantity: 2,
                source_sequence: Some(2),
                venue_order_id: None
            }
        )
        .is_err()
    );
}

#[test]
fn sequenced_status_cannot_be_overwritten_by_unsequenced_or_terminal_reversal() {
    let order = CanonicalOrder::new(common::intent("c1", 2)).unwrap();
    let current = reduce_order(
        &order,
        &OmsEvent::Status {
            state: OrderState::Canceled,
            cumulative_quantity: 0,
            source_sequence: Some(u64::MAX),
            venue_order_id: None,
        },
    )
    .unwrap();
    let duplicate = OmsEvent::Status {
        state: OrderState::Canceled,
        cumulative_quantity: 0,
        source_sequence: None,
        venue_order_id: None,
    };
    assert_eq!(reduce_order(&current, &duplicate).unwrap(), current);
    let reversal = OmsEvent::Status {
        state: OrderState::Acknowledged,
        cumulative_quantity: 0,
        source_sequence: None,
        venue_order_id: None,
    };
    assert!(matches!(
        reduce_order(&current, &reversal),
        Err(OmsError::InvalidTransition)
    ));
}
