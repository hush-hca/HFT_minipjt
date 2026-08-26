mod common;
use execution::{
    journal::OrderJournal,
    oms::{CanonicalOrder, OmsEvent, OrderState, reduce_order},
    reconcile::{FakeVenue, ReconcileReason, Reconciler, VenueSnapshot},
};
use funding_core::execution::{BalanceSnapshot, FundingIncome, Position};
use md_core::model::{AdapterId, CanonicalSymbol};

#[test]
fn read_only_snapshot_repairs_unknown_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut j = OrderJournal::open(dir.path().join("r.db")).unwrap();
    let i = common::intent("c1", 1);
    j.record_intent(&i).unwrap();
    j.apply_event(
        i.venue,
        "submitted",
        &i.client_order_id,
        &OmsEvent::Submitted,
    )
    .unwrap();
    j.apply_event(
        i.venue,
        "unknown",
        &i.client_order_id,
        &OmsEvent::UnknownSubmit,
    )
    .unwrap();
    let remote = reduce_order(
        &CanonicalOrder::new(i.clone()).unwrap(),
        &OmsEvent::Status {
            state: OrderState::Filled,
            cumulative_quantity: 1,
            source_sequence: Some(1),
            venue_order_id: None,
        },
    )
    .unwrap();
    let api = FakeVenue::new(VenueSnapshot::from_orders("s1", vec![remote]));
    let first = Reconciler::new(&mut j, &api)
        .run(ReconcileReason::Startup)
        .unwrap();
    assert!(first.exact);
    let second = Reconciler::new(&mut j, &api)
        .run(ReconcileReason::Startup)
        .unwrap();
    assert!(second.exact);
    assert_eq!(j.reconciliation_run_count().unwrap(), 2);
}

#[test]
fn authoritative_account_facts_are_replaced_atomically_and_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("facts.db");
    let mut j = OrderJournal::open(&path).unwrap();
    let mut snapshot = VenueSnapshot::from_orders("facts", Vec::new());
    snapshot.positions.push(Position {
        venue: AdapterId::BinanceUsdm,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        signed_quantity: -7,
    });
    snapshot.balances.push(BalanceSnapshot {
        venue: AdapterId::BinanceUsdm,
        asset: "USDT".into(),
        total: 100,
        available: 90,
        source_ts_us: 1,
    });
    snapshot.funding_income.push(FundingIncome {
        venue: AdapterId::BinanceUsdm,
        income_id: "income-1".into(),
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        amount: 3,
        source_ts_us: 2,
    });
    assert!(
        !Reconciler::new(&mut j, &FakeVenue::new(snapshot))
            .run(ReconcileReason::Startup)
            .unwrap()
            .exact
    );
    drop(j);
    let j = OrderJournal::open(&path).unwrap();
    let facts = j.snapshot().unwrap();
    assert_eq!(facts.positions.len(), 1);
    assert_eq!(facts.balances.len(), 1);
    assert_eq!(facts.funding_income.len(), 1);
}
