#[test]
#[ignore = "explicit release-profile million-order acceptance proof"]
fn one_million_orders_meet_reconciliation_targets() {
    let report = execution::soak::run_soak(execution::soak::SoakConfig {
        canonical_orders: 1_000_000,
        filled_orders: 10_000,
        seed: 7,
    })
    .unwrap();
    assert_eq!(report.canonical_orders, 1_000_000);
    assert!(report.filled_orders >= 10_000);
    assert!(report.order_state_attribution_ppm >= 999_000);
    assert!(report.fill_attribution_ppm >= 999_000);
    assert!(report.post_repair_exact);
    assert_eq!(report.duplicate_submitted_orders, 0);
    assert_eq!(report.unknown_terminal_orders, 0);
    assert_eq!(report.residual_positions, 0);
    assert_eq!(report.residual_delta, 0);
}
