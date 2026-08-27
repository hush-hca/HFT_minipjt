use execution::soak::{SoakConfig, fill_fault_budget, run_soak};

#[test]
fn fill_fault_budget_respects_ppm_boundary() {
    assert_eq!(fill_fault_budget(10_000).unwrap(), 10);
    assert_eq!(fill_fault_budget(10_999).unwrap(), 10);
    assert!(fill_fault_budget(0).is_err());
}

#[test]
fn small_soak_is_repeatable_and_repairs_exactly() {
    let c = SoakConfig {
        canonical_orders: 2_000,
        filled_orders: 1_000,
        seed: 7,
    };
    let a = run_soak(c).unwrap();
    let b = run_soak(c).unwrap();
    assert_eq!(a.canonical_digest_hex, b.canonical_digest_hex);
    assert!(a.order_state_attribution_ppm >= 999_000);
    assert!(a.fill_attribution_ppm >= 999_000);
    assert!(a.post_repair_exact);
    assert!(
        a.injected_duplicates > 0
            && a.injected_reorders > 0
            && a.injected_disconnects > 0
            && a.injected_cancel_fill_races > 0
            && a.injected_unknown_acks > 0
    );
}
