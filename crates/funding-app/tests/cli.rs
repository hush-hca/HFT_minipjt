use std::process::Command;

#[test]
fn collect_help_exposes_public_collection_controls_only() {
    let output = Command::new(env!("CARGO_BIN_EXE_funding-app"))
        .args(["collect", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout)
        .unwrap()
        .to_ascii_lowercase();
    assert!(help.contains("--config"));
    assert!(help.contains("--duration"));
    for forbidden in ["api-key", "secret", "order", "paper", "arm", "gui", "live"] {
        assert!(
            !help.contains(forbidden),
            "forbidden control {forbidden:?}: {help}"
        );
    }
}

#[cfg(feature = "gui")]
#[test]
fn gui_help_has_config_but_no_secret_or_order_flags() {
    let output = Command::new(env!("CARGO_BIN_EXE_funding-app"))
        .args(["gui", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout)
        .unwrap()
        .to_ascii_lowercase();
    assert!(help.contains("--config"));
    for forbidden in ["api-key", "api-secret", "place-order", "withdraw"] {
        assert!(
            !help.contains(forbidden),
            "forbidden GUI control {forbidden:?}: {help}"
        );
    }
}
