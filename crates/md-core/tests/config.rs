#[test]
fn default_config_has_required_assets_and_limits() {
    let cfg =
        md_core::config::CollectorConfig::load(std::path::Path::new("../../config/default.toml"))
            .unwrap();
    assert_eq!(cfg.assets.len(), 20);
    assert_eq!(cfg.channel_capacity, 65_536);
    assert_eq!(cfg.batch_rows, 8_192);
    assert_eq!(cfg.flush_interval_ms, 1_000);
    assert_eq!(cfg.enqueue_timeout_ms, 5_000);
    assert_eq!(cfg.assets.first().unwrap(), "BTC");
    assert_eq!(cfg.assets.last().unwrap(), "OP");
    cfg.validate().unwrap();
}
