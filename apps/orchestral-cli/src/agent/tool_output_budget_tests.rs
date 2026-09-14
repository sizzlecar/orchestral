use super::*;

#[test]
fn inline_budget_uses_the_declared_model_window_and_output_reserve() {
    let mut config = OrchestralConfig::default();
    config.agent.max_context_tokens = 12_288;
    config.agent.reserved_output_tokens = 4_096;
    assert_eq!(model_inline_output_limit(&config, None).get(), 2_048);
    assert_eq!(model_inline_output_limit(&config, Some(8_192)).get(), 1_024);
    assert_eq!(
        model_inline_output_limit(&config, Some(32_768)).get(),
        2_048
    );
    assert_eq!(config.tools.max_output_bytes, 1024 * 1024);
    config.tools.max_inline_output_bytes = std::num::NonZeroU64::new(768);
    assert_eq!(model_inline_output_limit(&config, Some(8_192)).get(), 768);
}

#[test]
fn inline_budget_config_is_optional_and_rejects_zero() {
    use orchestral_core::config::ToolsConfig;
    let implicit: ToolsConfig = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(implicit.max_inline_output_bytes, None);
    let explicit: ToolsConfig = serde_json::from_value(serde_json::json!({
        "max_inline_output_bytes": 512,
    }))
    .unwrap();
    assert_eq!(explicit.max_inline_output_bytes.unwrap().get(), 512);
    assert!(serde_json::from_value::<ToolsConfig>(serde_json::json!({
        "max_inline_output_bytes": 0,
    }))
    .is_err());
}
