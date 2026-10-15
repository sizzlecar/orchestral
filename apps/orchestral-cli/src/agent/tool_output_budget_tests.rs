use super::*;

#[test]
fn omitted_host_ceiling_follows_model_capacity_and_explicit_ceilings_still_apply() {
    let mut config = OrchestralConfig::default();
    assert_eq!(config.agent.max_context_tokens, None);
    assert_eq!(model_context_budget(&config, Some(1_048_576)), 1_048_576);
    assert_eq!(model_context_budget(&config, Some(32_768)), 32_768);
    config.agent.max_context_tokens = Some(65_536);
    assert_eq!(model_context_budget(&config, Some(1_048_576)), 65_536);
    assert_eq!(model_context_budget(&config, Some(32_768)), 32_768);
    assert_eq!(model_context_budget(&config, None), 65_536);
}

#[test]
fn inline_budget_uses_the_declared_model_window_and_output_reserve() {
    let mut config = OrchestralConfig::default();
    config.agent.max_context_tokens = Some(12_288);
    config.agent.reserved_output_tokens = Some(4_096);
    assert_eq!(model_inline_output_limit(&config, None, 4096).get(), 2_048);
    assert_eq!(
        model_inline_output_limit(&config, Some(8_192), 4096).get(),
        1_024
    );
    assert_eq!(
        model_inline_output_limit(&config, Some(32_768), 4096).get(),
        2_048
    );
    config.agent.max_context_tokens = None;
    assert_eq!(
        model_inline_output_limit(&config, Some(1_048_576), 65_536).get(),
        DEFAULT_MAX_INLINE_TOOL_OUTPUT_BYTES
    );
    assert_eq!(config.tools.max_output_bytes, 1024 * 1024);
    config.tools.max_inline_output_bytes = std::num::NonZeroU64::new(768);
    assert_eq!(
        model_inline_output_limit(&config, Some(8_192), 4096).get(),
        768
    );
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

#[test]
fn output_budget_uses_metadata_and_preserves_explicit_limits() {
    let mut config = OrchestralConfig::default();
    assert_eq!(config.agent.reserved_output_tokens, None);
    assert_eq!(
        model_output_budget(&config, None, Some(65_536)),
        (65_536, "model metadata")
    );
    assert_eq!(model_output_budget(&config, None, Some(131_072)).0, 131_072);
    config.agent.reserved_output_tokens = Some(32_768);
    assert_eq!(model_output_budget(&config, None, Some(65_536)).0, 32_768);
    assert_eq!(model_output_budget(&config, None, Some(8_192)).0, 8_192);
    let profile = ModelProfile {
        name: "fixture".into(),
        backend: "google".into(),
        model: "fixture".into(),
        temperature: None,
        max_tokens: Some(16_384),
        system_prompt: None,
        policy: Default::default(),
        config: serde_json::Value::Null,
    };
    assert_eq!(
        model_output_budget(&config, Some(&profile), Some(65_536)).0,
        16_384
    );
}
