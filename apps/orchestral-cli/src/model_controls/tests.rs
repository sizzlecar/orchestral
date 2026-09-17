use super::*;
use orchestral_model_openai::{
    DiscoveredModel, OpenAiReasoningCapabilities, OpenAiThinkingCapability,
};
use serde_json::json;

#[test]
fn absent_and_empty_capabilities_are_unknown_but_empty_efforts_are_declared() {
    for reasoning in [None, Some(OpenAiReasoningCapabilities::default())] {
        let model = discovered_model(DiscoveredModel {
            id: "model".into(),
            max_context_tokens: None,
            reasoning,
        });
        assert_eq!(model.reasoning, None);
    }
    let model = discovered_model(DiscoveredModel {
        id: "model".into(),
        max_context_tokens: None,
        reasoning: Some(OpenAiReasoningCapabilities {
            supported_efforts: Some(Vec::new()),
            thinking: None,
        }),
    });
    assert_eq!(model.reasoning, Some(vec![ReasoningPreference::Default]));
}

#[test]
fn binary_thinking_does_not_invent_effort_levels() {
    let model = discovered_model(DiscoveredModel {
        id: "arbitrary-api-id".into(),
        max_context_tokens: None,
        reasoning: Some(OpenAiReasoningCapabilities {
            supported_efforts: None,
            thinking: Some(OpenAiThinkingCapability {
                default_enabled: true,
            }),
        }),
    });
    assert_eq!(
        model.reasoning,
        Some(vec![
            ReasoningPreference::Default,
            ReasoningPreference::On,
            ReasoningPreference::Off
        ])
    );
    assert_eq!(model.thinking_default_enabled, Some(true));
}

#[test]
fn profile_reasoning_and_explicit_default_have_distinct_precedence() {
    let profile: ModelProfile = serde_json::from_value(json!({"name":"profile", "backend":"local", "model":"model", "config":{"reasoning":"high"}})).unwrap();
    assert_eq!(
        resolve_reasoning(None, Some(&profile)).unwrap(),
        ReasoningPreference::High
    );
    assert_eq!(
        resolve_reasoning(Some(ReasoningPreference::Default), Some(&profile)).unwrap(),
        ReasoningPreference::Default
    );
    let mut invalid = profile.clone();
    invalid.config["reasoning"] = json!(true);
    assert!(resolve_reasoning(None, Some(&invalid)).is_err());
}

#[test]
fn default_none_and_binary_off_map_to_distinct_controls() {
    let mut backend: BackendSpec =
        serde_json::from_value(json!({"name":"local", "kind":"openai"})).unwrap();
    assert_eq!(
        openai_reasoning(&backend, ReasoningPreference::Default).unwrap(),
        None
    );
    assert_eq!(
        openai_reasoning(&backend, ReasoningPreference::None).unwrap(),
        Some(OpenAiReasoningControl::Effort(OpenAiReasoningEffort::None))
    );
    assert_eq!(
        openai_reasoning(&backend, ReasoningPreference::Off).unwrap(),
        Some(OpenAiReasoningControl::Thinking(false))
    );
    backend.kind = "gemini".into();
    assert!(openai_reasoning(&backend, ReasoningPreference::Low).is_err());
    assert!(openai_reasoning(&backend, ReasoningPreference::Default).is_ok());
}
