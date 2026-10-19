use super::*;
use serde_json::json;

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
    for (value, expected) in [
        ("future-next", "future-next"),
        ("HIGH", "HIGH"),
        ("effort:off", "off"),
    ] {
        invalid.config["reasoning"] = json!(value);
        assert_eq!(
            resolve_reasoning(None, Some(&invalid)).unwrap(),
            ReasoningPreference::Custom(expected.into())
        );
    }
    invalid.config["reasoning"] = json!(true);
    assert!(resolve_reasoning(None, Some(&invalid)).is_err());
}
