use serde_json::Value;

use crate::types::{Step, StepKind};

use super::ValidationError;

pub(super) fn apply_agent_defaults(step: &mut Step) {
    if step.kind != StepKind::Agent {
        return;
    }

    if step.params.is_null() {
        step.params = Value::Object(serde_json::Map::new());
    }

    if let Some(params) = step.params.as_object_mut() {
        normalize_agent_output_rules(params);
        repair_leaf_mode_for_read_only_inputs(params);
        if agent_mode_from_params(params) == Some(AgentMode::Leaf) {
            append_leaf_goal_guard(params);
            let result_slot = params
                .entry("result_slot".to_string())
                .or_insert(Value::String("leaf_result".to_string()))
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("leaf_result")
                .to_string();
            params
                .entry("allowed_actions".to_string())
                .or_insert_with(|| serde_json::json!(["json_stdout"]));
            params
                .entry("max_iterations".to_string())
                .or_insert(Value::from(1_u64));
            let output_keys = params
                .get("output_keys")
                .and_then(|value| value.as_array())
                .map(|keys| {
                    keys.iter()
                        .filter_map(|value| value.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if !output_keys.is_empty() {
                ensure_leaf_output_rules(params, &result_slot, &output_keys);
            }
        } else {
            params
                .entry("max_iterations".to_string())
                .or_insert(Value::from(5_u64));
        }
    }
}

fn repair_leaf_mode_for_read_only_inputs(params: &mut serde_json::Map<String, Value>) {
    if agent_mode_from_params(params) != Some(AgentMode::Leaf) {
        return;
    }

    let Some(actions) = params.get("allowed_actions").and_then(Value::as_array) else {
        return;
    };
    let extra_actions = actions
        .iter()
        .filter_map(Value::as_str)
        .filter(|action| *action != "json_stdout")
        .collect::<Vec<_>>();
    if extra_actions.is_empty() {
        return;
    }
    if extra_actions.iter().all(|action| *action == "file_read")
        && !output_rules_require_explicit_action(params)
    {
        params.insert("mode".to_string(), Value::String("explore".to_string()));
        params.remove("result_slot");
    }
}

fn normalize_agent_output_rules(params: &mut serde_json::Map<String, Value>) {
    let Some(raw_rules) = params.get("output_rules").cloned() else {
        return;
    };
    let Some(items) = raw_rules.as_array() else {
        return;
    };

    let mut normalized = serde_json::Map::new();
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(slot) = obj
            .get("slot")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let mut rule = obj.clone();
        rule.remove("slot");
        normalized.insert(slot.to_string(), Value::Object(rule));
    }

    if !normalized.is_empty() {
        params.insert("output_rules".to_string(), Value::Object(normalized));
    }
}

fn append_leaf_goal_guard(params: &mut serde_json::Map<String, Value>) {
    let Some(goal) = params.get("goal").and_then(Value::as_str) else {
        return;
    };
    if goal.contains("Use only the provided bound inputs.")
        || goal.contains("Do not run commands, read files, or inspect external tools.")
    {
        return;
    }

    params.insert(
        "goal".to_string(),
        Value::String(format!(
            "{} Use only the provided bound inputs. Do not run commands, read files, or inspect external tools. Return only the final structured result for the declared output keys.",
            goal.trim()
        )),
    );
}

fn ensure_leaf_output_rules(
    params: &mut serde_json::Map<String, Value>,
    result_slot: &str,
    output_keys: &[String],
) {
    let default_rules = build_leaf_output_rules(result_slot, output_keys);
    let Some(default_obj) = default_rules.as_object() else {
        return;
    };

    let mut normalized = params
        .get("output_rules")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut changed = !params.get("output_rules").is_some_and(Value::is_object);

    for output_key in output_keys {
        let valid = normalized
            .get(output_key)
            .is_some_and(is_valid_leaf_output_rule);
        if valid {
            continue;
        }
        if let Some(default_rule) = default_obj.get(output_key) {
            normalized.insert(output_key.clone(), default_rule.clone());
            changed = true;
        }
    }

    if changed {
        params.insert("output_rules".to_string(), Value::Object(normalized));
    }
}

fn is_valid_leaf_output_rule(rule: &Value) -> bool {
    let Some(obj) = rule.as_object() else {
        return false;
    };
    if obj
        .get("slot")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    if obj
        .get("template")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    if obj
        .get("fallback_aliases")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            !items.is_empty()
                && items
                    .iter()
                    .all(|value| value.as_str().is_some_and(|entry| !entry.trim().is_empty()))
        })
    {
        return true;
    }
    obj.get("candidates")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            !items.is_empty()
                && items.iter().all(|candidate| {
                    candidate
                        .as_object()
                        .and_then(|obj| obj.get("slot"))
                        .and_then(Value::as_str)
                        .is_some_and(|slot| !slot.trim().is_empty())
                })
        })
}

pub(super) fn validate_agent_params(step: &Step) -> Result<(), ValidationError> {
    if step.kind != StepKind::Agent {
        return Ok(());
    }

    let Some(params) = step.params.as_object() else {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "params must be an object".to_string(),
        ));
    };

    let goal = params.get("goal").and_then(|v| v.as_str()).map(str::trim);
    if goal.is_none() || goal == Some("") {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "goal must be a non-empty string".to_string(),
        ));
    }

    let mode = agent_mode_from_params(params).ok_or_else(|| {
        ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "mode must be 'explore' or 'leaf' when provided".to_string(),
        )
    })?;

    let Some(allowed_actions) = params.get("allowed_actions").and_then(|v| v.as_array()) else {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "allowed_actions must be a non-empty string array".to_string(),
        ));
    };
    if allowed_actions.is_empty()
        || allowed_actions.iter().any(|v| {
            v.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
        })
    {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "allowed_actions must be a non-empty string array".to_string(),
        ));
    }

    let max_iterations = params.get("max_iterations").and_then(|v| v.as_u64());
    if !(Some(1)..=Some(10)).contains(&max_iterations) {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "max_iterations must be an integer between 1 and 10".to_string(),
        ));
    }

    let Some(output_keys) = params.get("output_keys").and_then(|v| v.as_array()) else {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "output_keys must be a non-empty string array".to_string(),
        ));
    };
    if output_keys.is_empty()
        || output_keys.iter().any(|v| {
            v.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
        })
    {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "output_keys must be a non-empty string array".to_string(),
        ));
    }

    if mode == AgentMode::Leaf {
        if !(Some(1)..=Some(3)).contains(&max_iterations) {
            return Err(ValidationError::InvalidAgentParams(
                step.id.to_string(),
                "leaf agent max_iterations must be an integer between 1 and 3".to_string(),
            ));
        }
        if allowed_actions
            .iter()
            .any(|value| value.as_str() != Some("json_stdout"))
        {
            return Err(ValidationError::InvalidAgentParams(
                step.id.to_string(),
                "leaf agent allowed_actions may only contain 'json_stdout'".to_string(),
            ));
        }
        let result_slot = params.get("result_slot").and_then(|value| value.as_str());
        if result_slot
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            return Err(ValidationError::InvalidAgentParams(
                step.id.to_string(),
                "leaf agent result_slot must be a non-empty string".to_string(),
            ));
        }
    }

    if mode == AgentMode::Explore && output_rules_require_explicit_action(params) {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "explore agent may not own side-effect-sensitive outputs with requires.action; split the workflow into explicit action/recipe stages and keep only local derivation in a leaf agent".to_string(),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentMode {
    Explore,
    Leaf,
}

fn agent_mode_from_params(params: &serde_json::Map<String, Value>) -> Option<AgentMode> {
    match params.get("mode").and_then(|value| value.as_str()) {
        None | Some("explore") => Some(AgentMode::Explore),
        Some("leaf") => Some(AgentMode::Leaf),
        Some(_) => None,
    }
}

fn build_leaf_output_rules(result_slot: &str, output_keys: &[String]) -> Value {
    let mut output_rules = serde_json::Map::new();
    for output_key in output_keys {
        output_rules.insert(
            output_key.clone(),
            serde_json::json!({
                "candidates": [
                    {
                        "slot": result_slot,
                        "path": output_key,
                        "requires": {
                            "action": "json_stdout"
                        }
                    }
                ]
            }),
        );
    }
    Value::Object(output_rules)
}

fn output_rules_require_explicit_action(params: &serde_json::Map<String, Value>) -> bool {
    let Some(output_rules) = params
        .get("output_rules")
        .and_then(|value| value.as_object())
    else {
        return false;
    };

    output_rules.values().any(output_rule_requires_action)
}

fn output_rule_requires_action(rule: &Value) -> bool {
    let Some(rule_obj) = rule.as_object() else {
        return false;
    };

    if requires_action(rule_obj.get("requires")) || requires_action(rule_obj.get("requires_action"))
    {
        return true;
    }

    rule_obj
        .get("candidates")
        .and_then(|value| value.as_array())
        .map(|candidates| {
            candidates.iter().any(|candidate| {
                let Some(candidate_obj) = candidate.as_object() else {
                    return false;
                };
                requires_action(candidate_obj.get("requires"))
                    || requires_action(candidate_obj.get("requires_action"))
            })
        })
        .unwrap_or(false)
}

fn requires_action(value: Option<&Value>) -> bool {
    match value {
        Some(Value::String(action)) => !action.trim().is_empty(),
        Some(Value::Object(obj)) => obj
            .get("action")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some(),
        _ => false,
    }
}

pub(super) fn agent_output_keys(step: &Step) -> Option<Vec<String>> {
    if step.kind != StepKind::Agent {
        return None;
    }
    step.params
        .get("output_keys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
}
