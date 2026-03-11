use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::materialize::parse_path_segments;
use super::types::{
    AgentCaptureMode, AgentDecision, AgentMode, AgentOutputCandidate, AgentOutputRule,
    AgentPathSegment, AgentStepParams,
};

pub(super) fn parse_output_rules(
    step_id: &str,
    output_rules: Option<&Value>,
    output_sources_legacy: Option<&Value>,
    output_keys: &[String],
) -> Result<HashMap<String, AgentOutputRule>, String> {
    let mut out = HashMap::new();
    if let Some(value) = output_rules {
        let parsed = parse_output_rules_object(step_id, "output_rules", value, output_keys)?;
        out.extend(parsed);
    }

    if let Some(value) = output_sources_legacy {
        let parsed = parse_legacy_output_sources(step_id, value, output_keys)?;
        for (key, rule) in parsed {
            out.entry(key).or_insert(rule);
        }
    }

    Ok(out)
}

fn parse_output_rules_object(
    step_id: &str,
    field_name: &str,
    value: &Value,
    output_keys: &[String],
) -> Result<HashMap<String, AgentOutputRule>, String> {
    let Some(obj) = value.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': {} must be an object",
            step_id, field_name
        ));
    };

    let allowed = output_keys
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut out = HashMap::with_capacity(obj.len());
    for (key, raw_rule) in obj {
        if !allowed.contains(key.as_str()) {
            return Err(format!(
                "invalid agent params in step '{}': {} key '{}' is not declared in output_keys",
                step_id, field_name, key
            ));
        }
        let rule = parse_output_rule_entry(step_id, key, raw_rule)?;
        out.insert(key.clone(), rule);
    }
    Ok(out)
}

fn parse_output_rule_entry(
    step_id: &str,
    output_key: &str,
    value: &Value,
) -> Result<AgentOutputRule, String> {
    let Some(obj) = value.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{} must be an object",
            step_id, output_key
        ));
    };

    let mut candidates = Vec::new();
    let required_action =
        parse_required_action_entry(step_id, &format!("output_rules.{}", output_key), obj)?;
    if let Some(slot) = parse_rule_slot(step_id, output_key, obj.get("slot"))? {
        let path = parse_rule_path(
            step_id,
            output_key,
            obj.get("path").or_else(|| obj.get("field")),
        )?;
        candidates.push(AgentOutputCandidate {
            slot,
            path,
            required_action: None,
        });
    }

    if let Some(raw_candidates) = obj.get("candidates") {
        let arr = raw_candidates.as_array().ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': output_rules.{}.candidates must be an array",
                step_id, output_key
            )
        })?;
        for raw_candidate in arr {
            candidates.push(parse_output_candidate_entry(
                step_id,
                output_key,
                raw_candidate,
            )?);
        }
    }

    let template = obj
        .get("template")
        .map(|v| {
            v.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    format!(
                        "invalid agent params in step '{}': output_rules.{}.template must be a non-empty string",
                        step_id, output_key
                    )
                })
        })
        .transpose()?;

    let fallback_aliases = obj
        .get("fallback_aliases")
        .map(|v| {
            v.as_array()
                .ok_or_else(|| {
                    format!(
                        "invalid agent params in step '{}': output_rules.{}.fallback_aliases must be an array",
                        step_id, output_key
                    )
                })?
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .ok_or_else(|| {
                            format!(
                                "invalid agent params in step '{}': output_rules.{}.fallback_aliases must contain non-empty strings",
                                step_id, output_key
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    if candidates.is_empty() && template.is_none() && fallback_aliases.is_empty() {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{} must provide slot/candidates/template/fallback_aliases",
            step_id, output_key
        ));
    }

    Ok(AgentOutputRule {
        candidates,
        template,
        fallback_aliases,
        required_action,
    })
}

fn parse_output_candidate_entry(
    step_id: &str,
    output_key: &str,
    value: &Value,
) -> Result<AgentOutputCandidate, String> {
    let Some(obj) = value.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{}.candidates[] must be an object",
            step_id, output_key
        ));
    };
    let slot = parse_rule_slot(step_id, output_key, obj.get("slot"))?.ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': output_rules.{}.candidates[].slot is required",
            step_id, output_key
        )
    })?;
    let path = parse_rule_path(step_id, output_key, obj.get("path"))?;
    let required_action = parse_required_action_entry(
        step_id,
        &format!("output_rules.{}.candidates[]", output_key),
        obj,
    )?;
    if slot == "outputs" && required_action.is_some() {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{}.candidates[].requires.action cannot target slot 'outputs'",
            step_id, output_key
        ));
    }
    Ok(AgentOutputCandidate {
        slot,
        path,
        required_action,
    })
}

fn parse_rule_slot(
    step_id: &str,
    output_key: &str,
    value: Option<&Value>,
) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let slot = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': output_rules.{}.slot must be a non-empty string",
                step_id, output_key
            )
        })?;
    Ok(Some(slot.to_string()))
}

fn parse_rule_path(
    step_id: &str,
    output_key: &str,
    value: Option<&Value>,
) -> Result<Vec<AgentPathSegment>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let raw = value.as_str().map(str::trim).ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': output_rules.{}.path must be a string",
            step_id, output_key
        )
    })?;
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    parse_path_segments(raw).ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': output_rules.{}.path has invalid syntax",
            step_id, output_key
        )
    })
}

fn parse_required_action_entry(
    step_id: &str,
    scope: &str,
    obj: &serde_json::Map<String, Value>,
) -> Result<Option<String>, String> {
    let direct = obj.get("requires_action");
    let structured = obj.get("requires");
    if direct.is_some() && structured.is_some() {
        return Err(format!(
            "invalid agent params in step '{}': {} cannot include both requires and requires_action",
            step_id, scope
        ));
    }

    if let Some(value) = direct {
        let action = value
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                format!(
                    "invalid agent params in step '{}': {}.requires_action must be a non-empty string",
                    step_id, scope
                )
            })?;
        return Ok(Some(action.to_string()));
    }

    let Some(value) = structured else {
        return Ok(None);
    };
    let requires = value.as_object().ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': {}.requires must be an object",
            step_id, scope
        )
    })?;
    let action = requires.get("action").ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': {}.requires.action is required",
            step_id, scope
        )
    })?;
    let action = action
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': {}.requires.action must be a non-empty string",
                step_id, scope
            )
        })?;
    Ok(Some(action.to_string()))
}

fn parse_legacy_output_sources(
    step_id: &str,
    value: &Value,
    output_keys: &[String],
) -> Result<HashMap<String, AgentOutputRule>, String> {
    parse_output_rules_object(step_id, "output_sources", value, output_keys)
}

fn parse_optional_slot_name(scope: &str, value: Option<&Value>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let name = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{}: save_as must be a non-empty string", scope))?;
    Ok(Some(name.to_string()))
}

fn parse_optional_capture_mode(
    scope: &str,
    value: Option<&Value>,
) -> Result<Option<AgentCaptureMode>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mode = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{}: capture must be a non-empty string", scope))?;
    match mode.to_ascii_lowercase().as_str() {
        "json_stdout" => Ok(Some(AgentCaptureMode::JsonStdout)),
        other => Err(format!("{}: unsupported capture mode '{}'", scope, other)),
    }
}

pub(super) fn parse_agent_params(step_id: &str, params: &Value) -> Result<AgentStepParams, String> {
    let Some(obj) = params.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': params must be an object",
            step_id
        ));
    };

    let mode = match obj.get("mode").and_then(|v| v.as_str()) {
        None | Some("explore") => AgentMode::Explore,
        Some("leaf") => AgentMode::Leaf,
        Some(other) => {
            return Err(format!(
                "invalid agent params in step '{}': unsupported mode '{}'",
                step_id, other
            ));
        }
    };

    let goal = obj
        .get("goal")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': goal is required",
                step_id
            )
        })?
        .to_string();

    let allowed_actions = obj
        .get("allowed_actions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': allowed_actions must be a non-empty array",
                step_id
            )
        })?
        .iter()
        .filter_map(|v| v.as_str().map(str::trim))
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect::<HashSet<_>>();
    if allowed_actions.is_empty() {
        return Err(format!(
            "invalid agent params in step '{}': allowed_actions must be a non-empty array",
            step_id
        ));
    }
    if mode == AgentMode::Leaf && allowed_actions.iter().any(|action| action != "json_stdout") {
        return Err(format!(
            "invalid agent params in step '{}': leaf mode only supports allowed_actions=['json_stdout']",
            step_id
        ));
    }

    let max_iterations = obj
        .get("max_iterations")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': max_iterations is required",
                step_id
            )
        })?;
    if !(1..=10).contains(&max_iterations) {
        return Err(format!(
            "invalid agent params in step '{}': max_iterations must be in [1,10]",
            step_id
        ));
    }
    if mode == AgentMode::Leaf && !(1..=3).contains(&max_iterations) {
        return Err(format!(
            "invalid agent params in step '{}': leaf mode max_iterations must be in [1,3]",
            step_id
        ));
    }

    let output_keys = obj
        .get("output_keys")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': output_keys must be a non-empty array",
                step_id
            )
        })?
        .iter()
        .filter_map(|v| v.as_str().map(str::trim))
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if output_keys.is_empty() {
        return Err(format!(
            "invalid agent params in step '{}': output_keys must be a non-empty array",
            step_id
        ));
    }

    let result_slot = obj
        .get("result_slot")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    if mode == AgentMode::Leaf && result_slot.is_none() {
        return Err(format!(
            "invalid agent params in step '{}': leaf mode requires result_slot",
            step_id
        ));
    }

    let output_rules = parse_output_rules(
        step_id,
        obj.get("output_rules"),
        obj.get("output_sources"),
        &output_keys,
    )?;

    let bound_inputs = obj
        .iter()
        .filter_map(|(key, value)| {
            if matches!(
                key.as_str(),
                "mode"
                    | "result_slot"
                    | "result_hint"
                    | "leaf_hint"
                    | "leaf_schema"
                    | "goal"
                    | "allowed_actions"
                    | "max_iterations"
                    | "output_keys"
                    | "output_rules"
                    | "output_sources"
            ) {
                None
            } else {
                Some((key.clone(), value.clone()))
            }
        })
        .collect::<HashMap<_, _>>();

    Ok(AgentStepParams {
        mode,
        goal,
        allowed_actions,
        max_iterations,
        output_keys,
        output_rules,
        result_slot,
        bound_inputs,
    })
}

pub(super) fn parse_tool_call_decision(name: &str, args: Value) -> Result<AgentDecision, String> {
    match name {
        "execute_action" => {
            let action = args
                .get("action")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "execute_action: missing 'action' field".to_string())?
                .to_string();
            let params = args
                .get("params")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            let save_as = parse_optional_slot_name("execute_action", args.get("save_as"))?;
            let capture = parse_optional_capture_mode("execute_action", args.get("capture"))?;
            Ok(AgentDecision::Action {
                name: action,
                params,
                save_as,
                capture,
            })
        }
        "finish" => Ok(AgentDecision::Finish),
        "return_final" => {
            let exports = args
                .get("exports")
                .and_then(|v| v.as_object())
                .ok_or_else(|| "return_final: missing object field 'exports'".to_string())?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>();
            Ok(AgentDecision::Final { exports })
        }
        other => Err(format!("unknown tool: '{}'", other)),
    }
}

pub(super) fn parse_agent_decision(json: &str) -> Result<AgentDecision, String> {
    let value: Value = serde_json::from_str(json).map_err(|e| format!("invalid json: {}", e))?;
    let Some(obj) = value.as_object() else {
        return Err("decision must be a JSON object".to_string());
    };

    let decision_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .map(|v| v.to_ascii_lowercase())
        .ok_or_else(|| "missing decision field 'type'".to_string())?;
    match decision_type.as_str() {
        "action" => {
            let name = obj
                .get("action")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "action decision missing 'action'".to_string())?
                .to_string();
            let params = obj
                .get("params")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            let save_as = parse_optional_slot_name("action", obj.get("save_as"))?;
            let capture = parse_optional_capture_mode("action", obj.get("capture"))?;
            Ok(AgentDecision::Action {
                name,
                params,
                save_as,
                capture,
            })
        }
        "finish" => Ok(AgentDecision::Finish),
        "final" => {
            let exports = obj
                .get("exports")
                .and_then(|v| v.as_object())
                .ok_or_else(|| "final decision missing object field 'exports'".to_string())?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>();
            Ok(AgentDecision::Final { exports })
        }
        other => Err(format!("unsupported decision type '{}'", other)),
    }
}

pub(super) fn extract_json(text: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut start = None;
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in chars.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if *ch == '\\' {
                escaped = true;
            } else if *ch == '"' {
                in_string = false;
            }
            continue;
        }
        if *ch == '"' {
            in_string = true;
            continue;
        }
        if *ch == '{' {
            if start.is_none() {
                start = Some(idx);
            }
            depth = depth.saturating_add(1);
        } else if *ch == '}' {
            if depth == 0 {
                continue;
            }
            depth -= 1;
            if depth == 0 {
                if let Some(start_idx) = start {
                    let candidate: String = chars[start_idx..=idx].iter().collect();
                    if serde_json::from_str::<Value>(&candidate).is_ok() {
                        return Some(candidate);
                    }
                }
                start = None;
            }
        }
    }
    None
}

pub(super) fn normalize_agent_action_params(action_name: &str, params: Value) -> Value {
    if action_name != "json_stdout" {
        return params;
    }
    let Some(obj) = params.as_object() else {
        return serde_json::json!({ "payload": params });
    };
    if obj.contains_key("payload") {
        return Value::Object(obj.clone());
    }
    serde_json::json!({ "payload": Value::Object(obj.clone()) })
}
