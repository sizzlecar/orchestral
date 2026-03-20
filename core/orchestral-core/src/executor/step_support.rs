use std::collections::HashMap;

use serde_json::Value;

use crate::store::WorkingSet;
use crate::types::Step;

pub(super) fn bind_param_value(params: &mut Value, key: &str, value: &Value) -> Result<(), String> {
    match params {
        Value::Object(map) => {
            map.insert(key.to_string(), value.clone());
            Ok(())
        }
        Value::Null => {
            let mut map = serde_json::Map::new();
            map.insert(key.to_string(), value.clone());
            *params = Value::Object(map);
            Ok(())
        }
        _ => Err("step.params must be an object (or null) when using io_bindings".to_string()),
    }
}

pub(super) fn resolve_param_templates(params: &mut Value, ws: &WorkingSet) -> Result<(), String> {
    let root_snapshot = params.clone();
    resolve_param_templates_inner(params, ws, &root_snapshot)
}

fn resolve_param_templates_inner(
    params: &mut Value,
    ws: &WorkingSet,
    root: &Value,
) -> Result<(), String> {
    match params {
        Value::Object(map) => {
            for value in map.values_mut() {
                resolve_param_templates_inner(value, ws, root)?;
            }
            Ok(())
        }
        Value::Array(items) => {
            for value in items {
                resolve_param_templates_inner(value, ws, root)?;
            }
            Ok(())
        }
        Value::String(text) => {
            if !text.contains("{{") {
                return Ok(());
            }
            if let Some(key) = exact_template_placeholder(text) {
                let value = ws
                    .get_task(key)
                    .or_else(|| lookup_template_value(root, key))
                    .ok_or_else(|| format!("missing template value '{}'", key))?;
                *params = value.clone();
            } else {
                *text = render_param_template(text, ws, root)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn exact_template_placeholder(template: &str) -> Option<&str> {
    let trimmed = template.trim();
    if !trimmed.starts_with("{{") || !trimmed.ends_with("}}") {
        return None;
    }
    if trimmed[2..trimmed.len() - 2].contains("{{") || trimmed[2..trimmed.len() - 2].contains("}}")
    {
        return None;
    }
    let key = trimmed[2..trimmed.len() - 2].trim();
    if key.is_empty() {
        None
    } else {
        Some(key)
    }
}

fn render_param_template(template: &str, ws: &WorkingSet, root: &Value) -> Result<String, String> {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        rendered.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return Err(format!("unclosed template placeholder in '{}'", template));
        };
        let key = after_start[..end].trim();
        if key.is_empty() {
            return Err(format!("empty template placeholder in '{}'", template));
        }
        let value = ws
            .get_task(key)
            .or_else(|| lookup_template_value(root, key))
            .ok_or_else(|| format!("missing template value '{}'", key))?;
        rendered.push_str(&template_value_to_string(value));
        rest = &after_start[end + 2..];
    }
    rendered.push_str(rest);
    Ok(rendered)
}

fn lookup_template_value<'a>(root: &'a Value, key: &str) -> Option<&'a Value> {
    let mut current = root;
    for segment in key.split('.') {
        if segment.is_empty() {
            return None;
        }
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            Value::Array(items) => {
                let index = segment.parse::<usize>().ok()?;
                current = items.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn template_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

pub(super) fn validate_declared_exports(
    step: &Step,
    exports: &HashMap<String, Value>,
    strict_exports: bool,
) -> Result<(), String> {
    if !strict_exports || step.exports.is_empty() {
        return Ok(());
    }

    for key in &step.exports {
        match exports.get(key) {
            Some(value) if !value.is_null() => {}
            Some(_) => return Err(format!("Step '{}' export '{}' is null", step.id, key)),
            None => {
                return Err(format!(
                    "Step '{}' missing declared export '{}'",
                    step.id, key
                ))
            }
        }
    }
    Ok(())
}
