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

pub fn render_working_set_template(template: &str, ws: &WorkingSet) -> Result<String, String> {
    if !template.contains("{{") {
        return Ok(template.to_string());
    }
    render_param_template(template, ws, &Value::Null)
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
                let value = lookup_working_set_value(ws, key)
                    .or_else(|| lookup_template_value(root, key))
                    .ok_or_else(|| format!("missing template value '{}'", key))?;
                *params = value;
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
        let value = lookup_working_set_value(ws, key)
            .or_else(|| lookup_template_value(root, key))
            .ok_or_else(|| format!("missing template value '{}'", key))?;
        rendered.push_str(&template_value_to_string(&value));
        rest = &after_start[end + 2..];
    }
    rendered.push_str(rest);
    Ok(rendered)
}

fn lookup_working_set_value(ws: &WorkingSet, key: &str) -> Option<Value> {
    if let Some(value) = ws.get_task(key) {
        return Some(value.clone());
    }

    let segments = parse_template_segments(key)?;
    let mut prefix = String::new();
    let mut checkpoints = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        if let TemplateSegment::Field(field) = segment {
            if !prefix.is_empty() {
                prefix.push('.');
            }
            prefix.push_str(field);
            checkpoints.push((index + 1, prefix.clone()));
        }
    }

    for (consumed, candidate) in checkpoints.into_iter().rev() {
        let Some(base) = ws.get_task(&candidate) else {
            continue;
        };
        if let Some(value) = resolve_value_segments(base, &segments[consumed..]) {
            return Some(value.clone());
        }
    }

    if segments.len() == 1 {
        if let Some(TemplateSegment::Field(field)) = segments.first() {
            if let Some(value) = synthesize_task_object_binding(ws, field) {
                return Some(value);
            }
        }
    }

    None
}

fn lookup_template_value(root: &Value, key: &str) -> Option<Value> {
    let segments = parse_template_segments(key)?;
    resolve_value_segments(root, &segments).cloned()
}

fn synthesize_task_object_binding(ws: &WorkingSet, key: &str) -> Option<Value> {
    let prefix = format!("{key}.");
    let mut object = serde_json::Map::new();
    for (candidate, value) in ws.export_task_data() {
        let Some(remainder) = candidate.strip_prefix(&prefix) else {
            continue;
        };
        if remainder.is_empty() || remainder.contains('.') {
            continue;
        }
        object.insert(remainder.to_string(), value);
    }

    if object.is_empty() {
        None
    } else {
        Some(Value::Object(object))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateSegment<'a> {
    Field(&'a str),
    Index(usize),
}

fn parse_template_segments(key: &str) -> Option<Vec<TemplateSegment<'_>>> {
    if key.trim().is_empty() {
        return None;
    }

    let bytes = key.as_bytes();
    let mut index = 0usize;
    let mut segments = Vec::new();

    while index < bytes.len() {
        if bytes[index] == b'.' {
            return None;
        }

        let field_start = index;
        while index < bytes.len() && bytes[index] != b'.' && bytes[index] != b'[' {
            index += 1;
        }
        if index > field_start {
            let token = &key[field_start..index];
            if let Ok(parsed) = token.parse::<usize>() {
                segments.push(TemplateSegment::Index(parsed));
            } else {
                segments.push(TemplateSegment::Field(token));
            }
        }

        while index < bytes.len() && bytes[index] == b'[' {
            let close = key[index + 1..].find(']')? + index + 1;
            let raw_index = &key[index + 1..close];
            let parsed = raw_index.parse::<usize>().ok()?;
            segments.push(TemplateSegment::Index(parsed));
            index = close + 1;
        }

        if index < bytes.len() {
            if bytes[index] != b'.' {
                return None;
            }
            index += 1;
            if index >= bytes.len() {
                return None;
            }
        }
    }

    if segments.is_empty() {
        None
    } else {
        Some(segments)
    }
}

fn resolve_value_segments<'a>(
    mut current: &'a Value,
    segments: &[TemplateSegment<'_>],
) -> Option<&'a Value> {
    for segment in segments {
        match segment {
            TemplateSegment::Field(field) => match current {
                Value::Object(map) => current = map.get(*field)?,
                Value::String(_) if *field == "path" => {}
                _ => return None,
            },
            TemplateSegment::Index(index) => match current {
                Value::Array(items) => current = items.get(*index)?,
                _ => return None,
            },
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_resolve_param_templates_supports_bracket_index_and_string_path_accessor() {
        let mut ws = WorkingSet::new();
        ws.set_task(
            "locate.artifact_candidates",
            json!(["docs/report.xlsx", "docs/backup.xlsx"]),
        );

        let mut params = json!({
            "path": "{{locate.artifact_candidates[0].path}}"
        });

        resolve_param_templates(&mut params, &ws).expect("template should resolve");
        assert_eq!(params["path"], json!("docs/report.xlsx"));
    }

    #[test]
    fn test_resolve_param_templates_keeps_legacy_dot_index_lookup() {
        let mut ws = WorkingSet::new();
        ws.set_task(
            "locate.artifact_candidates",
            json!(["docs/report.xlsx", "docs/backup.xlsx"]),
        );

        let mut params = json!({
            "path": "{{locate.artifact_candidates.0}}"
        });

        resolve_param_templates(&mut params, &ws).expect("legacy dot index should resolve");
        assert_eq!(params["path"], json!("docs/report.xlsx"));
    }

    #[test]
    fn test_resolve_param_templates_synthesizes_step_object_from_exported_fields() {
        let mut ws = WorkingSet::new();
        ws.set_task(
            "assess_patches.continuation",
            json!({
                "status": "commit_ready",
                "patch_spec": { "fills": [{ "cell": "F5", "value": "done" }] }
            }),
        );
        ws.set_task(
            "assess_patches.summary",
            json!("Spreadsheet probe ready for commit."),
        );

        let mut params = json!({
            "patch_probe": "{{assess_patches}}"
        });

        resolve_param_templates(&mut params, &ws).expect("step object alias should resolve");
        assert_eq!(
            params["patch_probe"],
            json!({
                "continuation": {
                    "status": "commit_ready",
                    "patch_spec": { "fills": [{ "cell": "F5", "value": "done" }] }
                },
                "summary": "Spreadsheet probe ready for commit."
            })
        );
    }

    #[test]
    fn test_render_working_set_template_renders_nested_scalar_bindings() {
        let mut ws = WorkingSet::new();
        ws.set_task("inspect.selected_region.row_count", json!(7));
        ws.set_task("inspect.max_column", json!(11));

        let rendered = render_working_set_template(
            "共有 {{inspect.selected_region.row_count}} 行，最大列 {{inspect.max_column}}。",
            &ws,
        )
        .expect("template should resolve");

        assert_eq!(rendered, "共有 7 行，最大列 11。");
    }

    #[test]
    fn test_render_working_set_template_errors_on_missing_binding() {
        let ws = WorkingSet::new();

        let err = render_working_set_template("缺少 {{inspect.selected_region.row_count}}", &ws)
            .expect_err("missing binding should fail");

        assert!(err.contains("missing template value"));
    }
}
