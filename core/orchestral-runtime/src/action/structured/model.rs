use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StructuredFormat {
    Json,
    Yaml,
    Toml,
}

impl StructuredFormat {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
            Self::Toml => "toml",
        }
    }

    pub(super) fn from_path(path: &Path) -> Option<Self> {
        match path
            .extension()
            .and_then(|value| value.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref()
        {
            Some("json") => Some(Self::Json),
            Some("yaml" | "yml") => Some(Self::Yaml),
            Some("toml") => Some(Self::Toml),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct StructuredPatchSpec {
    pub(super) files: Vec<StructuredPatchFile>,
    #[serde(default)]
    pub(super) summary: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct StructuredPatchFile {
    pub(super) path: String,
    pub(super) operations: Vec<StructuredPatchOperation>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct StructuredPatchOperation {
    pub(super) op: String,
    pub(super) path: String,
    #[serde(default)]
    pub(super) value: Option<Value>,
    #[serde(default)]
    pub(super) selector: Option<String>,
    #[serde(default)]
    pub(super) reason: Option<String>,
}

pub(super) fn parse_structured_patch_spec(
    patch_spec: &Value,
) -> Result<StructuredPatchSpec, String> {
    serde_json::from_value::<StructuredPatchSpec>(patch_spec.clone())
        .map_err(|err| format!("parse structured patch spec failed: {}", err))
}

pub(super) fn parse_structured_file(path: &Path) -> Result<Value, String> {
    let format = StructuredFormat::from_path(path)
        .ok_or_else(|| format!("unsupported structured file '{}'", path.display()))?;
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("read structured file '{}' failed: {}", path.display(), err))?;
    match format {
        StructuredFormat::Json => serde_json::from_str(&raw)
            .map_err(|err| format!("parse json '{}' failed: {}", path.display(), err)),
        StructuredFormat::Yaml => serde_yaml::from_str(&raw)
            .map_err(|err| format!("parse yaml '{}' failed: {}", path.display(), err)),
        StructuredFormat::Toml => {
            let value = toml::from_str::<toml::Value>(&raw)
                .map_err(|err| format!("parse toml '{}' failed: {}", path.display(), err))?;
            serde_json::to_value(value)
                .map_err(|err| format!("convert toml '{}' failed: {}", path.display(), err))
        }
    }
}

pub(super) fn write_structured_file(
    path: &Path,
    format: StructuredFormat,
    value: &Value,
) -> Result<(), String> {
    let content = match format {
        StructuredFormat::Json => {
            let mut rendered = serde_json::to_string_pretty(value)
                .map_err(|err| format!("serialize json '{}' failed: {}", path.display(), err))?;
            rendered.push('\n');
            rendered
        }
        StructuredFormat::Yaml => serde_yaml::to_string(value)
            .map_err(|err| format!("serialize yaml '{}' failed: {}", path.display(), err))?,
        StructuredFormat::Toml => {
            let toml_value = json_to_toml_value(value)?;
            let mut rendered = toml::to_string_pretty(&toml_value)
                .map_err(|err| format!("serialize toml '{}' failed: {}", path.display(), err))?;
            rendered.push('\n');
            rendered
        }
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "create structured dir '{}' failed: {}",
                parent.display(),
                err
            )
        })?;
    }
    fs::write(path, content.as_bytes())
        .map_err(|err| format!("write structured file '{}' failed: {}", path.display(), err))
}

pub(super) fn json_to_toml_value(value: &Value) -> Result<toml::Value, String> {
    match value {
        Value::Null => Err("toml does not support null values".to_string()),
        Value::Bool(boolean) => Ok(toml::Value::Boolean(*boolean)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(toml::Value::Integer(integer))
            } else if let Some(float) = number.as_f64() {
                Ok(toml::Value::Float(float))
            } else {
                Err(format!("unsupported numeric value '{}'", number))
            }
        }
        Value::String(text) => Ok(toml::Value::String(text.clone())),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(json_to_toml_value(item)?);
            }
            Ok(toml::Value::Array(out))
        }
        Value::Object(object) => {
            let mut out = toml::map::Map::new();
            for (key, value) in object {
                out.insert(key.clone(), json_to_toml_value(value)?);
            }
            Ok(toml::Value::Table(out))
        }
    }
}

pub(super) fn summarize_structured_value(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let keys = object.keys().take(8).cloned().collect::<Vec<_>>();
            format!(
                "object with {} top-level key(s): {}",
                object.len(),
                keys.join(", ")
            )
        }
        Value::Array(items) => format!("array with {} item(s)", items.len()),
        other => format!("scalar {}", other),
    }
}

pub(super) fn build_field_inventory(value: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    collect_field_inventory(value, String::new(), String::new(), &mut out);
    out
}

pub(super) fn apply_structured_operation(
    target: &mut Value,
    operation: &StructuredPatchOperation,
) -> Result<(), String> {
    match operation.op.as_str() {
        "set" => {
            let value = operation.value.clone().ok_or_else(|| {
                format!(
                    "structured set operation '{}' is missing value",
                    operation.path
                )
            })?;
            set_json_pointer_value(target, &operation.path, value)
        }
        "remove" => remove_json_pointer_value(target, &operation.path),
        other => Err(format!("unsupported structured operation '{}'", other)),
    }
}

pub(super) fn set_json_pointer_value(
    target: &mut Value,
    pointer: &str,
    value: Value,
) -> Result<(), String> {
    let segments = parse_json_pointer(pointer)?;
    set_by_segments(target, &segments, value)
}

pub(super) fn remove_json_pointer_value(target: &mut Value, pointer: &str) -> Result<(), String> {
    let segments = parse_json_pointer(pointer)?;
    remove_by_segments(target, &segments)
}

fn set_by_segments(target: &mut Value, segments: &[String], value: Value) -> Result<(), String> {
    if segments.is_empty() {
        *target = value;
        return Ok(());
    }

    let head = &segments[0];
    if segments.len() == 1 {
        match target {
            Value::Object(object) => {
                object.insert(head.clone(), value);
                Ok(())
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                if index == items.len() {
                    items.push(value);
                    Ok(())
                } else if index < items.len() {
                    items[index] = value;
                    Ok(())
                } else {
                    Err(format!("array index '{}' is out of bounds", head))
                }
            }
            other => Err(format!(
                "cannot set pointer segment '{}' on {}",
                head,
                type_name(other)
            )),
        }
    } else {
        match target {
            Value::Object(object) => {
                let child = object
                    .entry(head.clone())
                    .or_insert_with(|| empty_container_for(&segments[1]));
                set_by_segments(child, &segments[1..], value)
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                if index > items.len() {
                    return Err(format!("array index '{}' is out of bounds", head));
                }
                if index == items.len() {
                    items.push(empty_container_for(&segments[1]));
                }
                set_by_segments(
                    items
                        .get_mut(index)
                        .ok_or_else(|| format!("array index '{}' is out of bounds", head))?,
                    &segments[1..],
                    value,
                )
            }
            other => Err(format!(
                "cannot descend into {} for '{}'",
                type_name(other),
                head
            )),
        }
    }
}

fn remove_by_segments(target: &mut Value, segments: &[String]) -> Result<(), String> {
    if segments.is_empty() {
        return Err("cannot remove the root structured value".to_string());
    }

    let head = &segments[0];
    if segments.len() == 1 {
        match target {
            Value::Object(object) => {
                object.remove(head);
                Ok(())
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                if index < items.len() {
                    items.remove(index);
                    Ok(())
                } else {
                    Err(format!("array index '{}' is out of bounds", head))
                }
            }
            other => Err(format!(
                "cannot remove pointer segment '{}' from {}",
                head,
                type_name(other)
            )),
        }
    } else {
        match target {
            Value::Object(object) => {
                let Some(child) = object.get_mut(head) else {
                    return Ok(());
                };
                remove_by_segments(child, &segments[1..])
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                let Some(child) = items.get_mut(index) else {
                    return Ok(());
                };
                remove_by_segments(child, &segments[1..])
            }
            other => Err(format!(
                "cannot descend into {} for '{}'",
                type_name(other),
                head
            )),
        }
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, String> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(format!(
            "structured path '{}' must use JSON Pointer syntax like /service/enabled",
            pointer
        ));
    }
    Ok(pointer
        .split('/')
        .skip(1)
        .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
        .collect())
}

fn collect_field_inventory(value: &Value, pointer: String, selector: String, out: &mut Vec<Value>) {
    if !pointer.is_empty() {
        out.push(serde_json::json!({
            "pointer": pointer,
            "selector": selector,
            "value_type": type_name(value),
            "summary": summarize_structured_value(value),
            "value": value,
        }));
    }

    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let next_pointer = format!("{}/{}", pointer, escape_json_pointer_segment(key));
                let next_selector = if selector.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", selector, key)
                };
                collect_field_inventory(child, next_pointer, next_selector, out);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let next_pointer = format!("{}/{}", pointer, index);
                let next_selector = if selector.is_empty() {
                    format!("[{}]", index)
                } else {
                    format!("{}[{}]", selector, index)
                };
                collect_field_inventory(child, next_pointer, next_selector, out);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn escape_json_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn parse_array_index(segment: &str) -> Result<usize, String> {
    segment
        .parse::<usize>()
        .map_err(|_| format!("array segment '{}' is not a valid index", segment))
}

fn empty_container_for(next_segment: &str) -> Value {
    if next_segment.parse::<usize>().is_ok() {
        Value::Array(Vec::new())
    } else {
        Value::Object(Map::new())
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::build_field_inventory;

    #[test]
    fn test_build_field_inventory_includes_selector_and_pointer() {
        let value = json!({
            "server": {
                "port": 8080,
                "host": "127.0.0.1"
            }
        });

        let inventory = build_field_inventory(&value);
        let port = inventory
            .iter()
            .find(|item| item.get("pointer").and_then(|v| v.as_str()) == Some("/server/port"))
            .expect("port entry");
        assert_eq!(
            port.get("selector").and_then(|v| v.as_str()),
            Some("server.port")
        );
        assert_eq!(
            port.get("value_type").and_then(|v| v.as_str()),
            Some("number")
        );
        assert_eq!(port.get("value"), Some(&json!(8080)));
    }
}
