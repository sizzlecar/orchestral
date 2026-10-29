//! OpenAI function parameters require an object without root combinators.
//! The wire schema is a permissive projection; the Host still validates the
//! unchanged canonical Tool schema before preparing or executing an effect.

use std::collections::BTreeSet;

use orchestral_core::model_protocol::{ModelError, ModelErrorCode};
use serde_json::{json, Map, Value};

const ROOT_CONSTRAINTS: [&str; 6] = ["oneOf", "anyOf", "allOf", "enum", "const", "not"];
const MAX_DEPTH: usize = 64;
pub(super) const IDENTITY: &str = "openai-compatible/object-tool-schema/v1";

pub(super) fn parameters(schema: &Value) -> Result<Value, ModelError> {
    let mut root = schema.as_object().cloned().ok_or_else(invalid_object)?;
    match root.get("type") {
        None => {}
        Some(Value::String(kind)) if kind == "object" => {}
        Some(Value::Array(kinds)) if kinds.iter().any(|kind| kind == "object") => {}
        _ => return Err(invalid_object()),
    }
    if root.get("const").is_some_and(|value| !value.is_object())
        || root
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.iter().any(Value::is_object))
    {
        return Err(invalid_object());
    }
    root.insert("type".to_owned(), json!("object"));
    if ROOT_CONSTRAINTS.iter().any(|key| root.contains_key(*key)) || root.contains_key("$ref") {
        let mut names = BTreeSet::new();
        collect_names(schema, schema, &mut names, 0)?;
        let mut properties = Map::new();
        for name in names {
            properties.insert(name.clone(), property_constraint(schema, schema, &name, 0)?);
        }
        root.insert("properties".to_owned(), Value::Object(properties));
        for key in ROOT_CONSTRAINTS.into_iter().chain(["$ref"]) {
            root.remove(key);
        }
    }
    let projected = Value::Object(root);
    validate_references(&projected, &projected, 0)?;
    Ok(projected)
}

fn invalid_object() -> ModelError {
    unsupported("OpenAI function parameters require an object input schema")
}

fn unsupported(message: &str) -> ModelError {
    ModelError::new(ModelErrorCode::Unsupported, message)
}

fn check_depth(depth: usize) -> Result<(), ModelError> {
    if depth > MAX_DEPTH {
        return Err(unsupported(
            "function schema nesting or reference cycle exceeds the projection limit",
        ));
    }
    Ok(())
}

fn reference<'a>(schema: &Value, document: &'a Value) -> Result<Option<&'a Value>, ModelError> {
    schema
        .get("$ref")
        .and_then(Value::as_str)
        .map(|reference| {
            reference
                .strip_prefix('#')
                .and_then(|pointer| document.pointer(pointer))
                .ok_or_else(|| {
                    unsupported("function schema reference is unavailable in its object projection")
                })
        })
        .transpose()
}

fn collect_names(
    schema: &Value,
    document: &Value,
    names: &mut BTreeSet<String>,
    depth: usize,
) -> Result<(), ModelError> {
    check_depth(depth)?;
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        names.extend(properties.keys().cloned());
    }
    if let Some(value) = schema.get("const").and_then(Value::as_object) {
        names.extend(value.keys().cloned());
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        for value in values.iter().filter_map(Value::as_object) {
            names.extend(value.keys().cloned());
        }
    }
    if let Some(target) = reference(schema, document)? {
        collect_names(target, document, names, depth + 1)?;
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = schema.get(key).and_then(Value::as_array) {
            for branch in branches {
                collect_names(branch, document, names, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn property_constraint(
    schema: &Value,
    document: &Value,
    name: &str,
    depth: usize,
) -> Result<Value, ModelError> {
    check_depth(depth)?;
    if let Some(property) = schema
        .get("properties")
        .and_then(|properties| properties.get(name))
    {
        return Ok(property.clone());
    }
    if let Some(value) = schema.get("const").and_then(|value| value.get(name)) {
        return Ok(json!({"enum": [value]}));
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        let values = values
            .iter()
            .filter_map(|value| value.get(name))
            .collect::<Vec<_>>();
        if !values.is_empty() {
            return Ok(json!({"enum": values}));
        }
    }
    if let Some(target) = reference(schema, document)? {
        let constraint = property_constraint(target, document, name, depth + 1)?;
        if constraint != json!({}) {
            return Ok(constraint);
        }
    }
    if let Some(branches) = schema.get("allOf").and_then(Value::as_array) {
        for branch in branches {
            let constraint = property_constraint(branch, document, name, depth + 1)?;
            if constraint != json!({}) {
                return Ok(constraint);
            }
        }
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(branches) = schema.get(key).and_then(Value::as_array) {
            let mut alternatives = Vec::new();
            for branch in branches {
                // An open branch that omits this property permits any value.
                // Keep that permissive case instead of narrowing valid inputs.
                if branch.get("properties").and_then(|p| p.get(name)).is_none()
                    && branch.get("additionalProperties") == Some(&Value::Bool(false))
                    && branch.get("$ref").is_none()
                    && branch.get("patternProperties").is_none()
                {
                    continue;
                }
                let constraint = property_constraint(branch, document, name, depth + 1)?;
                if constraint == json!({}) {
                    return Ok(constraint);
                }
                if !alternatives.contains(&constraint) {
                    alternatives.push(constraint);
                }
            }
            return Ok(match alternatives.len() {
                0 => json!({}),
                1 => alternatives.remove(0),
                _ => json!({"anyOf": alternatives}),
            });
        }
    }
    Ok(json!({}))
}

fn validate_references(schema: &Value, document: &Value, depth: usize) -> Result<(), ModelError> {
    check_depth(depth)?;
    reference(schema, document)?;
    for key in [
        "properties",
        "patternProperties",
        "$defs",
        "definitions",
        "dependentSchemas",
    ] {
        if let Some(children) = schema.get(key).and_then(Value::as_object) {
            for child in children.values() {
                validate_references(child, document, depth + 1)?;
            }
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "contains",
        "propertyNames",
        "if",
        "then",
        "else",
        "not",
    ] {
        if let Some(child) = schema.get(key) {
            validate_references(child, document, depth + 1)?;
        }
    }
    for key in ["anyOf", "oneOf", "allOf", "prefixItems"] {
        if let Some(children) = schema.get(key).and_then(Value::as_array) {
            for child in children {
                validate_references(child, document, depth + 1)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
