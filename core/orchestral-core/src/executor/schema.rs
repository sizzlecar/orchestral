use serde_json::Value;

use crate::types::StepId;

pub(super) fn validate_schema(
    value: &Value,
    schema: &Value,
    label: &str,
    step_id: &StepId,
    action: &str,
) -> Result<(), String> {
    if schema.is_null() {
        return Ok(());
    }

    validate_value_against_schema(value, schema, "$").map_err(|reason| {
        format!(
            "Step '{}' action '{}' {} schema validation failed: {}",
            step_id, action, label, reason
        )
    })
}

fn validate_value_against_schema(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let schema_obj = schema
        .as_object()
        .ok_or_else(|| format!("schema at '{}' must be an object", path))?;

    if let Some(type_spec) = schema_obj.get("type") {
        validate_json_type(value, type_spec, path)?;
    }

    if let Some(constant) = schema_obj.get("const") {
        if value != constant {
            return Err(format!("{} expected const {}", path, constant));
        }
    }

    if let Some(variants) = schema_obj.get("enum").and_then(|v| v.as_array()) {
        if !variants.iter().any(|candidate| candidate == value) {
            return Err(format!("{} is not one of the allowed enum values", path));
        }
    }

    if let Some(required) = schema_obj.get("required").and_then(|v| v.as_array()) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{} must be an object for required fields", path))?;
        for key in required.iter().filter_map(|v| v.as_str()) {
            if !object.contains_key(key) {
                return Err(format!("{} missing required field '{}'", path, key));
            }
        }
    }

    if let Some(properties) = schema_obj.get("properties").and_then(|v| v.as_object()) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{} must be an object for properties validation", path))?;
        for (key, property_schema) in properties {
            if let Some(child_value) = object.get(key) {
                let child_path = format!("{}.{}", path, key);
                validate_value_against_schema(child_value, property_schema, &child_path)?;
            }
        }

        if schema_obj
            .get("additionalProperties")
            .and_then(|v| v.as_bool())
            == Some(false)
        {
            for key in object.keys() {
                if !properties.contains_key(key) {
                    return Err(format!("{} contains unknown field '{}'", path, key));
                }
            }
        }
    }

    if let Some(item_schema) = schema_obj.get("items") {
        let array = value
            .as_array()
            .ok_or_else(|| format!("{} must be an array for items validation", path))?;
        for (idx, item) in array.iter().enumerate() {
            let item_path = format!("{}[{}]", path, idx);
            validate_value_against_schema(item, item_schema, &item_path)?;
        }
    }

    Ok(())
}

fn validate_json_type(value: &Value, type_spec: &Value, path: &str) -> Result<(), String> {
    let matches = |t: &str, v: &Value| match t {
        "object" => v.is_object(),
        "array" => v.is_array(),
        "string" => v.is_string(),
        "number" => v.is_number(),
        "integer" => v.as_i64().is_some() || v.as_u64().is_some(),
        "boolean" => v.is_boolean(),
        "null" => v.is_null(),
        _ => false,
    };

    match type_spec {
        Value::String(type_name) => {
            if matches(type_name, value) {
                Ok(())
            } else {
                Err(format!("{} expected type '{}'", path, type_name))
            }
        }
        Value::Array(types) => {
            let mut any_match = false;
            for ty in types {
                if let Some(type_name) = ty.as_str() {
                    if matches(type_name, value) {
                        any_match = true;
                        break;
                    }
                }
            }
            if any_match {
                Ok(())
            } else {
                Err(format!("{} did not match any allowed types", path))
            }
        }
        _ => Err(format!("{} schema.type must be string or array", path)),
    }
}
