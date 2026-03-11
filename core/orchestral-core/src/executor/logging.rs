use std::collections::HashMap;

use serde_json::Value;

pub(super) const MAX_LOG_TEXT_CHARS: usize = 2_000;
pub(super) const MAX_LOG_JSON_CHARS: usize = 8_000;

pub(super) fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

pub(super) fn truncate_json_for_log(value: &Value, max_chars: usize) -> String {
    truncate_for_log(&value.to_string(), max_chars)
}

pub(super) fn truncate_json_map_for_log(map: &HashMap<String, Value>, max_chars: usize) -> String {
    let as_value = Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<serde_json::Map<String, Value>>(),
    );
    truncate_json_for_log(&as_value, max_chars)
}
