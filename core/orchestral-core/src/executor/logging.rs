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
