use serde_json::{json, Value};

/// A transient view of canonical JSON. Only top-level strings move to text;
/// nested values retain their JSON types and are not recursively flattened.
struct ToolTextView<'a> {
    metadata: Value,
    fields: Vec<(Option<&'a str>, &'a str)>,
}

impl<'a> ToolTextView<'a> {
    fn new(result: &'a Value, is_error: bool) -> Self {
        let mut fields = Vec::new();
        let mut metadata = match result {
            Value::Object(object) => {
                let mut remaining = serde_json::Map::new();
                // Do not depend on serde_json's preserve_order feature: both
                // the part order and the metadata bytes are format identity.
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                for (key, value) in entries {
                    if let Value::String(text) = value {
                        fields.push((Some(key.as_str()), text.as_str()));
                    } else {
                        remaining.insert(key.clone(), value.clone());
                    }
                }
                json!({"result": remaining, "is_error": is_error})
            }
            Value::String(text) => {
                fields.push((None, text.as_str()));
                json!({"is_error": is_error})
            }
            _ => json!({"result": result, "is_error": is_error}),
        };
        metadata.sort_all_objects();
        Self { metadata, fields }
    }
}

pub(super) fn encode(result: &Value, is_error: bool) -> Value {
    let view = ToolTextView::new(result, is_error);
    let mut parts = Vec::with_capacity(view.fields.len() + 1);
    parts.push(json!({
        "type": "text",
        "text": format!("Tool result metadata:\n{}\n", view.metadata),
    }));
    for (key, text) in view.fields {
        let label = match key {
            Some(key) => format!("Text field {}", json!(key)),
            None => "Result text".to_owned(),
        };
        parts.push(json!({"type": "text", "text": fenced_text(&label, text)}));
    }
    Value::Array(parts)
}

fn fenced_text(label: &str, text: &str) -> String {
    let mut longest = 0;
    let mut run = 0;
    for byte in text.bytes() {
        if byte == b'`' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    let fence = "`".repeat(3.max(longest + 1));
    let has_final_newline = text.ends_with('\n');
    let mut rendered = format!(
        "{label} (final newline: {})\n{fence}text\n",
        if has_final_newline { "yes" } else { "no" }
    );
    rendered.push_str(text);
    // This separator is framing, not source. Keep the explicit boundary label
    // and the entire fenced field in one part: servers may join parts with LF
    // and templates may trim the outside of the complete tool message.
    if !has_final_newline {
        rendered.push('\n');
    }
    rendered.push_str(&fence);
    // Some templates concatenate array parts without inserting separators.
    // End the closing fence's line here, outside the original source span.
    rendered.push('\n');
    rendered
}

#[cfg(test)]
mod tests;
