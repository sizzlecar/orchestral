use crate::skill::discovery::discover_skills;

use super::*;

impl Orchestrator {
    /// Re-discover skills from filesystem if a config path is set.
    /// Called before each planning turn for hot reload.
    pub(super) fn try_reload_skills(&self) {
        let Some(config_path) = &self.skill_config_path else {
            return;
        };
        let config = match orchestral_core::config::load_config(config_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "skill hot-reload: config load failed, skipping");
                return;
            }
        };
        let entries = match discover_skills(&config, config_path) {
            Ok(e) => e,
            Err(e) => {
                tracing::debug!(error = %e, "skill hot-reload: discovery failed, skipping");
                return;
            }
        };
        // Use try_write to avoid blocking — if catalog is locked, skip this reload.
        if let Ok(mut catalog) = self.skill_catalog.try_write() {
            let old_count = catalog.entries().len();
            catalog.reload(entries);
            let new_count = catalog.entries().len();
            if old_count != new_count {
                tracing::info!(old_count, new_count, "skill catalog hot-reloaded");
            }
        }
    }
}

pub(super) fn intent_from_event(
    event: &Event,
    interaction_id: Option<String>,
) -> Result<Intent, OrchestratorError> {
    let mut metadata = HashMap::new();
    metadata.insert(
        "event_type".to_string(),
        Value::String(event_type_label(event).to_string()),
    );
    if let Some(id) = interaction_id {
        metadata.insert("interaction_id".to_string(), Value::String(id));
    }

    let context = IntentContext {
        thread_id: Some(event.thread_id().to_string()),
        previous_task_id: None,
        metadata,
    };

    match event {
        Event::UserInput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::ExternalEvent { kind, payload, .. } => {
            let content = format!("external:{} {}", kind, payload_to_string(payload));
            Ok(Intent::with_context(content, context))
        }
        Event::AssistantOutput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::SystemTrace { level, payload, .. } => Ok(Intent::with_context(
            format!("trace:{} {}", level, payload_to_string(payload)),
            context,
        )),
        Event::Artifact { reference_id, .. } => Ok(Intent::with_context(
            format!("artifact:{}", reference_id),
            context,
        )),
    }
}

pub(super) fn payload_to_string(payload: &Value) -> String {
    if let Some(s) = payload.as_str() {
        return s.to_string();
    }
    for key in ["content", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    payload.to_string()
}

pub(super) fn event_to_history_item(event: &Event) -> Option<HistoryItem> {
    let timestamp = event.timestamp();
    match event {
        Event::UserInput { payload, .. } => Some(HistoryItem {
            role: "user".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::AssistantOutput { payload, .. } => Some(HistoryItem {
            role: "assistant".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::ExternalEvent { .. } | Event::SystemTrace { .. } | Event::Artifact { .. } => None,
    }
}

pub(super) fn context_window_to_history(window: &ContextWindow) -> Vec<HistoryItem> {
    let mut items = Vec::new();
    for slice in window.core.iter().chain(window.optional.iter()) {
        items.push(HistoryItem {
            role: slice.role.clone(),
            content: slice.content.clone(),
            timestamp: slice.timestamp.unwrap_or_else(chrono::Utc::now),
        });
    }
    items
}

pub(super) fn drop_current_turn_user_input(history: &mut Vec<HistoryItem>, current_intent: &str) {
    let Some(last) = history.last() else {
        return;
    };
    if last.role == "user" && last.content.trim() == current_intent.trim() {
        history.pop();
    }
}

pub(super) fn event_type_label(event: &Event) -> &'static str {
    match event {
        Event::UserInput { .. } => "user_input",
        Event::AssistantOutput { .. } => "assistant_output",
        Event::Artifact { .. } => "artifact",
        Event::ExternalEvent { .. } => "external_event",
        Event::SystemTrace { .. } => "system_trace",
    }
}

pub(super) fn summarize_working_set(snapshot: &HashMap<String, Value>) -> String {
    const MAX_WS_SUMMARY_CHARS: usize = 16_000;
    const LIMIT_STDOUT: usize = 4_000;
    const LIMIT_STDERR: usize = 1_000;
    const LIMIT_CONTENT: usize = 4_000;
    const LIMIT_DEFAULT: usize = 200;

    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
    enum KeyPriority {
        High = 0,
        Medium = 1,
        Low = 2,
    }

    fn classify_key(key: &str) -> (KeyPriority, usize) {
        if key == "stdout" || key.ends_with(".stdout") {
            (KeyPriority::High, LIMIT_STDOUT)
        } else if key == "content" || key.ends_with(".content") {
            (KeyPriority::High, LIMIT_CONTENT)
        } else if key == "stderr" || key.ends_with(".stderr") {
            (KeyPriority::Medium, LIMIT_STDERR)
        } else {
            (KeyPriority::Low, LIMIT_DEFAULT)
        }
    }

    fn truncate_plain(input: &str, max_chars: usize) -> String {
        let char_count = input.chars().count();
        if char_count <= max_chars {
            return input.to_string();
        }
        if max_chars <= 3 {
            return ".".repeat(max_chars);
        }
        let mut out: String = input.chars().take(max_chars - 3).collect();
        out.push_str("...");
        out
    }

    fn preview_value(value: &Value, max_chars: usize) -> String {
        match value {
            Value::String(s) => format!("\"{}\"", truncate_plain(s.trim(), max_chars)),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            other => truncate_plain(&other.to_string(), max_chars),
        }
    }

    let mut entries = snapshot.iter().collect::<Vec<_>>();
    entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut buckets: [Vec<(&String, &Value)>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for (key, value) in entries {
        let (priority, _) = classify_key(key);
        buckets[priority as usize].push((key, value));
    }

    let mut output = String::new();
    let mut remaining = MAX_WS_SUMMARY_CHARS;
    let mut omitted_count = 0usize;

    for bucket in &buckets {
        for (key, value) in bucket {
            if remaining == 0 {
                omitted_count += 1;
                continue;
            }
            let (_, per_key_limit) = classify_key(key.as_str());
            let preview = preview_value(value, per_key_limit);
            let line = format!("  {}: {}", key, preview);
            let line_len = line.chars().count();
            let sep_len = if output.is_empty() { 0 } else { 1 };

            if remaining <= sep_len {
                omitted_count += 1;
                continue;
            }
            let line_budget = remaining - sep_len;
            let line_to_append = if line_len > line_budget {
                truncate_plain(&line, line_budget)
            } else {
                line
            };
            if !output.is_empty() {
                output.push('\n');
                remaining -= 1;
            }
            remaining = remaining.saturating_sub(line_to_append.chars().count());
            output.push_str(&line_to_append);

            if line_len > line_budget {
                omitted_count += 1;
            }
        }
    }

    if output.is_empty() {
        return "(empty)".to_string();
    }

    if omitted_count > 0 && remaining > 0 {
        let summary_line = format!(
            "  ... ({} keys omitted due to summary limit)",
            omitted_count
        );
        let truncated = truncate_plain(&summary_line, remaining);
        if !truncated.is_empty() {
            output.push('\n');
            output.push_str(&truncated);
        }
    }

    output
}

fn canonical_binding_keys(snapshot: &HashMap<String, Value>) -> Vec<String> {
    const MAX_NESTED_DEPTH: usize = 3;

    fn collect_nested_object_paths(
        value: &Value,
        prefix: &str,
        depth: usize,
        out: &mut Vec<String>,
    ) {
        if depth == 0 {
            return;
        }
        let Some(object) = value.as_object() else {
            return;
        };

        let mut keys = object.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            let nested = format!("{}.{}", prefix, key);
            out.push(nested.clone());
            if let Some(next) = object.get(&key) {
                collect_nested_object_paths(next, &nested, depth.saturating_sub(1), out);
            }
        }
    }

    fn has_more_specific_alias(snapshot: &HashMap<String, Value>, key: &str) -> bool {
        !key.contains('.')
            && snapshot.keys().any(|candidate| {
                candidate.contains('.') && candidate.ends_with(&format!(".{}", key))
            })
    }

    let mut top_level_keys = snapshot.keys().cloned().collect::<Vec<_>>();
    top_level_keys.sort();
    top_level_keys.dedup();

    let mut bindings = Vec::new();
    for key in top_level_keys {
        if key.trim().is_empty() || has_more_specific_alias(snapshot, &key) {
            continue;
        }
        bindings.push(key.clone());
        if let Some(value) = snapshot.get(&key) {
            collect_nested_object_paths(value, &key, MAX_NESTED_DEPTH, &mut bindings);
        }
    }

    bindings.sort();
    bindings.dedup();
    bindings
}

fn lookup_binding_value<'a>(snapshot: &'a HashMap<String, Value>, key: &str) -> Option<&'a Value> {
    if let Some(value) = snapshot.get(key) {
        return Some(value);
    }

    let segments = key.split('.').collect::<Vec<_>>();
    if segments.len() < 2 {
        return None;
    }

    for split in (1..segments.len()).rev() {
        let prefix = segments[..split].join(".");
        let Some(mut current) = snapshot.get(&prefix) else {
            continue;
        };
        let mut resolved = true;
        for segment in &segments[split..] {
            let Some(object) = current.as_object() else {
                resolved = false;
                break;
            };
            let Some(next) = object.get(*segment) else {
                resolved = false;
                break;
            };
            current = next;
        }
        if resolved {
            return Some(current);
        }
    }

    None
}

fn summarize_value_shape(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(items) => {
            let item_shape = items
                .first()
                .map(summarize_value_shape)
                .unwrap_or_else(|| "unknown".to_string());
            format!("array<{}>", item_shape)
        }
        Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            if keys.len() > 4 {
                keys.truncate(4);
                keys.push("...".to_string());
            }
            format!("object {{{}}}", keys.join(", "))
        }
    }
}

pub(super) fn summarize_available_bindings(snapshot: &HashMap<String, Value>) -> Vec<String> {
    const MAX_BINDINGS: usize = 32;

    canonical_binding_keys(snapshot)
        .into_iter()
        .take(MAX_BINDINGS)
        .map(|key| format!("{{{{{}}}}}", key))
        .collect()
}

pub(super) fn summarize_binding_shapes(snapshot: &HashMap<String, Value>) -> Vec<String> {
    const MAX_BINDING_SHAPES: usize = 16;

    canonical_binding_keys(snapshot)
        .into_iter()
        .filter_map(|key| {
            let value = lookup_binding_value(snapshot, &key)?;
            Some(format!(
                "{{{{{}}}}} -> {}",
                key,
                summarize_value_shape(value)
            ))
        })
        .take(MAX_BINDING_SHAPES)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_summarize_available_bindings_prefers_step_scoped_aliases_and_nested_paths() {
        let snapshot = HashMap::from([
            ("continuation".to_string(), json!({"status":"commit_ready"})),
            (
                "assess.continuation".to_string(),
                json!({
                    "status": "commit_ready",
                    "fills": [{"cell": "F5", "value": 90}],
                    "patch_spec": {"fills": [{"cell": "F5", "value": 90}]}
                }),
            ),
            ("source_path".to_string(), json!("docs/report.xlsx")),
            ("locate.source_path".to_string(), json!("docs/report.xlsx")),
        ]);

        let bindings = summarize_available_bindings(&snapshot);

        assert!(bindings.contains(&"{{assess.continuation}}".to_string()));
        assert!(bindings.contains(&"{{assess.continuation.fills}}".to_string()));
        assert!(bindings.contains(&"{{assess.continuation.patch_spec}}".to_string()));
        assert!(bindings.contains(&"{{locate.source_path}}".to_string()));
        assert!(!bindings.contains(&"{{continuation}}".to_string()));
        assert!(!bindings.contains(&"{{source_path}}".to_string()));
    }

    #[test]
    fn test_summarize_binding_shapes_includes_nested_contract_hints() {
        let snapshot = HashMap::from([(
            "assess.continuation".to_string(),
            json!({
                "status": "commit_ready",
                "fills": [{"cell": "F5", "value": 90}],
                "patch_spec": {"fills": [{"cell": "F5", "value": 90}]}
            }),
        )]);

        let shapes = summarize_binding_shapes(&snapshot);

        assert!(shapes.contains(
            &"{{assess.continuation}} -> object {fills, patch_spec, status}".to_string()
        ));
        assert!(
            shapes.contains(&"{{assess.continuation.patch_spec}} -> object {fills}".to_string())
        );
        assert!(shapes
            .contains(&"{{assess.continuation.fills}} -> array<object {cell, value}>".to_string()));
    }
}
