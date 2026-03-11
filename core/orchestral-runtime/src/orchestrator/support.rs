use super::*;

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
