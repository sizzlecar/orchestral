use std::collections::HashMap;

use serde_json::Value;

use super::logging::truncate_for_log;
use super::{ExecutionProgressEvent, ExecutionResult, ExecutorContext};

pub(super) fn build_step_completion_metadata(
    action: &str,
    exports: &HashMap<String, Value>,
) -> Value {
    let mut export_keys: Vec<String> = exports.keys().cloned().collect();
    export_keys.sort();

    let mut metadata = serde_json::Map::new();
    metadata.insert("action".to_string(), Value::String(action.to_string()));
    metadata.insert(
        "export_count".to_string(),
        Value::Number(serde_json::Number::from(export_keys.len())),
    );
    metadata.insert(
        "export_keys".to_string(),
        Value::Array(export_keys.into_iter().map(Value::String).collect()),
    );

    if let Some(path) = completion_path_from_exports(exports) {
        metadata.insert(
            "path".to_string(),
            Value::String(truncate_for_log(path, 240)),
        );
    }
    if let Some(bytes) = exports.get("bytes").and_then(|v| v.as_u64()) {
        metadata.insert(
            "bytes".to_string(),
            Value::Number(serde_json::Number::from(bytes)),
        );
    }
    if let Some(status) = exports.get("status").and_then(|v| v.as_i64()) {
        metadata.insert(
            "status".to_string(),
            Value::Number(serde_json::Number::from(status)),
        );
    }
    if let Some(timed_out) = exports.get("timed_out").and_then(|v| v.as_bool()) {
        metadata.insert("timed_out".to_string(), Value::Bool(timed_out));
    }

    if let Some(preview) = output_preview(exports) {
        metadata.insert("output_preview".to_string(), Value::String(preview));
    }

    Value::Object(metadata)
}

fn completion_path_from_exports(exports: &HashMap<String, Value>) -> Option<&str> {
    const CANDIDATE_KEYS: [&str; 8] = [
        "path",
        "target_path",
        "output_path",
        "file_path",
        "destination_path",
        "dest_path",
        "target",
        "updated_file_path",
    ];
    for key in CANDIDATE_KEYS {
        if let Some(path) = exports.get(key).and_then(|v| v.as_str()) {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(path);
            }
        }
    }
    None
}

pub(super) fn build_step_start_metadata(action: &str, params: &Value) -> Value {
    let mut metadata = serde_json::Map::new();
    metadata.insert("action".to_string(), Value::String(action.to_string()));
    if let Some(summary) = summarize_step_input(action, params) {
        metadata.insert("input_summary".to_string(), Value::String(summary));
    }
    Value::Object(metadata)
}

fn summarize_step_input(action: &str, params: &Value) -> Option<String> {
    let obj = params.as_object()?;
    match action {
        "shell" => {
            let command = obj.get("command").and_then(|v| v.as_str())?;
            let args = obj
                .get("args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            let line = if args.is_empty() {
                command.to_string()
            } else {
                format!("{} {}", command, args)
            };
            Some(truncate_for_log(&line, 240))
        }
        "http" => {
            let method = obj.get("method").and_then(|v| v.as_str()).unwrap_or("GET");
            let url = obj.get("url").and_then(|v| v.as_str())?;
            Some(truncate_for_log(&format!("{} {}", method, url), 240))
        }
        "file_read" | "file_write" => obj
            .get("path")
            .and_then(|v| v.as_str())
            .map(|p| truncate_for_log(p, 240)),
        _ => None,
    }
}

fn output_preview(exports: &HashMap<String, Value>) -> Option<String> {
    for key in ["result", "content", "stdout", "stderr"] {
        if let Some(value) = exports.get(key).and_then(|v| v.as_str()) {
            return Some(truncate_for_log(value, 320));
        }
    }
    if let Some(headers) = exports.get("headers").and_then(|v| v.as_object()) {
        if !headers.is_empty() {
            let mut pairs = headers
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| format!("{}: {}", k, s)))
                .collect::<Vec<_>>();
            pairs.sort();
            let preview = pairs.into_iter().take(8).collect::<Vec<_>>().join("\n");
            if !preview.is_empty() {
                return Some(truncate_for_log(&preview, 320));
            }
        }
    }
    if let Some(value) = exports.get("body").and_then(|v| v.as_str()) {
        return Some(truncate_for_log(value, 320));
    }
    None
}

pub(super) async fn report_progress(ctx: &ExecutorContext, event: ExecutionProgressEvent) {
    if let Some(reporter) = &ctx.progress_reporter {
        if let Err(err) = reporter.report(event).await {
            tracing::warn!("failed to report execution progress: {}", err);
        }
    }
}

pub(super) fn choose_terminal_result(
    slot: &mut Option<ExecutionResult>,
    candidate: ExecutionResult,
) {
    let rank = |result: &ExecutionResult| match result {
        ExecutionResult::Failed { .. } => 4,
        ExecutionResult::NeedReplan { .. } => 3,
        ExecutionResult::WaitingUser { .. } => 2,
        ExecutionResult::WaitingEvent { .. } => 1,
        ExecutionResult::Completed => 0,
    };

    match slot {
        Some(current) if rank(current) >= rank(&candidate) => {}
        _ => *slot = Some(candidate),
    }
}
