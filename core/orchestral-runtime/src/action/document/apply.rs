use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde_json::{json, Value};

use super::support::normalize_document_updates;

pub(super) fn apply_document_patch(
    patch_spec: &Value,
    report_path: Option<&str>,
    inspection: Option<&Value>,
    patch_candidates: Option<&Value>,
) -> Result<HashMap<String, Value>, String> {
    let updates = normalize_document_updates(
        patch_spec,
        report_path,
        inspection,
        patch_candidates,
    )?;
    if updates.is_empty() {
        return Err("document patch_spec.updates is empty".to_string());
    }

    let mut updated_paths = Vec::new();
    for update in &updates {
        let path = Path::new(&update.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "create document parent '{}' failed: {}",
                    parent.display(),
                    err
                )
            })?;
        }
        fs::write(path, update.content.as_bytes())
            .map_err(|err| format!("write document '{}' failed: {}", update.path, err))?;
        updated_paths.push(update.path.clone());
    }

    if let Some(report_path) = report_path {
        if !updates.iter().any(|update| update.path == report_path) {
            let report_content = synthesize_report_markdown(
                patch_spec
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("Document patch applied."),
                &updated_paths,
            );
            let path = Path::new(report_path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|err| {
                    format!(
                        "create document report parent '{}' failed: {}",
                        parent.display(),
                        err
                    )
                })?;
            }
            fs::write(path, report_content.as_bytes()).map_err(|err| {
                format!("write document report '{}' failed: {}", report_path, err)
            })?;
            updated_paths.push(report_path.to_string());
        }
    }

    Ok([
        (
            "updated_paths".to_string(),
            Value::Array(updated_paths.iter().cloned().map(Value::String).collect()),
        ),
        ("patch_count".to_string(), json!(updated_paths.len())),
        (
            "summary".to_string(),
            Value::String(
                patch_spec
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("Document patch applied.")
                    .to_string(),
            ),
        ),
    ]
    .into_iter()
    .collect())
}

fn synthesize_report_markdown(summary: &str, updated_paths: &[String]) -> String {
    let mut lines = vec!["# Patch Summary".to_string(), String::new()];
    let trimmed = summary.trim();
    if !trimmed.is_empty() {
        lines.push(trimmed.to_string());
        lines.push(String::new());
    }
    lines.push("Updated files:".to_string());
    lines.extend(updated_paths.iter().map(|path| format!("- {}", path)));
    lines.join("\n")
}
