use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde_json::{json, Value};

use orchestral_core::types::{VerifyDecision, VerifyStatus};

use super::model::inspect_document_content;
use super::support::{normalize_document_updates, requested_report_path};

pub(super) fn verify_document_patch(
    patch_spec: &Value,
    inspection: &Value,
    patch_candidates: Option<&Value>,
    user_request: &str,
    resume_user_input: Option<&Value>,
) -> Result<(VerifyDecision, String), String> {
    let report_path = requested_report_path(user_request, resume_user_input);
    let updates = normalize_document_updates(
        patch_spec,
        report_path.as_deref(),
        Some(inspection),
        patch_candidates,
    )?;
    if updates.is_empty() {
        return Err("document verify received empty patch_spec.updates".to_string());
    }

    let mut actual_content = BTreeMap::new();
    for update in &updates {
        let content = fs::read_to_string(&update.path)
            .map_err(|err| format!("read patched document '{}' failed: {}", update.path, err))?;
        if content != update.content {
            return Ok((
                VerifyDecision {
                    status: VerifyStatus::Failed,
                    reason: format!(
                        "patched document '{}' does not match expected content",
                        update.path
                    ),
                    evidence: json!({ "path": update.path }),
                },
                "Document verify failed.".to_string(),
            ));
        }
        actual_content.insert(update.path.clone(), content);
    }

    let files = inspection
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut title_fixed = Vec::new();
    let mut todo_cleared = Vec::new();

    for file in files {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        let Some(content) = actual_content.get(path) else {
            continue;
        };
        let after = inspect_document_content(Path::new(path), content);
        let missing_title_before = file
            .get("missing_title")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if missing_title_before {
            if after.missing_title {
                return Ok((
                    VerifyDecision {
                        status: VerifyStatus::Failed,
                        reason: format!("document '{}' still has no top-level title", path),
                        evidence: json!({ "path": path }),
                    },
                    "Document verify failed.".to_string(),
                ));
            }
            title_fixed.push(path.to_string());
        }
        let todo_count_before = file
            .get("todo_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if todo_count_before > 0 {
            if after.todo_count > 0 {
                return Ok((
                    VerifyDecision {
                        status: VerifyStatus::Failed,
                        reason: format!("document '{}' still contains TODO markers", path),
                        evidence: json!({
                            "path": path,
                            "todo_lines": after.todo_lines,
                        }),
                    },
                    "Document verify failed.".to_string(),
                ));
            }
            todo_cleared.push(path.to_string());
        }
    }

    if let Some(report_path) = report_path {
        let content = fs::read_to_string(&report_path)
            .map_err(|err| format!("read report '{}' failed: {}", report_path, err))?;
        if content.trim().is_empty() || content.contains("Pending run.") {
            return Ok((
                VerifyDecision {
                    status: VerifyStatus::Failed,
                    reason: format!(
                        "report '{}' was not updated with a real summary",
                        report_path
                    ),
                    evidence: json!({ "path": report_path }),
                },
                "Document verify failed.".to_string(),
            ));
        }
    }

    Ok((
        VerifyDecision {
            status: VerifyStatus::Passed,
            reason: "document patch verified".to_string(),
            evidence: json!({
                "checked_paths": actual_content.keys().cloned().collect::<Vec<_>>(),
                "title_fixed": title_fixed,
                "todo_cleared": todo_cleared,
            }),
        },
        "Document patch verified.".to_string(),
    ))
}
