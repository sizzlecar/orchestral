use std::path::Path;

use serde_json::{json, Value};

use orchestral_core::types::{VerifyDecision, VerifyStatus};

use super::model::{parse_structured_file, parse_structured_patch_spec};
use super::support::{display_path, normalize_path};

pub(super) fn verify_structured_patch(
    patch_spec: &Value,
    inspection: Option<&Value>,
) -> Result<(VerifyDecision, String), String> {
    let patch_spec = parse_structured_patch_spec(patch_spec)?;
    if patch_spec.files.is_empty() {
        return Err("structured verify received empty patch_spec.files".to_string());
    }

    let mut checked_paths = Vec::new();
    let mut checked_ops = 0usize;
    let mut verified_changes = Vec::new();
    for file in &patch_spec.files {
        let path = normalize_path(Path::new(&file.path))?;
        let parsed = parse_structured_file(&path)?;
        checked_paths.push(display_path(&path));

        for operation in &file.operations {
            checked_ops += 1;
            match operation.op.as_str() {
                "set" => {
                    let Some(expected) = &operation.value else {
                        return Err(format!(
                            "structured set operation '{}' is missing value",
                            operation.path
                        ));
                    };
                    let Some(actual) = parsed.pointer(&operation.path) else {
                        return Ok((
                            VerifyDecision {
                                status: VerifyStatus::Failed,
                                reason: format!(
                                    "structured path '{}' is missing after patch",
                                    operation.path
                                ),
                                evidence: json!({
                                    "path": display_path(&path),
                                    "operation": operation,
                                }),
                            },
                            "Structured verify failed.".to_string(),
                        ));
                    };
                    if actual != expected {
                        return Ok((
                            VerifyDecision {
                                status: VerifyStatus::Failed,
                                reason: format!(
                                    "structured path '{}' does not match expected value",
                                    operation.path
                                ),
                                evidence: json!({
                                    "path": display_path(&path),
                                    "operation": operation,
                                    "actual": actual,
                                }),
                            },
                            "Structured verify failed.".to_string(),
                        ));
                    }
                    verified_changes.push(format_verified_change(file.path.as_str(), operation));
                }
                "remove" => {
                    if parsed.pointer(&operation.path).is_some() {
                        return Ok((
                            VerifyDecision {
                                status: VerifyStatus::Failed,
                                reason: format!(
                                    "structured path '{}' still exists after remove",
                                    operation.path
                                ),
                                evidence: json!({
                                    "path": display_path(&path),
                                    "operation": operation,
                                }),
                            },
                            "Structured verify failed.".to_string(),
                        ));
                    }
                    verified_changes.push(format_verified_change(file.path.as_str(), operation));
                }
                other => {
                    return Err(format!("unsupported structured operation '{}'", other));
                }
            }
        }
    }

    let summary = build_verify_summary(&verified_changes);

    Ok((
        VerifyDecision {
            status: VerifyStatus::Passed,
            reason: "structured patch verified".to_string(),
            evidence: json!({
                "checked_paths": checked_paths,
                "checked_operations": checked_ops,
                "verified_changes": verified_changes,
                "inspection_target_count": inspection
                    .and_then(|value| value.get("target_count"))
                    .cloned()
                    .unwrap_or(Value::Null),
            }),
        },
        summary,
    ))
}

fn build_verify_summary(changes: &[String]) -> String {
    if changes.is_empty() {
        return "Structured patch verified.".to_string();
    }
    let mut summary = format!("Structured patch verified. Changes: {}", changes.join("; "));
    if !summary.ends_with('.') {
        summary.push('.');
    }
    summary
}

fn format_verified_change(
    path: &str,
    operation: &super::model::StructuredPatchOperation,
) -> String {
    let target = operation
        .selector
        .as_deref()
        .unwrap_or(operation.path.as_str());
    let reason = operation.reason.as_deref().unwrap_or("verified");
    match operation.op.as_str() {
        "set" => {
            let rendered = operation
                .value
                .as_ref()
                .map(render_value)
                .unwrap_or_else(|| "<missing>".to_string());
            format!("{} -> set {} = {} ({})", path, target, rendered, reason)
        }
        "remove" => format!("{} -> removed {} ({})", path, target, reason),
        other => format!("{} -> {} {} ({})", path, other, target, reason),
    }
}

fn render_value(value: &Value) -> String {
    match value {
        Value::String(text) => format!("{:?}", text),
        other => other.to_string(),
    }
}
