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
                }
                other => {
                    return Err(format!("unsupported structured operation '{}'", other));
                }
            }
        }
    }

    Ok((
        VerifyDecision {
            status: VerifyStatus::Passed,
            reason: "structured patch verified".to_string(),
            evidence: json!({
                "checked_paths": checked_paths,
                "checked_operations": checked_ops,
                "inspection_target_count": inspection
                    .and_then(|value| value.get("target_count"))
                    .cloned()
                    .unwrap_or(Value::Null),
            }),
        },
        "Structured patch verified.".to_string(),
    ))
}
