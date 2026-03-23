use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use orchestral_core::types::{ContinuationState, ContinuationStatus};

/// Derivation policy for document assessment (local enum replacing the removed core type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DerivationPolicy {
    Strict,
    Permissive,
}

use super::support::{
    document_candidate_files, parse_patch_candidates_envelope, request_requires_confirmation,
};

pub(super) fn assess_document_readiness(
    user_request: &str,
    inspection: &Value,
    patch_candidates: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let policy = parse_derivation_policy(raw_policy)?;
    let files = inspection
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| "document inspection is missing files".to_string())?;
    if files.is_empty() {
        return Err("document inspection produced no files".to_string());
    }

    let summary = synthesize_document_plan_summary(inspection, patch_candidates);
    let mut unknowns = collect_candidate_unknowns(patch_candidates);
    let auto_title_paths = auto_title_candidate_paths(inspection, patch_candidates);
    if policy == DerivationPolicy::Permissive && !auto_title_paths.is_empty() {
        unknowns.retain(|item| {
            !is_title_preference_unknown(item)
                && !auto_title_paths
                    .iter()
                    .any(|path| item == &format!("{} requires additional user input", path))
        });
    }
    let needs_confirmation = request_requires_confirmation(user_request);
    let has_any_change = files.iter().any(|file| {
        file.get("missing_title")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || file
                .get("todo_count")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                > 0
    });

    let continuation = if !has_any_change {
        ContinuationState {
            status: ContinuationStatus::Done,
            reason: "document inspection found no missing titles or TODO placeholders".to_string(),
            unknowns: Vec::new(),
            assumptions: Vec::new(),
            user_message: Some("未发现需要写回的文档改动。".to_string()),
        }
    } else if needs_confirmation {
        let mut user_message = format!("{}\n\n回复“确认”后我会按这个计划写回。", summary);
        if !unknowns.is_empty() {
            user_message.push_str(&format!("\n仍有待确认信息：{}。", unknowns.join("；")));
        }
        ContinuationState {
            status: ContinuationStatus::WaitUser,
            reason: if unknowns.is_empty() || policy == DerivationPolicy::Permissive {
                "user requested review/confirmation before document writes".to_string()
            } else {
                "bounded derivation reported unresolved document unknowns".to_string()
            },
            unknowns: unknowns.clone(),
            assumptions: Vec::new(),
            user_message: Some(user_message),
        }
    } else if !unknowns.is_empty() {
        ContinuationState {
            status: ContinuationStatus::WaitUser,
            reason: "bounded derivation reported unresolved document unknowns".to_string(),
            unknowns: unknowns.clone(),
            assumptions: Vec::new(),
            user_message: Some(format!(
                "{}\n\n仍有待确认信息：{}。",
                summary,
                unknowns.join("；")
            )),
        }
    } else {
        ContinuationState {
            status: ContinuationStatus::CommitReady,
            reason: "document probe gathered enough structure to derive a typed patch".to_string(),
            unknowns: Vec::new(),
            assumptions: Vec::new(),
            user_message: None,
        }
    };

    Ok((
        serde_json::to_value(continuation).unwrap_or(Value::Null),
        summary,
    ))
}

pub(super) fn collect_candidate_unknowns(patch_candidates: &Value) -> Vec<String> {
    let mut unknowns = parse_patch_candidates_envelope(patch_candidates)
        .map(|value| value.unknowns)
        .unwrap_or_else(|| {
            patch_candidates
                .get("unknowns")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        });
    for file in document_candidate_files(patch_candidates) {
        if file
            .get("needs_user_input")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if let Some(path) = file.get("path").and_then(Value::as_str) {
                unknowns.push(format!("{} requires additional user input", path));
            }
        }
        if let Some(items) = file.get("unknowns").and_then(Value::as_array) {
            unknowns.extend(items.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    unknowns.sort();
    unknowns.dedup();
    unknowns
}

fn auto_title_candidate_paths(inspection: &Value, patch_candidates: &Value) -> BTreeSet<String> {
    let Some(files) = inspection.get("files").and_then(Value::as_array) else {
        return BTreeSet::new();
    };
    let inspection_by_path = files
        .iter()
        .filter_map(|file| {
            let path = file.get("path").and_then(Value::as_str)?;
            Some((path, file))
        })
        .collect::<BTreeMap<_, _>>();

    let candidate_files = document_candidate_files(patch_candidates);
    candidate_files
        .iter()
        .filter_map(|file| {
            let path = file.get("path").and_then(Value::as_str)?;
            let inspection_file = inspection_by_path.get(path)?;
            let eligible = inspection_file
                .get("missing_title")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && inspection_file
                    .get("suggested_title")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
                && inspection_file
                    .get("todo_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    == 0
                && file
                    .get("unknowns")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .all(|item| item.as_str().is_some_and(is_title_preference_unknown))
                    })
                    .unwrap_or(true)
                && file
                    .get("planned_changes")
                    .and_then(Value::as_array)
                    .map(|items| {
                        !items.is_empty()
                            && items.iter().all(|item| {
                                item.get("description")
                                    .and_then(Value::as_str)
                                    .or_else(|| item.as_str())
                                    .is_some_and(|text| {
                                        text.contains("title") || text.contains("标题")
                                    })
                            })
                    })
                    .unwrap_or(false);
            eligible.then(|| path.to_string())
        })
        .collect()
}

fn is_title_preference_unknown(unknown: &str) -> bool {
    unknown.contains("Preferred H1 title text") || unknown.contains("Exact H1 title wording")
}

fn synthesize_document_plan_summary(inspection: &Value, patch_candidates: &Value) -> String {
    let mut lines = vec!["计划修改以下文档：".to_string()];
    let candidate_files = document_candidate_files(patch_candidates);
    let mut candidate_map = BTreeMap::new();
    for file in candidate_files {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        candidate_map.insert(path.to_string(), file);
    }

    if let Some(files) = inspection.get("files").and_then(Value::as_array) {
        for file in files {
            let Some(path) = file.get("path").and_then(Value::as_str) else {
                continue;
            };
            let missing_title = file
                .get("missing_title")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let todo_count = file
                .get("todo_count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let mut changes = Vec::new();
            if missing_title {
                changes.push("补全一级标题".to_string());
            }
            if todo_count > 0 {
                changes.push(format!("替换 {} 处 TODO 占位符", todo_count));
            }
            if let Some(candidate) = candidate_map.get(path) {
                if let Some(extra) = candidate
                    .get("planned_changes")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                {
                    for item in extra {
                        if !item.trim().is_empty() {
                            changes.push(item);
                        }
                    }
                }
            }
            if changes.is_empty() {
                changes.push("检查并保持现有内容结构".to_string());
            }
            lines.push(format!("- {}：{}", path, changes.join("；")));
        }
    }

    if let Some(summary) = patch_candidates.get("summary").and_then(Value::as_str) {
        let trimmed = summary.trim();
        if !trimmed.is_empty() {
            lines.push(String::new());
            lines.push(trimmed.to_string());
        }
    }
    lines.join("\n")
}

fn parse_derivation_policy(raw_policy: &str) -> Result<DerivationPolicy, String> {
    match raw_policy.trim().to_ascii_lowercase().as_str() {
        "strict" => Ok(DerivationPolicy::Strict),
        "" | "permissive" | "filename_to_h1" | "markdown_h1_from_filename" => {
            Ok(DerivationPolicy::Permissive)
        }
        other => {
            tracing::debug!(
                derivation_policy = other,
                "document_assess_readiness defaulting unknown derivation_policy to permissive"
            );
            Ok(DerivationPolicy::Permissive)
        }
    }
}
