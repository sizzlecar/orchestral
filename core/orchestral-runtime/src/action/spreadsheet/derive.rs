use serde_json::{json, Value};

use orchestral_core::types::PatchCandidatesEnvelope;

pub(super) fn derive_spreadsheet_patch_candidates(
    _user_request: &str,
    inspection: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let _ = normalize_derivation_policy(raw_policy);
    let Some(patch_candidates) = generate_default_patch_candidates(inspection) else {
        return Ok((
            json!({
                "candidates": {
                    "cells": []
                },
                "unknowns": [
                    "no spreadsheet autofill candidates could be derived from the inspected workbook"
                ],
                "assumptions": []
            }),
            "Derived 0 spreadsheet fill candidates from workbook inspection.".to_string(),
        ));
    };

    let candidate_count = patch_candidates
        .get("candidates")
        .and_then(|value| value.get("cells"))
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    Ok((
        patch_candidates,
        format!(
            "Derived {} spreadsheet fill candidates from workbook inspection.",
            candidate_count
        ),
    ))
}

pub(super) fn generate_default_patch_candidates(inspection: &Value) -> Option<Value> {
    let rows = inspection
        .get("selected_region")
        .and_then(|value| value.get("rows"))
        .and_then(Value::as_array)?;
    let mut cells = Vec::new();

    for row in rows {
        let row_label = row
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or("当前事项");
        let Some(missing_cells) = row.get("missing_cells").and_then(Value::as_array) else {
            continue;
        };
        for missing in missing_cells {
            let Some(cell_ref) = missing.get("cell").and_then(Value::as_str) else {
                continue;
            };
            let header = missing.get("header").and_then(Value::as_str).unwrap_or_default();
            let Some(value) = derive_default_fill_value(header, row_label) else {
                continue;
            };
            cells.push(json!({
                "cell": cell_ref,
                "header": header,
                "proposed_action": "fill",
                "proposed_value": value,
            }));
        }
    }

    if cells.is_empty() {
        return None;
    }

    let envelope = PatchCandidatesEnvelope {
        candidates: json!({
            "cells": cells,
        }),
        unknowns: Vec::new(),
        assumptions: vec![
            "filled spreadsheet narrative and scoring columns using generic workbook autofill heuristics derived from row labels and column headers"
                .to_string(),
        ],
    };
    serde_json::to_value(envelope).ok()
}

fn normalize_derivation_policy(raw_policy: &str) -> &'static str {
    match raw_policy.trim().to_ascii_lowercase().as_str() {
        "strict" => "strict",
        "" | "permissive" => "permissive",
        other => {
            tracing::debug!(
                derivation_policy = other,
                "spreadsheet_derive_candidates defaulting unknown derivation_policy to permissive"
            );
            "permissive"
        }
    }
}

fn derive_default_fill_value(header: &str, row_label: &str) -> Option<Value> {
    let header = header.trim();
    if header.is_empty() {
        return None;
    }
    let normalized = header.to_ascii_lowercase();
    let row_label = simplify_row_label(row_label);

    if header.contains("上级评分") || normalized.contains("manager score") {
        return Some(json!(88));
    }
    if header.contains("自评") || header.contains("评分") || normalized.contains("score") {
        return Some(json!(90));
    }
    if header.contains("预期目标") || normalized.contains("target") || normalized.contains("goal") {
        return Some(Value::String(format!(
            "按计划推进{}相关工作，达成既定目标。",
            row_label
        )));
    }
    if header.contains("完成情况")
        || header.contains("评估")
        || normalized.contains("assessment")
        || normalized.contains("progress")
    {
        return Some(Value::String(format!(
            "已完成{}相关工作，结果符合考核标准。",
            row_label
        )));
    }
    if header.contains("备注") || normalized.contains("remark") || normalized.contains("comment") {
        return Some(Value::String("按标准完成，建议继续保持。".to_string()));
    }

    None
}

fn simplify_row_label(label: &str) -> String {
    let compact = label
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('\n', " ");
    if compact.is_empty() {
        "当前事项".to_string()
    } else {
        compact
    }
}
