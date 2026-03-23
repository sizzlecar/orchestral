use std::collections::HashSet;

use serde_json::{json, Value};

use orchestral_core::types::{ContinuationState, ContinuationStatus, PatchCandidatesEnvelope};

use super::derive::generate_default_patch_candidates;

/// Derivation policy for spreadsheet assessment (local enum replacing the removed core type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DerivationPolicy {
    Strict,
    Permissive,
}

pub(super) fn assess_readiness(
    inspection: &Value,
    patch_candidates: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let derivation_policy = match raw_policy.trim().to_ascii_lowercase().as_str() {
        "strict" => DerivationPolicy::Strict,
        "" | "permissive" => DerivationPolicy::Permissive,
        other => {
            tracing::debug!(
                derivation_policy = other,
                "spreadsheet_assess_readiness defaulting unknown derivation_policy to permissive"
            );
            DerivationPolicy::Permissive
        }
    };

    let patchable_cells = inspection
        .get("selected_region")
        .and_then(|value| value.get("patchable_cells"))
        .and_then(Value::as_array)
        .ok_or_else(|| "inspection.selected_region.patchable_cells must be an array".to_string())?;
    let generated_patch_candidates = if has_candidate_cells(patch_candidates) {
        None
    } else {
        generate_default_patch_candidates(inspection)
    };
    let patch_candidates = generated_patch_candidates
        .as_ref()
        .unwrap_or(patch_candidates);
    let patchable_refs = patchable_cells
        .iter()
        .filter_map(|cell| cell.get("cell").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<HashSet<_>>();
    let candidate_cells = extract_spreadsheet_candidate_cells(patch_candidates);

    if patchable_refs.is_empty() {
        let continuation = ContinuationState {
            status: ContinuationStatus::Done,
            reason: "selected spreadsheet region has no patchable cells".to_string(),
            unknowns: Vec::new(),
            assumptions: Vec::new(),
            user_message: None,
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            "Spreadsheet probe found no editable gaps.".to_string(),
        ));
    }

    let mut covered_refs = HashSet::new();
    let mut placeholder_refs = Vec::new();
    for candidate in &candidate_cells {
        let cell_ref = candidate
            .get("cell")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let proposed_action = candidate
            .get("proposed_action")
            .and_then(Value::as_str)
            .unwrap_or("fill");
        let proposed_value = candidate_value_text(&candidate).unwrap_or_default();
        if proposed_action != "fill" || proposed_value.trim().is_empty() {
            continue;
        }
        if patchable_refs.contains(&cell_ref) {
            covered_refs.insert(cell_ref.clone());
            if looks_like_placeholder_value(&proposed_value) {
                placeholder_refs.push(cell_ref);
            }
        }
    }

    let envelope = parse_patch_candidates_envelope(patch_candidates);
    let mut unknowns = envelope
        .as_ref()
        .map(|value| value.unknowns.clone())
        .unwrap_or_else(|| extract_string_array_field(patch_candidates, "unknowns"));
    let assumptions = envelope
        .as_ref()
        .map(|value| value.assumptions.clone())
        .unwrap_or_else(|| extract_string_array_field(patch_candidates, "assumptions"));
    let fills = build_fill_spec(&candidate_cells, &patchable_refs);

    if matches!(derivation_policy, DerivationPolicy::Permissive) && !covered_refs.is_empty() {
        let continuation = ContinuationState {
            status: ContinuationStatus::CommitReady,
            reason: format!(
                "probe identified the target spreadsheet region and produced {} candidate fills for {} patchable cells under permissive policy",
                covered_refs.len(),
                patchable_refs.len()
            ),
            unknowns,
            assumptions,
            user_message: None,
        };
        return Ok((
            continuation_with_fills(continuation, fills).map_err(|err| err.to_string())?,
            format!(
                "Spreadsheet probe ready for permissive commit with {} candidate fills and {} patchable cells.",
                covered_refs.len(),
                patchable_refs.len()
            ),
        ));
    }

    if covered_refs.len() == patchable_refs.len()
        && placeholder_refs.is_empty()
        && matches!(derivation_policy, DerivationPolicy::Strict)
    {
        let continuation = ContinuationState {
            status: ContinuationStatus::CommitReady,
            reason: format!(
                "probe produced complete non-placeholder candidate fills for all {} patchable cells",
                patchable_refs.len()
            ),
            unknowns,
            assumptions,
            user_message: None,
        };
        return Ok((
            continuation_with_fills(continuation, fills).map_err(|err| err.to_string())?,
            format!(
                "Spreadsheet probe ready for strict commit with {} candidate fills.",
                covered_refs.len()
            ),
        ));
    }

    if covered_refs.len() < patchable_refs.len() {
        unknowns.push(format!(
            "{} patchable cells still lack candidate values",
            patchable_refs.len().saturating_sub(covered_refs.len())
        ));
    }
    if !placeholder_refs.is_empty() && matches!(derivation_policy, DerivationPolicy::Strict) {
        unknowns.push(format!(
            "{} candidate fills still look like placeholders under strict policy",
            placeholder_refs.len()
        ));
    }
    if unknowns.is_empty() {
        unknowns.push("employee-specific spreadsheet facts are still unresolved".to_string());
    }

    let user_message = format!("Need more input before commit: {}.", unknowns.join("; "));
    let continuation = ContinuationState {
        status: ContinuationStatus::WaitUser,
        reason: user_message.clone(),
        unknowns,
        assumptions,
        user_message: Some(user_message.clone()),
    };
    Ok((
        serde_json::to_value(continuation).map_err(|err| err.to_string())?,
        "Spreadsheet probe needs user input before commit.".to_string(),
    ))
}

fn has_candidate_cells(patch_candidates: &Value) -> bool {
    !extract_spreadsheet_candidate_cells(patch_candidates).is_empty()
}

fn extract_spreadsheet_candidate_cells(patch_candidates: &Value) -> Vec<Value> {
    match patch_candidates {
        Value::Array(items) => items.clone(),
        Value::Object(_) => {
            let candidate_root = parse_patch_candidates_envelope(patch_candidates)
                .map(|value| value.candidates)
                .unwrap_or_else(|| patch_candidates.clone());
            candidate_root
                .get("cells")
                .and_then(Value::as_array)
                .cloned()
                .or_else(|| {
                    patch_candidates
                        .get("cells")
                        .and_then(Value::as_array)
                        .cloned()
                })
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn parse_patch_candidates_envelope(value: &Value) -> Option<PatchCandidatesEnvelope> {
    serde_json::from_value::<PatchCandidatesEnvelope>(value.clone()).ok()
}

fn continuation_with_fills(
    continuation: ContinuationState,
    fills: Vec<Value>,
) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(continuation)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("fills".to_string(), Value::Array(fills.clone()));
        object.insert("patch_spec".to_string(), json!({ "fills": fills }));
    }
    Ok(value)
}

fn build_fill_spec(candidate_cells: &[Value], patchable_refs: &HashSet<String>) -> Vec<Value> {
    candidate_cells
        .iter()
        .filter_map(|candidate| {
            let cell_ref = candidate.get("cell").and_then(Value::as_str)?;
            if !patchable_refs.contains(cell_ref) {
                return None;
            }
            let proposed_action = candidate
                .get("proposed_action")
                .and_then(Value::as_str)
                .unwrap_or("fill");
            if proposed_action != "fill" {
                return None;
            }
            let value = candidate
                .get("proposed_value")
                .or_else(|| candidate.get("suggested_value"))
                .or_else(|| candidate.get("value"))?
                .clone();
            match &value {
                Value::String(text) if text.trim().is_empty() => None,
                _ => Some(json!({
                    "cell": cell_ref,
                    "value": value,
                })),
            }
        })
        .collect()
}

fn extract_string_array_field(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn candidate_value_text(candidate: &Value) -> Option<String> {
    candidate
        .get("proposed_value")
        .or_else(|| candidate.get("suggested_value"))
        .or_else(|| candidate.get("value"))
        .and_then(value_to_text)
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        other => Some(other.to_string()),
    }
}

fn looks_like_placeholder_value(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.contains("待填写")
        || trimmed.contains("待补充")
        || trimmed.contains("placeholder")
        || trimmed.contains("XXX")
        || trimmed.contains("XX%")
        || trimmed.contains("X个")
        || trimmed.contains("X次")
}
