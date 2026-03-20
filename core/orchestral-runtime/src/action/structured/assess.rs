use serde_json::Value;

use orchestral_core::types::{ContinuationState, ContinuationStatus};

/// Derivation policy for structured assessment (local enum replacing the removed core type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DerivationPolicy {
    Strict,
    Permissive,
}

use super::support::{
    extract_assumptions, extract_unknowns, parse_patch_candidates_envelope,
    structured_candidate_files,
};

pub(super) fn assess_structured_readiness(
    inspection: &Value,
    patch_candidates: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let policy = parse_derivation_policy(raw_policy)?;
    let files = inspection
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| "structured inspection is missing files".to_string())?;
    if files.is_empty() {
        return Err("structured inspection produced no files".to_string());
    }

    let envelope = parse_patch_candidates_envelope(patch_candidates);
    let candidate_files = structured_candidate_files(patch_candidates);
    let mut unknowns = envelope
        .as_ref()
        .map(|value| value.unknowns.clone())
        .unwrap_or_else(|| extract_unknowns(patch_candidates));
    let assumptions = envelope
        .as_ref()
        .map(|value| value.assumptions.clone())
        .unwrap_or_else(|| extract_assumptions(patch_candidates));
    let mut operation_count = 0usize;
    let mut missing_input_files = Vec::new();

    for file in &candidate_files {
        let file_operation_count = file
            .get("operations")
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(0);
        if file
            .get("needs_user_input")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && file_operation_count == 0
        {
            if let Some(path) = file.get("path").and_then(Value::as_str) {
                missing_input_files.push(format!("{} requires additional user input", path));
            }
        }
        operation_count += file_operation_count;
    }

    unknowns.extend(missing_input_files);

    unknowns.sort();
    unknowns.dedup();

    if operation_count == 0 && unknowns.is_empty() {
        let continuation = ContinuationState {
            status: ContinuationStatus::Done,
            reason: "structured inspection found no required changes".to_string(),
            unknowns: Vec::new(),
            assumptions,
            user_message: None,
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            "Structured probe found no required changes.".to_string(),
        ));
    }

    let should_wait = match policy {
        DerivationPolicy::Strict => !unknowns.is_empty(),
        DerivationPolicy::Permissive => operation_count == 0 && !unknowns.is_empty(),
    };

    if should_wait {
        let user_message = format!("Need more input before commit: {}.", unknowns.join("; "));
        let continuation = ContinuationState {
            status: ContinuationStatus::WaitUser,
            reason: user_message.clone(),
            unknowns,
            assumptions,
            user_message: Some(user_message.clone()),
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            "Structured probe needs user input before commit.".to_string(),
        ));
    }

    let continuation = ContinuationState {
        status: ContinuationStatus::CommitReady,
        reason: format!(
            "structured probe derived {} patch operation(s) across {} file(s)",
            operation_count,
            candidate_files.len()
        ),
        unknowns: Vec::new(),
        assumptions,
        user_message: None,
    };
    Ok((
        serde_json::to_value(continuation).map_err(|err| err.to_string())?,
        format!(
            "Structured probe ready for commit with {} patch operation(s).",
            operation_count
        ),
    ))
}

fn parse_derivation_policy(raw_policy: &str) -> Result<DerivationPolicy, String> {
    match raw_policy {
        "strict" => Ok(DerivationPolicy::Strict),
        "permissive" => Ok(DerivationPolicy::Permissive),
        other => Err(format!("unsupported derivation_policy '{}'", other)),
    }
}
