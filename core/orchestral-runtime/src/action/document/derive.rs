use serde_json::{json, Value};

use orchestral_core::types::PatchCandidatesEnvelope;

/// Derivation policy for document derive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DerivationPolicy {
    Strict,
    Permissive,
}

pub(super) fn derive_document_patch_candidates(
    user_request: &str,
    inspection: &Value,
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

    let selected_files = select_target_files(user_request, files);
    let mut candidates = Vec::new();
    let mut unknowns = Vec::new();
    let mut assumptions = Vec::new();

    for file in selected_files {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        let missing_title = file
            .get("missing_title")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !missing_title {
            continue;
        }

        let suggested_title = file
            .get("suggested_title")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(str::to_string);
        let todo_count = file
            .get("todo_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();

        let mut planned_changes = Vec::new();
        let mut file_unknowns = Vec::new();

        if let Some(title) = suggested_title.as_deref() {
            planned_changes.push(json!({
                "type": "add_title",
                "title": title,
                "description": format!(
                    "Add top-level H1 title '# {}' derived from the file name.",
                    title
                ),
            }));
            assumptions.push(format!(
                "derived H1 title '{}' from document file name for {}",
                title, path
            ));
        } else {
            file_unknowns.push(format!(
                "Preferred H1 title text for '{}' is not derivable from the file name",
                path
            ));
        }

        if todo_count > 0 && policy == DerivationPolicy::Strict {
            file_unknowns.push(format!(
                "{} still contains TODO markers and may need user confirmation",
                path
            ));
        }

        let needs_user_input = !file_unknowns.is_empty() && planned_changes.is_empty();
        unknowns.extend(file_unknowns.clone());
        candidates.push(json!({
            "path": path,
            "planned_changes": planned_changes,
            "needs_user_input": needs_user_input,
            "unknowns": file_unknowns,
        }));
    }

    unknowns.sort();
    unknowns.dedup();
    assumptions.sort();
    assumptions.dedup();

    let envelope = PatchCandidatesEnvelope {
        candidates: json!({ "files": candidates }),
        unknowns,
        assumptions,
    };
    let candidate_count = envelope
        .candidates
        .get("files")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    let summary = format!(
        "Derived document patch candidates for {} file(s) missing H1 titles.",
        candidate_count
    );
    Ok((
        serde_json::to_value(envelope).map_err(|err| err.to_string())?,
        summary,
    ))
}

fn parse_derivation_policy(raw_policy: &str) -> Result<DerivationPolicy, String> {
    let normalized = raw_policy.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "permissive" | "filename_to_h1" | "markdown_h1_from_filename" => {
            Ok(DerivationPolicy::Permissive)
        }
        "strict" => Ok(DerivationPolicy::Strict),
        other => {
            tracing::debug!(
                derivation_policy = other,
                "document_derive_candidates defaulting unknown derivation_policy to permissive"
            );
            Ok(DerivationPolicy::Permissive)
        }
    }
}

fn select_target_files<'a>(user_request: &str, files: &'a [Value]) -> Vec<&'a Value> {
    let request_lower = user_request.to_ascii_lowercase();
    let matched = files
        .iter()
        .filter(|file| {
            file.get("path")
                .and_then(Value::as_str)
                .map(|path| request_lower.contains(&path.to_ascii_lowercase()))
                .unwrap_or(false)
                || file
                    .get("file_name")
                    .and_then(Value::as_str)
                    .map(|name| request_lower.contains(&name.to_ascii_lowercase()))
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    if matched.is_empty() {
        files.iter().collect()
    } else {
        matched
    }
}
