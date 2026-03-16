use serde::Deserialize;
use serde_json::{json, Number, Value};

use orchestral_core::types::{DerivationPolicy, PatchCandidatesEnvelope};

#[derive(Debug, Clone, Deserialize)]
struct StructuredInspection {
    files: Vec<StructuredInspectionFile>,
}

#[derive(Debug, Clone, Deserialize)]
struct StructuredInspectionFile {
    path: String,
    #[serde(default)]
    field_inventory: Vec<FieldInventoryEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct FieldInventoryEntry {
    pointer: String,
    selector: String,
    #[serde(default)]
    value_type: String,
    #[serde(default)]
    value: Value,
}

pub(super) fn derive_structured_patch_candidates(
    user_request: &str,
    inspection: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let _policy = parse_derivation_policy(raw_policy)?;
    let inspection: StructuredInspection = serde_json::from_value(inspection.clone())
        .map_err(|err| format!("parse structured inspection failed: {}", err))?;
    if inspection.files.is_empty() {
        return Err("structured inspection produced no files".to_string());
    }

    let selected_files = select_target_files(user_request, &inspection.files)?;
    let mut files = Vec::new();
    let mut unknowns = Vec::new();
    let mut assumptions = vec![
        "structured inspection field_inventory was treated as the authoritative path model"
            .to_string(),
    ];
    let mut total_operations = 0usize;

    for file in selected_files {
        let resolution = derive_file_operations(user_request, file);
        total_operations += resolution.operations.len();
        unknowns.extend(resolution.unknowns.clone());
        if resolution.explicit_reference_found && resolution.operations.is_empty() {
            assumptions.push(format!(
                "request referenced '{}' but did not yield a concrete structured patch",
                file.path
            ));
        }
        files.push(json!({
            "path": file.path,
            "operations": resolution.operations,
            "needs_user_input": !resolution.unknowns.is_empty() && resolution.operations.is_empty(),
            "unknowns": resolution.unknowns,
        }));
    }

    unknowns.sort();
    unknowns.dedup();
    assumptions.sort();
    assumptions.dedup();

    if total_operations == 0 && unknowns.is_empty() {
        unknowns.push(
            "no explicit structured field update or remove instruction could be resolved"
                .to_string(),
        );
    }

    let envelope = PatchCandidatesEnvelope {
        candidates: json!({ "files": files }),
        unknowns,
        assumptions,
    };
    let summary = format!(
        "Derived structured patch candidates with {} operation(s) across {} file(s).",
        total_operations,
        envelope
            .candidates
            .get("files")
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(0)
    );
    Ok((
        serde_json::to_value(envelope).map_err(|err| err.to_string())?,
        summary,
    ))
}

#[derive(Debug, Default)]
struct FileResolution {
    operations: Vec<Value>,
    unknowns: Vec<String>,
    explicit_reference_found: bool,
}

fn select_target_files<'a>(
    user_request: &str,
    files: &'a [StructuredInspectionFile],
) -> Result<Vec<&'a StructuredInspectionFile>, String> {
    if files.len() == 1 {
        return Ok(vec![&files[0]]);
    }

    let request_lower = user_request.to_lowercase();
    let matched = files
        .iter()
        .filter(|file| {
            request_lower.contains(&file.path.to_lowercase())
                || std::path::Path::new(&file.path)
                    .file_name()
                    .and_then(|value| value.to_str())
                    .map(|name| request_lower.contains(&name.to_lowercase()))
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    match matched.len() {
        0 => Err(
            "multiple structured files matched; specify the target file path in the request"
                .to_string(),
        ),
        1 => Ok(matched),
        _ => Err("request matches multiple structured files; clarify the target file".to_string()),
    }
}

fn derive_file_operations(user_request: &str, file: &StructuredInspectionFile) -> FileResolution {
    let mut resolution = FileResolution::default();
    let mut seen_pointers = std::collections::HashSet::new();
    let entries = sort_entries_by_specificity(&file.field_inventory);

    for entry in entries {
        let references = reference_forms(entry);
        if !references
            .iter()
            .any(|reference| request_mentions(user_request, reference))
        {
            continue;
        }
        resolution.explicit_reference_found = true;
        if seen_pointers.contains(&entry.pointer) {
            continue;
        }

        if let Some(remove_op) = derive_remove_operation(user_request, entry, &references) {
            seen_pointers.insert(entry.pointer.clone());
            resolution.operations.push(remove_op);
            continue;
        }

        match derive_set_operation(user_request, entry, &references) {
            Ok(Some(set_op)) => {
                seen_pointers.insert(entry.pointer.clone());
                resolution.operations.push(set_op);
            }
            Ok(None) => {
                resolution.unknowns.push(format!(
                    "target value for '{}' is not explicit",
                    entry.selector
                ));
            }
            Err(error) => resolution.unknowns.push(error),
        }
    }

    resolution.unknowns.sort();
    resolution.unknowns.dedup();
    resolution
}

fn sort_entries_by_specificity(entries: &[FieldInventoryEntry]) -> Vec<&FieldInventoryEntry> {
    let mut out = entries.iter().collect::<Vec<_>>();
    out.sort_by_key(|entry| std::cmp::Reverse(entry.selector.len()));
    out
}

fn reference_forms(entry: &FieldInventoryEntry) -> Vec<String> {
    let mut refs = vec![entry.selector.clone(), entry.pointer.clone()];
    if let Some(last) = entry.selector.rsplit('.').next() {
        if last != entry.selector {
            refs.push(last.to_string());
        }
    }
    refs.sort();
    refs.dedup();
    refs
}

fn derive_remove_operation(
    user_request: &str,
    entry: &FieldInventoryEntry,
    references: &[String],
) -> Option<Value> {
    let patterns = [
        "删除 {ref}",
        "移除 {ref}",
        "remove {ref}",
        "delete {ref}",
        "drop {ref}",
        "unset {ref}",
        "把 {ref} 删除",
    ];
    if references.iter().any(|reference| {
        patterns.iter().any(|pattern| {
            request_contains_pattern(user_request, &pattern.replace("{ref}", reference))
        })
    }) {
        return Some(json!({
            "op": "remove",
            "path": entry.pointer,
            "selector": entry.selector,
            "reason": "explicit remove request",
        }));
    }
    None
}

fn derive_set_operation(
    user_request: &str,
    entry: &FieldInventoryEntry,
    references: &[String],
) -> Result<Option<Value>, String> {
    let markers = [
        "{ref} 改成 ",
        "{ref} 改为 ",
        "{ref} 设为 ",
        "{ref} 设置为 ",
        "{ref} 变成 ",
        "{ref} 更新为 ",
        "把 {ref} 改成 ",
        "把 {ref} 改为 ",
        "set {ref} to ",
        "change {ref} to ",
        "update {ref} to ",
        "{ref} = ",
        "{ref}=",
    ];

    for reference in references {
        for marker in &markers {
            let concrete = marker.replace("{ref}", reference);
            if let Some(raw_value) = extract_suffix_after_marker(user_request, &concrete) {
                let value = parse_requested_value(raw_value, entry)?;
                return Ok(Some(json!({
                    "op": "set",
                    "path": entry.pointer,
                    "value": value,
                    "selector": entry.selector,
                    "reason": "explicit request",
                })));
            }
        }
    }

    Ok(None)
}

fn parse_requested_value(raw_value: &str, entry: &FieldInventoryEntry) -> Result<Value, String> {
    let trimmed = clean_value(raw_value);
    if trimmed.is_empty() {
        return Err(format!("target value for '{}' is empty", entry.selector));
    }

    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).map_err(|err| {
            format!(
                "parse structured value for '{}' failed: {}",
                entry.selector, err
            )
        });
    }

    if trimmed.eq_ignore_ascii_case("true") || trimmed == "是" {
        return Ok(Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") || trimmed == "否" {
        return Ok(Value::Bool(false));
    }
    if trimmed.eq_ignore_ascii_case("null") || trimmed.eq_ignore_ascii_case("none") {
        return Ok(Value::Null);
    }
    if let Ok(integer) = trimmed.parse::<i64>() {
        return Ok(Value::Number(Number::from(integer)));
    }
    if let Ok(float) = trimmed.parse::<f64>() {
        if let Some(number) = Number::from_f64(float) {
            return Ok(Value::Number(number));
        }
    }
    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        return Ok(Value::String(
            trimmed[1..trimmed.len().saturating_sub(1)].to_string(),
        ));
    }
    if entry.value_type == "string" || entry.value.is_string() {
        return Ok(Value::String(trimmed.to_string()));
    }
    Ok(Value::String(trimmed.to_string()))
}

fn clean_value(raw: &str) -> &str {
    raw.trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
}

fn request_mentions(user_request: &str, reference: &str) -> bool {
    user_request
        .to_lowercase()
        .contains(&reference.to_lowercase())
}

fn request_contains_pattern(user_request: &str, pattern: &str) -> bool {
    user_request
        .to_lowercase()
        .contains(&pattern.to_lowercase())
}

fn extract_suffix_after_marker<'a>(user_request: &'a str, marker: &str) -> Option<&'a str> {
    let haystack = user_request.to_lowercase();
    let needle = marker.to_lowercase();
    let start = haystack.find(&needle)? + needle.len();
    extract_value_slice(&user_request[start..])
}

fn extract_value_slice(suffix: &str) -> Option<&str> {
    let trimmed = suffix.trim_start();
    if trimmed.is_empty() {
        return None;
    }

    let start_offset = suffix.len().saturating_sub(trimmed.len());
    let value_len = match trimmed.chars().next()? {
        '"' | '\'' | '`' => consume_quoted_literal(trimmed)?,
        '{' => consume_balanced_literal(trimmed, '{', '}')?,
        '[' => consume_balanced_literal(trimmed, '[', ']')?,
        _ => consume_scalar_literal(trimmed),
    };

    Some(&suffix[start_offset..start_offset + value_len])
}

fn consume_quoted_literal(input: &str) -> Option<usize> {
    let quote = input.chars().next()?;
    let mut escaped = false;
    for (idx, ch) in input.char_indices().skip(1) {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Some(idx + ch.len_utf8());
        }
    }
    Some(input.len())
}

fn consume_balanced_literal(input: &str, open: char, close: char) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if let Some(active_quote) = quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == active_quote {
                quote = None;
            }
            continue;
        }

        match ch {
            '"' | '\'' => quote = Some(ch),
            _ if ch == open => depth += 1,
            _ if ch == close => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx + ch.len_utf8());
                }
            }
            _ => {}
        }
    }

    if depth > 0 {
        Some(input.len())
    } else {
        None
    }
}

fn consume_scalar_literal(input: &str) -> usize {
    input
        .char_indices()
        .find_map(|(idx, ch)| match ch {
            ',' | '，' | '。' | '；' | ';' | '\n' | '\r' | '\t' | ' ' => Some(idx),
            _ => None,
        })
        .unwrap_or(input.len())
}

fn parse_derivation_policy(raw_policy: &str) -> Result<DerivationPolicy, String> {
    match raw_policy {
        "strict" => Ok(DerivationPolicy::Strict),
        "permissive" => Ok(DerivationPolicy::Permissive),
        other => Err(format!("unsupported derivation_policy '{}'", other)),
    }
}
