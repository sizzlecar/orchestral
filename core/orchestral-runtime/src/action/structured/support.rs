use std::path::{Path, PathBuf};

use serde_json::Value;

use orchestral_core::types::PatchCandidatesEnvelope;

pub(super) fn parse_patch_candidates_envelope(value: &Value) -> Option<PatchCandidatesEnvelope> {
    serde_json::from_value::<PatchCandidatesEnvelope>(value.clone()).ok()
}

pub(super) fn structured_candidate_files(patch_candidates: &Value) -> Vec<Value> {
    let candidate_root = parse_patch_candidates_envelope(patch_candidates)
        .map(|value| value.candidates)
        .unwrap_or_else(|| patch_candidates.clone());
    candidate_root
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            candidate_root.as_array().map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.get("file").cloned().or_else(|| {
                            item.get("path")
                                .and_then(Value::as_str)
                                .map(|_| item.clone())
                        })
                    })
                    .collect::<Vec<_>>()
            })
        })
        .or_else(|| {
            patch_candidates
                .get("files")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            item.get("file").cloned().or_else(|| {
                                item.get("path")
                                    .and_then(Value::as_str)
                                    .map(|_| item.clone())
                            })
                        })
                        .collect::<Vec<_>>()
                })
        })
        .unwrap_or_default()
}

pub(super) fn extract_unknowns(patch_candidates: &Value) -> Vec<String> {
    let mut unknowns = patch_candidates
        .get("unknowns")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for file in structured_candidate_files(patch_candidates) {
        if let Some(items) = file.get("unknowns").and_then(Value::as_array) {
            unknowns.extend(items.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    unknowns
}

pub(super) fn extract_assumptions(patch_candidates: &Value) -> Vec<String> {
    patch_candidates
        .get("assumptions")
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

pub(super) fn normalize_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|err| format!("resolve current dir failed: {}", err))
    }
}

pub(super) fn display_path(path: &Path) -> String {
    let cwd = std::env::current_dir().ok();
    if let Some(cwd) = cwd {
        if let Ok(relative) = path.strip_prefix(&cwd) {
            return relative.to_string_lossy().replace('\\', "/");
        }
    }
    path.to_string_lossy().replace('\\', "/")
}
