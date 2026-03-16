use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

use orchestral_core::types::PatchCandidatesEnvelope;

use super::model::{parse_document_updates, DocumentUpdate};

pub(super) fn parse_patch_candidates_envelope(value: &Value) -> Option<PatchCandidatesEnvelope> {
    serde_json::from_value::<PatchCandidatesEnvelope>(value.clone()).ok()
}

pub(super) fn document_candidate_files(patch_candidates: &Value) -> Vec<Value> {
    let candidate_root = parse_patch_candidates_envelope(patch_candidates)
        .map(|value| value.candidates)
        .unwrap_or_else(|| patch_candidates.clone());
    candidate_root
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            patch_candidates
                .get("files")
                .and_then(Value::as_array)
                .cloned()
        })
        .unwrap_or_default()
}

pub(super) fn request_requires_confirmation(user_request: &str) -> bool {
    let lowered = user_request.to_ascii_lowercase();
    [
        "先不要写",
        "先不要写回",
        "先给出修改计划",
        "等待我确认",
        "等我确认",
        "wait for my confirmation",
        "do not write yet",
        "review first",
        "propose plan first",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

pub(super) fn requested_report_path(
    user_request: &str,
    resume_user_input: Option<&Value>,
) -> Option<String> {
    let mut texts = vec![user_request.to_string()];
    if let Some(value) = resume_user_input {
        if let Some(text) = value.as_str() {
            texts.push(text.to_string());
        } else if let Some(text) = value.get("message").and_then(Value::as_str) {
            texts.push(text.to_string());
        } else if let Some(text) = value.get("text").and_then(Value::as_str) {
            texts.push(text.to_string());
        }
    }
    for text in texts {
        for path in extract_path_like_tokens(&text) {
            if looks_like_report_path(Path::new(&path)) {
                return Some(path);
            }
        }
    }
    None
}

pub(super) fn normalize_document_updates(
    patch_spec: &Value,
    report_path: Option<&str>,
    inspection: Option<&Value>,
    patch_candidates: Option<&Value>,
) -> Result<Vec<DocumentUpdate>, String> {
    let mut updates = parse_document_updates(patch_spec)?;
    let Some(inspection) = inspection else {
        return Ok(updates);
    };
    let inspection_by_path = inspection_file_map(inspection);
    let candidate_by_path = patch_candidates.map(candidate_file_map).unwrap_or_default();
    let report_path = report_path.map(str::to_string);

    updates.retain(|update| {
        inspection_by_path.contains_key(&update.path)
            || report_path
                .as_deref()
                .is_some_and(|path| update.path == path)
    });

    updates.retain(|update| {
        let Some(file) = inspection_by_path.get(&update.path) else {
            return true;
        };
        let Some(candidate) = candidate_by_path.get(&update.path) else {
            return true;
        };
        if candidate_has_noop_plan(candidate) {
            return false;
        }
        if is_title_only_candidate(candidate) {
            return file
                .get("missing_title")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        }
        true
    });

    for (path, file) in &inspection_by_path {
        if !is_auto_title_candidate(file, candidate_by_path.get(path)) {
            continue;
        }
        let Some(content) = file.get("content").and_then(Value::as_str) else {
            continue;
        };
        let Some(title) = file.get("suggested_title").and_then(Value::as_str) else {
            continue;
        };
        let normalized = prepend_h1_title(title, content);
        if let Some(existing) = updates.iter_mut().find(|update| update.path == *path) {
            existing.content = normalized;
        } else {
            updates.push(DocumentUpdate {
                path: path.clone(),
                content: normalized,
            });
        }
    }

    updates.retain(|update| {
        if report_path
            .as_deref()
            .is_some_and(|expected_report| update.path == expected_report)
        {
            return true;
        }
        let Some(file) = inspection_by_path.get(&update.path) else {
            return true;
        };
        file.get("content")
            .and_then(Value::as_str)
            .is_none_or(|existing| existing != update.content)
    });

    Ok(updates)
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

pub(super) fn dedup_and_sort_paths(paths: &mut Vec<PathBuf>) {
    paths.sort();
    paths.dedup();
}

pub(super) fn collect_markdown_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if matches!(name, ".git" | ".orchestral" | "target" | "node_modules") {
                continue;
            }
            collect_markdown_files(&path, out)?;
        } else if is_markdown_path(&path) {
            out.push(path);
        }
    }
    Ok(())
}

pub(super) fn is_markdown_path(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "md" | "markdown" | "mdx" | "txt"
            )
        })
        .unwrap_or(false)
}

pub(super) fn looks_like_report_path(path: &Path) -> bool {
    let lower = display_path(path).to_ascii_lowercase();
    lower.contains("/report")
        || lower.contains("/reports/")
        || lower.ends_with("summary.md")
        || lower.ends_with("summary.markdown")
}

pub(super) fn extract_existing_paths(root: &Path, input: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for raw in extract_path_like_tokens(input) {
        let candidate = PathBuf::from(&raw);
        let resolved = if candidate.is_absolute() {
            candidate
        } else {
            root.join(&candidate)
        };
        if resolved.exists() && seen.insert(resolved.clone()) {
            results.push(resolved);
        }
    }
    results
}

fn extract_path_like_tokens(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .map(trim_path_token)
        .filter(|token| !token.is_empty())
        .filter(|token| {
            token.contains('/') || token.ends_with(".md") || token.ends_with(".markdown")
        })
        .collect()
}

fn inspection_file_map(inspection: &Value) -> BTreeMap<String, &Value> {
    inspection
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|file| {
            let path = file.get("path").and_then(Value::as_str)?;
            Some((path.to_string(), file))
        })
        .collect()
}

fn candidate_file_map(patch_candidates: &Value) -> BTreeMap<String, Value> {
    document_candidate_files(patch_candidates)
        .into_iter()
        .filter_map(|file| {
            let path = file.get("path").and_then(Value::as_str)?;
            Some((path.to_string(), file))
        })
        .collect()
}

fn is_auto_title_candidate(file: &Value, candidate: Option<&Value>) -> bool {
    if !file
        .get("missing_title")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return false;
    }
    if file
        .get("todo_count")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        > 0
    {
        return false;
    }
    if file
        .get("suggested_title")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
    {
        return false;
    }
    let Some(candidate) = candidate else {
        return false;
    };
    if candidate
        .get("needs_user_input")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return false;
    }
    if candidate
        .get("unknowns")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty())
    {
        return false;
    }
    is_title_only_candidate(candidate)
}

fn is_title_only_candidate(candidate: &Value) -> bool {
    candidate
        .get("planned_changes")
        .and_then(Value::as_array)
        .map(|items| {
            !items.is_empty()
                && items.iter().all(|item| {
                    item.get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == "add_title")
                        || item
                            .get("description")
                            .and_then(Value::as_str)
                            .or_else(|| item.as_str())
                            .is_some_and(|text| {
                                let lowered = text.to_ascii_lowercase();
                                lowered.contains("title") || text.contains("标题")
                            })
                })
        })
        .unwrap_or(false)
}

fn candidate_has_noop_plan(candidate: &Value) -> bool {
    candidate
        .get("planned_changes")
        .and_then(Value::as_array)
        .map(|items| {
            items.is_empty()
                || items.iter().all(|item| {
                    item.get("description")
                        .and_then(Value::as_str)
                        .or_else(|| item.as_str())
                        .is_some_and(|text| {
                            let lowered = text.to_ascii_lowercase();
                            lowered.contains("no changes needed")
                                || text.contains("无需修改")
                                || text.contains("无需变更")
                        })
                })
        })
        .unwrap_or(false)
}

fn prepend_h1_title(title: &str, content: &str) -> String {
    if content.is_empty() {
        format!("# {}\n", title.trim())
    } else {
        format!("# {}\n\n{}", title.trim(), content)
    }
}

fn trim_path_token(token: &str) -> String {
    token
        .trim_matches(|ch: char| {
            ch.is_whitespace()
                || matches!(
                    ch,
                    '"' | '\''
                        | '`'
                        | ','
                        | '，'
                        | '。'
                        | ':'
                        | '：'
                        | ';'
                        | '；'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                )
        })
        .to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::normalize_document_updates;

    #[test]
    fn test_normalize_document_updates_drops_unchanged_existing_files() {
        let patch_spec = json!({
            "updates": [
                { "path": "docs/a.md", "content": "# A\n\nsame" },
                { "path": "docs/b.md", "content": "# B\n\nchanged" },
            ]
        });
        let inspection = json!({
            "files": [
                {
                    "path": "docs/a.md",
                    "content": "# A\n\nsame",
                    "missing_title": false,
                    "todo_count": 0,
                },
                {
                    "path": "docs/b.md",
                    "content": "old",
                    "missing_title": true,
                    "todo_count": 0,
                    "suggested_title": "B"
                }
            ]
        });

        let updates = normalize_document_updates(&patch_spec, None, Some(&inspection), None)
            .expect("updates should normalize");

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].path, "docs/b.md");
    }
}
