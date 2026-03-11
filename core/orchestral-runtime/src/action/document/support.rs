use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

use orchestral_core::types::PatchCandidatesEnvelope;

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
