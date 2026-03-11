use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use super::model::StructuredFormat;
use super::support::{display_path, normalize_path};

pub(super) fn locate_structured_files(
    source_root: &Path,
    user_request: &str,
) -> Result<HashMap<String, Value>, String> {
    let root = normalize_path(source_root)?;
    let lowered_request = user_request.to_ascii_lowercase();
    let path_hints = extract_path_hints(user_request);
    let mut candidates = Vec::new();
    collect_structured_files(&root, &mut candidates).map_err(|err| err.to_string())?;
    if candidates.is_empty() {
        return Err(format!(
            "no structured artifact candidates found under {}",
            root.display()
        ));
    }

    let mut scored = candidates
        .into_iter()
        .map(|path| {
            let mut score = 0i32;
            let display = display_path(&path).to_ascii_lowercase();
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            for hint in &path_hints {
                if display == *hint || display.ends_with(hint) {
                    score += 24;
                } else if name == *hint || display.contains(hint) {
                    score += 12;
                }
            }
            if lowered_request.contains("config") && display.contains("config") {
                score += 3;
            }
            if lowered_request.contains("json") && display.ends_with(".json") {
                score += 4;
            }
            if (lowered_request.contains("yaml") || lowered_request.contains("yml"))
                && (display.ends_with(".yaml") || display.ends_with(".yml"))
            {
                score += 4;
            }
            if lowered_request.contains("toml") && display.ends_with(".toml") {
                score += 4;
            }
            if lowered_request.contains("fixtures") && display.starts_with("fixtures/") {
                score += 4;
            }
            let depth_penalty = display.matches('/').count() as i32;
            score -= depth_penalty.min(6);
            (score, path)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.as_os_str().cmp(right.1.as_os_str()))
    });

    let selected = scored
        .iter()
        .take(10)
        .map(|(_, path)| Value::String(display_path(path)))
        .collect::<Vec<_>>();

    Ok([
        ("source_paths".to_string(), Value::Array(selected.clone())),
        ("artifact_candidates".to_string(), Value::Array(selected)),
        ("artifact_count".to_string(), json!(scored.len())),
    ]
    .into_iter()
    .collect())
}

fn collect_structured_files(root: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
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
            collect_structured_files(&path, out)?;
            continue;
        }
        if StructuredFormat::from_path(&path).is_some() {
            out.push(path);
        }
    }
    Ok(())
}

fn extract_path_hints(user_request: &str) -> Vec<String> {
    let mut hints = user_request
        .split_whitespace()
        .map(trim_path_punctuation)
        .filter(|value| !value.is_empty())
        .filter(|value| {
            value.contains('/')
                || value.contains('\\')
                || value.ends_with(".json")
                || value.ends_with(".yaml")
                || value.ends_with(".yml")
                || value.ends_with(".toml")
        })
        .map(|value| value.replace('\\', "/").to_ascii_lowercase())
        .collect::<Vec<_>>();
    hints.sort();
    hints.dedup();
    hints
}

fn trim_path_punctuation(raw: &str) -> &str {
    raw.trim_matches(|ch: char| {
        ch.is_whitespace()
            || matches!(
                ch,
                '"' | '\''
                    | ','
                    | '，'
                    | '。'
                    | ':'
                    | '：'
                    | ';'
                    | '('
                    | ')'
                    | '['
                    | ']'
                    | '{'
                    | '}'
            )
    })
}
