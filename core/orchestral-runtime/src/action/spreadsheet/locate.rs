use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use super::support::{display_path, normalize_path};

pub(super) fn locate_workbook(
    source_root: &Path,
    user_request: &str,
) -> Result<HashMap<String, Value>, String> {
    let root = normalize_path(source_root)?;
    let lowered_request = user_request.to_ascii_lowercase();
    let path_hints = extract_path_hints(user_request);
    let mut candidates = Vec::new();
    collect_workbooks(&root, &mut candidates).map_err(|e| e.to_string())?;
    if candidates.is_empty() {
        return Err(format!(
            "no spreadsheet candidates found under {}",
            root.display()
        ));
    }
    if candidates.len() == 1 {
        let selected = candidates.pop().expect("single candidate must exist");
        return Ok([
            (
                "source_path".to_string(),
                Value::String(display_path(&selected)),
            ),
            (
                "artifact_candidates".to_string(),
                Value::Array(vec![Value::String(display_path(&selected))]),
            ),
            ("artifact_count".to_string(), json!(1u64)),
        ]
        .into_iter()
        .collect());
    }

    let mut scored = candidates
        .into_iter()
        .map(|path| {
            let mut score = 0i32;
            let display = display_path(&path).to_ascii_lowercase();
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            if request_mentions_path_scope(&lowered_request, &display) {
                score += 6;
            }
            for hint in &path_hints {
                if display == *hint || display.ends_with(hint) {
                    score += 24;
                } else if name == *hint || display.contains(hint) {
                    score += 12;
                } else if let Some(filename) = hint.rsplit('/').next() {
                    if !filename.is_empty() && name == filename {
                        score += 10;
                    }
                }
            }
            if lowered_request.contains("excel")
                || lowered_request.contains("xlsx")
                || lowered_request.contains("spreadsheet")
                || lowered_request.contains("workbook")
                || lowered_request.contains("表格")
                || lowered_request.contains("工作簿")
            {
                score += 1;
            }
            let depth_penalty = display.matches('/').count() as i32;
            score -= depth_penalty.min(4);
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
        .first()
        .map(|(_, path)| path)
        .ok_or_else(|| "spreadsheet candidate selection failed".to_string())?;

    Ok([
        (
            "source_path".to_string(),
            Value::String(display_path(selected)),
        ),
        (
            "artifact_candidates".to_string(),
            Value::Array(
                scored
                    .iter()
                    .take(10)
                    .map(|(_, path)| Value::String(display_path(path)))
                    .collect(),
            ),
        ),
        (
            "artifact_count".to_string(),
            json!(u64::try_from(scored.len()).unwrap_or(0)),
        ),
    ]
    .into_iter()
    .collect())
}

fn extract_path_hints(user_request: &str) -> Vec<String> {
    let mut hints = user_request
        .split_whitespace()
        .map(trim_path_punctuation)
        .filter(|value| !value.is_empty())
        .filter(|value| {
            value.contains('/')
                || value.contains('\\')
                || value.ends_with(".xlsx")
                || value.ends_with(".xlsm")
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

fn request_mentions_path_scope(lowered_request: &str, display_path: &str) -> bool {
    if lowered_request.contains("docs") && display_path.starts_with("docs/") {
        return true;
    }
    if lowered_request.contains("config") && display_path.starts_with("configs/") {
        return true;
    }
    false
}

fn collect_workbooks(root: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default();
            if name.starts_with('.') || name == "__pycache__" || name == ".orchestral" {
                continue;
            }
            collect_workbooks(&path, out)?;
            continue;
        }
        let ext = path
            .extension()
            .and_then(|v| v.to_str())
            .map(|v| v.to_ascii_lowercase())
            .unwrap_or_default();
        if ext == "xlsx" || ext == "xlsm" {
            out.push(path);
        }
    }
    Ok(())
}
