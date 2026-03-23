use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Clone, Deserialize)]
pub(super) struct DocumentUpdate {
    pub(super) path: String,
    pub(super) content: String,
}

pub(super) fn parse_document_updates(patch_spec: &Value) -> Result<Vec<DocumentUpdate>, String> {
    let updates = patch_spec
        .get("updates")
        .cloned()
        .ok_or_else(|| "patch_spec missing updates".to_string())?;
    serde_json::from_value::<Vec<DocumentUpdate>>(updates)
        .map_err(|err| format!("parse document patch updates failed: {}", err))
}

#[derive(Debug)]
pub(super) struct DocumentInspectionSummary {
    pub(super) file_name: String,
    pub(super) stem: String,
    pub(super) suggested_title: Option<String>,
    pub(super) title: Option<String>,
    pub(super) missing_title: bool,
    pub(super) todo_count: usize,
    pub(super) todo_lines: Vec<Value>,
    pub(super) heading_count: usize,
    pub(super) headings: Vec<Value>,
    pub(super) line_count: usize,
}

pub(super) fn inspect_document_content(path: &Path, content: &str) -> DocumentInspectionSummary {
    let mut todo_lines = Vec::new();
    let mut headings = Vec::new();
    let mut title = None;
    let mut first_nonempty = None;

    for (index, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if first_nonempty.is_none() && !trimmed.is_empty() {
            first_nonempty = Some(trimmed.to_string());
        }
        if trimmed.contains("TODO") {
            todo_lines.push(json!({
                "line": index + 1,
                "text": trimmed,
            }));
        }
        if trimmed.starts_with('#') {
            let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
            let text = trimmed[hashes..].trim().to_string();
            headings.push(json!({
                "line": index + 1,
                "level": hashes,
                "text": text,
            }));
            if title.is_none() && hashes == 1 && !text.is_empty() {
                title = Some(text);
            }
        }
    }

    let missing_title = first_nonempty
        .as_deref()
        .map(|line| !line.starts_with("# "))
        .unwrap_or(true);

    DocumentInspectionSummary {
        file_name: path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string(),
        stem: path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string(),
        suggested_title: suggested_title_from_stem(
            path.file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or_default(),
        ),
        title,
        missing_title,
        todo_count: todo_lines.len(),
        todo_lines,
        heading_count: headings.len(),
        headings,
        line_count: content.lines().count(),
    }
}

pub(super) fn suggested_title_from_stem(stem: &str) -> Option<String> {
    let words = stem
        .split(|ch: char| matches!(ch, '-' | '_' | '.' | '/' | '\\') || ch.is_whitespace())
        .filter(|part| !part.is_empty())
        .map(title_case_word)
        .collect::<Vec<_>>();
    if words.is_empty() {
        None
    } else {
        Some(words.join(" "))
    }
}

fn title_case_word(word: &str) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.extend(first.to_uppercase());
    out.push_str(&chars.as_str().to_ascii_lowercase());
    out
}
