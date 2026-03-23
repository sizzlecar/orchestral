use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde_json::{json, Value};

use super::support::{
    collect_markdown_files, dedup_and_sort_paths, display_path, extract_existing_paths,
    is_markdown_path, looks_like_report_path, normalize_path,
};

pub(super) fn locate_documents(
    source_root: &Path,
    user_request: &str,
) -> Result<HashMap<String, Value>, String> {
    let root = normalize_path(source_root)?;
    let mut explicit_files = Vec::new();
    let mut explicit_dirs = Vec::new();
    let mut report_path = None;
    let mut seen = HashSet::new();

    for candidate in extract_existing_paths(&root, user_request) {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        if candidate.is_dir() {
            explicit_dirs.push(candidate);
            continue;
        }
        if is_markdown_path(&candidate) {
            if looks_like_report_path(&candidate) && report_path.is_none() {
                report_path = Some(candidate);
            } else {
                explicit_files.push(candidate);
            }
        }
    }

    let mut sources = Vec::new();
    for dir in explicit_dirs {
        collect_markdown_files(&dir, &mut sources).map_err(|err| err.to_string())?;
    }
    sources.extend(explicit_files);
    if sources.is_empty() {
        collect_markdown_files(&root, &mut sources).map_err(|err| err.to_string())?;
    }
    dedup_and_sort_paths(&mut sources);

    if let Some(path) = &report_path {
        sources.retain(|candidate| candidate != path);
    }

    if sources.is_empty() {
        return Err(format!(
            "no markdown/document candidates found under {}",
            root.display()
        ));
    }

    let mut exports = HashMap::new();
    exports.insert(
        "source_paths".to_string(),
        Value::Array(
            sources
                .iter()
                .map(|path| Value::String(display_path(path)))
                .collect(),
        ),
    );
    exports.insert(
        "documents".to_string(),
        Value::Array(
            sources
                .iter()
                .map(|path| Value::String(display_path(path)))
                .collect(),
        ),
    );
    exports.insert(
        "artifact_candidates".to_string(),
        Value::Array(
            sources
                .iter()
                .map(|path| Value::String(display_path(path)))
                .collect(),
        ),
    );
    exports.insert("artifact_count".to_string(), json!(sources.len()));
    if let Some(path) = report_path {
        exports.insert(
            "report_path".to_string(),
            Value::String(display_path(&path)),
        );
    }
    Ok(exports)
}
