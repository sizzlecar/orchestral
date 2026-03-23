use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::support::{document_candidate_files, prepend_h1_title};

pub(super) fn build_document_patch_spec(
    patch_candidates: &Value,
    inspection: &Value,
) -> Result<(Value, String), String> {
    let inspection_by_path = inspection
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|file| {
            let path = file.get("path").and_then(Value::as_str)?;
            Some((path.to_string(), file))
        })
        .collect::<BTreeMap<_, _>>();

    let mut updates = Vec::new();

    for file in document_candidate_files(patch_candidates) {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        let Some(inspection_file) = inspection_by_path.get(path) else {
            continue;
        };
        let Some(content) = inspection_file.get("content").and_then(Value::as_str) else {
            continue;
        };

        let title = file
            .get("planned_changes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find_map(|item| item.get("title").and_then(Value::as_str))
            .or_else(|| {
                inspection_file
                    .get("suggested_title")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
            });

        let Some(title) = title else {
            continue;
        };
        let missing_title = inspection_file
            .get("missing_title")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !missing_title {
            continue;
        }

        updates.push(json!({
            "path": path,
            "content": prepend_h1_title(title, content),
        }));
    }

    if updates.is_empty() {
        return Err("document patch candidates did not produce any concrete updates".to_string());
    }

    let patch_spec = json!({
        "summary": format!("Prepared document patch for {} file(s).", updates.len()),
        "updates": updates,
    });
    let summary = patch_spec
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("Prepared document patch.")
        .to_string();
    Ok((patch_spec, summary))
}
