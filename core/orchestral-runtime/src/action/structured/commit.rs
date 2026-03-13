use serde_json::{json, Value};

use super::support::structured_candidate_files;

pub(super) fn build_structured_patch_spec(
    patch_candidates: &Value,
) -> Result<(Value, String), String> {
    let candidate_files = structured_candidate_files(patch_candidates);
    let mut files = Vec::new();
    let mut patch_count = 0usize;

    for file in candidate_files {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        let operations = file
            .get("operations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if operations.is_empty() {
            continue;
        }
        patch_count += operations.len();
        files.push(json!({
            "path": path,
            "operations": operations,
        }));
    }

    if files.is_empty() {
        return Err(
            "structured patch candidates did not produce any concrete operations".to_string(),
        );
    }

    let patch_spec = json!({
        "files": files,
        "summary": format!(
            "Prepared structured patch for {} file(s) with {} operation(s).",
            files.len(),
            patch_count
        ),
    });
    let summary = patch_spec
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("Prepared structured patch.")
        .to_string();
    Ok((patch_spec, summary))
}
