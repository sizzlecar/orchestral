use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde_json::{json, Value};

use super::model::parse_document_updates;

pub(super) fn apply_document_patch(patch_spec: &Value) -> Result<HashMap<String, Value>, String> {
    let updates = parse_document_updates(patch_spec)?;
    if updates.is_empty() {
        return Err("document patch_spec.updates is empty".to_string());
    }

    for update in &updates {
        let path = Path::new(&update.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "create document parent '{}' failed: {}",
                    parent.display(),
                    err
                )
            })?;
        }
        fs::write(path, update.content.as_bytes())
            .map_err(|err| format!("write document '{}' failed: {}", update.path, err))?;
    }

    Ok([
        (
            "updated_paths".to_string(),
            Value::Array(
                updates
                    .iter()
                    .map(|update| Value::String(update.path.clone()))
                    .collect(),
            ),
        ),
        ("patch_count".to_string(), json!(updates.len())),
        (
            "summary".to_string(),
            Value::String(
                patch_spec
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("Document patch applied.")
                    .to_string(),
            ),
        ),
    ]
    .into_iter()
    .collect())
}
