use std::collections::HashMap;
use std::path::Path;

use serde_json::{json, Value};

use super::model::{
    apply_structured_operation, parse_structured_file, parse_structured_patch_spec,
    write_structured_file, StructuredFormat,
};
use super::support::{display_path, normalize_path};

pub(super) fn apply_structured_patch(patch_spec: &Value) -> Result<HashMap<String, Value>, String> {
    let patch_spec = parse_structured_patch_spec(patch_spec)?;
    if patch_spec.files.is_empty() {
        return Err("structured patch_spec.files is empty".to_string());
    }

    let mut updated_paths = Vec::new();
    let mut patch_count = 0usize;
    for file in &patch_spec.files {
        let path = normalize_path(Path::new(&file.path))?;
        let format = StructuredFormat::from_path(&path)
            .ok_or_else(|| format!("unsupported structured file '{}'", path.display()))?;
        let mut current = parse_structured_file(&path)?;
        for operation in &file.operations {
            apply_structured_operation(&mut current, operation)?;
            patch_count += 1;
        }
        write_structured_file(&path, format, &current)?;
        updated_paths.push(Value::String(display_path(&path)));
    }

    Ok([
        ("updated_paths".to_string(), Value::Array(updated_paths)),
        ("patch_count".to_string(), json!(patch_count)),
        (
            "summary".to_string(),
            Value::String(
                patch_spec
                    .summary
                    .unwrap_or_else(|| "Structured patch applied.".to_string()),
            ),
        ),
    ]
    .into_iter()
    .collect())
}
