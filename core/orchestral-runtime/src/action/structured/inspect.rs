use std::path::Path;

use serde_json::{json, Value};

use super::model::{parse_structured_file, summarize_structured_value, StructuredFormat};
use super::support::{display_path, normalize_path};

pub(super) fn inspect_structured_files(source_paths: &[String]) -> Result<Value, String> {
    let mut files = Vec::new();
    for path in source_paths {
        let path_buf = normalize_path(Path::new(path))?;
        let format = StructuredFormat::from_path(&path_buf)
            .ok_or_else(|| format!("unsupported structured file '{}'", path_buf.display()))?;
        let parsed = parse_structured_file(&path_buf)?;
        let top_level_keys = parsed
            .as_object()
            .map(|object| {
                object
                    .keys()
                    .map(|key| Value::String(key.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        files.push(json!({
            "path": display_path(&path_buf),
            "format": format.as_str(),
            "top_level_keys": top_level_keys,
            "summary": summarize_structured_value(&parsed),
            "value": parsed,
        }));
    }

    Ok(json!({
        "target_count": files.len(),
        "files": files,
    }))
}
