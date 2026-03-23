use std::path::{Path, PathBuf};

use serde_json::Value;

pub(super) fn resolve_patch_spec_value<'a>(patch_spec: &'a Value) -> Result<&'a Value, String> {
    if patch_spec.get("fills").and_then(Value::as_array).is_some() {
        return Ok(patch_spec);
    }

    if let Some(nested) = patch_spec
        .get("patch_spec")
        .filter(|value| value.get("fills").and_then(Value::as_array).is_some())
    {
        tracing::debug!("spreadsheet patch_spec resolved from nested patch_spec envelope");
        return Ok(nested);
    }

    if let Some(continuation) = patch_spec.get("continuation") {
        if continuation.get("fills").and_then(Value::as_array).is_some() {
            tracing::debug!("spreadsheet patch_spec resolved from continuation envelope");
            return Ok(continuation);
        }
        if let Some(nested) = continuation
            .get("patch_spec")
            .filter(|value| value.get("fills").and_then(Value::as_array).is_some())
        {
            tracing::debug!(
                "spreadsheet patch_spec resolved from continuation.patch_spec envelope"
            );
            return Ok(nested);
        }
    }

    Err("patch_spec.fills must be an array".to_string())
}

pub(super) fn parse_cell_ref(cell_ref: &str) -> Result<(u32, u32), String> {
    let mut letters = String::new();
    let mut digits = String::new();
    for ch in cell_ref.chars() {
        if ch.is_ascii_alphabetic() {
            letters.push(ch.to_ascii_uppercase());
        } else if ch.is_ascii_digit() {
            digits.push(ch);
        }
    }
    if letters.is_empty() || digits.is_empty() {
        return Err(format!("invalid cell reference '{}'", cell_ref));
    }
    let row = digits
        .parse::<u32>()
        .map_err(|e| format!("invalid row in cell ref '{}': {}", cell_ref, e))?;
    let mut col = 0u32;
    for ch in letters.bytes() {
        col = col * 26 + u32::from(ch - b'A' + 1);
    }
    Ok((row, col))
}

pub(super) fn column_letter(mut col: u32) -> String {
    let mut out = String::new();
    while col > 0 {
        let rem = ((col - 1) % 26) as u8;
        out.insert(0, (b'A' + rem) as char);
        col = (col - 1) / 26;
    }
    out
}

pub(super) fn normalize_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|e| format!("resolve current dir failed: {}", e))
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

pub(super) fn preview(text: &str, max_chars: usize) -> String {
    let count = text.chars().count();
    if count <= max_chars {
        return text.trim().to_string();
    }
    let mut preview: String = text.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated {} chars]", count));
    preview
}

pub(super) fn decode_text(bytes: &[u8]) -> String {
    let raw = String::from_utf8_lossy(bytes);
    raw.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

pub(super) fn resolve_project_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .join("../..")
        .canonicalize()
        .map_err(|err| format!("resolve repository root failed: {}", err))?;
    Ok(root.join(path))
}

pub(super) fn resolve_validator_python() -> Result<PathBuf, String> {
    let repo_root = resolve_project_path(Path::new("."))?;
    let candidates = [
        repo_root.join(".claude/skills/xlsx/.venv/bin/python3"),
        repo_root.join(".venv/bin/python3"),
    ];
    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err("validator python interpreter not found under project virtual environments".to_string())
}

pub(super) fn xml_escape_text(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub(super) fn xml_escape_attr(input: &str) -> String {
    xml_escape_text(input)
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::resolve_patch_spec_value;

    #[test]
    fn test_resolve_patch_spec_value_accepts_direct_fills_array() {
        let patch_spec = json!({
            "fills": [{ "cell": "F5", "value": "done" }]
        });

        let resolved = resolve_patch_spec_value(&patch_spec).expect("direct spec");
        assert_eq!(resolved["fills"][0]["cell"], json!("F5"));
    }

    #[test]
    fn test_resolve_patch_spec_value_accepts_continuation_patch_spec_envelope() {
        let patch_spec = json!({
            "continuation": {
                "status": "commit_ready",
                "patch_spec": {
                    "fills": [{ "cell": "F5", "value": "done" }]
                }
            },
            "summary": "ready"
        });

        let resolved = resolve_patch_spec_value(&patch_spec).expect("continuation envelope");
        assert_eq!(resolved["fills"][0]["cell"], json!("F5"));
    }
}
