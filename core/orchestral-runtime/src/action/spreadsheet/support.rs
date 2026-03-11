use std::path::{Path, PathBuf};

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
