use std::fs;
use std::path::Path;

use anyhow::{bail, Context};

pub fn load_env_file(path: &Path) -> anyhow::Result<usize> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read env file '{}' failed", path.display()))?;
    let entries = parse_env_entries(&raw)?;
    for (key, value) in &entries {
        std::env::set_var(key, value);
    }
    Ok(entries.len())
}

fn parse_env_entries(raw: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut entries = Vec::new();
    for (idx, raw_line) in raw.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((raw_key, raw_value)) = line.split_once('=') else {
            bail!("invalid env file line {}: expected KEY=VALUE", line_no);
        };

        let key = raw_key.trim();
        if !is_valid_env_key(key) {
            bail!("invalid env key '{}' on line {}", key, line_no);
        }

        let value = unquote_env_value(raw_value.trim());
        entries.push((key.to_string(), value));
    }
    Ok(entries)
}

fn is_valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn unquote_env_value(value: &str) -> String {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let first = bytes[0];
        let last = bytes[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_env_entries;

    #[test]
    fn test_parse_env_entries_supports_export_prefix_and_quotes() {
        let entries = parse_env_entries(
            r#"
            # comment
            export OPENAI_API_KEY="openai-key"
            GOOGLE_API_KEY=google-key
            ANTHROPIC_API_KEY='anthropic-key'
            http_proxy=http://127.0.0.1:41809
            https_proxy='http://127.0.0.1:41809'
            RUST_LOG=info
            "#,
        )
        .expect("parse env entries");

        assert_eq!(
            entries,
            vec![
                ("OPENAI_API_KEY".to_string(), "openai-key".to_string()),
                ("GOOGLE_API_KEY".to_string(), "google-key".to_string()),
                (
                    "ANTHROPIC_API_KEY".to_string(),
                    "anthropic-key".to_string()
                ),
                (
                    "http_proxy".to_string(),
                    "http://127.0.0.1:41809".to_string()
                ),
                (
                    "https_proxy".to_string(),
                    "http://127.0.0.1:41809".to_string()
                ),
                ("RUST_LOG".to_string(), "info".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_env_entries_rejects_invalid_lines() {
        let err = parse_env_entries("not-an-assignment").expect_err("expected invalid line");
        assert!(err.to_string().contains("expected KEY=VALUE"));
    }
}
