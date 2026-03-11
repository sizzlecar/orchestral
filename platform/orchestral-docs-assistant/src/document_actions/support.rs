use std::collections::HashSet;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use kreuzberg::{extract_file, ExtractionConfig, OutputFormat, PageConfig};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub(super) struct ParsedDocument {
    pub(super) source_path: String,
    pub(super) markdown: String,
    pub(super) mime_type: String,
    pub(super) metadata: Value,
    pub(super) tables_markdown: Vec<String>,
    pub(super) page_count: Option<usize>,
}

pub(super) fn config_string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
}

pub(super) fn config_bool(config: &Value, key: &str) -> Option<bool> {
    config.get(key).and_then(|v| v.as_bool())
}

pub(super) fn config_u64(config: &Value, key: &str) -> Option<u64> {
    config.get(key).and_then(|v| v.as_u64())
}

pub(super) fn params_get_string(params: &Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
}

pub(super) fn params_get_bool(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|v| v.as_bool())
}

pub(super) fn params_get_u64(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(|v| v.as_u64())
}

pub(super) fn params_get_string_array(params: &Value, key: &str) -> Result<Vec<String>, String> {
    let Some(raw) = params.get(key) else {
        return Ok(Vec::new());
    };
    let Some(arr) = raw.as_array() else {
        return Err(format!("params.{} must be an array of strings", key));
    };

    let mut out = Vec::new();
    for item in arr {
        let Some(s) = item.as_str() else {
            return Err(format!("params.{} must contain only strings", key));
        };
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            out.push(trimmed.to_string());
        }
    }
    Ok(out)
}

pub(super) fn normalize_to_format(raw: Option<String>, default_value: &str) -> String {
    let value = raw.unwrap_or_else(|| default_value.to_string());
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return default_value.to_string();
    }
    if normalized == "md" || normalized == "markdown" || normalized == "gfm" {
        return "gfm-raw_html".to_string();
    }
    normalized
}

pub(super) fn is_markdown_format(format: &str) -> bool {
    let normalized = format.trim().to_ascii_lowercase();
    normalized == "markdown"
        || normalized == "md"
        || normalized == "gfm"
        || normalized == "gfm-raw_html"
}

pub(super) async fn parse_document_with_kreuzberg(
    source_path: &str,
    extract_pages: bool,
) -> Result<ParsedDocument, String> {
    let path = Path::new(source_path);
    if !path.exists() {
        return Err(format!("source file not found: {}", source_path));
    }

    let mut config = ExtractionConfig {
        output_format: OutputFormat::Markdown,
        ..ExtractionConfig::default()
    };
    if extract_pages {
        config.pages = Some(PageConfig {
            extract_pages: true,
            insert_page_markers: false,
            ..PageConfig::default()
        });
    }

    let result = extract_file(path, None, &config)
        .await
        .map_err(|err| format!("kreuzberg extraction failed for '{}': {}", source_path, err))?;

    let metadata = serde_json::to_value(&result.metadata).unwrap_or(Value::Null);
    let tables_markdown = result
        .tables
        .iter()
        .map(|t| t.markdown.clone())
        .collect::<Vec<_>>();

    Ok(ParsedDocument {
        source_path: source_path.to_string(),
        markdown: result.content,
        mime_type: result.mime_type.to_string(),
        metadata,
        tables_markdown,
        page_count: result.pages.as_ref().map(Vec::len),
    })
}

pub(crate) async fn parse_document_markdown(
    source_path: &str,
    extract_pages: bool,
) -> Result<(String, String), String> {
    let parsed = parse_document_with_kreuzberg(source_path, extract_pages).await?;
    Ok((parsed.markdown, parsed.mime_type))
}

pub(super) fn parse_headings(markdown: &str) -> Vec<Value> {
    let mut headings = Vec::new();
    for (idx, line) in markdown.lines().enumerate() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('#') {
            continue;
        }
        let level = trimmed.chars().take_while(|ch| *ch == '#').count();
        if level == 0 || level > 6 {
            continue;
        }
        let title = trimmed[level..].trim();
        if title.is_empty() {
            continue;
        }
        headings.push(json!({
            "line": idx + 1,
            "level": level,
            "title": title,
        }));
    }
    headings
}

pub(super) fn build_outline(headings: &[Value]) -> String {
    if headings.is_empty() {
        return "- (No headings found)".to_string();
    }

    let mut lines = Vec::new();
    for heading in headings {
        let level = heading.get("level").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
        let title = heading.get("title").and_then(|v| v.as_str()).unwrap_or("");
        let indent = "  ".repeat(level.saturating_sub(1));
        lines.push(format!("{}- {}", indent, title));
    }
    lines.join("\n")
}

pub(super) fn word_count(text: &str) -> usize {
    text.split_whitespace()
        .filter(|token| !token.is_empty())
        .count()
}

fn compact_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn split_sentences(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?' | '。' | '！' | '？') {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_string());
            }
            current.clear();
        }
    }

    let tail = current.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }

    out
}

pub(super) fn summarize_text(markdown: &str, max_sentences: usize) -> String {
    let text = compact_whitespace(markdown);
    if text.is_empty() {
        return String::new();
    }
    let sentences = split_sentences(&text);
    sentences
        .into_iter()
        .take(max_sentences.max(1))
        .collect::<Vec<_>>()
        .join(" ")
}

fn tokenize(text: &str) -> HashSet<String> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| token.len() >= 2)
        .collect()
}

pub(super) fn chunk_markdown(markdown: &str) -> Vec<String> {
    markdown
        .split("\n\n")
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(super) fn score_chunks(
    question: &str,
    chunks: &[String],
    top_k: usize,
) -> Vec<(usize, usize, String)> {
    let q_tokens = tokenize(question);
    let q_lower = question.to_ascii_lowercase();
    let mut scored = chunks
        .iter()
        .map(|chunk| {
            let chunk_tokens = tokenize(chunk);
            let overlap = q_tokens.intersection(&chunk_tokens).count();
            let contains_phrase = chunk.to_ascii_lowercase().contains(&q_lower);
            let mut score = overlap;
            if contains_phrase {
                score += 3;
            }
            let excerpt = if chunk.chars().count() > 320 {
                let mut s = chunk.chars().take(320).collect::<String>();
                s.push_str("...");
                s
            } else {
                chunk.clone()
            };
            (score, overlap, excerpt)
        })
        .filter(|(score, _, _)| *score > 0)
        .collect::<Vec<_>>();

    scored.sort_by(|a, b| b.0.cmp(&a.0));
    scored.truncate(top_k.max(1));
    scored
}

pub(super) fn build_qa_answer(question: &str, evidence: &[(usize, usize, String)]) -> String {
    if evidence.is_empty() {
        return format!(
            "没有在文档中检索到与问题“{}”直接相关的段落，请尝试更具体的问题。",
            question
        );
    }

    let mut answer = String::from("根据文档内容，以下是与问题最相关的信息：\n");
    for (idx, (_score, _overlap, excerpt)) in evidence.iter().enumerate() {
        answer.push_str(&format!("{}. {}\n", idx + 1, excerpt));
    }
    answer
}

pub(super) async fn write_text(path: &str, content: &str) -> Result<u64, String> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| format!("create output dir failed: {}", err))?;
        }
    }
    tokio::fs::write(path_ref, content.as_bytes())
        .await
        .map_err(|err| format!("write file '{}' failed: {}", path, err))?;

    let meta = tokio::fs::metadata(path_ref)
        .await
        .map_err(|err| format!("read output metadata failed: {}", err))?;
    Ok(meta.len())
}

#[derive(Debug)]
pub(super) struct PandocRunResult {
    pub(super) command: String,
    pub(super) content: String,
    pub(super) wrote_file: bool,
    pub(super) target_path: Option<String>,
    pub(super) bytes: Option<u64>,
}

pub(super) async fn run_pandoc_from_markdown(
    pandoc_command: &str,
    markdown: &str,
    to_format: &str,
    target_path: Option<&str>,
    extra_args: &[String],
    timeout_ms: Option<u64>,
) -> Result<PandocRunResult, String> {
    let mut display_args = vec![
        "--from".to_string(),
        "markdown".to_string(),
        "--to".to_string(),
        to_format.to_string(),
    ];
    display_args.extend(extra_args.iter().cloned());
    if let Some(path) = target_path {
        display_args.push("--output".to_string());
        display_args.push(path.to_string());
    }

    let mut cmd = Command::new(pandoc_command);
    cmd.arg("--from")
        .arg("markdown")
        .arg("--to")
        .arg(to_format)
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if let Some(path) = target_path {
        cmd.arg("--output").arg(path);
    }

    let mut child = cmd
        .spawn()
        .map_err(|err| format!("spawn pandoc failed: {}", err))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(markdown.as_bytes())
            .await
            .map_err(|err| format!("write pandoc stdin failed: {}", err))?;
        stdin
            .shutdown()
            .await
            .map_err(|err| format!("close pandoc stdin failed: {}", err))?;
    }

    let output = if let Some(ms) = timeout_ms {
        match timeout(Duration::from_millis(ms), child.wait_with_output()).await {
            Ok(result) => result.map_err(|err| format!("wait pandoc failed: {}", err))?,
            Err(_) => return Err("pandoc execution timed out".to_string()),
        }
    } else {
        child
            .wait_with_output()
            .await
            .map_err(|err| format!("wait pandoc failed: {}", err))?
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Err(format!(
            "pandoc exited with status {}: {}",
            output.status,
            if !stderr.is_empty() { stderr } else { stdout }
        ));
    }

    let content = if target_path.is_some() {
        String::new()
    } else {
        String::from_utf8_lossy(&output.stdout).to_string()
    };

    let bytes = if let Some(path) = target_path {
        let meta = tokio::fs::metadata(path)
            .await
            .map_err(|err| format!("read output metadata failed: {}", err))?;
        Some(meta.len())
    } else {
        None
    };

    Ok(PandocRunResult {
        command: format!("{} {}", pandoc_command, display_args.join(" ")),
        content,
        wrote_file: target_path.is_some(),
        target_path: target_path.map(ToString::to_string),
        bytes,
    })
}

pub(super) async fn doc_input_markdown_or_parse(
    params: &Value,
    extract_pages: bool,
) -> Result<(Option<String>, String), String> {
    if let Some(markdown) = params_get_string(params, "markdown") {
        if !markdown.is_empty() {
            return Ok((None, markdown));
        }
    }

    let source_path = params_get_string(params, "source_path")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "requires params.source_path or params.markdown".to_string())?;
    let parsed = parse_document_with_kreuzberg(&source_path, extract_pages).await?;
    Ok((Some(parsed.source_path), parsed.markdown))
}

pub(super) fn render_sections(sections: &[Value]) -> Result<String, String> {
    let mut out = String::new();
    for section in sections {
        let Some(obj) = section.as_object() else {
            return Err("params.sections must contain objects".to_string());
        };
        let heading = obj
            .get("heading")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| "params.sections[].heading is required".to_string())?;
        let content = obj
            .get("content")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("");
        out.push_str(&format!("## {}\n\n{}\n\n", heading, content));
    }
    Ok(out)
}
