use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use kreuzberg::{extract_file, ExtractionConfig, OutputFormat, PageConfig};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

use orchestral_config::ActionSpec;
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

/// Build built-in document actions.
pub fn build_document_action(spec: &ActionSpec) -> Option<Box<dyn Action>> {
    match spec.kind.as_str() {
        "doc_parse" => Some(Box::new(DocParseAction::from_spec(spec))),
        "doc_convert" => Some(Box::new(DocConvertAction::from_spec(spec))),
        "doc_summarize" => Some(Box::new(DocSummarizeAction::from_spec(spec))),
        "doc_generate" => Some(Box::new(DocGenerateAction::from_spec(spec))),
        "doc_qa" => Some(Box::new(DocQaAction::from_spec(spec))),
        "doc_merge" => Some(Box::new(DocMergeAction::from_spec(spec))),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct ParsedDocument {
    source_path: String,
    markdown: String,
    mime_type: String,
    metadata: Value,
    tables_markdown: Vec<String>,
    page_count: Option<usize>,
}

fn config_string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
}

fn config_bool(config: &Value, key: &str) -> Option<bool> {
    config.get(key).and_then(|v| v.as_bool())
}

fn config_u64(config: &Value, key: &str) -> Option<u64> {
    config.get(key).and_then(|v| v.as_u64())
}

fn params_get_string(params: &Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
}

fn params_get_bool(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|v| v.as_bool())
}

fn params_get_u64(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(|v| v.as_u64())
}

fn params_get_string_array(params: &Value, key: &str) -> Result<Vec<String>, String> {
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

fn normalize_to_format(raw: Option<String>, default_value: &str) -> String {
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

fn is_markdown_format(format: &str) -> bool {
    let normalized = format.trim().to_ascii_lowercase();
    normalized == "markdown"
        || normalized == "md"
        || normalized == "gfm"
        || normalized == "gfm-raw_html"
}

async fn parse_document_with_kreuzberg(
    source_path: &str,
    extract_pages: bool,
) -> Result<ParsedDocument, String> {
    let path = Path::new(source_path);
    if !path.exists() {
        return Err(format!("source file not found: {}", source_path));
    }

    let mut config = ExtractionConfig::default();
    config.output_format = OutputFormat::Markdown;
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

fn parse_headings(markdown: &str) -> Vec<Value> {
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

fn build_outline(headings: &[Value]) -> String {
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

fn word_count(text: &str) -> usize {
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

fn summarize_text(markdown: &str, max_sentences: usize) -> String {
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

fn chunk_markdown(markdown: &str) -> Vec<String> {
    markdown
        .split("\n\n")
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn score_chunks(question: &str, chunks: &[String], top_k: usize) -> Vec<(usize, usize, String)> {
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

fn build_qa_answer(question: &str, evidence: &[(usize, usize, String)]) -> String {
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

async fn write_text(path: &str, content: &str) -> Result<u64, String> {
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
struct PandocRunResult {
    command: String,
    content: String,
    wrote_file: bool,
    target_path: Option<String>,
    bytes: Option<u64>,
}

async fn run_pandoc_from_markdown(
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

async fn doc_input_markdown_or_parse(
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

pub struct DocParseAction {
    name: String,
    description: String,
    default_extract_pages: bool,
}

impl DocParseAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Parse documents with Kreuzberg Rust SDK and return normalized markdown",
            ),
            default_extract_pages: config_bool(&spec.config, "extract_pages").unwrap_or(false),
        }
    }
}

#[async_trait]
impl Action for DocParseAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": "string"},
                    "extract_pages": {"type": "boolean"}
                },
                "required": ["source_path"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "markdown": {"type": "string"},
                    "headings": {"type": "array"},
                    "word_count": {"type": "integer"},
                    "char_count": {"type": "integer"},
                    "table_count": {"type": "integer"},
                    "page_count": {"type": ["integer", "null"]},
                    "parser": {"type": "string"}
                },
                "required": ["source_path", "markdown", "parser"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let source_path = match params_get_string(params, "source_path") {
            Some(path) if !path.is_empty() => path,
            _ => return ActionResult::error("doc_parse requires params.source_path"),
        };

        let extract_pages =
            params_get_bool(params, "extract_pages").unwrap_or(self.default_extract_pages);
        let parsed = match parse_document_with_kreuzberg(&source_path, extract_pages).await {
            Ok(parsed) => parsed,
            Err(err) => return ActionResult::error(err),
        };

        let headings = parse_headings(&parsed.markdown);
        let mut exports = HashMap::new();
        exports.insert("source_path".to_string(), json!(parsed.source_path));
        exports.insert("mime_type".to_string(), json!(parsed.mime_type));
        exports.insert("markdown".to_string(), json!(parsed.markdown));
        exports.insert("headings".to_string(), Value::Array(headings));
        exports.insert(
            "word_count".to_string(),
            json!(word_count(&parsed.markdown)),
        );
        exports.insert(
            "char_count".to_string(),
            json!(parsed.markdown.chars().count()),
        );
        exports.insert(
            "table_count".to_string(),
            json!(parsed.tables_markdown.len()),
        );
        exports.insert("tables_markdown".to_string(), json!(parsed.tables_markdown));
        exports.insert("page_count".to_string(), json!(parsed.page_count));
        exports.insert("metadata".to_string(), parsed.metadata);
        exports.insert("parser".to_string(), json!("kreuzberg_rust_sdk"));
        ActionResult::success_with(exports)
    }
}

pub struct DocConvertAction {
    name: String,
    description: String,
    pandoc_command: String,
    default_to_format: String,
    timeout_ms: Option<u64>,
}

impl DocConvertAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Convert documents using pandoc with markdown normalized by Kreuzberg parser",
            ),
            pandoc_command: config_string(&spec.config, "pandoc_command")
                .unwrap_or_else(|| "pandoc".to_string()),
            default_to_format: config_string(&spec.config, "default_to_format")
                .unwrap_or_else(|| "gfm-raw_html".to_string()),
            timeout_ms: config_u64(&spec.config, "timeout_ms"),
        }
    }
}

#[async_trait]
impl Action for DocConvertAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": "string"},
                    "markdown": {"type": "string"},
                    "to_format": {"type": "string"},
                    "target_path": {"type": "string"},
                    "extra_args": {"type": "array", "items": {"type": "string"}},
                    "return_markdown": {"type": "boolean"}
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": ["string", "null"]},
                    "to_format": {"type": "string"},
                    "target_path": {"type": ["string", "null"]},
                    "wrote_file": {"type": "boolean"},
                    "bytes": {"type": ["integer", "null"]},
                    "content": {"type": "string"},
                    "pandoc_command": {"type": "string"},
                    "parser": {"type": "string"}
                },
                "required": ["to_format", "wrote_file", "pandoc_command", "parser"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let source_path = params_get_string(params, "source_path").filter(|v| !v.is_empty());
        let markdown_input = params_get_string(params, "markdown").filter(|v| !v.is_empty());

        let markdown = if let Some(markdown) = markdown_input {
            markdown
        } else if let Some(path) = source_path.as_ref() {
            match parse_document_with_kreuzberg(path, false).await {
                Ok(parsed) => parsed.markdown,
                Err(err) => return ActionResult::error(err),
            }
        } else {
            return ActionResult::error(
                "doc_convert requires params.source_path or params.markdown",
            );
        };

        let to_format = normalize_to_format(
            params_get_string(params, "to_format"),
            &self.default_to_format,
        );
        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let extra_args = match params_get_string_array(params, "extra_args") {
            Ok(args) => args,
            Err(err) => return ActionResult::error(err),
        };

        let return_markdown = params_get_bool(params, "return_markdown").unwrap_or(false);

        let converted = match run_pandoc_from_markdown(
            &self.pandoc_command,
            &markdown,
            &to_format,
            target_path.as_deref(),
            &extra_args,
            self.timeout_ms,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => return ActionResult::error(err),
        };

        let mut exports = HashMap::new();
        exports.insert("source_path".to_string(), json!(source_path));
        exports.insert("to_format".to_string(), json!(to_format));
        exports.insert("target_path".to_string(), json!(converted.target_path));
        exports.insert("wrote_file".to_string(), json!(converted.wrote_file));
        exports.insert("bytes".to_string(), json!(converted.bytes));
        exports.insert("content".to_string(), json!(converted.content));
        exports.insert("pandoc_command".to_string(), json!(converted.command));
        exports.insert("parser".to_string(), json!("kreuzberg_rust_sdk"));
        if return_markdown {
            exports.insert("markdown".to_string(), json!(markdown));
        }
        ActionResult::success_with(exports)
    }
}

pub struct DocSummarizeAction {
    name: String,
    description: String,
    default_sentences: usize,
    default_mode: String,
}

impl DocSummarizeAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Generate summary/outline/overview markdown from normalized document text",
            ),
            default_sentences: config_u64(&spec.config, "default_sentences")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(5),
            default_mode: config_string(&spec.config, "default_mode")
                .unwrap_or_else(|| "summary".to_string()),
        }
    }
}

#[async_trait]
impl Action for DocSummarizeAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let (source_path, markdown) = match doc_input_markdown_or_parse(params, false).await {
            Ok(v) => v,
            Err(err) => return ActionResult::error(format!("doc_summarize {}", err)),
        };

        let mode = params_get_string(params, "mode")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.default_mode.clone())
            .to_ascii_lowercase();
        let summary_sentences = params_get_u64(params, "summary_sentences")
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(self.default_sentences)
            .max(1);

        let headings = parse_headings(&markdown);
        let summary = summarize_text(&markdown, summary_sentences);
        let outline = build_outline(&headings);

        let generated_markdown = if mode == "outline" {
            format!("# Document Outline\n\n{}\n", outline)
        } else if mode == "overview" {
            format!(
                "# Document Overview\n\n- Word count: {}\n- Heading count: {}\n\n## Summary\n\n{}\n",
                word_count(&markdown),
                headings.len(),
                summary
            )
        } else {
            format!(
                "# Document Summary\n\n{}\n\n## Key Sections\n\n{}\n",
                summary, outline
            )
        };

        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let (wrote_file, bytes) = if let Some(path) = target_path.as_deref() {
            match write_text(path, &generated_markdown).await {
                Ok(bytes) => (true, Some(bytes)),
                Err(err) => return ActionResult::error(err),
            }
        } else {
            (false, None)
        };

        let mut exports = HashMap::new();
        exports.insert("source_path".to_string(), json!(source_path));
        exports.insert("mode".to_string(), json!(mode));
        exports.insert("generated_markdown".to_string(), json!(generated_markdown));
        exports.insert("target_path".to_string(), json!(target_path));
        exports.insert("wrote_file".to_string(), json!(wrote_file));
        exports.insert("bytes".to_string(), json!(bytes));
        exports.insert("word_count".to_string(), json!(word_count(&markdown)));
        exports.insert("heading_count".to_string(), json!(headings.len()));
        exports.insert("parser".to_string(), json!("kreuzberg_rust_sdk"));
        ActionResult::success_with(exports)
    }
}

pub struct DocGenerateAction {
    name: String,
    description: String,
    pandoc_command: String,
    timeout_ms: Option<u64>,
}

impl DocGenerateAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Generate structured documents and optionally convert format via pandoc",
            ),
            pandoc_command: config_string(&spec.config, "pandoc_command")
                .unwrap_or_else(|| "pandoc".to_string()),
            timeout_ms: config_u64(&spec.config, "timeout_ms"),
        }
    }
}

fn render_sections(sections: &[Value]) -> Result<String, String> {
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

#[async_trait]
impl Action for DocGenerateAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let title = params_get_string(params, "title")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "Generated Document".to_string());
        let body = params_get_string(params, "body").unwrap_or_default();
        let source_markdowns = match params_get_string_array(params, "source_markdowns") {
            Ok(v) => v,
            Err(err) => return ActionResult::error(err),
        };
        let sections = params
            .get("sections")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        if body.trim().is_empty() && sections.is_empty() && source_markdowns.is_empty() {
            return ActionResult::error(
                "doc_generate requires at least one of params.body / params.sections / params.source_markdowns",
            );
        }

        let mut markdown = format!("# {}\n\n", title.trim());
        if !body.trim().is_empty() {
            markdown.push_str(body.trim());
            markdown.push_str("\n\n");
        }
        match render_sections(&sections) {
            Ok(rendered) => markdown.push_str(&rendered),
            Err(err) => return ActionResult::error(err),
        }

        if !source_markdowns.is_empty() {
            markdown.push_str("## Source Notes\n\n");
            for (idx, src) in source_markdowns.iter().enumerate() {
                markdown.push_str(&format!("### Source {}\n\n{}\n\n", idx + 1, src));
            }
        }

        let to_format = params_get_string(params, "to_format")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "markdown".to_string());
        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let extra_args = match params_get_string_array(params, "extra_args") {
            Ok(args) => args,
            Err(err) => return ActionResult::error(err),
        };

        let mut exports = HashMap::new();
        exports.insert("mode".to_string(), json!("generate"));
        exports.insert("to_format".to_string(), json!(to_format));
        exports.insert("target_path".to_string(), json!(target_path));
        exports.insert("generated_markdown".to_string(), json!(markdown));

        if is_markdown_format(&to_format) {
            if let Some(path) = target_path.as_deref() {
                let bytes = match write_text(path, &markdown).await {
                    Ok(bytes) => bytes,
                    Err(err) => return ActionResult::error(err),
                };
                exports.insert("wrote_file".to_string(), json!(true));
                exports.insert("bytes".to_string(), json!(bytes));
                exports.insert("content".to_string(), json!(""));
            } else {
                exports.insert("wrote_file".to_string(), json!(false));
                exports.insert("bytes".to_string(), Value::Null);
                exports.insert("content".to_string(), json!(markdown));
            }
            return ActionResult::success_with(exports);
        }

        let converted = match run_pandoc_from_markdown(
            &self.pandoc_command,
            &markdown,
            &to_format,
            target_path.as_deref(),
            &extra_args,
            self.timeout_ms,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => return ActionResult::error(err),
        };

        exports.insert("wrote_file".to_string(), json!(converted.wrote_file));
        exports.insert("bytes".to_string(), json!(converted.bytes));
        exports.insert("content".to_string(), json!(converted.content));
        exports.insert("pandoc_command".to_string(), json!(converted.command));
        ActionResult::success_with(exports)
    }
}

pub struct DocQaAction {
    name: String,
    description: String,
    default_top_k: usize,
}

impl DocQaAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec
                .description_or("Answer questions from document content with evidence snippets"),
            default_top_k: config_u64(&spec.config, "default_top_k")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(3),
        }
    }
}

#[async_trait]
impl Action for DocQaAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let question = match params_get_string(params, "question") {
            Some(q) if !q.trim().is_empty() => q,
            _ => return ActionResult::error("doc_qa requires params.question"),
        };

        let top_k = params_get_u64(params, "top_k")
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(self.default_top_k)
            .max(1);

        let (source_path, markdown) = match doc_input_markdown_or_parse(params, false).await {
            Ok(v) => v,
            Err(err) => return ActionResult::error(format!("doc_qa {}", err)),
        };

        let chunks = chunk_markdown(&markdown);
        let scored = score_chunks(&question, &chunks, top_k);
        let answer = build_qa_answer(&question, &scored);

        let evidence = scored
            .iter()
            .enumerate()
            .map(|(idx, (score, overlap, excerpt))| {
                json!({
                    "rank": idx + 1,
                    "score": score,
                    "token_overlap": overlap,
                    "excerpt": excerpt,
                })
            })
            .collect::<Vec<_>>();

        let mut exports = HashMap::new();
        exports.insert("question".to_string(), json!(question));
        exports.insert("answer".to_string(), json!(answer));
        exports.insert("source_path".to_string(), json!(source_path));
        exports.insert("evidence".to_string(), json!(evidence));
        exports.insert("top_k".to_string(), json!(top_k));
        ActionResult::success_with(exports)
    }
}

pub struct DocMergeAction {
    name: String,
    description: String,
    pandoc_command: String,
    timeout_ms: Option<u64>,
    add_source_headers: bool,
}

impl DocMergeAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec
                .description_or("Merge multiple documents and optionally convert via pandoc"),
            pandoc_command: config_string(&spec.config, "pandoc_command")
                .unwrap_or_else(|| "pandoc".to_string()),
            timeout_ms: config_u64(&spec.config, "timeout_ms"),
            add_source_headers: config_bool(&spec.config, "add_source_headers").unwrap_or(true),
        }
    }
}

#[async_trait]
impl Action for DocMergeAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let source_paths = match params_get_string_array(params, "source_paths") {
            Ok(v) => v,
            Err(err) => return ActionResult::error(err),
        };
        let inline_markdowns = match params_get_string_array(params, "markdowns") {
            Ok(v) => v,
            Err(err) => return ActionResult::error(err),
        };

        if source_paths.is_empty() && inline_markdowns.is_empty() {
            return ActionResult::error(
                "doc_merge requires params.source_paths or params.markdowns",
            );
        }

        let separator =
            params_get_string(params, "separator").unwrap_or_else(|| "\n\n---\n\n".to_string());
        let add_source_headers =
            params_get_bool(params, "add_source_headers").unwrap_or(self.add_source_headers);

        let mut chunks = Vec::new();
        for path in &source_paths {
            let parsed = match parse_document_with_kreuzberg(path, false).await {
                Ok(parsed) => parsed,
                Err(err) => return ActionResult::error(err),
            };
            if add_source_headers {
                chunks.push(format!("## Source: {}\n\n{}", path, parsed.markdown));
            } else {
                chunks.push(parsed.markdown);
            }
        }
        chunks.extend(inline_markdowns.into_iter());

        let merged_markdown = chunks.join(&separator);
        let to_format = params_get_string(params, "to_format")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "markdown".to_string());
        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let extra_args = match params_get_string_array(params, "extra_args") {
            Ok(args) => args,
            Err(err) => return ActionResult::error(err),
        };

        let mut exports = HashMap::new();
        exports.insert("mode".to_string(), json!("merge"));
        exports.insert("source_count".to_string(), json!(source_paths.len()));
        exports.insert("target_path".to_string(), json!(target_path));
        exports.insert("to_format".to_string(), json!(to_format));
        exports.insert("merged_markdown".to_string(), json!(merged_markdown));

        if is_markdown_format(&to_format) {
            if let Some(path) = target_path.as_deref() {
                let bytes = match write_text(path, &merged_markdown).await {
                    Ok(bytes) => bytes,
                    Err(err) => return ActionResult::error(err),
                };
                exports.insert("wrote_file".to_string(), json!(true));
                exports.insert("bytes".to_string(), json!(bytes));
                exports.insert("content".to_string(), json!(""));
            } else {
                exports.insert("wrote_file".to_string(), json!(false));
                exports.insert("bytes".to_string(), Value::Null);
                exports.insert("content".to_string(), json!(merged_markdown));
            }
            return ActionResult::success_with(exports);
        }

        let converted = match run_pandoc_from_markdown(
            &self.pandoc_command,
            &merged_markdown,
            &to_format,
            target_path.as_deref(),
            &extra_args,
            self.timeout_ms,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => return ActionResult::error(err),
        };

        exports.insert("wrote_file".to_string(), json!(converted.wrote_file));
        exports.insert("bytes".to_string(), json!(converted.bytes));
        exports.insert("content".to_string(), json!(converted.content));
        exports.insert("pandoc_command".to_string(), json!(converted.command));
        ActionResult::success_with(exports)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_headings_and_outline() {
        let markdown = "# H1\n\ntext\n\n## H2\n\n### H3";
        let headings = parse_headings(markdown);
        assert_eq!(headings.len(), 3);
        let outline = build_outline(&headings);
        assert!(outline.contains("- H1"));
        assert!(outline.contains("  - H2"));
    }

    #[test]
    fn test_summarize_text() {
        let markdown = "First sentence. Second sentence! Third sentence? Fourth.";
        let summary = summarize_text(markdown, 2);
        assert!(summary.contains("First sentence."));
        assert!(summary.contains("Second sentence!"));
        assert!(!summary.contains("Third sentence?"));
    }

    #[test]
    fn test_score_chunks() {
        let question = "what is rust ownership";
        let chunks = vec![
            "Rust ownership ensures memory safety.".to_string(),
            "Cooking recipe for pasta.".to_string(),
        ];
        let evidence = score_chunks(question, &chunks, 2);
        assert_eq!(evidence.len(), 1);
        assert!(evidence[0].2.to_ascii_lowercase().contains("ownership"));
    }

    #[test]
    fn test_render_sections_validation() {
        let sections = vec![json!({"heading": "Intro", "content": "Hello"})];
        let rendered = render_sections(&sections).expect("render");
        assert!(rendered.contains("## Intro"));
        assert!(rendered.contains("Hello"));
    }
}
