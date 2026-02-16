#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use orchestral_actions::{ActionFactory, ActionInput, ActionSpec, DefaultActionFactory};
use orchestral_core::action::ActionContext;
use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError, WorkingSet};
use serde_json::{json, Value};
use tokio::sync::RwLock;

struct NoopReferenceStore;

#[async_trait]
impl ReferenceStore for NoopReferenceStore {
    async fn add(&self, _reference: Reference) -> Result<(), StoreError> {
        Ok(())
    }

    async fn get(&self, _id: &str) -> Result<Option<Reference>, StoreError> {
        Ok(None)
    }

    async fn query_by_type(&self, _ref_type: &ReferenceType) -> Result<Vec<Reference>, StoreError> {
        Ok(Vec::new())
    }

    async fn query_recent(&self, _limit: usize) -> Result<Vec<Reference>, StoreError> {
        Ok(Vec::new())
    }

    async fn delete(&self, _id: &str) -> Result<bool, StoreError> {
        Ok(false)
    }
}

fn test_ctx() -> ActionContext {
    ActionContext::new(
        "task-docs",
        "step-docs",
        "exec-docs",
        Arc::new(RwLock::new(WorkingSet::new())),
        Arc::new(NoopReferenceStore),
    )
}

fn action_spec(name: &str, kind: &str, config: Value) -> ActionSpec {
    ActionSpec {
        name: name.to_string(),
        kind: kind.to_string(),
        description: None,
        config,
        interface: None,
    }
}

async fn run_action(spec: &ActionSpec, params: Value) -> Value {
    let factory = DefaultActionFactory::new();
    let action = factory.build(spec).expect("build action");
    let result = action
        .run(ActionInput::with_params(params), test_ctx())
        .await;
    serde_json::to_value(result).expect("serialize action result")
}

fn expect_success(result: &Value) -> &serde_json::Map<String, Value> {
    assert_eq!(
        result.get("type").and_then(Value::as_str),
        Some("success"),
        "unexpected action result: {}",
        result
    );
    result
        .get("exports")
        .and_then(Value::as_object)
        .expect("success exports")
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos()
}

struct TestWorkspace {
    root: PathBuf,
}

impl TestWorkspace {
    fn new(prefix: &str) -> Self {
        let root = PathBuf::from(format!(
            "target/doc_assistant_integration_{}_{}",
            prefix,
            unique_suffix()
        ));
        std::fs::create_dir_all(&root).expect("create test workspace");
        Self { root }
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn write_file(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    std::fs::write(path, content).expect("write file");
}

fn create_mock_pandoc_script(root: &Path) -> PathBuf {
    let script_path = root.join("mock_pandoc.sh");
    let script = r#"#!/bin/sh
set -eu
to=""
out=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --to)
      to="$2"
      shift 2
      ;;
    --output)
      out="$2"
      shift 2
      ;;
    *)
      shift 1
      ;;
  esac
done
input="$(cat)"
if [ -n "$out" ]; then
  printf "FORMAT:%s\n%s" "$to" "$input" > "$out"
else
  printf "FORMAT:%s\n%s" "$to" "$input"
fi
"#;
    write_file(&script_path, script);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = std::fs::metadata(&script_path)
            .expect("script metadata")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).expect("set script executable");
    }
    script_path
}

#[tokio::test]
async fn test_doc_parse_then_summarize_flow() {
    let workspace = TestWorkspace::new("parse_summary");
    let source_path = workspace.path().join("input.txt");
    write_file(
        &source_path,
        "# Ownership\nRust ownership ensures memory safety without GC.\n\n## Borrowing\nBorrow rules prevent data races.",
    );

    let parse_spec = action_spec("doc_parse", "doc_parse", json!({ "extract_pages": false }));
    let parse_result = run_action(
        &parse_spec,
        json!({
            "source_path": source_path.to_string_lossy(),
        }),
    )
    .await;
    let parse_exports = expect_success(&parse_result);
    assert_eq!(
        parse_exports.get("parser").and_then(Value::as_str),
        Some("kreuzberg_rust_sdk")
    );
    let parsed_markdown = parse_exports
        .get("markdown")
        .and_then(Value::as_str)
        .expect("parsed markdown");
    assert!(parsed_markdown.contains("ownership") || parsed_markdown.contains("Ownership"));

    let summarize_spec = action_spec(
        "doc_summarize",
        "doc_summarize",
        json!({
            "default_mode": "summary",
            "default_sentences": 3
        }),
    );
    let summarize_result = run_action(
        &summarize_spec,
        json!({
            "markdown": parsed_markdown,
            "mode": "summary",
            "summary_sentences": 2
        }),
    )
    .await;
    let summarize_exports = expect_success(&summarize_result);
    let generated_markdown = summarize_exports
        .get("generated_markdown")
        .and_then(Value::as_str)
        .expect("generated summary");
    assert!(generated_markdown.contains("# Document Summary"));
    assert!(generated_markdown.contains("Ownership") || generated_markdown.contains("ownership"));
}

#[tokio::test]
async fn test_doc_qa_flow_from_source_file() {
    let workspace = TestWorkspace::new("qa");
    let source_path = workspace.path().join("knowledge.txt");
    write_file(
        &source_path,
        "Rust ownership model gives deterministic memory management.\nBorrow checking prevents data races.",
    );

    let qa_spec = action_spec("doc_qa", "doc_qa", json!({ "default_top_k": 2 }));
    let qa_result = run_action(
        &qa_spec,
        json!({
            "source_path": source_path.to_string_lossy(),
            "question": "What does ownership prevent?",
            "top_k": 2
        }),
    )
    .await;

    let qa_exports = expect_success(&qa_result);
    let evidence = qa_exports
        .get("evidence")
        .and_then(Value::as_array)
        .expect("qa evidence");
    assert!(!evidence.is_empty(), "qa evidence should not be empty");
    let answer = qa_exports
        .get("answer")
        .and_then(Value::as_str)
        .expect("qa answer");
    assert!(
        answer.contains("相关信息") || answer.contains("ownership") || answer.contains("Ownership")
    );
}

#[tokio::test]
async fn test_doc_convert_flow_with_mock_pandoc() {
    let workspace = TestWorkspace::new("convert");
    let mock_pandoc = create_mock_pandoc_script(workspace.path());

    let convert_spec = action_spec(
        "doc_convert",
        "doc_convert",
        json!({
            "pandoc_command": mock_pandoc.to_string_lossy(),
            "default_to_format": "html",
            "timeout_ms": 30_000
        }),
    );
    let convert_result = run_action(
        &convert_spec,
        json!({
            "markdown": "# Title\n\nDocument body.",
            "to_format": "html"
        }),
    )
    .await;

    let convert_exports = expect_success(&convert_result);
    let content = convert_exports
        .get("content")
        .and_then(Value::as_str)
        .expect("converted content");
    assert!(content.contains("FORMAT:html"));
    assert!(content.contains("Document body."));
    assert_eq!(
        convert_exports.get("parser").and_then(Value::as_str),
        Some("kreuzberg_rust_sdk")
    );
    let command = convert_exports
        .get("pandoc_command")
        .and_then(Value::as_str)
        .expect("pandoc command");
    assert!(command.contains("--to html"));
}

#[tokio::test]
async fn test_doc_generate_then_merge_flow_with_mock_pandoc() {
    let workspace = TestWorkspace::new("generate_merge");
    let mock_pandoc = create_mock_pandoc_script(workspace.path());

    let generate_spec = action_spec(
        "doc_generate",
        "doc_generate",
        json!({
            "pandoc_command": mock_pandoc.to_string_lossy(),
            "timeout_ms": 30_000
        }),
    );
    let generate_result = run_action(
        &generate_spec,
        json!({
            "title": "Release Notes",
            "sections": [
                {"heading": "Highlights", "content": "Added Rust SDK document actions."},
                {"heading": "Risks", "content": "PDF builds require PDFium."}
            ],
            "to_format": "html"
        }),
    )
    .await;
    let generate_exports = expect_success(&generate_result);
    let generated_markdown = generate_exports
        .get("generated_markdown")
        .and_then(Value::as_str)
        .expect("generated markdown")
        .to_string();
    assert!(generated_markdown.contains("## Highlights"));
    assert!(generated_markdown.contains("Rust SDK document actions"));

    let source_path = workspace.path().join("doc_fragment.txt");
    write_file(
        &source_path,
        "This fragment confirms document assistant actions are split by capability.",
    );

    let merge_spec = action_spec(
        "doc_merge",
        "doc_merge",
        json!({
            "pandoc_command": mock_pandoc.to_string_lossy(),
            "add_source_headers": true,
            "timeout_ms": 30_000
        }),
    );
    let merge_result = run_action(
        &merge_spec,
        json!({
            "source_paths": [source_path.to_string_lossy()],
            "markdowns": [generated_markdown],
            "separator": "\n\n---MERGED---\n\n",
            "to_format": "html"
        }),
    )
    .await;

    let merge_exports = expect_success(&merge_result);
    let merged_markdown = merge_exports
        .get("merged_markdown")
        .and_then(Value::as_str)
        .expect("merged markdown");
    assert!(merged_markdown.contains("Source:"));
    assert!(merged_markdown.contains("MERGED"));
    assert_eq!(
        merge_exports.get("source_count").and_then(Value::as_u64),
        Some(1)
    );
    let merged_content = merge_exports
        .get("content")
        .and_then(Value::as_str)
        .expect("merged converted content");
    assert!(merged_content.contains("FORMAT:html"));
}
