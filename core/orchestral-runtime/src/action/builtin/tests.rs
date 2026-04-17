use super::http::{headers_from_value, merge_headers, HttpAction};
use super::shell::ShellAction;
use super::support::{
    approval_decision_from_ctx, extract_inline_script_body, requires_destructive_approval,
    ApprovalDecision,
};
use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionResult};
use orchestral_core::config::ActionSpec;
use orchestral_core::store::WorkingSet;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

static TEST_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

fn test_ctx() -> ActionContext {
    ActionContext::new(
        "task-1",
        "s1",
        "exec-1",
        Arc::new(RwLock::new(WorkingSet::new())),
    )
}

fn unique_test_path(prefix: &str) -> String {
    let unique = TEST_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("target/{}_{}_{}.txt", prefix, std::process::id(), unique)
}

#[tokio::test]
async fn test_json_stdout_action_serializes_payload_to_stdout() {
    let action = JsonStdoutAction::internal();
    let result = action
        .run(
            ActionInput::with_params(json!({
                "payload": {
                    "change_spec": {
                        "cells": 2
                    }
                }
            })),
            test_ctx(),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(exports.get("status"), Some(&json!(0)));
            let stdout = exports
                .get("stdout")
                .and_then(|value| value.as_str())
                .expect("stdout");
            let parsed: Value = serde_json::from_str(stdout).expect("valid json");
            assert_eq!(parsed["change_spec"]["cells"], json!(2));
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn test_approval_decision_from_ctx_reads_string_payload() {
    tokio_test::block_on(async {
        let ctx = test_ctx();
        {
            let mut ws = ctx.working_set.write().await;
            ws.set_task("resume_user_input", json!("/approve"));
        }

        let decision = approval_decision_from_ctx(&ctx).await;
        assert_eq!(decision, Some(ApprovalDecision::Approve));

        let ws = ctx.working_set.read().await;
        assert!(ws.get_task("resume_user_input").is_none());
    });
}

#[test]
fn test_approval_decision_from_ctx_reads_object_message() {
    tokio_test::block_on(async {
        let ctx = test_ctx();
        {
            let mut ws = ctx.working_set.write().await;
            ws.set_task("resume_user_input", json!({"message": "/deny"}));
        }

        let decision = approval_decision_from_ctx(&ctx).await;
        assert_eq!(decision, Some(ApprovalDecision::Deny));

        let ws = ctx.working_set.read().await;
        assert!(ws.get_task("resume_user_input").is_none());
    });
}

#[test]
fn test_requires_destructive_approval_for_find_delete_shell_expression() {
    assert!(requires_destructive_approval(
        true,
        "find",
        "find . -name github_status.json -delete",
        &[],
    ));
}

#[test]
fn test_requires_destructive_approval_for_find_delete_args() {
    assert!(requires_destructive_approval(
        false,
        "find",
        "find",
        &[
            ".".to_string(),
            "-name".to_string(),
            "github_status.json".to_string(),
            "-delete".to_string(),
        ],
    ));
}

#[test]
fn test_requires_destructive_approval_for_find_without_delete() {
    assert!(!requires_destructive_approval(
        false,
        "find",
        "find",
        &[
            ".".to_string(),
            "-name".to_string(),
            "github_status.json".to_string()
        ],
    ));
}

#[test]
fn test_requires_destructive_approval_does_not_flag_git_log_shell_expression() {
    assert!(!requires_destructive_approval(
        true,
        "git",
        "git log --oneline -20",
        &[],
    ));
}

#[test]
fn test_requires_destructive_approval_flags_git_reset_shell_expression() {
    assert!(requires_destructive_approval(
        true,
        "git",
        "git reset --hard HEAD~1",
        &[],
    ));
}

#[test]
fn test_extract_inline_script_body_detects_python_c_and_shell_c() {
    let python_args = vec![
        "-c".to_string(),
        "print('hello')\nprint('world')".to_string(),
        "arg1".to_string(),
    ];
    assert_eq!(
        extract_inline_script_body(false, &python_args),
        Some("print('hello')\nprint('world')")
    );

    let shell_args = vec!["-c".to_string(), "echo hi && echo bye".to_string()];
    assert_eq!(
        extract_inline_script_body(true, &shell_args),
        Some("echo hi && echo bye")
    );
}

#[test]
fn test_file_write_allows_new_file_under_workspace_roots() {
    tokio_test::block_on(async {
        let path = unique_test_path("orchestral_file_write");
        let spec = ActionSpec {
            name: "file_write".to_string(),
            kind: "file_write".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "workspace_write",
                "sandbox_writable_roots": ["."],
                "create_dirs": true
            }),
            interface: None,
        };
        let action = FileWriteAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "path": path,
            "content": "hello"
        }));
        let ctx = test_ctx();

        let result = action.run(input, ctx).await;
        match result {
            ActionResult::Success { exports } => {
                assert_eq!(
                    exports.get("bytes").and_then(|v| v.as_u64()),
                    Some("hello".len() as u64)
                );
                let written_path = exports
                    .get("path")
                    .and_then(|v| v.as_str())
                    .expect("path export");
                let content = tokio::fs::read_to_string(written_path)
                    .await
                    .expect("read back");
                assert_eq!(content, "hello");
                let _ = tokio::fs::remove_file(written_path).await;
            }
            other => panic!("expected success, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_rejects_expression_without_explicit_shell_mode() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({}),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "echo hello"
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("Unsafe shell expression detected"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_blocks_command_by_policy() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({
                "blocked_commands": ["echo"]
            }),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "echo",
            "args": ["hello"]
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("blocked by policy"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_blocks_expression_when_contains_blocked_command() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({
                "allow_shell_expression": true,
                "blocked_commands": ["rm"]
            }),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "echo ok && rm demo.txt",
            "shell": true
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("contains blocked command"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_blocks_expression_when_contains_disallowed_command() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({
                "allow_shell_expression": true,
                "allowed_commands": ["echo"]
            }),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "echo ok && rm demo.txt",
            "shell": true
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("allowed_commands"));
                assert!(message.contains("rm"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_minimal_env_hides_secret_vars() {
    tokio_test::block_on(async {
        let secret_key = format!("ORCHESTRAL_TEST_SECRET_{}", std::process::id());
        std::env::set_var(&secret_key, "top-secret-value");

        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "none",
                "env_policy": "minimal"
            }),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "env"
        }));
        let result = action.run(input, test_ctx()).await;
        std::env::remove_var(&secret_key);

        match result {
            ActionResult::Success { exports } => {
                let stdout = exports
                    .get("stdout")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                assert!(!stdout.contains(&secret_key));
                assert!(!stdout.contains("top-secret-value"));
                assert_eq!(
                    exports.get("env_policy").and_then(|v| v.as_str()),
                    Some("minimal")
                );
            }
            other => panic!("expected success, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_non_zero_exit_is_error_by_default() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "none"
            }),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "sh",
            "args": ["-c", "exit 7"]
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(
                    message.contains("status 7"),
                    "unexpected message: {}",
                    message
                );
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_shell_can_allow_non_zero_exit() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "shell".to_string(),
            kind: "shell".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "none",
                "fail_on_non_zero": false
            }),
            interface: None,
        };
        let action = ShellAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "command": "sh",
            "args": ["-c", "exit 9"]
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Success { exports } => {
                assert_eq!(exports.get("status").and_then(|v| v.as_i64()), Some(9));
            }
            other => panic!("expected success, got {:?}", other),
        }
    });
}

#[test]
fn test_file_read_respects_size_limit_and_truncate() {
    tokio_test::block_on(async {
        tokio::fs::create_dir_all("target")
            .await
            .expect("create target dir");
        let path = unique_test_path("orchestral_file_read");
        tokio::fs::write(&path, "abcdefghij")
            .await
            .expect("seed file");

        let spec = ActionSpec {
            name: "file_read".to_string(),
            kind: "file_read".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "workspace_write",
                "sandbox_writable_roots": ["."],
                "max_read_bytes": 8
            }),
            interface: None,
        };
        let action = FileReadAction::from_spec(&spec);

        let too_large = ActionInput::with_params(json!({
            "path": path.clone(),
            "max_bytes": 4
        }));
        let result = action.run(too_large, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("File too large"));
            }
            other => panic!("expected error, got {:?}", other),
        }

        let truncated = ActionInput::with_params(json!({
            "path": path.clone(),
            "max_bytes": 4,
            "truncate": true
        }));
        let result = action.run(truncated, test_ctx()).await;
        match result {
            ActionResult::Success { exports } => {
                assert_eq!(
                    exports.get("content").and_then(|v| v.as_str()),
                    Some("abcd")
                );
                assert_eq!(exports.get("bytes").and_then(|v| v.as_u64()), Some(4));
                assert_eq!(
                    exports.get("truncated").and_then(|v| v.as_bool()),
                    Some(true)
                );
            }
            other => panic!("expected success, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    });
}

#[test]
fn test_file_write_rejects_in_read_only_mode() {
    tokio_test::block_on(async {
        let spec = ActionSpec {
            name: "file_write".to_string(),
            kind: "file_write".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "read_only",
                "sandbox_writable_roots": ["."]
            }),
            interface: None,
        };
        let action = FileWriteAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "path": "target/readonly.txt",
            "content": "hello"
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("read_only"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_file_write_rejects_empty_content_by_default() {
    tokio_test::block_on(async {
        let path = unique_test_path("orchestral_file_write_empty");
        let spec = ActionSpec {
            name: "file_write".to_string(),
            kind: "file_write".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "workspace_write",
                "sandbox_writable_roots": ["."],
                "create_dirs": true
            }),
            interface: None,
        };
        let action = FileWriteAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "path": path,
            "content": ""
        }));

        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("allow_empty=true"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_file_write_allows_empty_content_with_opt_in() {
    tokio_test::block_on(async {
        let path = unique_test_path("orchestral_file_write_empty_optin");
        let spec = ActionSpec {
            name: "file_write".to_string(),
            kind: "file_write".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "workspace_write",
                "sandbox_writable_roots": ["."],
                "create_dirs": true
            }),
            interface: None,
        };
        let action = FileWriteAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "path": path,
            "content": "",
            "allow_empty": true
        }));

        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Success { exports } => {
                assert_eq!(exports.get("bytes").and_then(|v| v.as_u64()), Some(0));
                let written_path = exports
                    .get("path")
                    .and_then(|v| v.as_str())
                    .expect("path export");
                let content = tokio::fs::read_to_string(written_path)
                    .await
                    .expect("read back");
                assert_eq!(content, "");
                let _ = tokio::fs::remove_file(written_path).await;
            }
            other => panic!("expected success, got {:?}", other),
        }
    });
}

#[test]
fn test_file_read_rejects_path_outside_workspace_roots() {
    tokio_test::block_on(async {
        tokio::fs::create_dir_all("target")
            .await
            .expect("create target dir");
        let spec = ActionSpec {
            name: "file_read".to_string(),
            kind: "file_read".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "workspace_write",
                "sandbox_writable_roots": ["target"]
            }),
            interface: None,
        };
        let action = FileReadAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "path": "Cargo.toml"
        }));
        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("sandbox root") || message.contains("sandbox roots"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    });
}

#[test]
fn test_file_read_trims_whitespace_around_path() {
    tokio_test::block_on(async {
        tokio::fs::create_dir_all("target")
            .await
            .expect("create target dir");
        let path = unique_test_path("orchestral_file_read_trim");
        tokio::fs::write(&path, "trim-me").await.expect("seed file");

        let spec = ActionSpec {
            name: "file_read".to_string(),
            kind: "file_read".to_string(),
            description: None,
            category: None,
            config: json!({
                "sandbox_mode": "workspace_write",
                "sandbox_writable_roots": ["."]
            }),
            interface: None,
        };
        let action = FileReadAction::from_spec(&spec);
        let input = ActionInput::with_params(json!({
            "path": format!("  {}\n", path)
        }));

        let result = action.run(input, test_ctx()).await;
        match result {
            ActionResult::Success { exports } => {
                assert_eq!(
                    exports.get("content").and_then(|value| value.as_str()),
                    Some("trim-me")
                );
            }
            other => panic!("expected success, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    });
}

// --- HTTP action tests ---

struct CapturedRequest {
    method_line: String,
    headers: Vec<String>,
    body: String,
}

async fn one_shot_http_mock(
    status: u16,
    response_body: String,
) -> (String, tokio::task::JoinHandle<CapturedRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 8192];
        let mut collected: Vec<u8> = Vec::new();
        let mut headers_done = false;
        let mut body_start = 0usize;
        let mut content_length = 0usize;
        loop {
            let n = socket.read(&mut buf).await.expect("read");
            if n == 0 {
                break;
            }
            collected.extend_from_slice(&buf[..n]);
            if !headers_done {
                if let Some(pos) = collected.windows(4).position(|w| w == b"\r\n\r\n") {
                    headers_done = true;
                    body_start = pos + 4;
                    let header_str = String::from_utf8_lossy(&collected[..pos]);
                    for line in header_str.split("\r\n") {
                        let lowered = line.to_ascii_lowercase();
                        if let Some(rest) = lowered.strip_prefix("content-length:") {
                            content_length = rest.trim().parse().unwrap_or(0);
                        }
                    }
                }
            }
            if headers_done && collected.len() - body_start >= content_length {
                break;
            }
        }

        let header_str =
            String::from_utf8_lossy(&collected[..body_start.saturating_sub(4)]).to_string();
        let mut lines = header_str.split("\r\n");
        let method_line = lines.next().unwrap_or("").to_string();
        let headers: Vec<String> = lines.map(|s| s.to_string()).collect();
        let body_end = (body_start + content_length).min(collected.len());
        let body = String::from_utf8_lossy(&collected[body_start..body_end]).to_string();

        let response = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            response_body.len(),
            response_body,
        );
        socket.write_all(response.as_bytes()).await.expect("write");
        socket.flush().await.expect("flush");
        let _ = socket.shutdown().await;

        CapturedRequest {
            method_line,
            headers,
            body,
        }
    });
    (format!("http://{addr}"), handle)
}

fn http_spec(name: &str, config: Value) -> ActionSpec {
    ActionSpec {
        kind: "http".to_string(),
        name: name.to_string(),
        description: None,
        category: None,
        config,
        interface: None,
    }
}

#[test]
fn http_headers_from_value_filters_non_string_values() {
    let parsed = headers_from_value(&json!({
        "Authorization": "Bearer x",
        "X-Count": 5,
        "X-Flag": true,
        "X-Text": "yes"
    }));
    assert_eq!(parsed.get("Authorization"), Some(&"Bearer x".to_string()));
    assert_eq!(parsed.get("X-Text"), Some(&"yes".to_string()));
    assert!(!parsed.contains_key("X-Count"));
    assert!(!parsed.contains_key("X-Flag"));
}

#[test]
fn http_headers_from_value_empty_on_non_object() {
    assert!(headers_from_value(&json!([])).is_empty());
    assert!(headers_from_value(&json!("string")).is_empty());
    assert!(headers_from_value(&Value::Null).is_empty());
}

#[test]
fn http_merge_headers_overrides_win_over_defaults() {
    let mut defaults = HashMap::new();
    defaults.insert("X-Common".to_string(), "default".to_string());
    defaults.insert("X-Keep".to_string(), "still-here".to_string());
    let mut overrides = HashMap::new();
    overrides.insert("X-Common".to_string(), "override".to_string());

    let merged = merge_headers(&defaults, &overrides);
    assert_eq!(merged.get("x-common").unwrap(), "override");
    assert_eq!(merged.get("x-keep").unwrap(), "still-here");
}

#[test]
fn http_merge_headers_drops_invalid_header_names() {
    let mut defaults = HashMap::new();
    defaults.insert("Valid".to_string(), "ok".to_string());
    defaults.insert("Bad\nHeader".to_string(), "oops".to_string());
    let merged = merge_headers(&defaults, &HashMap::new());
    assert_eq!(merged.get("valid").unwrap(), "ok");
    assert!(merged.keys().all(|k| !k.as_str().contains('\n')));
}

#[tokio::test]
async fn http_action_rejects_missing_url() {
    let action = HttpAction::from_spec(&http_spec("probe", json!({})));
    let result = action
        .run(ActionInput::with_params(json!({})), test_ctx())
        .await;
    match result {
        ActionResult::Error { message } => {
            assert!(message.to_lowercase().contains("url"), "message: {message}");
        }
        other => panic!("expected Error for missing url, got {other:?}"),
    }
}

#[tokio::test]
async fn http_action_rejects_invalid_method() {
    let action = HttpAction::from_spec(&http_spec(
        "probe",
        json!({"default_url": "http://127.0.0.1:1"}),
    ));
    let result = action
        .run(
            ActionInput::with_params(json!({"method": "BOGUS METHOD"})),
            test_ctx(),
        )
        .await;
    match result {
        ActionResult::Error { message } => {
            assert!(
                message.to_lowercase().contains("method"),
                "message: {message}"
            );
        }
        other => panic!("expected Error for invalid method, got {other:?}"),
    }
}

#[tokio::test]
async fn http_action_get_exports_status_url_headers_body() {
    let (base, handle) = one_shot_http_mock(200, r#"{"hello":"world"}"#.to_string()).await;
    let action = HttpAction::from_spec(&http_spec("probe", json!({})));
    let result = action
        .run(
            ActionInput::with_params(json!({ "method": "GET", "url": format!("{base}/ping") })),
            test_ctx(),
        )
        .await;

    let captured = handle.await.expect("server task");
    assert!(
        captured.method_line.starts_with("GET /ping"),
        "method line: {}",
        captured.method_line
    );

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(exports.get("status").and_then(|v| v.as_u64()), Some(200));
            let body = exports
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            assert!(body.contains("hello"), "body: {body}");
            let url = exports
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            assert!(url.ends_with("/ping"), "url: {url}");
            assert!(exports.get("headers").and_then(|v| v.as_object()).is_some());
        }
        other => panic!("expected Success, got {other:?}"),
    }
}

#[tokio::test]
async fn http_action_post_json_serializes_body_and_sets_content_type() {
    let (base, handle) = one_shot_http_mock(200, "{}".to_string()).await;
    let action = HttpAction::from_spec(&http_spec("probe", json!({})));
    let _ = action
        .run(
            ActionInput::with_params(json!({
                "method": "POST",
                "url": format!("{base}/do"),
                "json": {"key": "value"}
            })),
            test_ctx(),
        )
        .await;

    let captured = handle.await.expect("server task");
    assert!(
        captured.method_line.starts_with("POST /do"),
        "method line: {}",
        captured.method_line
    );
    assert!(
        captured.body.contains(r#""key":"value""#),
        "body: {}",
        captured.body
    );
    let joined_headers = captured.headers.join("\n").to_ascii_lowercase();
    assert!(
        joined_headers.contains("content-type:") && joined_headers.contains("application/json"),
        "headers: {joined_headers}"
    );
}

#[tokio::test]
async fn http_action_merges_default_headers_with_request_overrides() {
    let (base, handle) = one_shot_http_mock(200, "{}".to_string()).await;
    let action = HttpAction::from_spec(&http_spec(
        "probe",
        json!({
            "headers": {"X-Default": "from-spec", "X-Both": "spec"}
        }),
    ));
    let _ = action
        .run(
            ActionInput::with_params(json!({
                "method": "GET",
                "url": format!("{base}/h"),
                "headers": {"X-Both": "from-call", "X-Extra": "call"}
            })),
            test_ctx(),
        )
        .await;

    let captured = handle.await.expect("server task");
    let joined = captured.headers.join("\n").to_ascii_lowercase();
    assert!(joined.contains("x-default: from-spec"), "{joined}");
    assert!(joined.contains("x-both: from-call"), "{joined}");
    assert!(joined.contains("x-extra: call"), "{joined}");
}

#[tokio::test]
async fn http_action_surfaces_non_2xx_as_success_with_status() {
    let (base, handle) = one_shot_http_mock(500, r#"{"err":"internal"}"#.to_string()).await;
    let action = HttpAction::from_spec(&http_spec("probe", json!({})));
    let result = action
        .run(
            ActionInput::with_params(json!({ "method": "GET", "url": format!("{base}/x") })),
            test_ctx(),
        )
        .await;
    let _ = handle.await.expect("server task");

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(exports.get("status").and_then(|v| v.as_u64()), Some(500));
            let body = exports
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            assert!(body.contains("internal"), "body: {body}");
        }
        other => panic!("HTTP action should surface non-2xx as Success, got {other:?}"),
    }
}

#[tokio::test]
async fn http_action_maps_connection_refused_to_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);

    let action = HttpAction::from_spec(&http_spec("probe", json!({"timeout_ms": 500})));
    let result = action
        .run(
            ActionInput::with_params(json!({
                "method": "GET",
                "url": format!("http://{addr}/dead")
            })),
            test_ctx(),
        )
        .await;

    match result {
        ActionResult::Error { message } => {
            assert!(!message.is_empty());
        }
        other => panic!("expected Error for unreachable host, got {other:?}"),
    }
}
