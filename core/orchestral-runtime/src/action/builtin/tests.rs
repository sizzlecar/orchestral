use super::shell::ShellAction;
use super::support::{
    approval_decision_from_ctx, extract_inline_script_body, requires_destructive_approval,
    ApprovalDecision,
};
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionResult};
use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError, WorkingSet};
use serde_json::{json, Value};
use tokio::sync::RwLock;

struct NoopReferenceStore;
static TEST_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

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
        "task-1",
        "s1",
        "exec-1",
        Arc::new(RwLock::new(WorkingSet::new())),
        Arc::new(NoopReferenceStore),
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
