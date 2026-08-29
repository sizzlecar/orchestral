use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalVerifier,
    HostToolPolicy, InMemoryApprovalCapabilityStore, NetworkPolicy, ProcessPolicy, RunToolGrant,
    SandboxPolicy, ToolCallId, ToolId, ToolInvocation, ToolOutcome, ToolOutput, ToolPolicyBounds,
    ToolRestriction,
};
use orchestral_runtime::tools::{
    guarded_file_read_descriptor, guarded_file_search_descriptor, guarded_text_search_descriptor,
    GuardedFileReadExecutor, GuardedFileSearchExecutor, GuardedTextSearchExecutor,
};
use orchestral_runtime::{GuardedToolResult, GuardedToolRuntime};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"guarded-inspection-test-signing-key";

fn temp_workspace(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("orchestral-{label}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).unwrap();
    fs::canonicalize(path).unwrap()
}

fn bounds(workspace: &Path, max_output_bytes: u64) -> ToolPolicyBounds {
    ToolPolicyBounds {
        allowed_effects: BTreeSet::from([EffectScope::FilesystemRead]),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: BTreeSet::from(["workspace_read".to_owned()]),
        },
        process: ProcessPolicy::default(),
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([workspace.to_string_lossy().into_owned()]),
            writable_roots: BTreeSet::new(),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(max_output_bytes),
    }
}

fn new_runtime(bounds: ToolPolicyBounds) -> GuardedToolRuntime<InMemoryApprovalCapabilityStore> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    GuardedToolRuntime::new(HostToolPolicy { bounds }, verifier).unwrap()
}

async fn invoke(
    runtime: &GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    bounds: ToolPolicyBounds,
    call: &str,
    tool_id: &str,
    arguments: Value,
) -> GuardedToolResult {
    runtime
        .invoke(
            ToolInvocation {
                run_id: RunId::new("inspection-run"),
                call_id: ToolCallId::new(call),
                tool_id: ToolId::new(tool_id),
                arguments,
            },
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await
}

fn completed(result: GuardedToolResult) -> Value {
    match result {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(output),
                },
            cached: false,
        } => output,
        other => panic!("expected completed inline Tool result, got {other:?}"),
    }
}

#[tokio::test]
async fn file_read_pages_lines_and_reports_long_line_truncation_without_looping() {
    let workspace = temp_workspace("read-lines");
    let long_line = "界".repeat(30_000);
    fs::write(
        workspace.join("source.rs"),
        format!("first\n{long_line}\nafter\n"),
    )
    .unwrap();
    let policy = bounds(&workspace, 8 * 1024);
    let runtime = new_runtime(policy.clone());
    let descriptor = guarded_file_read_descriptor(ToolRestriction {
        bounds: policy.clone(),
    });
    descriptor.validate().unwrap();
    for forbidden in ["approval", "sandbox", "readable_roots", "max_bytes"] {
        assert!(descriptor.model_schema.input_schema["properties"]
            .get(forbidden)
            .is_none());
    }
    runtime
        .register(
            descriptor,
            Arc::new(GuardedFileReadExecutor::new(&workspace).unwrap()),
        )
        .unwrap();

    let first = completed(
        invoke(
            &runtime,
            policy.clone(),
            "read-first",
            "orchestral/file_read/v3",
            json!({"path": "source.rs", "limit": 1}),
        )
        .await,
    );
    assert_eq!(first["content"], "first\n");
    assert_eq!(first["next_offset"], 2);
    assert_eq!(first["eof"], false);

    let long = completed(
        invoke(
            &runtime,
            policy.clone(),
            "read-long",
            "orchestral/file_read/v3",
            json!({"path": "source.rs", "offset": 2, "limit": 1}),
        )
        .await,
    );
    assert_eq!(long["end_line"], 2);
    assert_eq!(long["next_offset"], 3);
    assert!(long["content"].as_str().unwrap().len() < 8 * 1024);
    assert!(long["truncation_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("line_too_long")));
    assert!(long["truncation_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("byte_limit")));
    assert_eq!(long["truncated_line_numbers"], json!([2]));

    let after = completed(
        invoke(
            &runtime,
            policy.clone(),
            "read-after",
            "orchestral/file_read/v3",
            json!({"path": "source.rs", "offset": long["next_offset"], "limit": 10}),
        )
        .await,
    );
    assert_eq!(after["content"], "after\n");
    assert_eq!(after["eof"], true);
    assert_eq!(after["revision"], first["revision"]);

    let absolute = invoke(
        &runtime,
        policy.clone(),
        "read-absolute",
        "orchestral/file_read/v3",
        json!({"path": workspace.join("source.rs")}),
    )
    .await;
    assert!(matches!(
        absolute,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "workspace_path_absolute"
    ));

    let past_end = invoke(
        &runtime,
        policy,
        "read-past-end",
        "orchestral/file_read/v3",
        json!({"path": "source.rs", "offset": 99}),
    )
    .await;
    assert!(matches!(
        past_end,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "file_read_offset_out_of_range"
    ));
    fs::remove_dir_all(workspace).unwrap();
}

#[tokio::test]
async fn file_search_is_gitignore_aware_stable_and_explicitly_partial_at_limits() {
    let workspace = temp_workspace("file-search");
    fs::create_dir_all(workspace.join("src/nested")).unwrap();
    fs::create_dir_all(workspace.join(".hidden")).unwrap();
    fs::create_dir_all(workspace.join("ignored")).unwrap();
    fs::create_dir_all(workspace.join("target")).unwrap();
    fs::write(workspace.join(".gitignore"), "ignored/\nsrc/ignored.rs\n").unwrap();
    fs::write(workspace.join("src/lib.rs"), "pub fn root() {}\n").unwrap();
    fs::write(workspace.join("src/ignored.rs"), "IGNORED\n").unwrap();
    fs::write(workspace.join("src/nested/mod.rs"), "pub fn nested() {}\n").unwrap();
    fs::write(
        workspace.join(".hidden/config.rs"),
        "const HIDDEN: bool = true;\n",
    )
    .unwrap();
    fs::write(workspace.join("ignored/secret.rs"), "SECRET\n").unwrap();
    fs::write(workspace.join("target/generated.rs"), "GENERATED\n").unwrap();
    let policy = bounds(&workspace, 64 * 1024);
    let runtime = new_runtime(policy.clone());
    let descriptor = guarded_file_search_descriptor(ToolRestriction {
        bounds: policy.clone(),
    });
    descriptor.validate().unwrap();
    runtime
        .register(
            descriptor,
            Arc::new(GuardedFileSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();

    let all = completed(
        invoke(
            &runtime,
            policy.clone(),
            "find-all",
            "orchestral/file_search/v1",
            json!({"pattern": "**/*.rs"}),
        )
        .await,
    );
    assert_eq!(
        all["matches"],
        json!([".hidden/config.rs", "src/lib.rs", "src/nested/mod.rs"])
    );
    assert_eq!(all["completeness"], "complete");

    let limited = completed(
        invoke(
            &runtime,
            policy.clone(),
            "find-limited",
            "orchestral/file_search/v1",
            json!({"pattern": "**/*.rs", "limit": 1}),
        )
        .await,
    );
    assert_eq!(limited["matches"], json!([".hidden/config.rs"]));
    assert_eq!(limited["completeness"], "partial");
    assert!(limited["partial_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("result_limit")));
    assert!(!limited["refinement"].as_str().unwrap().is_empty());

    let scoped = completed(
        invoke(
            &runtime,
            policy.clone(),
            "find-scoped",
            "orchestral/file_search/v1",
            json!({"pattern": "**/*.rs", "path": "src"}),
        )
        .await,
    );
    assert_eq!(
        scoped["matches"],
        json!(["src/lib.rs", "src/nested/mod.rs"])
    );

    let escaped = invoke(
        &runtime,
        policy,
        "find-escape",
        "orchestral/file_search/v1",
        json!({"pattern": "*", "path": "../"}),
    )
    .await;
    assert!(matches!(
        escaped,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "workspace_path_escape"
    ));
    fs::remove_dir_all(workspace).unwrap();
}

#[tokio::test]
async fn search_ignore_resolution_never_reads_rules_above_the_workspace() {
    let parent = temp_workspace("search-ignore-boundary");
    fs::write(parent.join(".gitignore"), "*\n").unwrap();
    let workspace = parent.join("workspace");
    fs::create_dir_all(workspace.join("src")).unwrap();
    fs::write(workspace.join("src/visible.rs"), "VISIBLE\n").unwrap();
    let workspace = fs::canonicalize(&workspace).unwrap();
    let policy = bounds(&workspace, 64 * 1024);
    let runtime = new_runtime(policy.clone());
    runtime
        .register(
            guarded_file_search_descriptor(ToolRestriction {
                bounds: policy.clone(),
            }),
            Arc::new(GuardedFileSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();
    runtime
        .register(
            guarded_text_search_descriptor(ToolRestriction {
                bounds: policy.clone(),
            }),
            Arc::new(GuardedTextSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();

    let paths = completed(
        invoke(
            &runtime,
            policy.clone(),
            "ignore-boundary-paths",
            "orchestral/file_search/v1",
            json!({"pattern": "**/*.rs"}),
        )
        .await,
    );
    assert_eq!(paths["matches"], json!(["src/visible.rs"]));

    let text = completed(
        invoke(
            &runtime,
            policy,
            "ignore-boundary-text",
            "orchestral/text_search/v1",
            json!({"pattern": "VISIBLE", "literal": true}),
        )
        .await,
    );
    assert_eq!(text["count"], 1);
    assert_eq!(text["matches"][0]["path"], "src/visible.rs");
    fs::remove_dir_all(parent).unwrap();
}

#[tokio::test]
async fn text_search_supports_regex_literal_filters_context_and_binary_accounting() {
    let workspace = temp_workspace("text-search");
    fs::create_dir_all(workspace.join("src")).unwrap();
    fs::write(
        workspace.join("src/a.rs"),
        "before\n前缀 Needle   α\nafter\nneedle lower\n",
    )
    .unwrap();
    fs::write(workspace.join("src/b.txt"), "Needle   α\n").unwrap();
    fs::write(workspace.join("src/blob.bin"), b"Needle\0hidden").unwrap();
    let mut large_source = "let padding = 0;\n".repeat(180_000);
    large_source.push_str("const LARGE_NEEDLE: bool = true;\n");
    fs::write(workspace.join("src/large.rs"), large_source).unwrap();
    let policy = bounds(&workspace, 64 * 1024);
    let runtime = new_runtime(policy.clone());
    let descriptor = guarded_text_search_descriptor(ToolRestriction {
        bounds: policy.clone(),
    });
    descriptor.validate().unwrap();
    runtime
        .register(
            descriptor,
            Arc::new(GuardedTextSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();

    let regex = completed(
        invoke(
            &runtime,
            policy.clone(),
            "grep-regex",
            "orchestral/text_search/v1",
            json!({
                "pattern": "Needle\\s+α",
                "path": "src",
                "include": "*.rs",
                "context": 1
            }),
        )
        .await,
    );
    assert_eq!(regex["count"], 1);
    assert_eq!(regex["matches"][0]["path"], "src/a.rs");
    assert_eq!(regex["matches"][0]["line_number"], 2);
    assert_eq!(regex["matches"][0]["column"], 4);
    assert_eq!(regex["matches"][0]["match_start_byte"], 7);
    assert_eq!(regex["matches"][0]["context_before"], json!(["before"]));
    assert_eq!(regex["matches"][0]["context_after"], json!(["after"]));
    assert_eq!(regex["completeness"], "complete");

    let literal = completed(
        invoke(
            &runtime,
            policy.clone(),
            "grep-literal",
            "orchestral/text_search/v1",
            json!({
                "pattern": "NEEDLE LOWER",
                "literal": true,
                "case_sensitive": false,
                "path": "src/a.rs"
            }),
        )
        .await,
    );
    assert_eq!(literal["count"], 1);
    assert_eq!(literal["matches"][0]["line_number"], 4);

    let large = completed(
        invoke(
            &runtime,
            policy.clone(),
            "grep-large-file",
            "orchestral/text_search/v1",
            json!({
                "pattern": "LARGE_NEEDLE",
                "literal": true,
                "path": "src/large.rs"
            }),
        )
        .await,
    );
    assert_eq!(large["count"], 1);
    assert_eq!(large["completeness"], "complete");
    assert!(large["stats"]["scanned_bytes"].as_u64().unwrap() > 2 * 1024 * 1024);

    let tight_policy = bounds(&workspace, 6 * 1024);
    let tight_runtime = new_runtime(tight_policy.clone());
    tight_runtime
        .register(
            guarded_text_search_descriptor(ToolRestriction {
                bounds: tight_policy.clone(),
            }),
            Arc::new(GuardedTextSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();
    let output_limited = completed(
        invoke(
            &tight_runtime,
            tight_policy,
            "grep-output-limit",
            "orchestral/text_search/v1",
            json!({
                "pattern": "padding",
                "literal": true,
                "path": "src/large.rs",
                "limit": 50
            }),
        )
        .await,
    );
    assert_eq!(output_limited["completeness"], "partial");
    assert!(output_limited["partial_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("output_limit")));
    assert!(output_limited["count"].as_u64().unwrap() < 50);

    let invalid = invoke(
        &runtime,
        policy.clone(),
        "grep-invalid-regex",
        "orchestral/text_search/v1",
        json!({"pattern": "("}),
    )
    .await;
    assert!(matches!(
        invalid,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "text_search_pattern_invalid"
    ));

    let with_binary = completed(
        invoke(
            &runtime,
            policy,
            "grep-binary",
            "orchestral/text_search/v1",
            json!({"pattern": "Needle", "path": "src", "limit": 10}),
        )
        .await,
    );
    assert_eq!(with_binary["stats"]["skipped_binary_files"], 1);
    assert_eq!(with_binary["completeness"], "complete");
    fs::remove_dir_all(workspace).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn search_tools_never_follow_workspace_symlinks() {
    let parent = temp_workspace("search-symlink");
    let workspace = parent.join("workspace");
    let outside = parent.join("outside");
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("secret.rs"), "OUTSIDE_SENTINEL\n").unwrap();
    std::os::unix::fs::symlink(&outside, workspace.join("linked")).unwrap();
    let workspace = fs::canonicalize(&workspace).unwrap();
    let policy = bounds(&workspace, 64 * 1024);
    let runtime = new_runtime(policy.clone());
    runtime
        .register(
            guarded_file_search_descriptor(ToolRestriction {
                bounds: policy.clone(),
            }),
            Arc::new(GuardedFileSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();
    runtime
        .register(
            guarded_text_search_descriptor(ToolRestriction {
                bounds: policy.clone(),
            }),
            Arc::new(GuardedTextSearchExecutor::new(&workspace).unwrap()),
        )
        .unwrap();

    let paths = completed(
        invoke(
            &runtime,
            policy.clone(),
            "symlink-path",
            "orchestral/file_search/v1",
            json!({"pattern": "**/*.rs"}),
        )
        .await,
    );
    assert_eq!(paths["count"], 0);
    let text = completed(
        invoke(
            &runtime,
            policy,
            "symlink-text",
            "orchestral/text_search/v1",
            json!({"pattern": "OUTSIDE_SENTINEL"}),
        )
        .await,
    );
    assert_eq!(text["count"], 0);
    fs::remove_dir_all(parent).unwrap();
}
