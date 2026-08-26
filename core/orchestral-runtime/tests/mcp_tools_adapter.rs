#![cfg(unix)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::mcp_protocol::{McpProtocolEra, McpServerId};
use orchestral_core::tool_effect::{
    replay_tool_effect, InMemoryToolEffectJournalStore, ToolEffectJournalStore, ToolEffectKey,
    ToolEffectPhase,
};
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalIssuer,
    HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore, NetworkPolicy,
    ProcessPolicy, RunToolGrant, SandboxPolicy, ToolCallId, ToolId, ToolInvocation, ToolOutcome,
    ToolOutput, ToolPolicyBounds, ToolRestriction,
};
use orchestral_runtime::{
    GuardedMcpServerConfig, GuardedToolResult, GuardedToolRuntime, InMemoryBlobStore,
    McpServerHealth, McpToolsAdapterRegistry, ToolArtifactStore,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn unique_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orchestral-{label}-{}", uuid::Uuid::new_v4()))
}

fn canonical_shell() -> PathBuf {
    std::fs::canonicalize("/bin/sh").expect("/bin/sh should exist")
}

fn bounds(program: &Path, root: &Path, timeout_ms: u64) -> ToolPolicyBounds {
    ToolPolicyBounds {
        allowed_effects: BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
        ]),
        approval: ApprovalPolicy::Required,
        sandbox: SandboxPolicy::default(),
        process: ProcessPolicy {
            allowed_programs: BTreeSet::from([program.to_string_lossy().to_string()]),
            allow_shell_expression: false,
        },
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([root.to_string_lossy().to_string()]),
            writable_roots: BTreeSet::from([root.to_string_lossy().to_string()]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(timeout_ms),
        max_output_bytes: Some(16 * 1024),
    }
}

fn runtime(
    bounds: ToolPolicyBounds,
    journal: Arc<InMemoryToolEffectJournalStore>,
) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    Arc::new(
        GuardedToolRuntime::new_with_effect_journal(HostToolPolicy { bounds }, verifier, journal)
            .unwrap(),
    )
}

fn runtime_with_artifacts(
    bounds: ToolPolicyBounds,
    journal: Arc<InMemoryToolEffectJournalStore>,
    artifacts: ToolArtifactStore,
) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    Arc::new(
        GuardedToolRuntime::new_with_effect_journal_and_artifacts(
            HostToolPolicy { bounds },
            verifier,
            journal,
            artifacts,
        )
        .unwrap(),
    )
}

fn config(program: PathBuf, script: String, tool_timeout: Duration) -> GuardedMcpServerConfig {
    GuardedMcpServerConfig {
        server_id: McpServerId::new("mock"),
        required: true,
        program,
        args: vec!["-c".to_owned(), script],
        environment: Default::default(),
        startup_timeout: Duration::from_secs(2),
        tool_timeout,
        enabled_tools: Default::default(),
        disabled_tools: BTreeSet::from(["hidden".to_owned()]),
    }
}

fn invocation(call_id: &str, tool: &str) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new("mcp-run"),
        call_id: ToolCallId::new(call_id),
        tool_id: ToolId::new(format!("mcp/mock/{tool}/v1")),
        arguments: json!({}),
    }
}

async fn invoke_with_approval(
    runtime: &GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    invocation: ToolInvocation,
    grant: RunToolGrant,
    cancellation: CancellationToken,
) -> GuardedToolResult {
    let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
        .invoke(
            invocation.clone(),
            grant.clone(),
            None,
            cancellation.clone(),
        )
        .await
    else {
        panic!("MCP Tool must enter the same Host approval pipeline")
    };
    let capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    runtime
        .invoke(invocation, grant, Some(capability), cancellation)
        .await
}

#[tokio::test]
async fn one_server_process_publishes_filtered_tools_and_all_calls_use_guarded_runtime() {
    let marker = unique_path("mcp-process-marker");
    let script = format!(
        r#"
printf S >> "{marker}"
while IFS= read -r line; do
  case "$line" in
    *'"method":"server/discover"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"supportedVersions":["2026-07-28"],"capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"mock","version":"1"}}}}}}'
      ;;
    *'"method":"tools/list"'*'"io.modelcontextprotocol/protocolVersion":"2026-07-28"'*'"cursor":"page-2"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":3,"result":{{"resultType":"complete","tools":[{{"name":"beta","description":"beta","inputSchema":{{"type":"object","additionalProperties":false}}}}]}}}}'
      ;;
    *'"method":"tools/list"'*'"io.modelcontextprotocol/protocolVersion":"2026-07-28"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"resultType":"complete","tools":[{{"name":"alpha","description":"alpha","inputSchema":{{"type":"object","additionalProperties":false}}}},{{"name":"hidden","inputSchema":{{"type":"object"}}}}],"nextCursor":"page-2"}}}}'
      ;;
    *'"method":"tools/call"'*'"name":"alpha"'*)
      printf C >> "{marker}"
      if [ "${{HOME+x}}" = x ]; then env_state=inherited; else env_state=cleared; fi
      printf '{{"jsonrpc":"2.0","id":4,"result":{{"resultType":"complete","content":[{{"type":"text","text":"alpha-result/%s"}}],"isError":false}}}}\n' "$env_state"
      ;;
    *'"method":"tools/call"'*'"name":"beta"'*)
      printf C >> "{marker}"
      printf '%s\n' '{{"jsonrpc":"2.0","id":5,"result":{{"resultType":"complete","content":[{{"type":"text","text":"beta-result"}}],"isError":false}}}}'
      ;;
  esac
done
"#,
        marker = marker.display()
    );
    let program = canonical_shell();
    let root = marker.parent().unwrap();
    let policy = bounds(&program, root, 5_000);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let runtime = runtime(policy.clone(), journal);
    let registry = McpToolsAdapterRegistry::register(
        runtime.as_ref(),
        vec![config(program, script, Duration::from_secs(3))],
        ToolRestriction {
            bounds: policy.clone(),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(registry.tool_count(), 2);
    assert_eq!(registry.server_names(), BTreeSet::from(["mock".to_owned()]));
    assert!(registry.skipped_optional_servers().is_empty());
    let schemas = runtime.model_tool_schemas().unwrap();
    assert_eq!(
        schemas
            .iter()
            .map(|schema| schema.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["mcp__mock__alpha", "mcp__mock__beta"])
    );
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "S");

    let grant = RunToolGrant {
        bounds: policy.clone(),
    };
    let alpha = invoke_with_approval(
        runtime.as_ref(),
        invocation("alpha-call", "alpha"),
        grant.clone(),
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        alpha,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            cached: false,
        } if output["server"] == json!("mock")
            && output["tool"] == json!("alpha")
            && output["result"]["content"][0]["text"] == json!("alpha-result/cleared")
    ));
    let beta = invoke_with_approval(
        runtime.as_ref(),
        invocation("beta-call", "beta"),
        grant,
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        beta,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            cached: false,
        } if output["result"]["content"][0]["text"] == json!("beta-result")
    ));
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "SCC");
    assert_eq!(
        registry
            .manager(&McpServerId::new("mock"))
            .unwrap()
            .connection_generation(),
        1
    );
    assert_eq!(
        registry
            .manager(&McpServerId::new("mock"))
            .unwrap()
            .snapshot()
            .mcp_protocol_version,
        "2026-07-28"
    );

    registry.shutdown().await;
    let _ = std::fs::remove_file(marker);
}

#[tokio::test]
async fn oversized_mcp_result_is_always_spilled_to_a_verified_artifact() {
    let large = "mcp-large-result/".repeat(256);
    let script = format!(
        r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"server/discover"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"supportedVersions":["2026-07-28"],"capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"mock","version":"1"}}}}}}'
      ;;
    *'"method":"tools/list"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"resultType":"complete","tools":[{{"name":"large","description":"large","inputSchema":{{"type":"object","additionalProperties":false}}}}]}}}}'
      ;;
    *'"method":"tools/call"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":3,"result":{{"resultType":"complete","content":[{{"type":"text","text":"{large}"}}],"isError":false}}}}'
      ;;
  esac
done
"#,
    );
    let program = canonical_shell();
    let root = std::env::temp_dir();
    let mut policy = bounds(&program, &root, 5_000);
    policy.max_output_bytes = Some(128);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let artifacts =
        ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 64 * 1024, 96).unwrap();
    let runtime = runtime_with_artifacts(policy.clone(), journal, artifacts.clone());
    let registry = McpToolsAdapterRegistry::register(
        runtime.as_ref(),
        vec![config(program, script, Duration::from_secs(3))],
        ToolRestriction {
            bounds: policy.clone(),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let result = invoke_with_approval(
        runtime.as_ref(),
        invocation("large-call", "large"),
        RunToolGrant { bounds: policy },
        CancellationToken::new(),
    )
    .await;
    let GuardedToolResult::Outcome {
        outcome:
            ToolOutcome::Completed {
                output: ToolOutput::Artifact(artifact),
            },
        cached: false,
    } = result
    else {
        panic!("oversized MCP result must be an Artifact")
    };
    assert!(artifact.summary.contains("Preview:"));
    let bytes = artifacts.resolve(&artifact).await.unwrap();
    let resolved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(resolved["result"]["content"][0]["text"], json!(large));
    registry.shutdown().await;
}

#[tokio::test]
async fn cancellation_after_dispatch_records_unknown_effect_and_reaps_the_server() {
    let marker = unique_path("mcp-cancel-marker");
    let pid_file = unique_path("mcp-cancel-pid");
    let script = format!(
        r#"
printf '%s' "$$" > "{pid_file}"
while IFS= read -r line; do
  case "$line" in
    *'"method":"server/discover"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":1,"error":{{"code":-32601,"message":"method not found"}}}}'
      ;;
    *'"method":"initialize"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"protocolVersion":"2025-06-18","capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"mock","version":"1"}}}}}}'
      ;;
    *'"method":"tools/list"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":3,"result":{{"tools":[{{"name":"alpha","inputSchema":{{"type":"object","additionalProperties":false}}}}]}}}}'
      ;;
    *'"method":"tools/call"'*)
      printf C > "{marker}"
      /bin/sleep 30
      ;;
  esac
done
"#,
        marker = marker.display(),
        pid_file = pid_file.display(),
    );
    let program = canonical_shell();
    let root = marker.parent().unwrap();
    let policy = bounds(&program, root, 30_000);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let runtime = runtime(policy.clone(), journal.clone());
    let registry = McpToolsAdapterRegistry::register(
        runtime.as_ref(),
        vec![config(program, script, Duration::from_secs(30))],
        ToolRestriction {
            bounds: policy.clone(),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let manager = registry.manager(&McpServerId::new("mock")).unwrap();
    assert_eq!(manager.snapshot().mcp_protocol_version, "2025-06-18");
    assert_eq!(
        manager.snapshot().mcp_protocol_era,
        McpProtocolEra::LegacyHandshake
    );
    let cancellation = CancellationToken::new();
    let task_runtime = runtime.clone();
    let task_cancellation = cancellation.clone();
    let task_policy = policy.clone();
    let task = tokio::spawn(async move {
        invoke_with_approval(
            task_runtime.as_ref(),
            invocation("cancel-call", "alpha"),
            RunToolGrant {
                bounds: task_policy,
            },
            task_cancellation,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mock MCP Tool should be dispatched");
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancelled MCP call should settle")
        .unwrap();
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { .. },
            cached: false,
        }
    ));

    let key = ToolEffectKey::new(RunId::new("mcp-run"), ToolCallId::new("cancel-call"));
    let records = journal.load_effect(&key).await.unwrap();
    let projection = replay_tool_effect(&key, &records).unwrap().unwrap();
    assert!(matches!(
        projection.phase,
        ToolEffectPhase::UnknownEffect { .. }
    ));

    let pid = std::fs::read_to_string(&pid_file)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            // SAFETY: signal 0 performs a read-only process existence check.
            if unsafe { libc::kill(pid, 0) } == -1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled MCP server process should be killed and reaped");
    assert_ne!(manager.health(), McpServerHealth::Ready);

    registry.shutdown().await;
    let _ = std::fs::remove_file(marker);
    let _ = std::fs::remove_file(pid_file);
}
