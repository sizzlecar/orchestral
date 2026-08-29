#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
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
    ToolOutput, ToolPolicyBounds, ToolRestriction, TransportLaunchPolicy,
};
use orchestral_mcp_streamable_http::{
    ResolvedCredentialHeader, StreamableHttpMcpTransportConfig, StreamableHttpMcpTransportFactory,
};
use orchestral_runtime::{
    GuardedMcpServerConfig, GuardedToolResult, GuardedToolRuntime, InMemoryBlobStore,
    McpServerHealth, McpToolsAdapterRegistry, StdioMcpSandboxPolicy, StdioMcpTransportFactory,
    ToolArtifactStore, MCP_STDIO_SANDBOX_PROFILE,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

#[derive(Debug)]
struct CapturedHttpRequest {
    headers: BTreeMap<String, String>,
    body: serde_json::Value,
}

async fn read_http_request(socket: &mut TcpStream) -> CapturedHttpRequest {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 2048];
    let header_end = loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        request.extend_from_slice(&buffer[..count]);
        if let Some(index) = request.windows(4).position(|value| value == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let headers = header_text
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let content_length = headers
        .get("content-length")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    while request.len() < header_end + content_length {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        request.extend_from_slice(&buffer[..count]);
    }
    CapturedHttpRequest {
        headers,
        body: serde_json::from_slice(&request[header_end..header_end + content_length]).unwrap(),
    }
}

fn json_http_response(message: serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(&message).unwrap();
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    response
}

fn sse_http_response(message: serde_json::Value) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: catalog\r\ndata: {}\r\n\r\n",
        message
    )
    .into_bytes()
}

async fn spawn_scripted_http_server(
    responses: Vec<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<Vec<CapturedHttpRequest>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let mut captured = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured.push(read_http_request(&mut socket).await);
            socket.write_all(&response).await.unwrap();
        }
        captured
    });
    (format!("http://{address}/mcp"), handle)
}

async fn spawn_cancellable_http_server() -> (
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::task::JoinHandle<bool>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (call_started, call_observed) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let responses = [
            json_http_response(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "supportedVersions": ["2026-07-28"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "http-cancel", "version": "1"}
                }
            })),
            json_http_response(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "resultType": "complete",
                    "ttlMs": 1000,
                    "cacheScope": "private",
                    "tools": [{
                        "name": "wait",
                        "inputSchema": {
                            "type": "object",
                            "additionalProperties": false
                        }
                    }]
                }
            })),
        ];
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut socket).await;
            socket.write_all(&response).await.unwrap();
        }
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: waiting\r\n\r\n",
            )
            .await
            .unwrap();
        call_started.send(()).unwrap();
        let mut byte = [0_u8; 1];
        match tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte)).await {
            Ok(Ok(0)) => true,
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                true
            }
            _ => false,
        }
    });
    (format!("http://{address}/mcp"), call_observed, handle)
}

fn unique_path(label: &str) -> PathBuf {
    std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!("orchestral-{label}-{}", uuid::Uuid::new_v4()))
}

fn canonical_shell() -> PathBuf {
    std::fs::canonicalize("/bin/bash").expect("/bin/bash should exist")
}

fn bounds(program: &Path, root: &Path, timeout_ms: u64) -> ToolPolicyBounds {
    let root = std::fs::canonicalize(root).unwrap();
    ToolPolicyBounds {
        allowed_effects: BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
        ]),
        approval: ApprovalPolicy::Required,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: BTreeSet::from([MCP_STDIO_SANDBOX_PROFILE.to_owned()]),
        },
        process: ProcessPolicy {
            interactive: Default::default(),
            transport: TransportLaunchPolicy {
                allowed_programs: BTreeSet::from([program.to_string_lossy().to_string()]),
            },
        },
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([root.to_string_lossy().into_owned()]),
            writable_roots: BTreeSet::from([root.to_string_lossy().into_owned()]),
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

fn config(
    program: PathBuf,
    script: String,
    tool_timeout: Duration,
    root: &Path,
) -> GuardedMcpServerConfig {
    GuardedMcpServerConfig {
        server_id: McpServerId::new("mock"),
        required: true,
        transport: Arc::new(
            StdioMcpTransportFactory::new(
                program,
                vec!["-c".to_owned(), script],
                Default::default(),
                StdioMcpSandboxPolicy::workspace(root),
            )
            .unwrap(),
        ),
        startup_timeout: Duration::from_secs(2),
        tool_timeout,
        enabled_tools: Default::default(),
        disabled_tools: BTreeSet::from(["hidden".to_owned()]),
    }
}

fn http_config(
    factory: StreamableHttpMcpTransportFactory,
    tool_timeout: Duration,
) -> GuardedMcpServerConfig {
    GuardedMcpServerConfig {
        server_id: McpServerId::new("http-mock"),
        required: true,
        transport: Arc::new(factory),
        startup_timeout: Duration::from_secs(2),
        tool_timeout,
        enabled_tools: Default::default(),
        disabled_tools: Default::default(),
    }
}

fn http_bounds(config: &GuardedMcpServerConfig, timeout_ms: u64) -> ToolPolicyBounds {
    ToolPolicyBounds {
        allowed_effects: config.effect_scopes(),
        approval: ApprovalPolicy::Required,
        sandbox: SandboxPolicy::default(),
        process: ProcessPolicy::default(),
        filesystem: FilesystemPolicy::default(),
        network: NetworkPolicy {
            allowed_targets: config.allowed_network_targets(),
        },
        environment: EnvironmentPolicy::default(),
        allowed_credentials: config.credential_references(),
        max_timeout_ms: Some(timeout_ms),
        max_output_bytes: Some(16 * 1024),
    }
}

fn invocation(call_id: &str, tool: &str) -> ToolInvocation {
    invocation_with_arguments(call_id, tool, json!({}))
}

fn invocation_with_arguments(
    call_id: &str,
    tool: &str,
    arguments: serde_json::Value,
) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new("mcp-run"),
        call_id: ToolCallId::new(call_id),
        tool_id: ToolId::new(format!("mcp/mock/{tool}/v1")),
        arguments,
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
async fn streamable_http_discovery_headers_and_call_share_the_guarded_runtime() {
    let responses = vec![
        json_http_response(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "supportedVersions": ["2026-07-28"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "http-mock", "version": "1"}
            }
        })),
        sse_http_response(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {
                "resultType": "complete",
                "ttlMs": 1000,
                "cacheScope": "private",
                "tools": [
                    {
                        "name": "route",
                        "description": "route a request",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "region": {
                                    "type": "string",
                                    "x-mcp-header": "Region"
                                },
                                "routing": {
                                    "type": "object",
                                    "properties": {
                                        "priority": {
                                            "type": "integer",
                                            "x-mcp-header": "Priority"
                                        }
                                    }
                                }
                            },
                            "required": ["region", "routing"],
                            "additionalProperties": false
                        }
                    },
                    {
                        "name": "invalid",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "values": {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                        "x-mcp-header": "Invalid"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        })),
        json_http_response(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "result": {
                "resultType": "complete",
                "content": [{"type": "text", "text": "http-result"}],
                "isError": false
            }
        })),
    ];
    let (endpoint, captured) = spawn_scripted_http_server(responses).await;
    let mut transport_config = StreamableHttpMcpTransportConfig::unauthenticated(endpoint);
    transport_config.credential_headers.insert(
        "Authorization".to_owned(),
        ResolvedCredentialHeader {
            reference: "env:MCP_HTTP_TOKEN".to_owned(),
            value: "Bearer test-token".to_owned(),
        },
    );
    let server = http_config(
        StreamableHttpMcpTransportFactory::new(transport_config).unwrap(),
        Duration::from_secs(2),
    );
    let policy = http_bounds(&server, 5_000);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let runtime = runtime(policy.clone(), journal);
    let registry = McpToolsAdapterRegistry::register(
        runtime.as_ref(),
        vec![server],
        ToolRestriction {
            bounds: policy.clone(),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(registry.tool_count(), 1);
    assert_eq!(
        runtime
            .model_tool_schemas()
            .unwrap()
            .into_iter()
            .map(|schema| schema.name)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["mcp__http-mock__route".to_owned()])
    );
    let result = invoke_with_approval(
        runtime.as_ref(),
        ToolInvocation {
            run_id: RunId::new("mcp-http-run"),
            call_id: ToolCallId::new("http-call"),
            tool_id: ToolId::new("mcp/http-mock/route/v1"),
            arguments: json!({"region": "世界", "routing": {"priority": 7}}),
        },
        RunToolGrant {
            bounds: policy.clone(),
        },
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            cached: false,
        } if output["server"] == json!("http-mock")
            && output["tool"] == json!("route")
            && output["result"]["content"][0]["text"] == json!("http-result")
    ));

    let captured = captured.await.unwrap();
    assert_eq!(captured.len(), 3);
    for request in &captured {
        assert_eq!(
            request
                .headers
                .get("mcp-protocol-version")
                .map(String::as_str),
            Some("2026-07-28")
        );
        assert_eq!(
            request.headers.get("authorization").map(String::as_str),
            Some("Bearer test-token")
        );
        assert_eq!(
            request.body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
            json!("2026-07-28")
        );
    }
    let call = &captured[2];
    assert_eq!(
        call.headers.get("mcp-method").map(String::as_str),
        Some("tools/call")
    );
    assert_eq!(
        call.headers.get("mcp-name").map(String::as_str),
        Some("route")
    );
    assert_eq!(
        call.headers.get("mcp-param-priority").map(String::as_str),
        Some("7")
    );
    assert!(call
        .headers
        .get("mcp-param-region")
        .is_some_and(|value| value.starts_with("=?base64?")));
    assert_eq!(policy.network.allowed_targets.len(), 1);
    assert_eq!(
        policy.allowed_credentials,
        BTreeSet::from(["env:MCP_HTTP_TOKEN".to_owned()])
    );
    registry.shutdown().await;
}

#[tokio::test]
async fn streamable_http_cancellation_closes_the_request_and_records_unknown_effect() {
    let (endpoint, call_observed, server_task) = spawn_cancellable_http_server().await;
    let server = http_config(
        StreamableHttpMcpTransportFactory::new(StreamableHttpMcpTransportConfig::unauthenticated(
            endpoint,
        ))
        .unwrap(),
        Duration::from_secs(30),
    );
    let policy = http_bounds(&server, 30_000);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let runtime = runtime(policy.clone(), journal.clone());
    let registry = McpToolsAdapterRegistry::register(
        runtime.as_ref(),
        vec![server],
        ToolRestriction {
            bounds: policy.clone(),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let manager = registry.manager(&McpServerId::new("http-mock")).unwrap();
    let cancellation = CancellationToken::new();
    let task_runtime = runtime.clone();
    let task_policy = policy.clone();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        invoke_with_approval(
            task_runtime.as_ref(),
            ToolInvocation {
                run_id: RunId::new("mcp-http-cancel-run"),
                call_id: ToolCallId::new("http-cancel-call"),
                tool_id: ToolId::new("mcp/http-mock/wait/v1"),
                arguments: json!({}),
            },
            RunToolGrant {
                bounds: task_policy,
            },
            task_cancellation,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), call_observed)
        .await
        .expect("HTTP MCP call should be dispatched")
        .unwrap();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("HTTP MCP cancellation should settle within one second")
        .unwrap();
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { .. },
            cached: false,
        }
    ));
    let key = ToolEffectKey::new(
        RunId::new("mcp-http-cancel-run"),
        ToolCallId::new("http-cancel-call"),
    );
    let records = journal.load_effect(&key).await.unwrap();
    let projection = replay_tool_effect(&key, &records).unwrap().unwrap();
    assert!(matches!(
        projection.phase,
        ToolEffectPhase::UnknownEffect { .. }
    ));
    assert!(tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("cancelled HTTP response stream should close within one second")
        .unwrap());
    assert_ne!(manager.health(), McpServerHealth::Ready);
    registry.shutdown().await;
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
      printf '%s\n' '{{"jsonrpc":"2.0","id":3,"result":{{"resultType":"complete","ttlMs":1000,"cacheScope":"private","tools":[{{"name":"beta","description":"beta","inputSchema":{{"type":"object","additionalProperties":false}}}}]}}}}'
      ;;
    *'"method":"tools/list"'*'"io.modelcontextprotocol/protocolVersion":"2026-07-28"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"resultType":"complete","ttlMs":1000,"cacheScope":"private","tools":[{{"name":"alpha","description":"alpha","inputSchema":{{"type":"object","additionalProperties":false}}}},{{"name":"hidden","inputSchema":{{"type":"object"}}}}],"nextCursor":"page-2"}}}}'
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
        vec![config(program, script, Duration::from_secs(3), root)],
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn one_thousand_stdio_mcp_reads_outside_its_host_roots_leak_zero_secrets() {
    const ATTEMPTS: usize = 1_000;

    let parent = unique_path("mcp-secret-gate");
    let workspace = parent.join("workspace");
    let outside = parent.join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let parent = std::fs::canonicalize(parent).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let outside = std::fs::canonicalize(outside).unwrap();
    let leak_marker = workspace.join("leaked.txt");
    let mut read_attempts = String::new();
    for index in 0..ATTEMPTS {
        let secret_path = outside.join(format!("secret-{index}.txt"));
        std::fs::write(
            &secret_path,
            format!("ORCHESTRAL_MCP_SENTINEL_SECRET_{index}"),
        )
        .unwrap();
        read_attempts.push_str(&format!(
            "if IFS= read -r secret < \"{}\"; then leaked=\"$leaked$secret\"; fi\n",
            secret_path.display()
        ));
    }
    let script = format!(
        r#"
leaked=
{read_attempts}
printf '%s' "$leaked" > "{leak_marker}"
while IFS= read -r line; do
  case "$line" in
    *'"method":"server/discover"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"supportedVersions":["2026-07-28"],"capabilities":{{"tools":{{}}}},"serverInfo":{{"name":"secret-gate","version":"1"}}}}}}'
      ;;
    *'"method":"tools/list"'*)
      printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"resultType":"complete","ttlMs":1000,"cacheScope":"private","tools":[]}}}}'
      ;;
  esac
done
"#,
        leak_marker = leak_marker.display()
    );
    let program = canonical_shell();
    let policy = bounds(&program, &workspace, 5_000);
    let runtime = runtime(
        policy.clone(),
        Arc::new(InMemoryToolEffectJournalStore::default()),
    );
    let registry = McpToolsAdapterRegistry::register(
        runtime.as_ref(),
        vec![config(program, script, Duration::from_secs(3), &workspace)],
        ToolRestriction { bounds: policy },
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read_to_string(&leak_marker).unwrap(), "");
    registry.shutdown().await;
    std::fs::remove_dir_all(parent).unwrap();
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
      printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"resultType":"complete","ttlMs":1000,"cacheScope":"private","tools":[{{"name":"large","description":"large","inputSchema":{{"type":"object","additionalProperties":false}}}}]}}}}'
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
        vec![config(program, script, Duration::from_secs(3), &root)],
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
      while :; do :; done
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
        vec![config(program, script, Duration::from_secs(30), root)],
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
