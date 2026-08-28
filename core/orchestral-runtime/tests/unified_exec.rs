use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalIssuer,
    HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore,
    InteractiveCommandPolicy, NetworkPolicy, ProcessPolicy, RunToolGrant, SandboxPolicy,
    ToolCallId, ToolId, ToolInvocation, ToolOutcome, ToolOutput, ToolPolicyBounds, ToolRestriction,
    TransportLaunchPolicy,
};
use orchestral_runtime::tools::{
    guarded_exec_command_descriptor, guarded_write_stdin_descriptor, CommandEnvironmentSnapshot,
    GuardedExecCommandExecutor, GuardedWriteStdinExecutor, GUARDED_EXEC_SANDBOX_PROFILE,
};
use orchestral_runtime::{
    ExecSessionManager, ExecSpawnSpec, GuardedToolResult, GuardedToolRuntime,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn effects() -> BTreeSet<EffectScope> {
    BTreeSet::from([
        EffectScope::Process,
        EffectScope::Network,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::EnvironmentRead,
        EffectScope::ExternalSideEffect,
    ])
}

fn bounds(workspace: &Path, shell: &Path) -> ToolPolicyBounds {
    let workspace = workspace.to_string_lossy().into_owned();
    let shell = shell.to_string_lossy().into_owned();
    ToolPolicyBounds {
        allowed_effects: effects(),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: BTreeSet::from([GUARDED_EXEC_SANDBOX_PROFILE.to_owned()]),
        },
        process: ProcessPolicy {
            interactive: InteractiveCommandPolicy {
                enabled: true,
                command_shells: BTreeSet::from([shell]),
                allow_child_processes: true,
            },
            transport: TransportLaunchPolicy::default(),
        },
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([workspace.clone()]),
            writable_roots: BTreeSet::from([workspace]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy {
            allowed_variables: BTreeSet::from(["PATH".to_owned(), "VISIBLE".to_owned()]),
            inherit_host_environment: false,
        },
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(2_000),
        max_output_bytes: Some(16 * 1024),
    }
}

fn runtime(bounds: ToolPolicyBounds) -> GuardedToolRuntime<InMemoryApprovalCapabilityStore> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    GuardedToolRuntime::new(HostToolPolicy { bounds }, verifier).unwrap()
}

fn invocation(run: &str, call: &str, tool: &str, arguments: Value) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new(run),
        call_id: ToolCallId::new(call),
        tool_id: ToolId::new(tool),
        arguments,
    }
}

fn inline_output(result: GuardedToolResult) -> Value {
    match result {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(output),
                },
            ..
        } => output,
        other => panic!("expected inline completed output, got {other:?}"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn pipe_sessions_return_short_results_and_keep_long_processes_addressable() {
    let manager = ExecSessionManager::new(16 * 1024).unwrap();
    let run_id = RunId::new("pipe-run");
    let cwd = std::fs::canonicalize(".").unwrap();
    let shell = std::fs::canonicalize("/bin/sh").unwrap();

    let short = manager
        .spawn(ExecSpawnSpec {
            run_id: run_id.clone(),
            program: shell.to_string_lossy().into_owned(),
            args: vec!["-c".to_owned(), "printf short-ok".to_owned()],
            cwd: cwd.clone(),
            environment: BTreeMap::new(),
            tty: false,
            backend_starts_new_session: false,
        })
        .await
        .unwrap();
    let result = manager
        .write_and_poll(
            &run_id,
            short,
            None,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.stdout, "short-ok");
    assert_eq!(result.exit_code, Some(0));
    assert!(!result.alive);
    assert!(manager.list(&run_id).unwrap().is_empty());

    let long = manager
        .spawn(ExecSpawnSpec {
            run_id: run_id.clone(),
            program: shell.to_string_lossy().into_owned(),
            args: vec![
                "-c".to_owned(),
                "read value; printf 'got:%s' \"$value\"".to_owned(),
            ],
            cwd,
            environment: BTreeMap::new(),
            tty: false,
            backend_starts_new_session: false,
        })
        .await
        .unwrap();
    let pending = manager
        .write_and_poll(
            &run_id,
            long,
            None,
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(pending.alive);
    assert!(matches!(
        manager
            .write_and_poll(
                &RunId::new("another-run"),
                long,
                None,
                Duration::from_millis(10),
                &CancellationToken::new(),
            )
            .await,
        Err(orchestral_runtime::ExecProcessError::NotFound(_))
    ));
    let completed = manager
        .write_and_poll(
            &run_id,
            long,
            Some("hello\n"),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(completed.stdout, "got:hello");
    assert_eq!(completed.exit_code, Some(0));
    assert!(manager.list(&run_id).unwrap().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn closing_a_run_reaps_pipe_and_pty_sessions() {
    let manager = ExecSessionManager::new(16 * 1024).unwrap();
    let run_id = RunId::new("cancel-run");
    let parent =
        std::env::temp_dir().join(format!("orchestral-exec-cancel-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&parent).unwrap();
    let cwd = std::fs::canonicalize(&parent).unwrap();
    let shell = std::fs::canonicalize("/bin/sh").unwrap();
    let mut sessions = Vec::new();
    for (index, tty) in [false, true].into_iter().enumerate() {
        let session = manager
            .spawn(ExecSpawnSpec {
                run_id: run_id.clone(),
                program: shell.to_string_lossy().into_owned(),
                args: vec![
                    "-c".to_owned(),
                    format!("echo $$ > process-{index}.pid; while :; do sleep 1; done"),
                ],
                cwd: cwd.clone(),
                environment: BTreeMap::new(),
                tty,
                backend_starts_new_session: false,
            })
            .await
            .unwrap();
        sessions.push(session);
    }
    for session in sessions {
        let result = manager
            .write_and_poll(
                &run_id,
                session,
                None,
                Duration::from_millis(200),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(result.alive);
    }
    assert_eq!(manager.list(&run_id).unwrap().len(), 2);
    let pids = [0, 1].map(|index| {
        std::fs::read_to_string(cwd.join(format!("process-{index}.pid")))
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap()
    });
    assert_eq!(manager.close_run(&run_id).await.unwrap(), 2);
    assert!(manager.list(&run_id).unwrap().is_empty());
    for pid in pids {
        // SAFETY: signal 0 is a read-only process existence check.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "process {pid} survived");
    }
    std::fs::remove_dir_all(parent).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn guarded_unified_surface_executes_children_and_continues_one_tty_session() {
    let parent =
        std::env::temp_dir().join(format!("orchestral-unified-exec-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&parent).unwrap();
    let workspace = std::fs::canonicalize(&parent).unwrap();
    let shell = std::fs::canonicalize("/bin/sh").unwrap();
    let bounds = bounds(&workspace, &shell);
    let runtime = runtime(bounds.clone());
    let manager = Arc::new(ExecSessionManager::new(16 * 1024).unwrap());
    let restriction = || ToolRestriction {
        bounds: bounds.clone(),
    };
    runtime
        .register(
            guarded_exec_command_descriptor(restriction()),
            Arc::new(
                GuardedExecCommandExecutor::new(
                    manager.clone(),
                    shell,
                    [PathBuf::from("/bin"), PathBuf::from("/usr/bin")],
                    CommandEnvironmentSnapshot::from_values([
                        ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
                        ("VISIBLE".to_owned(), "captured-at-composition".to_owned()),
                        ("MCP_CREDENTIAL".to_owned(), "must-not-leak".to_owned()),
                    ]),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    runtime
        .register(
            guarded_write_stdin_descriptor(restriction()),
            Arc::new(GuardedWriteStdinExecutor::new(manager.clone())),
        )
        .unwrap();
    let names = runtime
        .model_tool_schemas()
        .unwrap()
        .into_iter()
        .map(|schema| schema.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        BTreeSet::from(["exec_command".to_owned(), "write_stdin".to_owned()])
    );

    let short = invocation(
        "unified-run",
        "short",
        "orchestral/exec_command/v1",
        json!({ "cmd": "printf short-ok; /bin/echo child-ok", "yield_time_ms": 1000 }),
    );
    let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
        .invoke(
            short.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
    else {
        panic!("exec_command must require exact approval")
    };
    let approval = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let output = inline_output(
        runtime
            .invoke(
                short,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                Some(approval),
                CancellationToken::new(),
            )
            .await,
    );
    assert_eq!(output["exit_code"], json!(0));
    assert!(output["output"].as_str().unwrap().contains("short-ok"));
    assert!(output["output"].as_str().unwrap().contains("child-ok"));
    assert!(output.get("session_id").is_none());

    let environment_probe = invocation(
        "unified-run",
        "environment",
        "orchestral/exec_command/v1",
        json!({
            "cmd": "printf '%s|%s|%s' \"$VISIBLE\" \"${MCP_CREDENTIAL-unset}\" \"${HOME-unset}\"",
            "yield_time_ms": 1000
        }),
    );
    let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
        .invoke(
            environment_probe.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
    else {
        panic!("environment probe must require exact approval")
    };
    let approval = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let output = inline_output(
        runtime
            .invoke(
                environment_probe,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                Some(approval),
                CancellationToken::new(),
            )
            .await,
    );
    assert_eq!(
        output["stdout"],
        json!("captured-at-composition|unset|unset")
    );

    let interactive = invocation(
        "unified-run",
        "interactive",
        "orchestral/exec_command/v1",
        json!({
            "cmd": "read value; printf 'got:%s\\n' \"$value\"",
            "tty": true,
            "yield_time_ms": 100
        }),
    );
    let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
        .invoke(
            interactive.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
    else {
        panic!("interactive exec must require exact approval")
    };
    let approval = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let started = inline_output(
        runtime
            .invoke(
                interactive,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                Some(approval),
                CancellationToken::new(),
            )
            .await,
    );
    let session_id = started["session_id"].as_u64().unwrap();
    let completed = inline_output(
        runtime
            .invoke(
                invocation(
                    "unified-run",
                    "stdin",
                    "orchestral/write_stdin/v1",
                    json!({ "session_id": session_id, "chars": "hello\n", "yield_time_ms": 1000 }),
                ),
                RunToolGrant { bounds },
                None,
                CancellationToken::new(),
            )
            .await,
    );
    assert_eq!(completed["exit_code"], json!(0));
    assert!(completed["output"].as_str().unwrap().contains("got:hello"));
    assert!(manager.list(&RunId::new("unified-run")).unwrap().is_empty());

    std::fs::remove_dir_all(parent).unwrap();
}
