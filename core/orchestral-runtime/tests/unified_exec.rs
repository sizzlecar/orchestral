use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalIssuer,
    HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore,
    InteractiveCommandPolicy, NetworkPolicy, ProcessPolicy, RunToolGrant, SandboxPolicy,
    ToolCallId, ToolId, ToolInvocation, ToolOperationPlan, ToolOperationRisk, ToolOutcome,
    ToolOutput, ToolPolicyBounds, ToolRestriction, TransportLaunchPolicy,
};
use orchestral_runtime::tools::{
    guarded_exec_command_descriptor, guarded_write_stdin_descriptor,
    workspace_exec_command_descriptor, CommandEnvironmentSnapshot, GuardedExecCommandExecutor,
    GuardedWriteStdinExecutor, GUARDED_EXEC_SANDBOX_PROFILE,
};
use orchestral_runtime::{
    ExecProcessError, ExecSessionStatus, ExecSpawnSpec, GuardedToolResult, GuardedToolRuntime,
    ProcessSupervisor, WorkspacePermissionPolicy,
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

fn test_operation(summary: &str) -> ToolOperationPlan {
    ToolOperationPlan {
        effect_scopes: BTreeSet::from([EffectScope::Process]),
        targets: BTreeSet::from(["test-process".to_owned()]),
        risk: ToolOperationRisk::Routine,
        summary: summary.to_owned(),
    }
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
    let manager = ProcessSupervisor::new(16 * 1024).unwrap();
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
            operation: test_operation("run a short test process"),
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
            operation: test_operation("run an interactive pipe test process"),
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
async fn process_supervisor_observes_and_reaps_pipe_exit_without_model_polling() {
    let manager = ProcessSupervisor::new(16 * 1024).unwrap();
    let mut events = manager.subscribe();
    let run_id = RunId::new("supervised-pipe-run");
    let parent = std::env::temp_dir().join(format!(
        "orchestral-supervised-pipe-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&parent).unwrap();
    let cwd = std::fs::canonicalize(&parent).unwrap();
    let shell = std::fs::canonicalize("/bin/sh").unwrap();
    let operation = test_operation("supervise a finite pipe process");
    let session_id = manager
        .spawn(ExecSpawnSpec {
            run_id: run_id.clone(),
            program: shell.to_string_lossy().into_owned(),
            args: vec![
                "-c".to_owned(),
                "echo $$ > child.pid; sleep 0.05; printf supervised-output".to_owned(),
            ],
            cwd: cwd.clone(),
            environment: BTreeMap::new(),
            tty: false,
            backend_starts_new_session: false,
            operation: operation.clone(),
        })
        .await
        .unwrap();

    let started = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("started event is prompt")
        .expect("started event is observable");
    assert_eq!(started.snapshot.session_id, session_id);
    assert_eq!(started.snapshot.status, ExecSessionStatus::Running);
    assert_eq!(started.snapshot.operation, operation);

    let exited = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let event = events.recv().await.expect("exit event channel stays open");
            if event.snapshot.status.is_terminal() {
                break event;
            }
        }
    })
    .await
    .expect("exit is observed without write_stdin");
    assert_eq!(
        exited.snapshot.status,
        ExecSessionStatus::Exited { exit_code: 0 }
    );
    assert!(manager.list(&run_id).unwrap().is_empty());
    assert_eq!(
        manager.snapshot(&run_id, session_id).unwrap().status,
        ExecSessionStatus::Exited { exit_code: 0 }
    );

    let pid = std::fs::read_to_string(cwd.join("child.pid"))
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    // SAFETY: signal 0 is a read-only process existence check. The watcher
    // must have reaped the child before publishing Exited.
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);

    let final_result = manager
        .write_and_poll(
            &run_id,
            session_id,
            None,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("terminal output remains retrievable once");
    assert_eq!(final_result.stdout, "supervised-output");
    assert_eq!(final_result.exit_code, Some(0));
    assert!(matches!(
        manager.snapshot(&run_id, session_id),
        Err(ExecProcessError::NotFound(_))
    ));
    std::fs::remove_dir_all(parent).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn process_supervisor_publishes_one_terminal_transition_when_close_races_exit() {
    let manager = ProcessSupervisor::new(16 * 1024).unwrap();
    let mut events = manager.subscribe();
    let run_id = RunId::new("supervisor-close-race-run");
    let session_id = manager
        .spawn(ExecSpawnSpec {
            run_id: run_id.clone(),
            program: std::fs::canonicalize("/bin/sh")
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            args: vec!["-c".to_owned(), "sleep 30".to_owned()],
            cwd: std::fs::canonicalize(".").unwrap(),
            environment: BTreeMap::new(),
            tty: false,
            backend_starts_new_session: false,
            operation: test_operation("supervise a cancellable process"),
        })
        .await
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap().snapshot.status,
        ExecSessionStatus::Running
    );

    manager.close(&run_id, session_id).await.unwrap();
    let terminal = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("close publishes terminal state")
        .expect("event channel stays open");
    assert_eq!(terminal.snapshot.status, ExecSessionStatus::Terminated);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(manager.list(&run_id).unwrap().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn process_supervisor_observes_pty_exit_without_a_write_stdin_poll() {
    let manager = ProcessSupervisor::new(16 * 1024).unwrap();
    let mut events = manager.subscribe();
    let run_id = RunId::new("supervised-pty-run");
    let session_id = manager
        .spawn(ExecSpawnSpec {
            run_id: run_id.clone(),
            program: std::fs::canonicalize("/bin/sh")
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            args: vec!["-c".to_owned(), "printf supervised-pty; exit 7".to_owned()],
            cwd: std::fs::canonicalize(".").unwrap(),
            environment: BTreeMap::new(),
            tty: true,
            backend_starts_new_session: false,
            operation: test_operation("supervise a finite PTY process"),
        })
        .await
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap().snapshot.status,
        ExecSessionStatus::Running
    );

    let exited = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let event = events.recv().await.expect("PTY event channel stays open");
            if event.snapshot.status.is_terminal() {
                break event.snapshot.status;
            }
        }
    })
    .await
    .expect("PTY exit is observed without polling");
    assert_eq!(exited, ExecSessionStatus::Exited { exit_code: 7 });
    assert!(manager.list(&run_id).unwrap().is_empty());

    let final_result = manager
        .write_and_poll(
            &run_id,
            session_id,
            None,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("terminal PTY output remains retrievable");
    assert!(final_result.stdout.contains("supervised-pty"));
    assert_eq!(final_result.exit_code, Some(7));
}

#[cfg(unix)]
#[tokio::test]
async fn closing_a_run_reaps_pipe_and_pty_sessions() {
    let manager = ProcessSupervisor::new(16 * 1024).unwrap();
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
                operation: test_operation("run a cancellable test process"),
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
    let manager = Arc::new(ProcessSupervisor::new(16 * 1024).unwrap());
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
                    [],
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
    let stdin = invocation(
        "unified-run",
        "stdin",
        "orchestral/write_stdin/v1",
        json!({ "session_id": session_id, "chars": "hello\n", "yield_time_ms": 1000 }),
    );
    let GuardedToolResult::ApprovalRequired { binding, summary } = runtime
        .invoke(
            stdin.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
    else {
        panic!("non-empty process input must require exact approval")
    };
    assert!(summary.contains("hello\\n"));
    let approval = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let mut completed = inline_output(
        runtime
            .invoke(
                stdin,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                Some(approval),
                CancellationToken::new(),
            )
            .await,
    );
    if completed["alive"] == json!(true) {
        let poll = invocation(
            "unified-run",
            "poll",
            "orchestral/write_stdin/v1",
            json!({ "session_id": session_id, "yield_time_ms": 1000 }),
        );
        let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
            .invoke(
                poll.clone(),
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await
        else {
            panic!("safe SDK descriptor keeps polling behind exact approval")
        };
        let approval = HostApprovalIssuer::new(SIGNING_KEY)
            .unwrap()
            .issue(binding, i64::MAX)
            .unwrap();
        completed = inline_output(
            runtime
                .invoke(
                    poll,
                    RunToolGrant { bounds },
                    Some(approval),
                    CancellationToken::new(),
                )
                .await,
        );
    }
    assert_eq!(completed["exit_code"], json!(0), "{completed:#?}");
    assert!(completed["output"].as_str().unwrap().contains("got:hello"));
    assert!(manager.list(&RunId::new("unified-run")).unwrap().is_empty());

    std::fs::remove_dir_all(parent).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn workspace_auto_run_confines_reads_and_mutations_to_the_real_sandbox() {
    use std::os::unix::fs::PermissionsExt;

    let parent = std::env::temp_dir().join(format!(
        "orchestral-workspace-auto-exec-{}",
        uuid::Uuid::new_v4()
    ));
    let workspace = parent.join("workspace");
    let runtime_bin = parent.join("runtime-bin");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&runtime_bin).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let runtime_bin = std::fs::canonicalize(runtime_bin).unwrap();
    let escaped = workspace.join("escaped.txt");
    let fake_ls = runtime_bin.join("ls");
    std::fs::write(
        &fake_ls,
        format!("#!/bin/sh\nprintf escaped > '{}'\n", escaped.display()),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&fake_ls).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&fake_ls, permissions).unwrap();

    let shell = std::fs::canonicalize("/bin/sh").unwrap();
    let bounds = bounds(&workspace, &shell);
    let runtime =
        runtime(bounds.clone()).with_permission_policy(Arc::new(WorkspacePermissionPolicy));
    let manager = Arc::new(ProcessSupervisor::new(16 * 1024).unwrap());
    runtime
        .register(
            workspace_exec_command_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(
                GuardedExecCommandExecutor::new(
                    manager,
                    shell,
                    [runtime_bin.clone(), PathBuf::from("/bin")],
                    [],
                    CommandEnvironmentSnapshot::from_values([(
                        "PATH".to_owned(),
                        runtime_bin.to_string_lossy().into_owned(),
                    )]),
                )
                .unwrap(),
            ),
        )
        .unwrap();

    let result = runtime
        .invoke(
            invocation(
                "workspace-auto-run",
                "read-only",
                "orchestral/exec_command/v1",
                json!({ "cmd": "ls", "yield_time_ms": 1000 }),
            ),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            cached: false,
        }
    ));
    assert!(
        !escaped.exists(),
        "routine command escaped the read-only workspace sandbox"
    );

    let mutation = workspace.join("mutation.txt");
    let result = runtime
        .invoke(
            invocation(
                "workspace-auto-run",
                "mutation",
                "orchestral/exec_command/v1",
                json!({ "cmd": format!("printf mutation > {}", mutation.display()) }),
            ),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            cached: false,
        }
    ));
    assert_eq!(std::fs::read_to_string(&mutation).unwrap(), "mutation");
    std::fs::remove_dir_all(parent).unwrap();
}
