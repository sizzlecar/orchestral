#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    CapabilityRequest, EffectScope, ToolOperationPlan, ToolOperationRisk,
};
use orchestral_runtime::{
    ExecProcessError, ExecSessionId, ExecSpawnSpec, ExecWaitMode, ExecWaitOptions,
    ProcessSupervisor,
};
use tokio_util::sync::CancellationToken;

async fn spawn(manager: &ProcessSupervisor, run: &RunId, tty: bool, script: &str) -> ExecSessionId {
    manager
        .spawn(ExecSpawnSpec {
            run_id: run.clone(),
            program: std::fs::canonicalize("/bin/sh")
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            args: vec!["-c".into(), script.into()],
            cwd: std::fs::canonicalize(".").unwrap(),
            environment: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            tty,
            backend_starts_new_session: false,
            operation: ToolOperationPlan {
                required_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                    EffectScope::Process,
                ])),
                risk: ToolOperationRisk::Routine,
                session_approval_scope: None,
                summary: "Exercise a bounded process wait".into(),
            },
        })
        .await
        .unwrap()
}

fn options(mode: ExecWaitMode, duration: Duration) -> ExecWaitOptions {
    ExecWaitOptions {
        mode,
        duration,
        yield_requested: CancellationToken::new(),
    }
}

#[tokio::test]
async fn completion_wait_aggregates_bursts_and_returns_at_exit_for_pipe_and_pty() {
    for tty in [false, true] {
        let manager = ProcessSupervisor::new(16 * 1024).unwrap();
        let run = RunId::new("burst-wait");
        let session = spawn(
            &manager,
            &run,
            tty,
            "for n in 0 1 2 3 4 5; do printf 'chunk-%s\\n' \"$n\"; sleep 0.12; done; exit 7",
        )
        .await;
        let started = Instant::now();
        let result = manager
            .write_and_poll_with_options(
                &run,
                session,
                None,
                options(ExecWaitMode::Completion, Duration::from_secs(5)),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(!result.alive, "tty={tty}: {result:?}");
        assert_eq!(result.exit_code, Some(7));
        for n in 0..6 {
            assert_eq!(result.stdout.matches(&format!("chunk-{n}")).count(), 1);
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "wait ignored process exit"
        );
        assert!(manager.list(&run).unwrap().is_empty());
    }
}

#[tokio::test]
async fn output_wait_returns_a_prompt_and_input_continues_the_same_process() {
    for tty in [false, true] {
        let manager = ProcessSupervisor::new(16 * 1024).unwrap();
        let run = RunId::new("interactive-wait");
        let session = spawn(
            &manager,
            &run,
            tty,
            "printf 'ready\\n'; read value; printf 'done:%s\\n' \"$value\"",
        )
        .await;
        let started = Instant::now();
        let prompt = manager
            .write_and_poll_with_options(
                &run,
                session,
                None,
                options(ExecWaitMode::Output, Duration::from_secs(5)),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(prompt.alive);
        assert!(prompt.stdout.contains("ready"));
        assert!(started.elapsed() < Duration::from_secs(1));
        let result = manager
            .write_and_poll_with_options(
                &run,
                session,
                Some("answer\n"),
                options(ExecWaitMode::Completion, Duration::from_secs(5)),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("done:answer"));
        assert!(!result.stdout.contains("ready"));
    }
}

#[tokio::test]
async fn observation_deadline_keeps_a_silent_process_alive_and_addressable() {
    for tty in [false, true] {
        let manager = ProcessSupervisor::new(16 * 1024).unwrap();
        let run = RunId::new("silent-wait");
        let session = spawn(
            &manager,
            &run,
            tty,
            "read value; printf 'completed:%s' \"$value\"",
        )
        .await;
        let started = Instant::now();
        let pending = manager
            .write_and_poll_with_options(
                &run,
                session,
                None,
                options(ExecWaitMode::Completion, Duration::from_millis(200)),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(pending.alive);
        assert_eq!(pending.exit_code, None);
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(manager.list(&run).unwrap(), [session]);
        let result = manager
            .write_and_poll_with_options(
                &run,
                session,
                Some("continue\n"),
                options(ExecWaitMode::Completion, Duration::from_secs(2)),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("completed:continue"));
    }
}

#[tokio::test]
async fn host_yield_returns_an_observation_without_cancelling_or_replaying_the_process() {
    for tty in [false, true] {
        for already_requested in [false, true] {
            let manager = ProcessSupervisor::new(16 * 1024).unwrap();
            let run = RunId::new("yield-wait");
            let session = spawn(
                &manager,
                &run,
                tty,
                "printf 'started\\n'; read value; printf 'done:%s\\n' \"$value\"",
            )
            .await;
            let wait = options(ExecWaitMode::Completion, Duration::from_secs(10));
            if already_requested {
                wait.yield_requested.cancel();
            } else {
                let signal = wait.yield_requested.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    signal.cancel();
                });
            }
            let started = Instant::now();
            let pending = manager
                .write_and_poll_with_options(&run, session, None, wait, &CancellationToken::new())
                .await
                .unwrap();
            assert!(pending.alive);
            assert!(started.elapsed() < Duration::from_secs(1));
            let result = manager
                .write_and_poll_with_options(
                    &run,
                    session,
                    Some("once\n"),
                    options(ExecWaitMode::Completion, Duration::from_secs(2)),
                    &CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(result.exit_code, Some(0));
            let combined = format!("{}{}", pending.stdout, result.stdout);
            assert_eq!(combined.matches("started").count(), 1);
            assert_eq!(combined.matches("done:once").count(), 1);
        }
    }
}

#[tokio::test]
async fn cancellation_remains_responsive_during_a_long_completion_wait() {
    for tty in [false, true] {
        let manager = ProcessSupervisor::new(16 * 1024).unwrap();
        let run = RunId::new("cancel-wait");
        let session = spawn(&manager, &run, tty, "printf waiting; sleep 10").await;
        let cancellation = CancellationToken::new();
        let signal = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            signal.cancel();
        });
        let started = Instant::now();
        let result = manager
            .write_and_poll_with_options(
                &run,
                session,
                None,
                options(ExecWaitMode::Completion, Duration::from_secs(10)),
                &cancellation,
            )
            .await;
        assert!(matches!(result, Err(ExecProcessError::Cancelled)));
        assert!(started.elapsed() < Duration::from_secs(1));
        manager.close_run(&run).await.unwrap();
        assert!(manager.list(&run).unwrap().is_empty());
    }
}
