#![cfg(windows)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use std::time::{Duration, Instant};

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    CapabilityRequest, EffectScope, ToolOperationPlan, ToolOperationRisk,
};
use orchestral_runtime::{
    ExecPollResult, ExecSessionId, ExecSpawnSpec, ExecWaitMode, ExecWaitOptions, ProcessSupervisor,
};
use tokio_util::sync::CancellationToken;
use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{
    OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
};

const POWERSHELL_COMMAND_ENTERED: &str = "ORCHESTRAL-PS-COMMAND-ENTERED";

fn windows_environment(root: &Path) -> BTreeMap<String, String> {
    // Keep the original environment unchanged apart from temporary storage.
    // Do not inherit the runner's full environment or user profile variables.
    let mut environment = BTreeMap::from([
        ("SystemRoot".into(), std::env::var("SystemRoot").unwrap()),
        ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
    ]);
    // Guarded exec supplies Run-owned TEMP/TMP. These direct supervisor tests
    // instead retain temporary storage under their own isolated fixture owner.
    let temporary = root.join("process-temp");
    std::fs::create_dir_all(&temporary).unwrap();
    for name in ["TEMP", "TMP"] {
        environment.insert(name.to_owned(), temporary.to_string_lossy().into_owned());
    }
    environment
}

fn spec(root: &Path, command: &str, tty: bool) -> ExecSpawnSpec {
    let system = std::env::var("SystemRoot").unwrap();
    let shell = Path::new(&system).join("System32/WindowsPowerShell/v1.0/powershell.exe");
    ExecSpawnSpec {
        run_id: RunId::new("windows-process-test"),
        program: shell.to_string_lossy().into_owned(),
        args: vec![
            "-NoProfile".into(),
            "-Command".into(),
            // A fixed stderr marker separates command entry from later cmdlet
            // execution without leaking arguments, environment values or paths.
            // It leaves the descendant PID on stdout unchanged.
            format!(
                "[Console]::Error.WriteLine('{POWERSHELL_COMMAND_ENTERED}'); [Console]::Error.Flush(); [Console]::OutputEncoding=[Text.Encoding]::UTF8; {command}"
            ),
        ],
        cwd: root.to_path_buf(),
        environment: windows_environment(root),
        tty,
        backend_starts_new_session: false,
        operation: ToolOperationPlan {
            required_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                EffectScope::Process,
            ])),
            risk: ToolOperationRisk::Routine,
            session_approval_scope: None,
            summary: "verify supervised Windows process lifetime".into(),
        },
    }
}

// These are monotonic observation times, not inferred OS output timestamps.
// Existing completion-mode polls can observe bytes after their actual arrival.
struct Stages {
    case: &'static str,
    started: Instant,
    spawned_ms: Option<u128>,
    first_output_observed_ms: Option<u128>,
    command_entered_observed_ms: Option<u128>,
    exit_observed_ms: Option<u128>,
    close_completed_ms: Option<u128>,
}

impl Stages {
    fn new(case: &'static str) -> Self {
        Self {
            case,
            started: Instant::now(),
            spawned_ms: None,
            first_output_observed_ms: None,
            command_entered_observed_ms: None,
            exit_observed_ms: None,
            close_completed_ms: None,
        }
    }

    fn spawned(&mut self) {
        self.spawned_ms = Some(self.started.elapsed().as_millis());
    }

    fn observe(&mut self, result: &ExecPollResult) {
        let elapsed = self.started.elapsed().as_millis();
        if !result.stdout.is_empty() || !result.stderr.is_empty() {
            self.first_output_observed_ms.get_or_insert(elapsed);
        }
        if result.stdout.contains(POWERSHELL_COMMAND_ENTERED)
            || result.stderr.contains(POWERSHELL_COMMAND_ENTERED)
        {
            // Positive-only diagnostic: a marker split across polls may not be
            // observed here. Absence does not prove command entry never happened.
            self.command_entered_observed_ms.get_or_insert(elapsed);
        }
        if result.exit_code.is_some() {
            self.exit_observed_ms.get_or_insert(elapsed);
        }
    }

    fn closed(&mut self) {
        self.close_completed_ms = Some(self.started.elapsed().as_millis());
    }
}

impl Drop for Stages {
    fn drop(&mut self) {
        eprintln!(
            "windows exec stages: {}",
            serde_json::json!({
                "case":self.case,
                "spawn_returned_ms":self.spawned_ms,
                "first_output_observed_ms":self.first_output_observed_ms,
                "command_entered_observed_ms":self.command_entered_observed_ms,
                "exit_observed_ms":self.exit_observed_ms,
                "close_completed_ms":self.close_completed_ms,
                "elapsed_ms":self.started.elapsed().as_millis(),
            })
        );
    }
}

#[tokio::test]
async fn native_pipe_preserves_unicode_output_and_space_paths() {
    let workspace = tempfile::Builder::new()
        .prefix("windows exec 中文 ")
        .tempdir()
        .unwrap();
    let manager = ProcessSupervisor::new(16384).unwrap();
    let spawn = spec(
        workspace.path(),
        "Write-Output 'hello 中文'; (Get-Location).Path",
        false,
    );
    let run = spawn.run_id.clone();
    let mut stages = Stages::new("powershell_pipe_unicode");
    let id = manager.spawn(spawn).await.unwrap();
    stages.spawned();
    let result = manager
        .write_and_poll(
            &run,
            id,
            None,
            Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    stages.observe(&result);
    assert_eq!(result.exit_code, Some(0), "{result:?}");
    assert!(result.stdout.contains("hello 中文"), "{result:?}");
    assert!(result.stdout.contains("windows exec 中文"), "{result:?}");
}

#[tokio::test]
async fn cancelling_a_native_pipe_terminates_its_child_process() {
    let workspace = tempfile::tempdir().unwrap();
    let manager = ProcessSupervisor::new(16384).unwrap();
    let spawn = spec(workspace.path(), "$child=Start-Process powershell.exe -ArgumentList '-NoProfile','-Command','Start-Sleep -Seconds 60' -PassThru; Write-Output $child.Id; Start-Sleep -Seconds 60", false);
    let run = spawn.run_id.clone();
    let mut stages = Stages::new("powershell_descendant_cancel");
    let id = manager.spawn(spawn).await.unwrap();
    stages.spawned();
    let result = manager
        .write_and_poll(
            &run,
            id,
            None,
            Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    stages.observe(&result);
    let pid: u32 = result.stdout.trim().parse().expect("child PID");
    // SAFETY: OpenProcess borrows an OS PID and returns an owned wait handle.
    let child = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(!child.is_null());
    manager.close_run(&run).await.unwrap();
    stages.closed();
    // SAFETY: the handle remains live until CloseHandle below.
    let waited = unsafe { WaitForSingleObject(child, 5000) };
    unsafe {
        CloseHandle(child);
    }
    assert_ne!(waited, WAIT_TIMEOUT, "child outlived cancellation");
}

#[tokio::test]
async fn native_conpty_can_run_and_close_a_session() {
    let workspace = tempfile::tempdir().unwrap();
    let manager = ProcessSupervisor::new(16384).unwrap();
    let spawn = spec(
        workspace.path(),
        "Write-Output 'conpty-ready'; Start-Sleep -Seconds 60",
        true,
    );
    let run = spawn.run_id.clone();
    let mut stages = Stages::new("powershell_conpty");
    let id = manager.spawn(spawn).await.unwrap();
    stages.spawned();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut output = String::new();
    while !output.contains("conpty-ready") && tokio::time::Instant::now() < deadline {
        let result = manager
            .write_and_poll(
                &run,
                id,
                None,
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        stages.observe(&result);
        output.push_str(&result.stdout);
        if !result.alive {
            break;
        }
    }
    assert!(output.contains("conpty-ready"), "{output}");
    manager.close_run(&run).await.unwrap();
    stages.closed();
    assert!(manager.list(&run).unwrap().is_empty());
}

const CHILD_MODE: &str = "ORCHESTRAL_WINDOWS_EXEC_CHILD_MODE";
const CHILD_TEST: &str = "rust_process_child";
const CHILD_ARGS: [&str; 5] = [
    "--exact",
    CHILD_TEST,
    "--ignored",
    "--nocapture",
    "--test-threads=1",
];

fn rust_spec(root: &Path, mode: &str, tty: bool) -> ExecSpawnSpec {
    let mut spawn = spec(root, "", tty);
    spawn.program = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    spawn.args = CHILD_ARGS.iter().map(|arg| (*arg).into()).collect();
    spawn.environment.insert(CHILD_MODE.into(), mode.into());
    spawn
}

#[test]
#[ignore = "executed by parent tests through the real Windows ProcessSupervisor"]
fn rust_process_child() {
    match std::env::var(CHILD_MODE).unwrap().as_str() {
        "unicode" => {
            println!("\nRUST-PIPE-READY 中文");
            println!("cwd={}", std::env::current_dir().unwrap().display());
        }
        "conpty" => println!("\nRUST-CONPTY-READY:{};", std::process::id()),
        "descendant" => println!("\nRUST-DESCENDANT-READY:{};", std::process::id()),
        "parent" => {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(CHILD_ARGS)
                .env(CHILD_MODE, "descendant")
                .spawn()
                .unwrap();
            println!("\nRUST-PARENT-READY:{};", std::process::id());
            println!("RUST-CHILD-PID:{};", child.id());
            std::io::stdout().flush().unwrap();
            // The Job must terminate both processes before this natural exit.
            let status = child.wait().unwrap();
            assert!(status.success());
            return;
        }
        _ => panic!("unknown isolated child mode"),
    }
    std::io::stdout().flush().unwrap();
    if std::env::var(CHILD_MODE).unwrap() != "unicode" {
        std::thread::sleep(Duration::from_secs(60));
    }
}

async fn observe_output(
    manager: &ProcessSupervisor,
    run: &RunId,
    id: ExecSessionId,
    ready: impl Fn(&str) -> bool,
    stages: &mut Stages,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut output = String::new();
    while !ready(&output) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let result = manager
            .write_and_poll_with_options(
                run,
                id,
                None,
                ExecWaitOptions {
                    duration: remaining,
                    mode: ExecWaitMode::Output,
                    yield_requested: CancellationToken::new(),
                },
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        stages.observe(&result);
        output.push_str(&result.stdout);
        if !result.alive {
            break;
        }
    }
    output
}

fn pid_after(output: &str, prefix: &str) -> Option<u32> {
    // Ignore a partial PID token; ConPTY may surround the marker with cursor
    // control bytes, so neither a physical line start nor a newline is required.
    output.split_once(prefix)?.1.split_once(';')?.0.parse().ok()
}

fn process_handle(pid: Option<u32>) -> Option<OwnedHandle> {
    // SAFETY: OpenProcess takes an OS PID and returns an owned wait handle.
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid?) };
    if handle.is_null() {
        None
    } else {
        // SAFETY: the successful OpenProcess call transferred ownership to us.
        Some(unsafe { OwnedHandle::from_raw_handle(handle) })
    }
}

fn assert_terminated(handle: Option<OwnedHandle>) {
    let handle = handle.expect("native marker PID identifies a live process");
    // SAFETY: handle remains owned until after this wait, then Drop closes it.
    let waited = unsafe { WaitForSingleObject(handle.as_raw_handle(), 5000) };
    assert_eq!(
        waited, WAIT_OBJECT_0,
        "process outlived cancellation or wait failed"
    );
}

#[tokio::test]
async fn rust_pipe_preserves_unicode_output_and_space_paths() {
    let workspace = tempfile::Builder::new()
        .prefix("windows exec 中文 ")
        .tempdir()
        .unwrap();
    let manager = ProcessSupervisor::new(16384).unwrap();
    let spawn = rust_spec(workspace.path(), "unicode", false);
    let run = spawn.run_id.clone();
    let mut stages = Stages::new("rust_pipe_unicode");
    let id = manager.spawn(spawn).await.unwrap();
    stages.spawned();
    let result = manager
        .write_and_poll(
            &run,
            id,
            None,
            Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    stages.observe(&result);
    assert_eq!(result.exit_code, Some(0), "{result:?}");
    assert!(result.stdout.contains("RUST-PIPE-READY 中文"), "{result:?}");
    assert!(result.stdout.contains("windows exec 中文"), "{result:?}");
}

#[tokio::test]
async fn rust_conpty_can_run_and_close_a_session() {
    let workspace = tempfile::tempdir().unwrap();
    let manager = ProcessSupervisor::new(16384).unwrap();
    let spawn = rust_spec(workspace.path(), "conpty", true);
    let run = spawn.run_id.clone();
    let mut stages = Stages::new("rust_conpty");
    let id = manager.spawn(spawn).await.unwrap();
    stages.spawned();
    let output = observe_output(
        &manager,
        &run,
        id,
        |output| pid_after(output, "RUST-CONPTY-READY:").is_some(),
        &mut stages,
    )
    .await;
    let handle = process_handle(pid_after(&output, "RUST-CONPTY-READY:"));
    manager.close_run(&run).await.unwrap();
    stages.closed();
    assert_terminated(handle);
    assert!(manager.list(&run).unwrap().is_empty());
}

#[tokio::test]
async fn cancelling_a_rust_pipe_terminates_its_descendant() {
    let workspace = tempfile::tempdir().unwrap();
    let manager = ProcessSupervisor::new(16384).unwrap();
    let spawn = rust_spec(workspace.path(), "parent", false);
    let run = spawn.run_id.clone();
    let mut stages = Stages::new("rust_descendant_cancel");
    let id = manager.spawn(spawn).await.unwrap();
    stages.spawned();
    let output = observe_output(
        &manager,
        &run,
        id,
        |output| {
            [
                "RUST-PARENT-READY:",
                "RUST-CHILD-PID:",
                "RUST-DESCENDANT-READY:",
            ]
            .iter()
            .all(|prefix| pid_after(output, prefix).is_some())
        },
        &mut stages,
    )
    .await;
    let parent_pid = pid_after(&output, "RUST-PARENT-READY:");
    let child_pid = pid_after(&output, "RUST-CHILD-PID:");
    // Hold real process handles across close so PID reuse cannot satisfy the check.
    let handles = [parent_pid, child_pid].map(process_handle);
    manager.close_run(&run).await.unwrap();
    stages.closed();
    assert_eq!(
        child_pid,
        pid_after(&output, "RUST-DESCENDANT-READY:"),
        "{output}"
    );
    for handle in handles {
        assert_terminated(handle);
    }
    assert!(manager.list(&run).unwrap().is_empty());
}
