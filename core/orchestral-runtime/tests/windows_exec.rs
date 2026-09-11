#![cfg(windows)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    CapabilityRequest, EffectScope, ToolOperationPlan, ToolOperationRisk,
};
use orchestral_runtime::{ExecSpawnSpec, ProcessSupervisor};
use tokio_util::sync::CancellationToken;
use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{
    OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
};

fn spec(root: &Path, command: &str, tty: bool) -> ExecSpawnSpec {
    let system = std::env::var("SystemRoot").unwrap();
    let shell = Path::new(&system).join("System32/WindowsPowerShell/v1.0/powershell.exe");
    ExecSpawnSpec {
        run_id: RunId::new("windows-process-test"),
        program: shell.to_string_lossy().into_owned(),
        args: vec![
            "-NoProfile".into(),
            "-Command".into(),
            format!("[Console]::OutputEncoding=[Text.Encoding]::UTF8; {command}"),
        ],
        cwd: root.to_path_buf(),
        environment: BTreeMap::from([
            ("SystemRoot".into(), system),
            ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
        ]),
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
    let id = manager.spawn(spawn).await.unwrap();
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
    let id = manager.spawn(spawn).await.unwrap();
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
    let pid: u32 = result.stdout.trim().parse().expect("child PID");
    // SAFETY: OpenProcess borrows an OS PID and returns an owned wait handle.
    let child = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(!child.is_null());
    manager.close_run(&run).await.unwrap();
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
    let id = manager.spawn(spawn).await.unwrap();
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
        output.push_str(&result.stdout);
        if !result.alive {
            break;
        }
    }
    assert!(output.contains("conpty-ready"), "{output}");
    manager.close_run(&run).await.unwrap();
    assert!(manager.list(&run).unwrap().is_empty());
}
