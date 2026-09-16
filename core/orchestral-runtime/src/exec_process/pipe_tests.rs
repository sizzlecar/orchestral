use super::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;
use tokio::sync::oneshot;

/// Bytes already owned by a pipe reader whose scheduling is deliberately held
/// until after the process-exit observation. No wall-clock race is needed.
struct GatedReader {
    release: oneshot::Receiver<()>,
    bytes: &'static [u8],
    released: bool,
}

impl AsyncRead for GatedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.released {
            match Pin::new(&mut self.release).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => self.released = true,
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(std::io::Error::other("reader failed")))
                }
            }
        }
        let count = destination.remaining().min(self.bytes.len());
        destination.put_slice(&self.bytes[..count]);
        self.bytes = &self.bytes[count..];
        Poll::Ready(Ok(()))
    }
}

fn gated_output(
    bytes: &'static [u8],
    stop: &CancellationToken,
) -> (Arc<SharedOutput>, oneshot::Sender<()>) {
    let output = SharedOutput::new(1024);
    let (release, receiver) = oneshot::channel();
    spawn_reader(
        GatedReader {
            release: receiver,
            bytes,
            released: false,
        },
        output.clone(),
        stop.clone(),
    );
    (output, release)
}

fn exited_lifecycle() -> Arc<SessionLifecycle> {
    let lifecycle = SessionLifecycle::running();
    lifecycle
        .transition(ExecSessionStatus::Exited { exit_code: 7 })
        .unwrap();
    lifecycle
}

fn completion_options(duration: Duration) -> ExecWaitOptions {
    ExecWaitOptions {
        duration,
        mode: ExecWaitMode::Completion,
        yield_requested: CancellationToken::new(),
    }
}

#[tokio::test]
async fn pipe_exit_waits_for_both_readers_across_poll_deadlines() {
    let stop = CancellationToken::new();
    let (stdout, release_stdout) = gated_output(b"stdout tail", &stop);
    let (stderr, release_stderr) = gated_output(b"stderr tail", &stop);
    let lifecycle = exited_lifecycle();
    let started = Instant::now();
    let cancellation = CancellationToken::new();
    let short = completion_options(Duration::from_millis(1));

    let pending = poll_pipe_output(&stdout, &stderr, &lifecycle, started, &short, &cancellation)
        .await
        .unwrap();
    assert!(pending.alive, "exit cannot discard readers waiting to run");
    assert_eq!(pending.exit_code, None);
    assert!(pending.stdout.is_empty() && pending.stderr.is_empty());

    release_stdout.send(()).unwrap();
    stdout.wait_closed().await.unwrap();
    let partial = poll_pipe_output(&stdout, &stderr, &lifecycle, started, &short, &cancellation)
        .await
        .unwrap();
    assert!(partial.alive, "stderr EOF is independently required");
    assert_eq!(partial.stdout, "stdout tail");
    assert!(partial.stderr.is_empty());

    release_stderr.send(()).unwrap();
    let complete = poll_pipe_output(
        &stdout,
        &stderr,
        &lifecycle,
        started,
        &completion_options(Duration::from_secs(1)),
        &cancellation,
    )
    .await
    .unwrap();
    assert!(!complete.alive);
    assert_eq!(complete.exit_code, Some(7));
    assert!(
        complete.stdout.is_empty(),
        "drained output is not duplicated"
    );
    assert_eq!(complete.stderr, "stderr tail");
}

#[tokio::test]
async fn pipe_exit_yield_preserves_unfinished_readers_for_the_next_poll() {
    let stop = CancellationToken::new();
    let (stdout, release_stdout) = gated_output(b"out", &stop);
    let (stderr, release_stderr) = gated_output(b"err", &stop);
    let lifecycle = exited_lifecycle();
    let options = completion_options(Duration::from_secs(1));
    options.yield_requested.cancel();
    let pending = poll_pipe_output(
        &stdout,
        &stderr,
        &lifecycle,
        Instant::now(),
        &options,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(pending.alive);
    assert_eq!(pending.exit_code, None);

    release_stdout.send(()).unwrap();
    release_stderr.send(()).unwrap();
    let complete = poll_pipe_output(
        &stdout,
        &stderr,
        &lifecycle,
        Instant::now(),
        &completion_options(Duration::from_secs(1)),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        (complete.stdout.as_str(), complete.stderr.as_str()),
        ("out", "err")
    );
    assert!(!complete.alive);
    assert_eq!(complete.exit_code, Some(7));
}

#[tokio::test]
async fn inherited_pipe_keeps_polls_bounded_until_explicit_reader_cleanup() {
    let stop = CancellationToken::new();
    let stdout = SharedOutput::new(1024);
    let stderr = SharedOutput::new(1024);
    let (mut inherited_writer, reader) = tokio::io::duplex(1024);
    inherited_writer.write_all(b"retained").await.unwrap();
    spawn_reader(reader, stdout.clone(), stop.clone());
    spawn_reader(tokio::io::empty(), stderr.clone(), stop.clone());
    let pending = tokio::time::timeout(
        Duration::from_secs(1),
        poll_pipe_output(
            &stdout,
            &stderr,
            &exited_lifecycle(),
            Instant::now(),
            &completion_options(Duration::from_millis(10)),
            &CancellationToken::new(),
        ),
    )
    .await
    .expect("an inherited writer cannot extend a caller's observation deadline")
    .unwrap();
    assert!(pending.alive);
    assert_eq!(pending.exit_code, None);
    assert!(
        !stop.is_cancelled(),
        "observation must not truncate a live pipe"
    );
    assert_eq!(pending.stdout, "retained");
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::try_join!(stdout.wait_closed(), stderr.wait_closed()).unwrap();
    })
    .await
    .expect("explicit cleanup completes both readers");
    assert!(stdout.snapshot().unwrap().1 && stderr.snapshot().unwrap().1);
    assert!(inherited_writer.write_all(b"after closure").await.is_err());
    assert!(stdout.drain().unwrap().0.is_empty());
}

#[tokio::test]
async fn reader_error_still_completes_the_pipe_drain() {
    let stop = CancellationToken::new();
    let (stdout, release_stdout) = gated_output(b"unread", &stop);
    let (stderr, release_stderr) = gated_output(b"unread", &stop);
    drop(release_stdout);
    drop(release_stderr);
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::try_join!(stdout.wait_closed(), stderr.wait_closed()).unwrap();
    })
    .await
    .expect("I/O errors must close reader completion markers");
    assert!(stdout.snapshot().unwrap().1 && stderr.snapshot().unwrap().1);
    assert!(!stop.is_cancelled(), "reader errors need no drain timeout");
}

#[cfg(unix)]
#[tokio::test]
async fn supervisor_keeps_an_exited_session_addressable_until_reader_completion() {
    use orchestral_core::tool_protocol::{CapabilityRequest, EffectScope, ToolOperationRisk};
    use std::collections::BTreeSet;

    let manager = ProcessSupervisor::new(1024).unwrap();
    let run_id = RunId::new("reader-completion");
    let session_id = ExecSessionId::new(1).unwrap();
    let stop = CancellationToken::new();
    let (stdout, release_stdout) = gated_output(b"retained stdout", &stop);
    let (stderr, release_stderr) = gated_output(b"retained stderr", &stop);
    // Reap a real child before making its deliberately delayed reader fixtures
    // observable, so the manager-level exit-before-drain ordering is guaranteed.
    let mut child = Command::new("/bin/sh")
        .args(["-c", "exit 7"])
        .spawn()
        .unwrap();
    assert_eq!(child.wait().await.unwrap().code(), Some(7));
    manager.sessions.lock().unwrap().insert(
        (run_id.clone(), session_id),
        ManagedSession {
            process: ManagedProcess::Pipe(Arc::new(PipeSession {
                child: AsyncMutex::new(child),
                stdin: AsyncMutex::new(None),
                stdout,
                stderr,
                stop_readers: stop,
                process_group_id: None,
            })),
            lifecycle: exited_lifecycle(),
            started: Instant::now(),
            tty: false,
            operation: ToolOperationPlan {
                required_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                    EffectScope::Process,
                ])),
                risk: ToolOperationRisk::Routine,
                session_approval_scope: None,
                summary: "Observe delayed pipe readers".to_owned(),
            },
            _runtime_temp: None,
        },
    );
    let pending = manager
        .write_and_poll(
            &run_id,
            session_id,
            None,
            Duration::from_millis(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(pending.alive);
    assert_eq!(pending.exit_code, None);
    assert_eq!(
        manager.snapshot(&run_id, session_id).unwrap().status,
        ExecSessionStatus::Exited { exit_code: 7 }
    );
    release_stdout.send(()).unwrap();
    release_stderr.send(()).unwrap();
    let terminal = manager
        .write_and_poll(
            &run_id,
            session_id,
            Some("late input"),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(!terminal.alive);
    assert_eq!(terminal.exit_code, Some(7));
    assert_eq!(terminal.stdout, "retained stdout");
    assert_eq!(terminal.stderr, "retained stderr");
    assert!(matches!(
        manager.snapshot(&run_id, session_id),
        Err(ExecProcessError::NotFound(_))
    ));
}
