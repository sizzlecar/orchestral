//! Run-scoped process sessions behind the model-visible unified exec Tools.
//!
//! Pipe and PTY are execution details. Both are addressed by one integer
//! session ID and remain strictly scoped to the owning Agent Run.

mod lifecycle;

pub use lifecycle::{ExecSessionEvent, ExecSessionSnapshot, ExecSessionStatus};

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::ToolOperationPlan;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{broadcast, Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::pty_process::{PtyProcessId, PtyProcessManager, PtySpawnSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecSessionId(u64);

impl ExecSessionId {
    pub fn new(value: u64) -> Result<Self, ExecProcessError> {
        if value == 0 {
            return Err(ExecProcessError::Invalid(
                "exec session ID must be positive".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct ExecSpawnSpec {
    pub run_id: RunId,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub tty: bool,
    pub backend_starts_new_session: bool,
    /// Exact Host-derived authority under which subsequent input executes.
    pub operation: ToolOperationPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecPollResult {
    pub stdout: String,
    pub stderr: String,
    pub dropped_bytes: u64,
    pub alive: bool,
    pub exit_code: Option<i32>,
    pub wall_time_seconds: f64,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecProcessError {
    #[error("invalid exec operation: {0}")]
    Invalid(String),
    #[error("exec session was not found in this Run: {0}")]
    NotFound(u64),
    #[error("exec process operation was cancelled")]
    Cancelled,
    #[error("exec process manager state is unavailable")]
    Unavailable,
    #[error("exec process I/O failed: {0}")]
    Io(String),
}

type SessionKey = (RunId, ExecSessionId);

#[derive(Clone)]
enum ManagedProcess {
    Pipe(Arc<PipeSession>),
    Pty { process_id: PtyProcessId },
}

#[derive(Clone)]
struct ManagedSession {
    process: ManagedProcess,
    lifecycle: Arc<SessionLifecycle>,
    started: Instant,
    tty: bool,
    operation: ToolOperationPlan,
}

use lifecycle::SessionLifecycle;

#[derive(Default)]
struct OutputState {
    bytes: VecDeque<u8>,
    dropped_bytes: u64,
    generation: u64,
    closed: bool,
}

struct SharedOutput {
    state: Mutex<OutputState>,
    changed: Notify,
    max_bytes: usize,
}

impl SharedOutput {
    fn new(max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OutputState::default()),
            changed: Notify::new(),
            max_bytes,
        })
    }

    fn push(&self, bytes: &[u8]) {
        if let Ok(mut state) = self.state.lock() {
            for byte in bytes {
                if state.bytes.len() == self.max_bytes {
                    state.bytes.pop_front();
                    state.dropped_bytes = state.dropped_bytes.saturating_add(1);
                }
                state.bytes.push_back(*byte);
            }
            state.generation = state.generation.saturating_add(1);
        }
        self.changed.notify_waiters();
    }

    fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.generation = state.generation.saturating_add(1);
        }
        self.changed.notify_waiters();
    }

    fn snapshot(&self) -> Result<(u64, bool, bool), ExecProcessError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?;
        Ok((state.generation, state.closed, !state.bytes.is_empty()))
    }

    fn drain(&self) -> Result<(String, u64), ExecProcessError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?;
        let raw = state.bytes.drain(..).collect::<Vec<_>>();
        let dropped = std::mem::take(&mut state.dropped_bytes);
        Ok((String::from_utf8_lossy(&raw).into_owned(), dropped))
    }
}

struct PipeSession {
    child: AsyncMutex<Child>,
    stdin: AsyncMutex<Option<ChildStdin>>,
    stdout: Arc<SharedOutput>,
    stderr: Arc<SharedOutput>,
    process_group_id: Option<u32>,
}

impl PipeSession {
    fn spawn(spec: &ExecSpawnSpec, max_output_bytes: usize) -> Result<Arc<Self>, ExecProcessError> {
        if spec.run_id.is_empty() || spec.program.trim().is_empty() || !spec.cwd.is_absolute() {
            return Err(ExecProcessError::Invalid(
                "exec spawn requires run/program/absolute cwd".to_owned(),
            ));
        }
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .env_clear()
            .envs(&spec.environment)
            .current_dir(&spec.cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        isolate_process_group(&mut command, spec.backend_starts_new_session);
        let mut child = command
            .spawn()
            .map_err(|error| ExecProcessError::Io(error.to_string()))?;
        let process_group_id = child.id();
        let stdin = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ExecProcessError::Io("exec stdout pipe was not created".to_owned()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ExecProcessError::Io("exec stderr pipe was not created".to_owned()))?;
        let stdout_buffer = SharedOutput::new(max_output_bytes);
        let stderr_buffer = SharedOutput::new(max_output_bytes);
        spawn_reader(stdout, stdout_buffer.clone());
        spawn_reader(stderr, stderr_buffer.clone());
        Ok(Arc::new(Self {
            child: AsyncMutex::new(child),
            stdin: AsyncMutex::new(stdin),
            stdout: stdout_buffer,
            stderr: stderr_buffer,
            process_group_id,
        }))
    }

    async fn send(&self, input: &str) -> Result<(), ExecProcessError> {
        if input.is_empty() {
            return Ok(());
        }
        let mut stdin = self.stdin.lock().await;
        let stdin = stdin
            .as_mut()
            .ok_or_else(|| ExecProcessError::Io("exec stdin is closed".to_owned()))?;
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(|error| ExecProcessError::Io(error.to_string()))?;
        stdin
            .flush()
            .await
            .map_err(|error| ExecProcessError::Io(error.to_string()))
    }

    async fn poll(
        &self,
        lifecycle: &SessionLifecycle,
        started_at: Instant,
        wait: Duration,
        cancellation: &CancellationToken,
    ) -> Result<ExecPollResult, ExecProcessError> {
        let started = Instant::now();
        let settle = Duration::from_millis(50).min(wait);
        let mut observed = (u64::MAX, u64::MAX);
        let mut last_change = Instant::now();
        let exit_code = loop {
            if cancellation.is_cancelled() {
                return Err(ExecProcessError::Cancelled);
            }
            let status = lifecycle.status()?;
            let exit_code = match &status {
                ExecSessionStatus::Running => None,
                ExecSessionStatus::Exited { exit_code } => Some(*exit_code),
                ExecSessionStatus::Terminated => return Err(ExecProcessError::Cancelled),
                ExecSessionStatus::Failed { message } => {
                    return Err(ExecProcessError::Io(message.clone()))
                }
            };
            let stdout = self.stdout.snapshot()?;
            let stderr = self.stderr.snapshot()?;
            let generation = (stdout.0, stderr.0);
            if generation != observed {
                observed = generation;
                last_change = Instant::now();
            }
            if exit_code.is_some() && ((stdout.1 && stderr.1) || last_change.elapsed() >= settle) {
                break exit_code;
            }
            if started.elapsed() >= wait
                || ((stdout.2 || stderr.2) && last_change.elapsed() >= settle)
            {
                break exit_code;
            }
            let remaining = wait.saturating_sub(started.elapsed());
            let pause = remaining.min(Duration::from_millis(20));
            tokio::select! {
                _ = cancellation.cancelled() => return Err(ExecProcessError::Cancelled),
                _ = self.stdout.changed.notified() => {},
                _ = self.stderr.changed.notified() => {},
                _ = lifecycle.changed.notified() => {},
                _ = tokio::time::sleep(pause) => {},
            }
        };
        let (stdout, stdout_dropped) = self.stdout.drain()?;
        let (stderr, stderr_dropped) = self.stderr.drain()?;
        Ok(ExecPollResult {
            stdout,
            stderr,
            dropped_bytes: stdout_dropped.saturating_add(stderr_dropped),
            alive: exit_code.is_none(),
            exit_code,
            wall_time_seconds: started_at.elapsed().as_secs_f64(),
        })
    }

    async fn terminate(&self) {
        self.stdin.lock().await.take();
        let mut child = self.child.lock().await;
        terminate_process_group(self.process_group_id);
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

fn spawn_reader<R>(mut reader: R, output: Arc<SharedOutput>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) => break,
                Ok(count) => output.push(&chunk[..count]),
                Err(_) => break,
            }
        }
        output.close();
    });
}

const PROCESS_EVENT_BUFFER: usize = 256;
const PROCESS_WATCH_INTERVAL: Duration = Duration::from_millis(20);

/// Run-scoped owner and observer for pipe and PTY execution resources.
pub struct ProcessSupervisor {
    sessions: Mutex<BTreeMap<SessionKey, ManagedSession>>,
    next_session_id: AtomicU64,
    pty: Arc<PtyProcessManager>,
    max_output_bytes: usize,
    events: broadcast::Sender<ExecSessionEvent>,
}

impl ProcessSupervisor {
    pub fn new(max_output_bytes: usize) -> Result<Self, ExecProcessError> {
        if max_output_bytes == 0 {
            return Err(ExecProcessError::Invalid(
                "exec output limit must be positive".to_owned(),
            ));
        }
        let pty = PtyProcessManager::new(max_output_bytes, Duration::from_secs(10 * 60))
            .map_err(|error| ExecProcessError::Io(error.to_string()))?;
        let (events, _) = broadcast::channel(PROCESS_EVENT_BUFFER);
        Ok(Self {
            sessions: Mutex::new(BTreeMap::new()),
            next_session_id: AtomicU64::new(1),
            pty: Arc::new(pty),
            max_output_bytes,
            events,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ExecSessionEvent> {
        self.events.subscribe()
    }

    pub async fn spawn(&self, spec: ExecSpawnSpec) -> Result<ExecSessionId, ExecProcessError> {
        spec.operation
            .validate_shape()
            .map_err(|error| ExecProcessError::Invalid(error.message))?;
        let session_id = ExecSessionId::new(self.next_session_id.fetch_add(1, Ordering::Relaxed))?;
        let lifecycle = SessionLifecycle::running();
        let started = Instant::now();
        let process = if spec.tty {
            let process_id = PtyProcessId::new(format!("exec-{}", session_id.get()))
                .map_err(|error| ExecProcessError::Invalid(error.to_string()))?;
            let pty_spec = PtySpawnSpec {
                run_id: spec.run_id.clone(),
                process_id: process_id.clone(),
                program: spec.program.clone(),
                args: spec.args.clone(),
                cwd: spec.cwd.clone(),
                environment: spec.environment.clone(),
                rows: 24,
                cols: 120,
            };
            let pty = self.pty.clone();
            tokio::task::spawn_blocking(move || pty.create(pty_spec))
                .await
                .map_err(|error| ExecProcessError::Io(error.to_string()))?
                .map_err(|error| ExecProcessError::Io(error.to_string()))?;
            ManagedProcess::Pty { process_id }
        } else {
            ManagedProcess::Pipe(PipeSession::spawn(&spec, self.max_output_bytes)?)
        };
        let session = ManagedSession {
            process,
            lifecycle,
            started,
            tty: spec.tty,
            operation: spec.operation,
        };
        let key = (spec.run_id, session_id);
        self.sessions
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?
            .insert(key.clone(), session.clone());
        publish_session_event(&self.events, &key, &session);
        spawn_exit_watcher(key, session, self.pty.clone(), self.events.clone());
        Ok(session_id)
    }

    pub async fn write_and_poll(
        &self,
        run_id: &RunId,
        session_id: ExecSessionId,
        input: Option<&str>,
        wait: Duration,
        cancellation: &CancellationToken,
    ) -> Result<ExecPollResult, ExecProcessError> {
        if wait.is_zero() {
            return Err(ExecProcessError::Invalid(
                "exec poll duration must be positive".to_owned(),
            ));
        }
        let session = self.session(run_id, session_id)?;
        let requested_input = input.filter(|input| !input.is_empty());
        let input_to_deliver = if requested_input.is_some() {
            match session.lifecycle.status()? {
                ExecSessionStatus::Running => requested_input,
                // A process can exit after exec_command reports it as alive but
                // before the model's follow-up write arrives. Preserve the
                // terminal observation instead of turning that normal race into
                // a Tool failure.
                ExecSessionStatus::Exited { .. } => None,
                ExecSessionStatus::Terminated => return Err(ExecProcessError::Cancelled),
                ExecSessionStatus::Failed { message } => return Err(ExecProcessError::Io(message)),
            }
        } else {
            None
        };
        let result = match session.process.clone() {
            ManagedProcess::Pipe(process) => {
                if let Some(input) = input_to_deliver {
                    if let Err(error) = process.send(input).await {
                        if !matches!(
                            session.lifecycle.status()?,
                            ExecSessionStatus::Exited { .. }
                        ) {
                            return Err(error);
                        }
                    }
                }
                process
                    .poll(&session.lifecycle, session.started, wait, cancellation)
                    .await?
            }
            ManagedProcess::Pty { process_id } => {
                if let Some(input) = input_to_deliver {
                    let pty = self.pty.clone();
                    let run_id = run_id.clone();
                    let process_id = process_id.clone();
                    let input = input.to_owned();
                    tokio::task::spawn_blocking(move || pty.send(&run_id, &process_id, &input))
                        .await
                        .map_err(|error| ExecProcessError::Io(error.to_string()))?
                        .map_err(|error| ExecProcessError::Io(error.to_string()))?;
                }
                let pty = self.pty.clone();
                let run_id = run_id.clone();
                let process_id = process_id.clone();
                let cancellation = cancellation.clone();
                let read = tokio::task::spawn_blocking(move || {
                    pty.read(
                        &run_id,
                        &process_id,
                        wait,
                        Duration::from_millis(50).min(wait),
                        &cancellation,
                    )
                })
                .await
                .map_err(|error| ExecProcessError::Io(error.to_string()))?
                .map_err(|error| match error {
                    crate::pty_process::PtyProcessError::Cancelled => ExecProcessError::Cancelled,
                    error => ExecProcessError::Io(error.to_string()),
                })?;
                ExecPollResult {
                    stdout: read.output,
                    stderr: String::new(),
                    dropped_bytes: read.dropped_bytes,
                    alive: read.alive,
                    exit_code: read.exit_code,
                    wall_time_seconds: session.started.elapsed().as_secs_f64(),
                }
            }
        };
        if !result.alive {
            if let Some(exit_code) = result.exit_code {
                transition_session(
                    &self.events,
                    &(run_id.clone(), session_id),
                    &session,
                    ExecSessionStatus::Exited { exit_code },
                )?;
            }
            self.remove_finished(run_id, session_id, session).await;
        }
        Ok(result)
    }

    pub async fn close(
        &self,
        run_id: &RunId,
        session_id: ExecSessionId,
    ) -> Result<(), ExecProcessError> {
        let session = self
            .sessions
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?
            .remove(&(run_id.clone(), session_id))
            .ok_or(ExecProcessError::NotFound(session_id.get()))?;
        transition_session(
            &self.events,
            &(run_id.clone(), session_id),
            &session,
            ExecSessionStatus::Terminated,
        )?;
        self.terminate_session(run_id, session).await;
        Ok(())
    }

    pub async fn close_run(&self, run_id: &RunId) -> Result<usize, ExecProcessError> {
        let owned = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| ExecProcessError::Unavailable)?;
            let keys = sessions
                .keys()
                .filter(|(owner, _)| owner == run_id)
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| sessions.remove(&key).map(|session| (key, session)))
                .collect::<Vec<_>>()
        };
        let count = owned.len();
        for (key, session) in owned {
            transition_session(&self.events, &key, &session, ExecSessionStatus::Terminated)?;
            self.terminate_session(run_id, session).await;
        }
        Ok(count)
    }

    pub fn list(&self, run_id: &RunId) -> Result<Vec<ExecSessionId>, ExecProcessError> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?;
        let mut active = Vec::new();
        for ((owner, session_id), session) in sessions.iter() {
            if owner == run_id && session.lifecycle.status()? == ExecSessionStatus::Running {
                active.push(*session_id);
            }
        }
        Ok(active)
    }

    pub fn snapshot(
        &self,
        run_id: &RunId,
        session_id: ExecSessionId,
    ) -> Result<ExecSessionSnapshot, ExecProcessError> {
        let session = self.session(run_id, session_id)?;
        session_snapshot(&(run_id.clone(), session_id), &session)
    }

    pub fn operation_plan(
        &self,
        run_id: &RunId,
        session_id: ExecSessionId,
    ) -> Result<ToolOperationPlan, ExecProcessError> {
        Ok(self.session(run_id, session_id)?.operation)
    }

    fn session(
        &self,
        run_id: &RunId,
        session_id: ExecSessionId,
    ) -> Result<ManagedSession, ExecProcessError> {
        self.sessions
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?
            .get(&(run_id.clone(), session_id))
            .cloned()
            .ok_or(ExecProcessError::NotFound(session_id.get()))
    }

    async fn remove_finished(
        &self,
        run_id: &RunId,
        session_id: ExecSessionId,
        session: ManagedSession,
    ) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.remove(&(run_id.clone(), session_id));
        }
        if let ManagedProcess::Pty { process_id } = session.process {
            let pty = self.pty.clone();
            let run_id = run_id.clone();
            let _ = tokio::task::spawn_blocking(move || pty.close(&run_id, &process_id)).await;
        }
    }

    async fn terminate_session(&self, run_id: &RunId, session: ManagedSession) {
        match session {
            ManagedSession {
                process: ManagedProcess::Pipe(process),
                ..
            } => process.terminate().await,
            ManagedSession {
                process: ManagedProcess::Pty { process_id },
                ..
            } => {
                let pty = self.pty.clone();
                let run_id = run_id.clone();
                let _ = tokio::task::spawn_blocking(move || pty.close(&run_id, &process_id)).await;
            }
        }
    }
}

fn session_snapshot(
    (run_id, session_id): &SessionKey,
    session: &ManagedSession,
) -> Result<ExecSessionSnapshot, ExecProcessError> {
    Ok(ExecSessionSnapshot {
        run_id: run_id.clone(),
        session_id: *session_id,
        tty: session.tty,
        status: session.lifecycle.status()?,
        operation: session.operation.clone(),
        wall_time_seconds: session.started.elapsed().as_secs_f64(),
    })
}

fn publish_session_event(
    events: &broadcast::Sender<ExecSessionEvent>,
    key: &SessionKey,
    session: &ManagedSession,
) {
    if let Ok(snapshot) = session_snapshot(key, session) {
        let _ = events.send(ExecSessionEvent { snapshot });
    }
}

fn transition_session(
    events: &broadcast::Sender<ExecSessionEvent>,
    key: &SessionKey,
    session: &ManagedSession,
    status: ExecSessionStatus,
) -> Result<bool, ExecProcessError> {
    let changed = session.lifecycle.transition(status)?;
    if changed {
        publish_session_event(events, key, session);
    }
    Ok(changed)
}

fn spawn_exit_watcher(
    key: SessionKey,
    session: ManagedSession,
    pty: Arc<PtyProcessManager>,
    events: broadcast::Sender<ExecSessionEvent>,
) {
    tokio::spawn(async move {
        loop {
            if session
                .lifecycle
                .status()
                .is_ok_and(|status| status.is_terminal())
            {
                return;
            }
            let observed = match &session.process {
                ManagedProcess::Pipe(process) => process
                    .child
                    .lock()
                    .await
                    .try_wait()
                    .map(|status| status.map(|status| exit_status_code(&status)))
                    .map_err(|error| error.to_string()),
                ManagedProcess::Pty { process_id } => {
                    let pty = pty.clone();
                    let run_id = key.0.clone();
                    let process_id = process_id.clone();
                    match tokio::task::spawn_blocking(move || pty.status(&run_id, &process_id))
                        .await
                    {
                        Ok(result) => result.map_err(|error| error.to_string()),
                        Err(error) => Err(error.to_string()),
                    }
                }
            };
            match observed {
                Ok(Some(exit_code)) => {
                    let _ = transition_session(
                        &events,
                        &key,
                        &session,
                        ExecSessionStatus::Exited { exit_code },
                    );
                    if let ManagedProcess::Pipe(process) = &session.process {
                        process.stdin.lock().await.take();
                    }
                    return;
                }
                Ok(None) => {}
                Err(message) => {
                    let _ = transition_session(
                        &events,
                        &key,
                        &session,
                        ExecSessionStatus::Failed { message },
                    );
                    match &session.process {
                        ManagedProcess::Pipe(process) => process.terminate().await,
                        ManagedProcess::Pty { process_id } => {
                            let pty = pty.clone();
                            let run_id = key.0.clone();
                            let process_id = process_id.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                pty.close(&run_id, &process_id)
                            })
                            .await;
                        }
                    }
                    return;
                }
            }
            tokio::time::sleep(PROCESS_WATCH_INTERVAL).await;
        }
    });
}

fn exit_status_code(status: &ExitStatus) -> i32 {
    status.code().unwrap_or(-1)
}

#[cfg(unix)]
fn isolate_process_group(command: &mut Command, backend_starts_new_session: bool) {
    if !backend_starts_new_session {
        command.process_group(0);
    }
}

#[cfg(not(unix))]
fn isolate_process_group(_command: &mut Command, _backend_starts_new_session: bool) {}

#[cfg(unix)]
fn terminate_process_group(process_group_id: Option<u32>) {
    if let Some(process_group_id) = process_group_id.filter(|id| *id <= i32::MAX as u32) {
        // SAFETY: the child is the leader of a fresh process group/session.
        unsafe {
            libc::kill(-(process_group_id as i32), libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn terminate_process_group(_process_group_id: Option<u32>) {}
