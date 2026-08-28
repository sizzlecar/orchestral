//! Run-scoped PTY process ownership for guarded Agent Tools.
//!
//! A PTY process is an execution resource, not an Agent Session. Handles are
//! scoped by `RunId`; callers cannot address another Run's process by name.

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use orchestral_core::agent_protocol::wire::RunId;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PtyProcessId(String);

impl PtyProcessId {
    pub fn new(value: impl Into<String>) -> Result<Self, PtyProcessError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(PtyProcessError::Invalid(
                "PTY process ID must not be empty".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PtyProcessId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct PtySpawnSpec {
    pub run_id: RunId,
    pub process_id: PtyProcessId,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub rows: u16,
    pub cols: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyReadResult {
    pub output: String,
    pub dropped_bytes: u64,
    pub alive: bool,
    pub exit_code: Option<i32>,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PtyProcessError {
    #[error("invalid PTY operation: {0}")]
    Invalid(String),
    #[error("PTY process already exists in this Run: {0}")]
    Conflict(PtyProcessId),
    #[error("PTY process was not found in this Run: {0}")]
    NotFound(PtyProcessId),
    #[error("PTY process operation was cancelled")]
    Cancelled,
    #[error("PTY process manager state is unavailable")]
    Unavailable,
    #[error("PTY process I/O failed: {0}")]
    Io(String),
}

type ProcessKey = (RunId, PtyProcessId);

struct PtyOutputBuffer {
    bytes: VecDeque<u8>,
    dropped_bytes: u64,
    generation: u64,
    closed: bool,
}

impl PtyOutputBuffer {
    fn new() -> Self {
        Self {
            bytes: VecDeque::new(),
            dropped_bytes: 0,
            generation: 0,
            closed: false,
        }
    }

    fn push_bounded(&mut self, bytes: &[u8], max_bytes: usize) {
        for byte in bytes {
            if self.bytes.len() == max_bytes {
                self.bytes.pop_front();
                self.dropped_bytes = self.dropped_bytes.saturating_add(1);
            }
            self.bytes.push_back(*byte);
        }
        self.generation = self.generation.saturating_add(1);
    }

    fn drain(&mut self) -> (Vec<u8>, u64) {
        let bytes = self.bytes.drain(..).collect();
        let dropped = std::mem::take(&mut self.dropped_bytes);
        (bytes, dropped)
    }
}

type SharedOutput = Arc<(Mutex<PtyOutputBuffer>, Condvar)>;

struct PtyProcess {
    writer: Option<Box<dyn Write + Send>>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    process_group_id: Option<u32>,
    output: SharedOutput,
    last_activity: Instant,
    reader_thread: Option<std::thread::JoinHandle<()>>,
}

impl PtyProcess {
    fn spawn(spec: &PtySpawnSpec, max_output_bytes: usize) -> Result<Self, PtyProcessError> {
        if spec.run_id.is_empty()
            || spec.program.trim().is_empty()
            || !spec.cwd.is_absolute()
            || spec.rows == 0
            || spec.cols == 0
        {
            return Err(PtyProcessError::Invalid(
                "PTY spawn requires run/program/absolute cwd and positive dimensions".to_owned(),
            ));
        }
        let pty_pair = native_pty_system()
            .openpty(PtySize {
                rows: spec.rows,
                cols: spec.cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|error| PtyProcessError::Io(error.to_string()))?;
        let mut command = CommandBuilder::new(&spec.program);
        command.args(&spec.args);
        command.cwd(&spec.cwd);
        command.env_clear();
        for (key, value) in &spec.environment {
            command.env(key, value);
        }
        let child = pty_pair
            .slave
            .spawn_command(command)
            .map_err(|error| PtyProcessError::Io(error.to_string()))?;
        let process_group_id = child.process_id();
        let writer = pty_pair
            .master
            .take_writer()
            .map_err(|error| PtyProcessError::Io(error.to_string()))?;
        let mut reader = pty_pair
            .master
            .try_clone_reader()
            .map_err(|error| PtyProcessError::Io(error.to_string()))?;
        let output = Arc::new((Mutex::new(PtyOutputBuffer::new()), Condvar::new()));
        let reader_output = output.clone();
        let reader_thread = std::thread::spawn(move || {
            let mut chunk = [0_u8; 4096];
            loop {
                match reader.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(count) => {
                        let (buffer, changed) = &*reader_output;
                        if let Ok(mut buffer) = buffer.lock() {
                            buffer.push_bounded(&chunk[..count], max_output_bytes);
                            changed.notify_all();
                        } else {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let (buffer, changed) = &*reader_output;
            if let Ok(mut buffer) = buffer.lock() {
                buffer.closed = true;
                buffer.generation = buffer.generation.saturating_add(1);
                changed.notify_all();
            }
        });
        Ok(Self {
            writer: Some(writer),
            child,
            process_group_id,
            output,
            last_activity: Instant::now(),
            reader_thread: Some(reader_thread),
        })
    }

    fn send(&mut self, input: &str) -> Result<(), PtyProcessError> {
        if input.is_empty() {
            return Err(PtyProcessError::Invalid(
                "PTY input must not be empty".to_owned(),
            ));
        }
        if self
            .child
            .try_wait()
            .map_err(|error| PtyProcessError::Io(error.to_string()))?
            .is_some()
        {
            return Err(PtyProcessError::Io(
                "PTY process has already exited".to_owned(),
            ));
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| PtyProcessError::Io("PTY writer is closed".to_owned()))?;
        writer
            .write_all(input.as_bytes())
            .and_then(|_| writer.flush())
            .map_err(|error| PtyProcessError::Io(error.to_string()))?;
        self.last_activity = Instant::now();
        Ok(())
    }

    fn status(&mut self) -> Result<Option<i32>, PtyProcessError> {
        self.child
            .try_wait()
            .map(|status| status.map(|status| status.exit_code() as i32))
            .map_err(|error| PtyProcessError::Io(error.to_string()))
    }

    fn terminate(&mut self) {
        self.writer.take();
        #[cfg(unix)]
        if let Some(process_group_id) = self
            .process_group_id
            .filter(|process_group_id| *process_group_id <= i32::MAX as u32)
        {
            // portable-pty establishes the child as a session leader, so its
            // PID is also the process-group ID. Kill the whole tree.
            unsafe {
                libc::kill(-(process_group_id as i32), libc::SIGKILL);
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader_thread) = self.reader_thread.take() {
            let _ = reader_thread.join();
        }
        let (buffer, changed) = &*self.output;
        if let Ok(mut buffer) = buffer.lock() {
            buffer.closed = true;
            changed.notify_all();
        }
    }
}

impl Drop for PtyProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}

pub struct PtyProcessManager {
    processes: Mutex<BTreeMap<ProcessKey, Arc<Mutex<PtyProcess>>>>,
    max_output_bytes: usize,
    idle_timeout: Duration,
}

impl PtyProcessManager {
    pub fn new(max_output_bytes: usize, idle_timeout: Duration) -> Result<Self, PtyProcessError> {
        if max_output_bytes == 0 || idle_timeout.is_zero() {
            return Err(PtyProcessError::Invalid(
                "PTY output and idle limits must be positive".to_owned(),
            ));
        }
        Ok(Self {
            processes: Mutex::new(BTreeMap::new()),
            max_output_bytes,
            idle_timeout,
        })
    }

    pub fn create(&self, spec: PtySpawnSpec) -> Result<PtyProcessId, PtyProcessError> {
        let key = (spec.run_id.clone(), spec.process_id.clone());
        let mut processes = self
            .processes
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?;
        if processes.contains_key(&key) {
            return Err(PtyProcessError::Conflict(spec.process_id));
        }
        let process = PtyProcess::spawn(&spec, self.max_output_bytes)?;
        processes.insert(key, Arc::new(Mutex::new(process)));
        Ok(spec.process_id)
    }

    pub fn send(
        &self,
        run_id: &RunId,
        process_id: &PtyProcessId,
        input: &str,
    ) -> Result<(), PtyProcessError> {
        let process = self.process(run_id, process_id)?;
        let result = process
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?
            .send(input);
        result
    }

    pub fn read(
        &self,
        run_id: &RunId,
        process_id: &PtyProcessId,
        timeout: Duration,
        settle: Duration,
        cancellation: &CancellationToken,
    ) -> Result<PtyReadResult, PtyProcessError> {
        if timeout.is_zero() || settle.is_zero() {
            return Err(PtyProcessError::Invalid(
                "PTY read timeout and settle duration must be positive".to_owned(),
            ));
        }
        let process = self.process(run_id, process_id)?;
        let output = process
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?
            .output
            .clone();
        let started = Instant::now();
        let mut last_change = Instant::now();
        let mut observed_generation = 0_u64;
        let (buffer, changed) = &*output;
        let mut buffer = buffer.lock().map_err(|_| PtyProcessError::Unavailable)?;
        loop {
            if cancellation.is_cancelled() {
                return Err(PtyProcessError::Cancelled);
            }
            if buffer.generation != observed_generation {
                observed_generation = buffer.generation;
                last_change = Instant::now();
            }
            if (!buffer.bytes.is_empty() && last_change.elapsed() >= settle)
                || buffer.closed
                || started.elapsed() >= timeout
            {
                break;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            let wait = remaining.min(Duration::from_millis(50));
            let (next, _) = changed
                .wait_timeout(buffer, wait)
                .map_err(|_| PtyProcessError::Unavailable)?;
            buffer = next;
        }
        let (raw, dropped_bytes) = buffer.drain();
        drop(buffer);
        let exit_code = process
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?
            .status()?;
        let output = strip_ansi_escapes::strip(&raw);
        Ok(PtyReadResult {
            output: String::from_utf8_lossy(&output).replace('\r', ""),
            dropped_bytes,
            alive: exit_code.is_none(),
            exit_code,
        })
    }

    pub fn close(&self, run_id: &RunId, process_id: &PtyProcessId) -> Result<(), PtyProcessError> {
        let process = self
            .processes
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?
            .remove(&(run_id.clone(), process_id.clone()))
            .ok_or_else(|| PtyProcessError::NotFound(process_id.clone()))?;
        process
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?
            .terminate();
        Ok(())
    }

    pub fn list(&self, run_id: &RunId) -> Result<Vec<PtyProcessId>, PtyProcessError> {
        let processes = self
            .processes
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?;
        Ok(processes
            .keys()
            .filter(|(owner, _)| owner == run_id)
            .map(|(_, process_id)| process_id.clone())
            .collect())
    }

    pub fn close_run(&self, run_id: &RunId) -> Result<usize, PtyProcessError> {
        let owned = {
            let mut processes = self
                .processes
                .lock()
                .map_err(|_| PtyProcessError::Unavailable)?;
            let keys = processes
                .keys()
                .filter(|(owner, _)| owner == run_id)
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| processes.remove(&key))
                .collect::<Vec<_>>()
        };
        let count = owned.len();
        for process in owned {
            if let Ok(mut process) = process.lock() {
                process.terminate();
            }
        }
        Ok(count)
    }

    pub fn gc(&self) -> Result<usize, PtyProcessError> {
        let expired = {
            let processes = self
                .processes
                .lock()
                .map_err(|_| PtyProcessError::Unavailable)?;
            processes
                .iter()
                .filter_map(|(key, process)| {
                    process
                        .lock()
                        .ok()
                        .filter(|process| process.last_activity.elapsed() > self.idle_timeout)
                        .map(|_| key.clone())
                })
                .collect::<Vec<_>>()
        };
        for (run_id, process_id) in &expired {
            let _ = self.close(run_id, process_id);
        }
        Ok(expired.len())
    }

    fn process(
        &self,
        run_id: &RunId,
        process_id: &PtyProcessId,
    ) -> Result<Arc<Mutex<PtyProcess>>, PtyProcessError> {
        self.processes
            .lock()
            .map_err(|_| PtyProcessError::Unavailable)?
            .get(&(run_id.clone(), process_id.clone()))
            .cloned()
            .ok_or_else(|| PtyProcessError::NotFound(process_id.clone()))
    }
}

impl Drop for PtyProcessManager {
    fn drop(&mut self) {
        if let Ok(mut processes) = self.processes.lock() {
            for (_, process) in processes.iter_mut() {
                if let Ok(mut process) = process.lock() {
                    process.terminate();
                }
            }
            processes.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_handles_are_run_scoped() {
        let manager = PtyProcessManager::new(1024, Duration::from_secs(60)).unwrap();
        let process_id = PtyProcessId::new("process-1").unwrap();
        assert!(matches!(
            manager.send(&RunId::new("another-run"), &process_id, "hello"),
            Err(PtyProcessError::NotFound(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn create_write_read_and_close_are_bounded_by_the_owning_run() {
        let manager = PtyProcessManager::new(4 * 1024, Duration::from_secs(60)).unwrap();
        let run_id = RunId::new("run-1");
        let process_id = PtyProcessId::new("process-1").unwrap();
        let program = std::fs::canonicalize("/bin/cat").unwrap();
        let cwd = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        manager
            .create(PtySpawnSpec {
                run_id: run_id.clone(),
                process_id: process_id.clone(),
                program: program.to_string_lossy().to_string(),
                args: Vec::new(),
                cwd,
                environment: BTreeMap::new(),
                rows: 24,
                cols: 80,
            })
            .unwrap();

        assert!(matches!(
            manager.send(&RunId::new("run-2"), &process_id, "escape\n"),
            Err(PtyProcessError::NotFound(_))
        ));
        manager.send(&run_id, &process_id, "hello\n").unwrap();
        let read = manager
            .read(
                &run_id,
                &process_id,
                Duration::from_secs(2),
                Duration::from_millis(50),
                &CancellationToken::new(),
            )
            .unwrap();
        assert!(read.output.contains("hello"));
        assert_eq!(read.dropped_bytes, 0);
        assert!(read.alive);
        assert_eq!(manager.list(&run_id).unwrap(), vec![process_id.clone()]);
        manager.close(&run_id, &process_id).unwrap();
        assert!(manager.list(&run_id).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn one_thousand_pty_read_cancellations_leave_no_read_handle_and_have_subsecond_p99() {
        const CANCELLATION_CASES: usize = 1_000;

        let manager = Arc::new(PtyProcessManager::new(4 * 1024, Duration::from_secs(60)).unwrap());
        let run_id = RunId::new("pty-read-cancel-run");
        let process_id = PtyProcessId::new("pty-read-cancel-process").unwrap();
        manager
            .create(PtySpawnSpec {
                run_id: run_id.clone(),
                process_id: process_id.clone(),
                program: std::fs::canonicalize("/bin/cat")
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                args: Vec::new(),
                cwd: std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap(),
                environment: BTreeMap::new(),
                rows: 24,
                cols: 80,
            })
            .unwrap();

        const BATCH_SIZE: usize = 50;
        let mut latencies = Vec::with_capacity(CANCELLATION_CASES);
        for _ in 0..(CANCELLATION_CASES / BATCH_SIZE) {
            let cancellation = CancellationToken::new();
            let barrier = Arc::new(std::sync::Barrier::new(BATCH_SIZE + 1));
            let mut readers = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                let manager = manager.clone();
                let run_id = run_id.clone();
                let process_id = process_id.clone();
                let cancellation = cancellation.clone();
                let barrier = barrier.clone();
                readers.push(std::thread::spawn(move || {
                    barrier.wait();
                    let result = manager.read(
                        &run_id,
                        &process_id,
                        Duration::from_secs(60),
                        Duration::from_secs(60),
                        &cancellation,
                    );
                    (result, std::time::Instant::now())
                }));
            }
            barrier.wait();
            let cancelled_at = std::time::Instant::now();
            cancellation.cancel();
            for reader in readers {
                let (result, finished_at) = reader.join().unwrap();
                assert!(matches!(result, Err(PtyProcessError::Cancelled)));
                latencies.push(finished_at.duration_since(cancelled_at));
            }
        }
        latencies.sort_unstable();
        let p99 = latencies[(CANCELLATION_CASES * 99 / 100).saturating_sub(1)];
        assert!(
            p99 <= Duration::from_secs(1),
            "PTY read cancel p99 was {p99:?}"
        );
        assert_eq!(manager.list(&run_id).unwrap(), vec![process_id.clone()]);
        manager.close(&run_id, &process_id).unwrap();
        assert!(manager.list(&run_id).unwrap().is_empty());
    }
}
