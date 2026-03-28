//! SessionManager: persistent PTY subprocess management.
//!
//! Manages long-lived interactive processes (codex, claude, etc.) across
//! multiple action executions within a thread.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tracing::{debug, info, warn};

/// A single persistent PTY session.
struct Session {
    name: String,
    writer: Box<dyn Write + Send>,
    #[allow(dead_code)]
    reader: Arc<Mutex<Box<dyn Read + Send>>>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    last_activity: Instant,
    output_buffer: Arc<Mutex<Vec<u8>>>,
    _reader_handle: Option<std::thread::JoinHandle<()>>,
}

impl Session {
    fn start(name: &str, command: &str, args: &[&str], cwd: Option<&str>) -> Result<Self, String> {
        let pty_system = native_pty_system();
        let pty_pair = pty_system
            .openpty(PtySize {
                rows: 24,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| format!("Failed to open PTY: {}", e))?;

        let mut cmd = CommandBuilder::new(command);
        cmd.args(args);
        if let Some(dir) = cwd {
            cmd.cwd(dir);
        }
        // Inherit environment
        for (key, value) in std::env::vars() {
            cmd.env(key, value);
        }

        let child = pty_pair
            .slave
            .spawn_command(cmd)
            .map_err(|e| format!("Failed to spawn '{}': {}", command, e))?;

        let writer = pty_pair
            .master
            .take_writer()
            .map_err(|e| format!("Failed to get PTY writer: {}", e))?;
        let reader = pty_pair
            .master
            .try_clone_reader()
            .map_err(|e| format!("Failed to get PTY reader: {}", e))?;

        // Background thread: continuously read PTY output into buffer
        let output_buffer = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = output_buffer.clone();
        let reader_arc = Arc::new(Mutex::new(reader));
        let reader_for_thread = reader_arc.clone();
        let session_name = name.to_string();

        let reader_handle = std::thread::spawn(move || {
            let mut chunk = [0u8; 4096];
            loop {
                let n = {
                    let mut r = match reader_for_thread.lock() {
                        Ok(r) => r,
                        Err(_) => break,
                    };
                    match r.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => {
                            debug!(session = %session_name, error = %e, "PTY read ended");
                            break;
                        }
                    }
                };
                if let Ok(mut buf) = buffer_clone.lock() {
                    buf.extend_from_slice(&chunk[..n]);
                }
            }
        });

        Ok(Session {
            name: name.to_string(),
            writer,
            reader: reader_arc,
            child,
            last_activity: Instant::now(),
            output_buffer,
            _reader_handle: Some(reader_handle),
        })
    }

    fn send(&mut self, input: &str) -> Result<(), String> {
        self.last_activity = Instant::now();
        self.writer
            .write_all(input.as_bytes())
            .map_err(|e| format!("Write to session '{}' failed: {}", self.name, e))?;
        if !input.ends_with('\n') {
            self.writer
                .write_all(b"\n")
                .map_err(|e| format!("Write newline failed: {}", e))?;
        }
        self.writer
            .flush()
            .map_err(|e| format!("Flush failed: {}", e))?;
        Ok(())
    }

    fn read(&mut self, timeout: Duration) -> String {
        self.last_activity = Instant::now();

        // Wait for output to settle: keep reading until no new data for `settle` duration
        let settle = Duration::from_millis(500);
        let start = Instant::now();
        let mut last_len = 0usize;
        let mut stable_since = Instant::now();

        loop {
            let current_len = self
                .output_buffer
                .lock()
                .map(|b| b.len())
                .unwrap_or(last_len);

            if current_len != last_len {
                last_len = current_len;
                stable_since = Instant::now();
            }

            if current_len > 0 && stable_since.elapsed() >= settle {
                break;
            }
            if start.elapsed() >= timeout {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        // Drain the buffer
        let raw = {
            let mut buf = match self.output_buffer.lock() {
                Ok(buf) => buf,
                Err(_) => return String::new(),
            };
            let data = buf.clone();
            buf.clear();
            data
        };

        // Strip ANSI escape sequences
        let stripped = strip_ansi_escapes::strip(&raw);
        String::from_utf8_lossy(&stripped)
            .replace('\r', "")
            .to_string()
    }

    fn is_alive(&mut self) -> bool {
        match self.child.try_wait() {
            Ok(Some(_)) => false,
            Ok(None) => true,
            Err(_) => false,
        }
    }

    fn close(&mut self) {
        info!(session = %self.name, "Closing session");
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

/// Manages multiple persistent PTY sessions.
pub struct SessionManager {
    sessions: Mutex<HashMap<String, Session>>,
    idle_timeout: Duration,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            idle_timeout: Duration::from_secs(600), // 10 min idle timeout
        }
    }

    pub fn create(
        &self,
        name: &str,
        command: &str,
        args: &[&str],
        cwd: Option<&str>,
    ) -> Result<String, String> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Session lock poisoned".to_string())?;

        // Close existing session with same name
        if let Some(mut old) = sessions.remove(name) {
            warn!(session = %name, "Replacing existing session");
            old.close();
        }

        let session = Session::start(name, command, args, cwd)?;
        info!(session = %name, command = %command, "Session created");
        sessions.insert(name.to_string(), session);
        Ok(name.to_string())
    }

    pub fn send(&self, name: &str, input: &str) -> Result<(), String> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Session lock poisoned".to_string())?;
        let session = sessions
            .get_mut(name)
            .ok_or_else(|| format!("Session '{}' not found", name))?;
        session.send(input)
    }

    pub fn read(&self, name: &str, timeout_secs: u64) -> Result<String, String> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Session lock poisoned".to_string())?;
        let session = sessions
            .get_mut(name)
            .ok_or_else(|| format!("Session '{}' not found", name))?;
        Ok(session.read(Duration::from_secs(timeout_secs)))
    }

    pub fn send_and_read(
        &self,
        name: &str,
        input: &str,
        timeout_secs: u64,
    ) -> Result<String, String> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Session lock poisoned".to_string())?;
        let session = sessions
            .get_mut(name)
            .ok_or_else(|| format!("Session '{}' not found", name))?;
        session.send(input)?;
        // Drop lock briefly to allow concurrent reads if needed
        drop(sessions);

        // Re-acquire to read
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Session lock poisoned".to_string())?;
        let session = sessions
            .get_mut(name)
            .ok_or_else(|| format!("Session '{}' not found after send", name))?;
        Ok(session.read(Duration::from_secs(timeout_secs)))
    }

    pub fn close(&self, name: &str) -> Result<(), String> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Session lock poisoned".to_string())?;
        if let Some(mut session) = sessions.remove(name) {
            session.close();
            Ok(())
        } else {
            Err(format!("Session '{}' not found", name))
        }
    }

    pub fn list(&self) -> Vec<String> {
        self.sessions
            .lock()
            .map(|s| s.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_alive(&self, name: &str) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|mut s| s.get_mut(name).map(|sess| sess.is_alive()))
            .unwrap_or(false)
    }

    /// Close sessions that have been idle longer than the timeout.
    pub fn gc(&self) {
        let mut sessions = match self.sessions.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        let expired: Vec<String> = sessions
            .iter()
            .filter(|(_, s)| s.last_activity.elapsed() > self.idle_timeout)
            .map(|(name, _)| name.clone())
            .collect();
        for name in expired {
            if let Some(mut session) = sessions.remove(&name) {
                info!(session = %name, "GC: closing idle session");
                session.close();
            }
        }
    }

    /// Close all sessions.
    pub fn close_all(&self) {
        if let Ok(mut sessions) = self.sessions.lock() {
            for (_, mut session) in sessions.drain() {
                session.close();
            }
        }
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        self.close_all();
    }
}
