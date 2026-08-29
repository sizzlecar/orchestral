use std::sync::{Arc, Mutex};

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::ToolOperationPlan;
use tokio::sync::Notify;

use super::{ExecProcessError, ExecSessionId};

/// Monotonic lifecycle of one supervised execution resource.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecSessionStatus {
    Running,
    Exited { exit_code: i32 },
    Terminated,
    Failed { message: String },
}

impl ExecSessionStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Authoritative point-in-time view used by UI and lifecycle reducers.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecSessionSnapshot {
    pub run_id: RunId,
    pub session_id: ExecSessionId,
    pub tty: bool,
    pub status: ExecSessionStatus,
    pub operation: ToolOperationPlan,
    pub wall_time_seconds: f64,
}

/// Lossy process activity notification. `snapshot` remains authoritative when
/// a receiver lags.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecSessionEvent {
    pub snapshot: ExecSessionSnapshot,
}

pub(super) struct SessionLifecycle {
    status: Mutex<ExecSessionStatus>,
    pub(super) changed: Notify,
}

impl SessionLifecycle {
    pub(super) fn running() -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(ExecSessionStatus::Running),
            changed: Notify::new(),
        })
    }

    pub(super) fn status(&self) -> Result<ExecSessionStatus, ExecProcessError> {
        self.status
            .lock()
            .map(|status| status.clone())
            .map_err(|_| ExecProcessError::Unavailable)
    }

    pub(super) fn transition(&self, status: ExecSessionStatus) -> Result<bool, ExecProcessError> {
        let mut current = self
            .status
            .lock()
            .map_err(|_| ExecProcessError::Unavailable)?;
        if current.is_terminal() {
            return Ok(false);
        }
        *current = status;
        drop(current);
        self.changed.notify_waiters();
        Ok(true)
    }
}
