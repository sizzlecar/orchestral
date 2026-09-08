//! Read models for browsing durable sessions without starting an Agent.

use serde::{Deserialize, Serialize};

use crate::agent_protocol::spi::StoredAgentRun;
use crate::agent_protocol::wire::{AgentEvent, AgentSessionId, Extensions, RunId};
use crate::agent_session::AgentSessionRecord;

/// Descriptive Host metadata, never an authorization or permission source.
pub const SESSION_ORIGIN_EXTENSION: &str = "orchestral/session-origin/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable project provenance attached by the Host when starting a Run.
pub struct SessionOrigin {
    /// Canonical primary workspace directory.
    pub workspace: String,
    /// Additional directories selected explicitly by the Host for this Run.
    #[serde(default)]
    pub additional_workspaces: Vec<String>,
    /// Model identity for display; it does not select a backend on recovery.
    pub model: String,
}

impl SessionOrigin {
    /// Encodes provenance in the versioned Agent Run extension namespace.
    pub fn extensions(&self) -> Extensions {
        Extensions::from([(SESSION_ORIGIN_EXTENSION.to_owned(), serde_json::json!(self))])
    }

    /// Older Runs without this extension return `None` without migration.
    pub fn from_extensions(extensions: &Extensions) -> Result<Option<Self>, serde_json::Error> {
        extensions
            .get(SESSION_ORIGIN_EXTENSION)
            .cloned()
            .map(serde_json::from_value)
            .transpose()
    }
}

/// `Unfinished` describes a missing durable terminal, not proof that a process
/// is still running. Only the Controller can reconcile live execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionHistoryStatus {
    Unfinished,
    Delivered,
    Incomplete,
    Cancelled,
    Failed,
}

impl SessionHistoryStatus {
    /// Reads the committed terminal fact, retaining uncertainty when absent.
    pub fn of_run(run: &StoredAgentRun) -> Self {
        run.records
            .iter()
            .rev()
            .find_map(|record| match record.event.payload {
                AgentEvent::DeliveryCommitted { .. } => Some(Self::Delivered),
                AgentEvent::RunIncomplete { .. } => Some(Self::Incomplete),
                AgentEvent::RunCancelled { .. } => Some(Self::Cancelled),
                AgentEvent::RunFailed { .. } => Some(Self::Failed),
                _ => None,
            })
            .unwrap_or(Self::Unfinished)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Searchable metadata derived from a session's registered Runs.
pub struct SessionHistorySummary {
    pub session_id: AgentSessionId,
    pub title: String,
    /// Absent for legacy sessions or sessions with inconsistent origins.
    pub origin: Option<SessionOrigin>,
    /// Storage activity time; never used as a causal event ordering authority.
    pub updated_at_unix_ms: i64,
    pub run_count: usize,
    pub status: SessionHistoryStatus,
    pub unfinished_run_ids: Vec<RunId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Durable conversation facts and Run lifecycles for presentation and resume.
pub struct SessionHistory {
    pub summary: SessionHistorySummary,
    /// Ordered by their first Session fact, with registration-only Runs last.
    pub runs: Vec<StoredAgentRun>,
    /// Original conversation facts, including records shadowed for model
    /// context by compaction. Presentation must not replace them with summaries.
    pub records: Vec<AgentSessionRecord>,
}

#[derive(Debug, Clone, Default)]
/// Filters a journal-derived catalog; absent workspace includes legacy sessions.
pub struct SessionHistoryQuery {
    pub workspace: Option<String>,
    /// Case-insensitive title or session identity search.
    pub search: Option<String>,
}
