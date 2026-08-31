//! Provider-neutral discovery and session-control contracts for complete Agents.
//!
//! [`crate::agent_protocol`] remains the authoritative execution protocol for
//! an individual Run. This module adds the directory plane that is needed to
//! discover and resume sessions owned by an external Agent. Concrete wire
//! protocols belong in plugins.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent_protocol::wire::{AgentSessionId, Content, PendingRequest, ProviderBindingRef};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.trim().is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(AgentConnectorId);
string_id!(AgentSessionActionId);
string_id!(AgentSessionTurnId);
string_id!(AgentSessionActivityId);

pub const SESSION_COMPACT_ACTION: &str = "session.compact";
pub const SESSION_REVIEW_ACTION: &str = "session.review";
pub const SESSION_FORK_ACTION: &str = "session.fork";
pub const SESSION_RENAME_ACTION: &str = "session.rename";
pub const SESSION_SET_MODEL_ACTION: &str = "session.set_model";
pub const SESSION_SET_REASONING_ACTION: &str = "session.set_reasoning";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentConnectorHealthStatus {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConnectorHealth {
    pub status: AgentConnectorHealthStatus,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl AgentConnectorHealth {
    pub fn ready(version: Option<String>) -> Self {
        Self {
            status: AgentConnectorHealthStatus::Ready,
            version,
            message: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionActionDescriptor {
    pub action_id: AgentSessionActionId,
    pub title: String,
    pub description: String,
    /// JSON Schema for action arguments. `None` means the action takes no
    /// arguments; it does not mean arbitrary arguments are accepted.
    #[serde(default)]
    pub input_schema: Option<Value>,
}

impl AgentSessionActionDescriptor {
    pub fn validate(&self) -> Result<(), AgentConnectorError> {
        if self.action_id.is_empty()
            || self.title.trim().is_empty()
            || self.description.trim().is_empty()
        {
            return Err(AgentConnectorError::invalid(
                "session action requires an id, title, and description",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionCapabilities {
    pub list: bool,
    pub read: bool,
    pub create: bool,
}

impl AgentSessionCapabilities {
    pub const fn discoverable() -> Self {
        Self {
            list: true,
            read: true,
            create: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConnectorDescriptor {
    pub connector_id: AgentConnectorId,
    pub provider_binding: ProviderBindingRef,
    /// Stable family name such as `coding-agent`; never a concrete wire method.
    pub agent_family: String,
    pub display_name: String,
    pub capabilities: AgentSessionCapabilities,
    #[serde(default)]
    pub actions: Vec<AgentSessionActionDescriptor>,
}

impl AgentConnectorDescriptor {
    pub fn validate(&self) -> Result<(), AgentConnectorError> {
        if self.connector_id.is_empty()
            || self.provider_binding.is_empty()
            || self.agent_family.trim().is_empty()
            || self.display_name.trim().is_empty()
        {
            return Err(AgentConnectorError::invalid(
                "connector descriptor requires connector, provider, family, and display identities",
            ));
        }
        if !self.capabilities.list || !self.capabilities.read {
            return Err(AgentConnectorError::invalid(
                "connector must support session listing and reading",
            ));
        }
        let mut action_ids = BTreeSet::new();
        for action in &self.actions {
            action.validate()?;
            if !action_ids.insert(action.action_id.clone()) {
                return Err(AgentConnectorError::invalid(
                    "connector action ids must be unique",
                ));
            }
        }
        Ok(())
    }

    pub fn action(
        &self,
        action_id: &AgentSessionActionId,
    ) -> Option<&AgentSessionActionDescriptor> {
        self.actions
            .iter()
            .find(|action| &action.action_id == action_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentSessionState {
    Detached,
    Idle,
    Active,
    WaitingInput,
    WaitingApproval,
    BusyElsewhere,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionSummary {
    pub connector_id: AgentConnectorId,
    /// Connector-issued opaque identity. Consumers must never parse it.
    pub session_id: AgentSessionId,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub created_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub updated_at_unix_ms: Option<i64>,
    pub state: AgentSessionState,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

impl AgentSessionSummary {
    pub fn validate_for(&self, connector_id: &AgentConnectorId) -> Result<(), AgentConnectorError> {
        if &self.connector_id != connector_id || self.session_id.is_empty() {
            return Err(AgentConnectorError::protocol(
                "connector returned a session with mismatched or empty identity",
            ));
        }
        if self.cwd.as_ref().is_some_and(|cwd| cwd.trim().is_empty())
            || self
                .created_at_unix_ms
                .is_some_and(|timestamp| timestamp < 0)
            || self
                .updated_at_unix_ms
                .is_some_and(|timestamp| timestamp < 0)
        {
            return Err(AgentConnectorError::protocol(
                "connector returned invalid session metadata",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentSessionTurnStatus {
    Pending,
    Active,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentSessionActivityKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    Plan,
    Command,
    FileChange,
    ToolCall,
    Compaction,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentSessionActivityStatus {
    Pending,
    Active,
    Completed,
    Failed,
    Declined,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionActivity {
    pub activity_id: AgentSessionActivityId,
    pub kind: AgentSessionActivityKind,
    pub status: AgentSessionActivityStatus,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub content: Vec<Content>,
    /// Structured connector data that has no stable cross-Agent equivalent.
    /// UIs must work without understanding this value.
    #[serde(default)]
    pub details: Value,
}

impl AgentSessionActivity {
    fn validate(&self) -> Result<(), AgentConnectorError> {
        if self.activity_id.is_empty() {
            return Err(AgentConnectorError::protocol(
                "session activity id must not be empty",
            ));
        }
        for content in &self.content {
            content
                .validate_integrity()
                .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionTurn {
    pub turn_id: AgentSessionTurnId,
    pub status: AgentSessionTurnStatus,
    #[serde(default)]
    pub activities: Vec<AgentSessionActivity>,
}

impl AgentSessionTurn {
    fn validate(&self) -> Result<(), AgentConnectorError> {
        if self.turn_id.is_empty() {
            return Err(AgentConnectorError::protocol(
                "session turn id must not be empty",
            ));
        }
        let mut activity_ids = BTreeSet::new();
        for activity in &self.activities {
            activity.validate()?;
            if !activity_ids.insert(activity.activity_id.clone()) {
                return Err(AgentConnectorError::protocol(
                    "activity ids must be unique within a turn",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionDetail {
    pub summary: AgentSessionSummary,
    #[serde(default)]
    pub turns: Vec<AgentSessionTurn>,
    #[serde(default)]
    pub pending_requests: Vec<PendingRequest>,
}

impl AgentSessionDetail {
    pub fn validate_for(&self, connector_id: &AgentConnectorId) -> Result<(), AgentConnectorError> {
        self.summary.validate_for(connector_id)?;
        let mut turn_ids = BTreeSet::new();
        for turn in &self.turns {
            turn.validate()?;
            if !turn_ids.insert(turn.turn_id.clone()) {
                return Err(AgentConnectorError::protocol(
                    "turn ids must be unique within a session",
                ));
            }
        }
        let mut request_ids = BTreeSet::new();
        for request in &self.pending_requests {
            request
                .validate_integrity()
                .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
            if !request_ids.insert(request.request_id.clone()) {
                return Err(AgentConnectorError::protocol(
                    "pending request ids must be unique within a session",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionListQuery {
    #[serde(default)]
    pub cursor: Option<String>,
    pub limit: u32,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub search: Option<String>,
}

impl Default for AgentSessionListQuery {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: 50,
            cwd: None,
            search: None,
        }
    }
}

impl AgentSessionListQuery {
    pub fn validate(&self) -> Result<(), AgentConnectorError> {
        if self.limit == 0 || self.limit > 200 {
            return Err(AgentConnectorError::invalid(
                "session list limit must be between 1 and 200",
            ));
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.trim().is_empty())
            || self.cwd.as_ref().is_some_and(|cwd| cwd.trim().is_empty())
            || self
                .search
                .as_ref()
                .is_some_and(|search| search.trim().is_empty())
        {
            return Err(AgentConnectorError::invalid(
                "session list filters and cursor must not be empty strings",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionPage {
    pub sessions: Vec<AgentSessionSummary>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

impl AgentSessionPage {
    pub fn validate_for(
        &self,
        connector_id: &AgentConnectorId,
        requested_limit: u32,
    ) -> Result<(), AgentConnectorError> {
        if self.sessions.len() > requested_limit as usize
            || self
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.trim().is_empty())
        {
            return Err(AgentConnectorError::protocol(
                "connector returned an invalid session page",
            ));
        }
        let mut session_ids = BTreeSet::new();
        for session in &self.sessions {
            session.validate_for(connector_id)?;
            if !session_ids.insert(session.session_id.clone()) {
                return Err(AgentConnectorError::protocol(
                    "connector returned duplicate sessions in one page",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAgentSessionRequest {
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeAgentSessionActionRequest {
    pub session_id: AgentSessionId,
    pub action_id: AgentSessionActionId,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionActionOutcome {
    #[serde(default)]
    pub session: Option<AgentSessionSummary>,
    #[serde(default)]
    pub content: Vec<Content>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentConnectorErrorCode {
    InvalidRequest,
    NotFound,
    Busy,
    LeaseConflict,
    Unsupported,
    Unavailable,
    Protocol,
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConnectorError {
    pub code: AgentConnectorErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

impl AgentConnectorError {
    pub fn new(code: AgentConnectorErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            details: Value::Null,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(AgentConnectorErrorCode::InvalidRequest, message, false)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(AgentConnectorErrorCode::Unsupported, message, false)
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(AgentConnectorErrorCode::Protocol, message, false)
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

impl fmt::Display for AgentConnectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[{:?}] {}", self.code, self.message)
    }
}

impl std::error::Error for AgentConnectorError {}

/// Session directory and session-level capability boundary for a complete
/// Agent implementation. Active Run execution is deliberately delegated to a
/// separately registered [`crate::agent_protocol::spi::AgentProvider`].
#[async_trait]
pub trait AgentConnector: Send + Sync {
    fn describe(&self) -> AgentConnectorDescriptor;

    async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError>;

    async fn list_sessions(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError>;

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError>;

    async fn create_session(
        &self,
        _request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "connector does not support session creation",
        ))
    }

    async fn invoke_action(
        &self,
        _request: InvokeAgentSessionActionRequest,
    ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "connector does not support session actions",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> AgentConnectorDescriptor {
        AgentConnectorDescriptor {
            connector_id: AgentConnectorId::new("fixture/default"),
            provider_binding: ProviderBindingRef::new("fixture/provider"),
            agent_family: "coding-agent".to_owned(),
            display_name: "Fixture Agent".to_owned(),
            capabilities: AgentSessionCapabilities::discoverable(),
            actions: vec![AgentSessionActionDescriptor {
                action_id: AgentSessionActionId::new(SESSION_COMPACT_ACTION),
                title: "Compact".to_owned(),
                description: "Compact session history".to_owned(),
                input_schema: None,
            }],
        }
    }

    #[test]
    fn descriptor_rejects_duplicate_actions() {
        let mut descriptor = descriptor();
        descriptor.actions.push(descriptor.actions[0].clone());
        let error = descriptor.validate().expect_err("duplicates must fail");
        assert_eq!(error.code, AgentConnectorErrorCode::InvalidRequest);
    }

    #[test]
    fn list_query_enforces_bounded_pages() {
        let error = AgentSessionListQuery {
            limit: 201,
            ..Default::default()
        }
        .validate()
        .expect_err("oversized query must fail");
        assert_eq!(error.code, AgentConnectorErrorCode::InvalidRequest);
    }

    #[test]
    fn session_page_rejects_wrong_connector_identity() {
        let page = AgentSessionPage {
            sessions: vec![AgentSessionSummary {
                connector_id: AgentConnectorId::new("other/default"),
                session_id: AgentSessionId::new("session-1"),
                title: None,
                preview: None,
                cwd: None,
                created_at_unix_ms: None,
                updated_at_unix_ms: None,
                state: AgentSessionState::Detached,
                extensions: BTreeMap::new(),
            }],
            next_cursor: None,
        };
        let error = page
            .validate_for(&descriptor().connector_id, 50)
            .expect_err("mismatched connector must fail");
        assert_eq!(error.code, AgentConnectorErrorCode::Protocol);
    }
}
