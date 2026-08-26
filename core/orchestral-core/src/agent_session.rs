//! Append-only Agent Session facts used to reconstruct model context.
//!
//! Agent Run lifecycle remains in `agent_protocol`. This journal owns the
//! cross-Run conversation projection: committed user input, final assistant
//! output, atomic Tool call/result exchanges, and traceable compaction.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::agent_protocol::wire::{AgentSessionId, Digest, RunId};
use crate::model_protocol::{
    ModelContent, ModelMessage, ModelRequestId, ModelRole, ModelToolCallId, ModelUsage,
};
use crate::skill_protocol::SkillActivation;

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

string_id!(AgentSessionEventId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSourceRange {
    pub first_session_seq: u64,
    pub last_session_seq: u64,
}

impl SessionSourceRange {
    pub fn validate(&self) -> Result<(), AgentSessionError> {
        if self.first_session_seq == 0 || self.last_session_seq < self.first_session_seq {
            return Err(AgentSessionError::InvalidEvent(
                "compaction source range must be non-empty and ordered".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn contains(&self, sequence: u64) -> bool {
        (self.first_session_seq..=self.last_session_seq).contains(&sequence)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentSessionEvent {
    RunInputCommitted {
        message: ModelMessage,
    },
    /// Assistant Tool calls and their Tool results are one atomic journal fact,
    /// so Context projection can never retain only one half of the exchange.
    ToolExchangeCommitted {
        request_id: ModelRequestId,
        assistant: ModelMessage,
        tool: ModelMessage,
        #[serde(default)]
        usage: Option<ModelUsage>,
    },
    RunOutputCommitted {
        request_id: ModelRequestId,
        message: ModelMessage,
        #[serde(default)]
        usage: Option<ModelUsage>,
    },
    /// A Skill is context data, not a Tool exchange. Persisting the immutable
    /// package makes activation replay independent from a mutable filesystem.
    SkillActivated {
        activation: SkillActivation,
    },
    CompactionCommitted {
        source: SessionSourceRange,
        source_digest: Digest,
        summary: ModelMessage,
        strategy: String,
        #[serde(default)]
        model: Option<String>,
        version: String,
    },
}

impl AgentSessionEvent {
    pub fn validate(&self) -> Result<(), AgentSessionError> {
        match self {
            Self::RunInputCommitted { message } => {
                validate_message(message)?;
                if message.role != ModelRole::User {
                    return Err(AgentSessionError::InvalidEvent(
                        "Run input must be a User model message".to_owned(),
                    ));
                }
            }
            Self::RunOutputCommitted {
                request_id,
                message,
                ..
            } => {
                if request_id.is_empty() {
                    return Err(AgentSessionError::InvalidEvent(
                        "Run output requires a model request identity".to_owned(),
                    ));
                }
                validate_message(message)?;
                if message.role != ModelRole::Assistant
                    || message.content.iter().any(|content| {
                        matches!(
                            content,
                            ModelContent::ToolCall { .. } | ModelContent::ToolResult { .. }
                        )
                    })
                {
                    return Err(AgentSessionError::InvalidEvent(
                        "Run output must be a final Assistant message without Tool blocks"
                            .to_owned(),
                    ));
                }
            }
            Self::ToolExchangeCommitted {
                request_id,
                assistant,
                tool,
                ..
            } => {
                if request_id.is_empty() {
                    return Err(AgentSessionError::InvalidEvent(
                        "Tool exchange requires a model request identity".to_owned(),
                    ));
                }
                validate_tool_exchange(assistant, tool)?;
            }
            Self::SkillActivated { activation } => activation
                .validate()
                .map_err(|error| AgentSessionError::InvalidEvent(error.to_string()))?,
            Self::CompactionCommitted {
                source,
                source_digest,
                summary,
                strategy,
                model,
                version,
            } => {
                source.validate()?;
                if !source_digest.is_sha256()
                    || strategy.trim().is_empty()
                    || version.trim().is_empty()
                    || model.as_ref().is_some_and(|model| model.trim().is_empty())
                {
                    return Err(AgentSessionError::InvalidEvent(
                        "compaction requires source digest, strategy, and version".to_owned(),
                    ));
                }
                validate_message(summary)?;
                if summary.role != ModelRole::System
                    || summary
                        .content
                        .iter()
                        .any(|content| !matches!(content, ModelContent::Text { .. }))
                {
                    return Err(AgentSessionError::InvalidEvent(
                        "compaction summary must be a System text message".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionEventDraft {
    pub event_id: AgentSessionEventId,
    pub session_id: AgentSessionId,
    pub run_id: RunId,
    pub payload: AgentSessionEvent,
}

impl AgentSessionEventDraft {
    pub fn validate(&self) -> Result<(), AgentSessionError> {
        if self.event_id.is_empty() || self.session_id.is_empty() || self.run_id.is_empty() {
            return Err(AgentSessionError::InvalidEvent(
                "Session event identities must not be empty".to_owned(),
            ));
        }
        self.payload.validate()
    }

    pub fn digest(&self) -> Result<Digest, AgentSessionError> {
        canonical_digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionRecord {
    pub session_seq: u64,
    pub draft_digest: Digest,
    pub event_digest: Digest,
    pub event_id: AgentSessionEventId,
    pub session_id: AgentSessionId,
    pub run_id: RunId,
    pub payload: AgentSessionEvent,
}

#[derive(Serialize)]
struct SessionRecordDigestView<'a> {
    session_seq: u64,
    draft_digest: &'a Digest,
    event_id: &'a AgentSessionEventId,
    session_id: &'a AgentSessionId,
    run_id: &'a RunId,
    payload: &'a AgentSessionEvent,
}

impl AgentSessionRecord {
    pub fn seal(
        draft: AgentSessionEventDraft,
        session_seq: u64,
    ) -> Result<Self, AgentSessionError> {
        draft.validate()?;
        if session_seq == 0 {
            return Err(AgentSessionError::InvalidEvent(
                "session_seq must be positive".to_owned(),
            ));
        }
        let draft_digest = draft.digest()?;
        let mut record = Self {
            session_seq,
            draft_digest,
            event_digest: Digest::sha256([]),
            event_id: draft.event_id,
            session_id: draft.session_id,
            run_id: draft.run_id,
            payload: draft.payload,
        };
        record.event_digest = record.computed_event_digest()?;
        Ok(record)
    }

    pub fn computed_event_digest(&self) -> Result<Digest, AgentSessionError> {
        canonical_digest(&SessionRecordDigestView {
            session_seq: self.session_seq,
            draft_digest: &self.draft_digest,
            event_id: &self.event_id,
            session_id: &self.session_id,
            run_id: &self.run_id,
            payload: &self.payload,
        })
    }

    pub fn validate(&self) -> Result<(), AgentSessionError> {
        if self.session_seq == 0
            || self.event_id.is_empty()
            || self.session_id.is_empty()
            || self.run_id.is_empty()
            || !self.draft_digest.is_sha256()
            || self.computed_event_digest()? != self.event_digest
        {
            return Err(AgentSessionError::InvalidEvent(
                "Session record identity or digest is invalid".to_owned(),
            ));
        }
        self.payload.validate()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentSessionAppend {
    pub record: AgentSessionRecord,
    pub exact_duplicate: bool,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AgentSessionError {
    #[error("invalid Agent Session event: {0}")]
    InvalidEvent(String),
    #[error("Agent Session event identity conflict: {0}")]
    EventConflict(AgentSessionEventId),
    #[error("Agent Session journal is unavailable: {0}")]
    StoreUnavailable(String),
    #[error("Agent Session journal is corrupt: {0}")]
    Corrupt(String),
}

/// Store assigns `session_seq` atomically and deduplicates exact event drafts.
#[async_trait]
pub trait AgentSessionJournalStore: Send + Sync {
    async fn load_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<Vec<AgentSessionRecord>, AgentSessionError>;

    async fn append(
        &self,
        draft: AgentSessionEventDraft,
    ) -> Result<AgentSessionAppend, AgentSessionError>;
}

#[derive(Default)]
pub struct InMemoryAgentSessionJournalStore {
    sessions: RwLock<BTreeMap<AgentSessionId, Vec<AgentSessionRecord>>>,
}

#[async_trait]
impl AgentSessionJournalStore for InMemoryAgentSessionJournalStore {
    async fn load_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<Vec<AgentSessionRecord>, AgentSessionError> {
        let records = self
            .sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        validate_session_trace(session_id, &records)?;
        Ok(records)
    }

    async fn append(
        &self,
        draft: AgentSessionEventDraft,
    ) -> Result<AgentSessionAppend, AgentSessionError> {
        draft.validate()?;
        let draft_digest = draft.digest()?;
        let mut sessions = self.sessions.write().await;
        let records = sessions.entry(draft.session_id.clone()).or_default();
        if let Some(existing) = records
            .iter()
            .find(|record| record.event_id == draft.event_id)
        {
            return if existing.draft_digest == draft_digest {
                Ok(AgentSessionAppend {
                    record: existing.clone(),
                    exact_duplicate: true,
                })
            } else {
                Err(AgentSessionError::EventConflict(draft.event_id))
            };
        }
        let record = AgentSessionRecord::seal(draft, records.len() as u64 + 1)?;
        records.push(record.clone());
        Ok(AgentSessionAppend {
            record,
            exact_duplicate: false,
        })
    }
}

pub fn validate_session_trace(
    session_id: &AgentSessionId,
    records: &[AgentSessionRecord],
) -> Result<(), AgentSessionError> {
    let mut event_ids = BTreeSet::new();
    for (index, record) in records.iter().enumerate() {
        record.validate()?;
        if record.session_id != *session_id
            || record.session_seq != index as u64 + 1
            || !event_ids.insert(record.event_id.clone())
        {
            return Err(AgentSessionError::Corrupt(format!(
                "Session trace diverged at sequence {}",
                index + 1
            )));
        }
        if let AgentSessionEvent::CompactionCommitted { source, .. } = &record.payload {
            if source.last_session_seq >= record.session_seq {
                return Err(AgentSessionError::Corrupt(
                    "compaction cannot shadow itself or future events".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

pub fn session_range_digest(
    records: &[AgentSessionRecord],
    source: &SessionSourceRange,
) -> Result<Digest, AgentSessionError> {
    source.validate()?;
    let selected = records
        .iter()
        .filter(|record| source.contains(record.session_seq))
        .collect::<Vec<_>>();
    let expected_len = source.last_session_seq - source.first_session_seq + 1;
    if selected.len() as u64 != expected_len {
        return Err(AgentSessionError::Corrupt(
            "compaction source range is not fully present".to_owned(),
        ));
    }
    canonical_digest(&selected)
}

fn validate_message(message: &ModelMessage) -> Result<(), AgentSessionError> {
    message
        .validate()
        .map_err(|error| AgentSessionError::InvalidEvent(error.to_string()))
}

fn validate_tool_exchange(
    assistant: &ModelMessage,
    tool: &ModelMessage,
) -> Result<(), AgentSessionError> {
    validate_message(assistant)?;
    validate_message(tool)?;
    if assistant.role != ModelRole::Assistant || tool.role != ModelRole::Tool {
        return Err(AgentSessionError::InvalidEvent(
            "Tool exchange roles must be Assistant then Tool".to_owned(),
        ));
    }
    let calls = assistant
        .content
        .iter()
        .filter_map(|content| match content {
            ModelContent::ToolCall { call_id, .. } => Some(call_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<ModelToolCallId>>();
    let results = tool
        .content
        .iter()
        .filter_map(|content| match content {
            ModelContent::ToolResult { call_id, .. } => Some(call_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<ModelToolCallId>>();
    if calls.is_empty()
        || calls != results
        || tool
            .content
            .iter()
            .any(|content| !matches!(content, ModelContent::ToolResult { .. }))
    {
        return Err(AgentSessionError::InvalidEvent(
            "Tool calls and Tool results must form one exact, non-empty pair set".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, AgentSessionError> {
    let bytes = serde_jcs::to_vec(value)
        .map_err(|error| AgentSessionError::InvalidEvent(error.to_string()))?;
    Ok(Digest::sha256(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input_draft(id: &str) -> AgentSessionEventDraft {
        AgentSessionEventDraft {
            event_id: AgentSessionEventId::new(id),
            session_id: AgentSessionId::new("session-1"),
            run_id: RunId::new("run-1"),
            payload: AgentSessionEvent::RunInputCommitted {
                message: ModelMessage::text(ModelRole::User, "hello"),
            },
        }
    }

    #[tokio::test]
    async fn exact_event_retry_is_deduplicated_before_sequence_assignment() {
        let store = InMemoryAgentSessionJournalStore::default();
        let first = store.append(input_draft("input-1")).await.unwrap();
        let duplicate = store.append(input_draft("input-1")).await.unwrap();
        assert!(!first.exact_duplicate);
        assert!(duplicate.exact_duplicate);
        assert_eq!(first.record, duplicate.record);
        assert_eq!(
            store
                .load_session(&AgentSessionId::new("session-1"))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn tool_exchange_rejects_orphan_results() {
        let event = AgentSessionEvent::ToolExchangeCommitted {
            request_id: ModelRequestId::new("request-1"),
            assistant: ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: ModelToolCallId::new("call-1"),
                    name: "echo".to_owned(),
                    arguments: json!({}),
                }],
            },
            tool: ModelMessage {
                role: ModelRole::Tool,
                content: vec![ModelContent::ToolResult {
                    call_id: ModelToolCallId::new("different-call"),
                    result: json!({}),
                    is_error: false,
                }],
            },
            usage: None,
        };
        assert!(event.validate().is_err());
    }
}
