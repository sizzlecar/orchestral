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

use crate::agent_protocol::wire::{AgentSessionId, ArtifactRefWithDigest, Digest, RunId};
use crate::model_protocol::{
    ModelContent, ModelMessage, ModelRequestId, ModelRole, ModelToolCallId, ModelUsage,
};
use crate::skill_protocol::SkillLoad;
use crate::tool_protocol::ToolCallId;

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
        /// Artifact identities are retained even when ordinary history would
        /// otherwise be evicted. Every entry must also occur in `tool`.
        retained_artifacts: Vec<ArtifactRefWithDigest>,
        #[serde(default)]
        usage: Option<ModelUsage>,
    },
    /// Host safety fact emitted when an effect may have happened but cannot be
    /// observed authoritatively. It remains pinned until a future explicit
    /// reconciliation protocol supersedes it.
    EffectUncertaintyCommitted {
        effect_call_id: ToolCallId,
        model_call_id: ModelToolCallId,
        tool_name: String,
        message: String,
    },
    RunOutputCommitted {
        request_id: ModelRequestId,
        message: ModelMessage,
        #[serde(default)]
        usage: Option<ModelUsage>,
    },
    /// A Skill is context data, not a Tool exchange. Persisting the immutable
    /// package makes replay independent from a mutable filesystem.
    SkillLoaded {
        load: Box<SkillLoad>,
    },
    CompactionCommitted {
        source: SessionSourceRange,
        source_digest: Digest,
        policy_digest: Digest,
        summary_config_digest: Digest,
        summary: ModelMessage,
        strategy: String,
        #[serde(default)]
        model: Option<String>,
        version: String,
    },
    /// Durable pressure relief for a long-running Run. Unlike ordinary
    /// historical compaction, this may shadow only complete Tool exchanges
    /// from the same Run. User input, loaded Skills, safety facts, and
    /// artifact-bearing exchanges remain verbatim.
    ActiveRunCompactionCommitted {
        source: SessionSourceRange,
        source_digest: Digest,
        policy_digest: Digest,
        summary_config_digest: Digest,
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
                retained_artifacts,
                ..
            } => {
                if request_id.is_empty() {
                    return Err(AgentSessionError::InvalidEvent(
                        "Tool exchange requires a model request identity".to_owned(),
                    ));
                }
                validate_tool_exchange(assistant, tool)?;
                validate_retained_artifacts(tool, retained_artifacts)?;
            }
            Self::EffectUncertaintyCommitted {
                effect_call_id,
                model_call_id,
                tool_name,
                message,
            } => {
                if effect_call_id.is_empty()
                    || model_call_id.is_empty()
                    || tool_name.trim().is_empty()
                    || message.trim().is_empty()
                {
                    return Err(AgentSessionError::InvalidEvent(
                        "effect uncertainty requires call identities, Tool name, and message"
                            .to_owned(),
                    ));
                }
            }
            Self::SkillLoaded { load } => load
                .validate()
                .map_err(|error| AgentSessionError::InvalidEvent(error.to_string()))?,
            Self::CompactionCommitted {
                source,
                source_digest,
                policy_digest,
                summary_config_digest,
                summary,
                strategy,
                model,
                version,
            }
            | Self::ActiveRunCompactionCommitted {
                source,
                source_digest,
                policy_digest,
                summary_config_digest,
                summary,
                strategy,
                model,
                version,
            } => {
                source.validate()?;
                if !source_digest.is_sha256()
                    || !policy_digest.is_sha256()
                    || !summary_config_digest.is_sha256()
                    || strategy.trim().is_empty()
                    || version.trim().is_empty()
                    || model.as_ref().is_some_and(|model| model.trim().is_empty())
                {
                    return Err(AgentSessionError::InvalidEvent(
                        "compaction requires source, policy, and summary config digests, strategy, and version"
                            .to_owned(),
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
        if let AgentSessionEvent::CompactionCommitted { source, .. }
        | AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = &record.payload
        {
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

fn validate_retained_artifacts(
    tool: &ModelMessage,
    retained_artifacts: &[ArtifactRefWithDigest],
) -> Result<(), AgentSessionError> {
    let mut identities = BTreeSet::new();
    for artifact in retained_artifacts {
        artifact
            .validate_integrity()
            .map_err(|error| AgentSessionError::InvalidEvent(error.to_string()))?;
        if !identities.insert(artifact.artifact_ref.as_str()) {
            return Err(AgentSessionError::InvalidEvent(
                "retained Artifact identities must be unique".to_owned(),
            ));
        }
        let expected = serde_json::to_value(artifact)
            .map_err(|error| AgentSessionError::InvalidEvent(error.to_string()))?;
        let present = tool.content.iter().any(|content| {
            matches!(
                content,
                ModelContent::ToolResult { result, .. }
                    if json_contains_value(result, &expected)
            )
        });
        if !present {
            return Err(AgentSessionError::InvalidEvent(
                "retained Artifact must occur in the atomic Tool result".to_owned(),
            ));
        }
    }
    Ok(())
}

fn json_contains_value(value: &serde_json::Value, expected: &serde_json::Value) -> bool {
    if value == expected {
        return true;
    }
    match value {
        serde_json::Value::Array(values) => values
            .iter()
            .any(|candidate| json_contains_value(candidate, expected)),
        serde_json::Value::Object(values) => values
            .values()
            .any(|candidate| json_contains_value(candidate, expected)),
        _ => false,
    }
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
                    extensions: Default::default(),
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
            retained_artifacts: Vec::new(),
            usage: None,
        };
        assert!(event.validate().is_err());
    }

    #[test]
    fn retained_artifact_must_be_digest_bound_inside_the_atomic_tool_result() {
        let artifact = ArtifactRefWithDigest {
            artifact_ref: crate::agent_protocol::wire::ArtifactRef::new("artifact-1"),
            digest: Digest::sha256("artifact-bytes"),
        };
        let event = AgentSessionEvent::ToolExchangeCommitted {
            request_id: ModelRequestId::new("request-1"),
            assistant: ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: ModelToolCallId::new("call-1"),
                    name: "read".to_owned(),
                    arguments: serde_json::json!({}),
                    extensions: Default::default(),
                }],
            },
            tool: ModelMessage {
                role: ModelRole::Tool,
                content: vec![ModelContent::ToolResult {
                    call_id: ModelToolCallId::new("call-1"),
                    result: serde_json::json!({"artifact": artifact.clone()}),
                    is_error: false,
                }],
            },
            retained_artifacts: vec![artifact.clone()],
            usage: None,
        };
        assert!(event.validate().is_ok());

        let mut mismatched = event;
        if let AgentSessionEvent::ToolExchangeCommitted {
            retained_artifacts, ..
        } = &mut mismatched
        {
            retained_artifacts[0].digest = Digest::sha256("different-bytes");
        }
        assert!(mismatched.validate().is_err());
    }

    #[test]
    fn compaction_rejects_an_unbound_summary_configuration() {
        let event = AgentSessionEvent::CompactionCommitted {
            source: SessionSourceRange {
                first_session_seq: 1,
                last_session_seq: 2,
            },
            source_digest: Digest::sha256("source"),
            policy_digest: Digest::sha256("policy"),
            summary_config_digest: Digest::new("not-a-digest"),
            summary: ModelMessage::text(ModelRole::System, "summary"),
            strategy: "extractive".to_owned(),
            model: None,
            version: "1".to_owned(),
        };
        assert!(event.validate().is_err());
    }
}
