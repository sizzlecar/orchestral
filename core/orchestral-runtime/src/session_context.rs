//! Replay-derived model context for the Generic Agent.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::{AgentSessionId, Digest, RunId};
use orchestral_core::agent_session::{
    session_range_digest, validate_session_trace, AgentSessionError, AgentSessionEvent,
    AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore, AgentSessionRecord,
    SessionSourceRange,
};
use orchestral_core::model_protocol::{ModelMessage, ModelRole, ModelToolDefinition};
use orchestral_core::skill_protocol::SkillId;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionContextError {
    #[error(transparent)]
    Journal(#[from] AgentSessionError),
    #[error("invalid Session Context request: {0}")]
    InvalidRequest(String),
    #[error("pinned Session Context exceeds the model input budget: used {used}, budget {budget}")]
    ContextOverflow { used: u64, budget: u64 },
    #[error("Session compaction failed: {0}")]
    Compaction(String),
}

/// Model-family token accounting boundary. Production adapters should provide
/// their tokenizer; the JSON meter is a deterministic fallback for tests and
/// conservative configuration, not a claim of provider-exact accounting.
pub trait ModelTokenMeter: Send + Sync {
    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, SessionContextError>;
}

pub struct JsonSizeTokenMeter {
    bytes_per_token: u64,
}

impl JsonSizeTokenMeter {
    pub fn new(bytes_per_token: u64) -> Result<Self, SessionContextError> {
        if bytes_per_token == 0 {
            return Err(SessionContextError::InvalidRequest(
                "bytes_per_token must be positive".to_owned(),
            ));
        }
        Ok(Self { bytes_per_token })
    }
}

impl Default for JsonSizeTokenMeter {
    fn default() -> Self {
        Self { bytes_per_token: 3 }
    }
}

impl ModelTokenMeter for JsonSizeTokenMeter {
    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, SessionContextError> {
        let bytes = serde_jcs::to_vec(&(messages, tools)).map_err(|error| {
            SessionContextError::InvalidRequest(format!(
                "could not serialize model context for metering: {error}"
            ))
        })?;
        Ok((bytes.len() as u64).div_ceil(self.bytes_per_token))
    }
}

pub struct SessionContextRequest {
    pub session_id: AgentSessionId,
    pub current_run_id: RunId,
    pub system_message: Option<ModelMessage>,
    pub tools: Vec<ModelToolDefinition>,
    pub max_context_tokens: u64,
    pub reserved_output_tokens: u64,
    pub config_digest: Digest,
    /// Exact Skill packages visible to this immutable Run binding.
    pub allowed_skill_digests: BTreeMap<SkillId, Digest>,
}

pub struct SessionContextProjection {
    pub messages: Vec<ModelMessage>,
    pub included_ranges: Vec<SessionSourceRange>,
    pub deferred_ranges: Vec<SessionSourceRange>,
    pub used_input_tokens: u64,
    pub input_budget_tokens: u64,
    pub through_session_seq: u64,
    pub config_digest: Digest,
}

pub struct AgentSessionContextEngine {
    journal: Arc<dyn AgentSessionJournalStore>,
    token_meter: Arc<dyn ModelTokenMeter>,
}

impl AgentSessionContextEngine {
    pub fn new(
        journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Self {
        Self {
            journal,
            token_meter,
        }
    }

    pub async fn project(
        &self,
        request: SessionContextRequest,
    ) -> Result<SessionContextProjection, SessionContextError> {
        validate_context_request(&request)?;
        let records = self.journal.load_session(&request.session_id).await?;
        validate_session_trace(&request.session_id, &records)?;
        let groups = replay_groups(
            &records,
            &request.current_run_id,
            &request.allowed_skill_digests,
        )?;
        let input_budget = request
            .max_context_tokens
            .saturating_sub(request.reserved_output_tokens);
        let mut selected = groups
            .values()
            .filter(|group| group.pinned)
            .map(|group| group.key)
            .collect::<BTreeSet<_>>();
        let pinned_messages = assemble_messages(&request.system_message, &groups, &selected);
        let pinned_tokens = self
            .token_meter
            .count_request_input(&pinned_messages, &request.tools)?;
        if pinned_tokens > input_budget {
            return Err(SessionContextError::ContextOverflow {
                used: pinned_tokens,
                budget: input_budget,
            });
        }

        for group in groups.values().rev().filter(|group| !group.pinned) {
            let mut candidate = selected.clone();
            candidate.insert(group.key);
            let messages = assemble_messages(&request.system_message, &groups, &candidate);
            if self
                .token_meter
                .count_request_input(&messages, &request.tools)?
                <= input_budget
            {
                selected = candidate;
            }
        }
        let messages = assemble_messages(&request.system_message, &groups, &selected);
        let used_input_tokens = self
            .token_meter
            .count_request_input(&messages, &request.tools)?;
        let mut included_ranges = Vec::new();
        let mut deferred_ranges = Vec::new();
        for group in groups.values() {
            if selected.contains(&group.key) {
                included_ranges.push(group.source.clone());
            } else {
                deferred_ranges.push(group.source.clone());
            }
        }
        Ok(SessionContextProjection {
            messages,
            included_ranges,
            deferred_ranges,
            used_input_tokens,
            input_budget_tokens: input_budget,
            through_session_seq: records.last().map(|record| record.session_seq).unwrap_or(0),
            config_digest: request.config_digest,
        })
    }
}

struct MessageGroup {
    key: u64,
    source: SessionSourceRange,
    messages: Vec<ModelMessage>,
    pinned: bool,
}

fn replay_groups(
    records: &[AgentSessionRecord],
    current_run_id: &RunId,
    allowed_skill_digests: &BTreeMap<SkillId, Digest>,
) -> Result<BTreeMap<u64, MessageGroup>, SessionContextError> {
    let mut groups = BTreeMap::new();
    let mut activated_skills = BTreeMap::new();
    for record in records {
        match &record.payload {
            AgentSessionEvent::RunInputCommitted { message } => {
                groups.insert(
                    record.session_seq,
                    MessageGroup {
                        key: record.session_seq,
                        source: single_range(record.session_seq),
                        messages: vec![message.clone()],
                        pinned: record.run_id == *current_run_id,
                    },
                );
            }
            AgentSessionEvent::ToolExchangeCommitted {
                assistant, tool, ..
            } => {
                groups.insert(
                    record.session_seq,
                    MessageGroup {
                        key: record.session_seq,
                        source: single_range(record.session_seq),
                        messages: vec![assistant.clone(), tool.clone()],
                        pinned: record.run_id == *current_run_id,
                    },
                );
            }
            AgentSessionEvent::RunOutputCommitted { message, .. } => {
                groups.insert(
                    record.session_seq,
                    MessageGroup {
                        key: record.session_seq,
                        source: single_range(record.session_seq),
                        messages: vec![message.clone()],
                        pinned: record.run_id == *current_run_id,
                    },
                );
            }
            AgentSessionEvent::SkillActivated { activation } => {
                let descriptor = &activation.package.descriptor;
                if allowed_skill_digests.get(&descriptor.skill_id) != Some(&descriptor.digest) {
                    continue;
                }
                if let Some(previous) =
                    activated_skills.insert(descriptor.skill_id.clone(), descriptor.digest.clone())
                {
                    if previous != descriptor.digest {
                        return Err(SessionContextError::Journal(AgentSessionError::Corrupt(
                            format!(
                                "Skill '{}' changed digest inside one Session without an explicit replacement protocol",
                                descriptor.skill_id
                            ),
                        )));
                    }
                    // Exact re-activation is semantically idempotent and must
                    // not duplicate full instructions in model context.
                    continue;
                }
                groups.insert(
                    record.session_seq,
                    MessageGroup {
                        key: record.session_seq,
                        source: single_range(record.session_seq),
                        messages: vec![skill_activation_message(activation)],
                        // Activated instructions are Session state. They are
                        // never evicted or shadowed by ordinary compaction.
                        pinned: true,
                    },
                );
            }
            AgentSessionEvent::CompactionCommitted {
                source,
                source_digest,
                summary,
                ..
            } => {
                let observed = session_range_digest(records, source)?;
                if observed != *source_digest {
                    return Err(SessionContextError::Journal(AgentSessionError::Corrupt(
                        format!(
                            "compaction source digest mismatch at session_seq {}",
                            record.session_seq
                        ),
                    )));
                }
                if records.iter().any(|candidate| {
                    source.contains(candidate.session_seq)
                        && matches!(candidate.payload, AgentSessionEvent::SkillActivated { .. })
                }) {
                    return Err(SessionContextError::Journal(AgentSessionError::Corrupt(
                        "compaction cannot shadow durable Skill activation state".to_owned(),
                    )));
                }
                if groups
                    .values()
                    .any(|group| group.pinned && ranges_overlap(&group.source, source))
                {
                    return Err(SessionContextError::Journal(AgentSessionError::Corrupt(
                        "compaction attempted to shadow the current Run".to_owned(),
                    )));
                }
                groups.retain(|_, group| !range_contains(source, &group.source));
                groups.insert(
                    source.last_session_seq,
                    MessageGroup {
                        key: source.last_session_seq,
                        source: source.clone(),
                        messages: vec![summary.clone()],
                        pinned: false,
                    },
                );
            }
            _ => {
                return Err(SessionContextError::Journal(AgentSessionError::Corrupt(
                    "unsupported Session event cannot be projected safely".to_owned(),
                )))
            }
        }
    }
    Ok(groups)
}

pub(crate) fn skill_activation_message(
    activation: &orchestral_core::skill_protocol::SkillActivation,
) -> ModelMessage {
    let descriptor = &activation.package.descriptor;
    let version = descriptor.version.as_deref().unwrap_or("unversioned");
    ModelMessage::text(
        ModelRole::System,
        format!(
            "Activated Skill (immutable Session context)\nname: {}\nskill_id: {}\nsource: {:?}:{}\ntrust: {:?}\nversion: {}\ndigest: {}\nactivation_reason: {}\n\nInstructions:\n{}",
            descriptor.name,
            descriptor.skill_id,
            descriptor.source.kind,
            descriptor.source.locator,
            descriptor.trust,
            version,
            descriptor.digest,
            activation.reason,
            activation.package.instructions
        ),
    )
}

fn assemble_messages(
    system_message: &Option<ModelMessage>,
    groups: &BTreeMap<u64, MessageGroup>,
    selected: &BTreeSet<u64>,
) -> Vec<ModelMessage> {
    let mut messages = Vec::new();
    if let Some(system) = system_message {
        messages.push(system.clone());
    }
    for group in groups.values() {
        if selected.contains(&group.key)
            && group
                .messages
                .iter()
                .all(|message| message.role == ModelRole::System)
        {
            messages.extend(group.messages.clone());
        }
    }
    for group in groups.values() {
        if selected.contains(&group.key)
            && !group
                .messages
                .iter()
                .all(|message| message.role == ModelRole::System)
        {
            messages.extend(group.messages.clone());
        }
    }
    messages
}

fn validate_context_request(request: &SessionContextRequest) -> Result<(), SessionContextError> {
    if request.session_id.is_empty()
        || request.current_run_id.is_empty()
        || request.max_context_tokens == 0
        || request.reserved_output_tokens >= request.max_context_tokens
        || !request.config_digest.is_sha256()
    {
        return Err(SessionContextError::InvalidRequest(
            "Session/context identities, digest, and token budget are invalid".to_owned(),
        ));
    }
    if let Some(system) = &request.system_message {
        system.validate().map_err(|error| {
            SessionContextError::InvalidRequest(format!("invalid system message: {error}"))
        })?;
        if system.role != ModelRole::System {
            return Err(SessionContextError::InvalidRequest(
                "configured system message must have the System role".to_owned(),
            ));
        }
    }
    for tool in &request.tools {
        tool.validate().map_err(|error| {
            SessionContextError::InvalidRequest(format!("invalid Tool schema: {error}"))
        })?;
    }
    Ok(())
}

fn single_range(sequence: u64) -> SessionSourceRange {
    SessionSourceRange {
        first_session_seq: sequence,
        last_session_seq: sequence,
    }
}

fn range_contains(outer: &SessionSourceRange, inner: &SessionSourceRange) -> bool {
    outer.first_session_seq <= inner.first_session_seq
        && outer.last_session_seq >= inner.last_session_seq
}

fn ranges_overlap(left: &SessionSourceRange, right: &SessionSourceRange) -> bool {
    left.first_session_seq <= right.last_session_seq
        && right.first_session_seq <= left.last_session_seq
}

#[derive(Debug, Clone)]
pub struct SessionCompactionPolicy {
    pub minimum_source_records: usize,
    pub keep_recent_records: usize,
}

impl Default for SessionCompactionPolicy {
    fn default() -> Self {
        Self {
            minimum_source_records: 8,
            keep_recent_records: 8,
        }
    }
}

pub fn select_compaction_source(
    records: &[AgentSessionRecord],
    current_run_id: &RunId,
    policy: &SessionCompactionPolicy,
) -> Option<SessionSourceRange> {
    if policy.minimum_source_records == 0
        || records.len() < policy.minimum_source_records + policy.keep_recent_records
    {
        return None;
    }
    if let Some(previous_compaction) = records.iter().rev().find(|record| {
        matches!(
            record.payload,
            AgentSessionEvent::CompactionCommitted { .. }
        )
    }) {
        let records_since_compaction = records
            .len()
            .saturating_sub(previous_compaction.session_seq as usize);
        if records_since_compaction < policy.minimum_source_records + policy.keep_recent_records {
            return None;
        }
    }
    let source_len = records.len().saturating_sub(policy.keep_recent_records);
    let source = &records[..source_len];
    if source.len() < policy.minimum_source_records
        || source.iter().any(|record| {
            record.run_id == *current_run_id
                || matches!(record.payload, AgentSessionEvent::SkillActivated { .. })
        })
    {
        return None;
    }
    Some(SessionSourceRange {
        first_session_seq: 1,
        last_session_seq: source.last()?.session_seq,
    })
}

pub struct SessionCompactionInput {
    pub session_id: AgentSessionId,
    pub source: SessionSourceRange,
    pub messages: Vec<ModelMessage>,
}

pub struct SessionSummary {
    pub message: ModelMessage,
    pub strategy: String,
    pub model: Option<String>,
    pub version: String,
}

#[async_trait]
pub trait AgentSessionSummarizer: Send + Sync {
    async fn summarize(
        &self,
        input: SessionCompactionInput,
    ) -> Result<SessionSummary, SessionContextError>;
}

pub struct AgentSessionCompactor {
    journal: Arc<dyn AgentSessionJournalStore>,
    summarizer: Arc<dyn AgentSessionSummarizer>,
    policy: SessionCompactionPolicy,
}

impl AgentSessionCompactor {
    pub fn new(
        journal: Arc<dyn AgentSessionJournalStore>,
        summarizer: Arc<dyn AgentSessionSummarizer>,
        policy: SessionCompactionPolicy,
    ) -> Self {
        Self {
            journal,
            summarizer,
            policy,
        }
    }

    pub async fn compact_if_needed(
        &self,
        session_id: &AgentSessionId,
        current_run_id: &RunId,
    ) -> Result<Option<AgentSessionRecord>, SessionContextError> {
        let records = self.journal.load_session(session_id).await?;
        validate_session_trace(session_id, &records)?;
        let Some(source) = select_compaction_source(&records, current_run_id, &self.policy) else {
            return Ok(None);
        };
        let source_digest = session_range_digest(&records, &source)?;
        let groups = replay_groups(&records, current_run_id, &BTreeMap::new())?;
        let mut messages = Vec::new();
        for group in groups
            .values()
            .filter(|group| range_contains(&source, &group.source))
        {
            messages.extend(group.messages.clone());
        }
        let summary = self
            .summarizer
            .summarize(SessionCompactionInput {
                session_id: session_id.clone(),
                source: source.clone(),
                messages,
            })
            .await?;
        summary.message.validate().map_err(|error| {
            SessionContextError::Compaction(format!("invalid summary message: {error}"))
        })?;
        let event_id = AgentSessionEventId::new(format!(
            "compaction-{}-{}-{}",
            session_id.as_str(),
            source.first_session_seq,
            source.last_session_seq
        ));
        let appended = self
            .journal
            .append(AgentSessionEventDraft {
                event_id,
                session_id: session_id.clone(),
                run_id: current_run_id.clone(),
                payload: AgentSessionEvent::CompactionCommitted {
                    source,
                    source_digest,
                    summary: summary.message,
                    strategy: summary.strategy,
                    model: summary.model,
                    version: summary.version,
                },
            })
            .await?;
        Ok(Some(appended.record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::agent_session::{
        AgentSessionEvent, AgentSessionEventDraft, InMemoryAgentSessionJournalStore,
    };
    use orchestral_core::model_protocol::{ModelContent, ModelRequestId, ModelToolCallId};
    use serde_json::json;

    struct FixedSummarizer;

    #[async_trait]
    impl AgentSessionSummarizer for FixedSummarizer {
        async fn summarize(
            &self,
            input: SessionCompactionInput,
        ) -> Result<SessionSummary, SessionContextError> {
            Ok(SessionSummary {
                message: ModelMessage::text(
                    ModelRole::System,
                    format!("summary of {} messages", input.messages.len()),
                ),
                strategy: "fixed-test-summary".to_owned(),
                model: None,
                version: "1".to_owned(),
            })
        }
    }

    async fn append_input(
        store: &Arc<InMemoryAgentSessionJournalStore>,
        sequence: u64,
        run_id: &str,
        text: String,
    ) {
        store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!("input-{sequence}")),
                session_id: AgentSessionId::new("session-1"),
                run_id: RunId::new(run_id),
                payload: AgentSessionEvent::RunInputCommitted {
                    message: ModelMessage::text(ModelRole::User, text),
                },
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn newest_history_cannot_be_evicted_by_older_messages() {
        let store = Arc::new(InMemoryAgentSessionJournalStore::default());
        for index in 1..=12 {
            append_input(
                &store,
                index,
                if index == 12 { "current" } else { "old" },
                format!("message-{index}-{}", "x".repeat(60)),
            )
            .await;
        }
        let engine =
            AgentSessionContextEngine::new(store, Arc::new(JsonSizeTokenMeter::new(1).unwrap()));
        let projection = engine
            .project(SessionContextRequest {
                session_id: AgentSessionId::new("session-1"),
                current_run_id: RunId::new("current"),
                system_message: Some(ModelMessage::text(ModelRole::System, "system")),
                tools: Vec::new(),
                max_context_tokens: 600,
                reserved_output_tokens: 100,
                config_digest: Digest::sha256("config"),
                allowed_skill_digests: BTreeMap::new(),
            })
            .await
            .unwrap();
        let rendered = serde_json::to_string(&projection.messages).unwrap();
        assert!(rendered.contains("message-12"));
        assert!(rendered.contains("message-11"));
        assert!(!rendered.contains("message-1-"));
        assert!(projection.used_input_tokens <= projection.input_budget_tokens);
    }

    #[tokio::test]
    async fn tool_call_and_result_are_selected_as_one_atomic_group() {
        let store = Arc::new(InMemoryAgentSessionJournalStore::default());
        append_input(&store, 1, "old", "old".to_owned()).await;
        store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new("tool-pair"),
                session_id: AgentSessionId::new("session-1"),
                run_id: RunId::new("old"),
                payload: AgentSessionEvent::ToolExchangeCommitted {
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
                            call_id: ModelToolCallId::new("call-1"),
                            result: json!({ "large": "x".repeat(100) }),
                            is_error: false,
                        }],
                    },
                    usage: None,
                },
            })
            .await
            .unwrap();
        append_input(&store, 3, "current", "current".to_owned()).await;
        let engine =
            AgentSessionContextEngine::new(store, Arc::new(JsonSizeTokenMeter::new(1).unwrap()));
        let projection = engine
            .project(SessionContextRequest {
                session_id: AgentSessionId::new("session-1"),
                current_run_id: RunId::new("current"),
                system_message: None,
                tools: Vec::new(),
                max_context_tokens: 180,
                reserved_output_tokens: 20,
                config_digest: Digest::sha256("config"),
                allowed_skill_digests: BTreeMap::new(),
            })
            .await
            .unwrap();
        let has_call = projection.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|content| matches!(content, ModelContent::ToolCall { .. }))
        });
        let has_result = projection.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|content| matches!(content, ModelContent::ToolResult { .. }))
        });
        assert_eq!(has_call, has_result);
    }

    #[tokio::test]
    async fn compaction_is_traceable_ordered_and_does_not_repeat_without_new_history() {
        let store = Arc::new(InMemoryAgentSessionJournalStore::default());
        for index in 1..=6 {
            append_input(&store, index, "old", format!("old-{index}")).await;
        }
        append_input(&store, 7, "current", "current-input".to_owned()).await;
        let policy = SessionCompactionPolicy {
            minimum_source_records: 3,
            keep_recent_records: 2,
        };
        let compactor =
            AgentSessionCompactor::new(store.clone(), Arc::new(FixedSummarizer), policy);
        let compacted = compactor
            .compact_if_needed(&AgentSessionId::new("session-1"), &RunId::new("current"))
            .await
            .unwrap()
            .expect("old prefix is compacted");
        let (source, source_digest) = match &compacted.payload {
            AgentSessionEvent::CompactionCommitted {
                source,
                source_digest,
                ..
            } => (source, source_digest),
            _ => panic!("expected compaction record"),
        };
        assert_eq!(source.first_session_seq, 1);
        assert_eq!(source.last_session_seq, 5);
        let records = store
            .load_session(&AgentSessionId::new("session-1"))
            .await
            .unwrap();
        assert_eq!(
            session_range_digest(&records, source).unwrap(),
            *source_digest
        );
        assert!(compactor
            .compact_if_needed(&AgentSessionId::new("session-1"), &RunId::new("current"),)
            .await
            .unwrap()
            .is_none());

        let engine =
            AgentSessionContextEngine::new(store, Arc::new(JsonSizeTokenMeter::new(1).unwrap()));
        let projection = engine
            .project(SessionContextRequest {
                session_id: AgentSessionId::new("session-1"),
                current_run_id: RunId::new("current"),
                system_message: None,
                tools: Vec::new(),
                max_context_tokens: 10_000,
                reserved_output_tokens: 100,
                config_digest: Digest::sha256("config"),
                allowed_skill_digests: BTreeMap::new(),
            })
            .await
            .unwrap();
        let rendered = projection
            .messages
            .iter()
            .map(|message| serde_json::to_string(message).unwrap())
            .collect::<Vec<_>>();
        assert!(rendered[0].contains("summary of 5 messages"));
        assert!(!rendered.join(" ").contains("old-1"));
        assert!(rendered.last().unwrap().contains("current-input"));
    }
}
