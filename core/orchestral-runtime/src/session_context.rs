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
use serde::{Deserialize, Serialize};

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
    /// Optional append-only Journal cursor used to reproduce a prior model
    /// request exactly during recovery. `None` projects the current head.
    pub through_session_seq: Option<u64>,
    pub system_message: Option<ModelMessage>,
    pub tools: Vec<ModelToolDefinition>,
    /// Maximum number of non-current Run history groups eligible for this
    /// request. Current Run state and activated Skills remain pinned.
    pub history_limit: usize,
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
        let records = match request.through_session_seq {
            Some(through) if through > records.len() as u64 => {
                return Err(SessionContextError::InvalidRequest(format!(
                    "Session Context cursor {through} is past Journal head {}",
                    records.len()
                )))
            }
            Some(through) => &records[..through as usize],
            None => records.as_slice(),
        };
        let groups = replay_groups(
            records,
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

        for group in groups
            .values()
            .rev()
            .filter(|group| !group.pinned)
            .take(request.history_limit)
        {
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
        || request.history_limit == 0
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

impl SessionCompactionPolicy {
    pub fn validate(&self) -> Result<(), SessionContextError> {
        if self.minimum_source_records == 0
            || self.keep_recent_records == 0
            || self
                .minimum_source_records
                .checked_add(self.keep_recent_records)
                .is_none()
        {
            return Err(SessionContextError::InvalidRequest(
                "Session compaction limits must be positive and bounded".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest, SessionContextError> {
        self.validate()?;
        serde_jcs::to_vec(self)
            .map(Digest::sha256)
            .map_err(|error| {
                SessionContextError::InvalidRequest(format!(
                    "could not digest Session compaction policy: {error}"
                ))
            })
    }
}

pub fn select_compaction_source(
    records: &[AgentSessionRecord],
    current_run_id: &RunId,
    policy: &SessionCompactionPolicy,
) -> Option<SessionSourceRange> {
    policy.validate().ok()?;
    let minimum_records = policy
        .minimum_source_records
        .checked_add(policy.keep_recent_records)?;
    if records.len() < minimum_records {
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
        if records_since_compaction < minimum_records {
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
    ) -> Result<Self, SessionContextError> {
        policy.validate()?;
        Ok(Self {
            journal,
            summarizer,
            policy,
        })
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
        let policy_digest = self.policy.digest()?;
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
                    policy_digest,
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

    async fn append_session_payload(
        store: &Arc<InMemoryAgentSessionJournalStore>,
        session_id: &AgentSessionId,
        run_id: &RunId,
        event_id: String,
        payload: AgentSessionEvent,
    ) {
        store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(event_id),
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                payload,
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
                through_session_seq: None,
                system_message: Some(ModelMessage::text(ModelRole::System, "system")),
                tools: Vec::new(),
                history_limit: 100,
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
                through_session_seq: None,
                system_message: None,
                tools: Vec::new(),
                history_limit: 100,
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

        let prior = engine
            .project(SessionContextRequest {
                session_id: AgentSessionId::new("session-1"),
                current_run_id: RunId::new("current"),
                through_session_seq: Some(1),
                system_message: None,
                tools: Vec::new(),
                history_limit: 100,
                max_context_tokens: 180,
                reserved_output_tokens: 20,
                config_digest: Digest::sha256("config"),
                allowed_skill_digests: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(prior.through_session_seq, 1);
        assert!(prior.messages.iter().all(|message| {
            message.content.iter().all(|content| {
                !matches!(
                    content,
                    ModelContent::ToolCall { .. } | ModelContent::ToolResult { .. }
                )
            })
        }));
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
        let policy_digest = policy.digest().unwrap();
        let compactor =
            AgentSessionCompactor::new(store.clone(), Arc::new(FixedSummarizer), policy).unwrap();
        let compacted = compactor
            .compact_if_needed(&AgentSessionId::new("session-1"), &RunId::new("current"))
            .await
            .unwrap()
            .expect("old prefix is compacted");
        let (source, source_digest, persisted_policy_digest) = match &compacted.payload {
            AgentSessionEvent::CompactionCommitted {
                source,
                source_digest,
                policy_digest,
                ..
            } => (source, source_digest, policy_digest),
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
        assert_eq!(*persisted_policy_digest, policy_digest);
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
                through_session_seq: None,
                system_message: None,
                tools: Vec::new(),
                history_limit: 100,
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

    #[test]
    fn ten_thousand_journal_policy_combinations_select_one_deterministic_traceable_source() {
        const HISTORIES: usize = 100;
        const POLICIES_PER_HISTORY: usize = 100;

        let mut seed = 0xC04D_AC71_0A11_CE55u64;
        let mut selected = 0usize;
        let mut deferred = 0usize;

        for history_index in 0..HISTORIES {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let record_count = 16 + (seed % 33) as usize;
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let current_sequence = 1 + (seed % record_count as u64);
            let session_id = AgentSessionId::new(format!("compaction-session-{history_index}"));
            let old_run_id = RunId::new(format!("compaction-old-{history_index}"));
            let current_run_id = RunId::new(format!("compaction-current-{history_index}"));
            let records = (1..=record_count as u64)
                .map(|session_seq| {
                    AgentSessionRecord::seal(
                        AgentSessionEventDraft {
                            event_id: AgentSessionEventId::new(format!(
                                "compaction-event-{history_index}-{session_seq}"
                            )),
                            session_id: session_id.clone(),
                            run_id: if session_seq == current_sequence {
                                current_run_id.clone()
                            } else {
                                old_run_id.clone()
                            },
                            payload: AgentSessionEvent::RunInputCommitted {
                                message: ModelMessage::text(
                                    ModelRole::User,
                                    format!("history-{history_index}-{session_seq}"),
                                ),
                            },
                        },
                        session_seq,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            validate_session_trace(&session_id, &records).unwrap();

            for _ in 0..POLICIES_PER_HISTORY {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let minimum_source_records = 1 + (seed % 12) as usize;
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let keep_recent_records = 1 + (seed % 12) as usize;
                let policy = SessionCompactionPolicy {
                    minimum_source_records,
                    keep_recent_records,
                };
                let policy_digest = policy.digest().unwrap();
                assert_eq!(policy.digest().unwrap(), policy_digest);
                assert_ne!(
                    SessionCompactionPolicy {
                        minimum_source_records,
                        keep_recent_records: keep_recent_records + 1,
                    }
                    .digest()
                    .unwrap(),
                    policy_digest
                );

                let first = select_compaction_source(&records, &current_run_id, &policy);
                let second = select_compaction_source(&records, &current_run_id, &policy);
                assert_eq!(first, second);

                let source_len = record_count.saturating_sub(keep_recent_records);
                let expected = (record_count >= minimum_source_records + keep_recent_records
                    && current_sequence as usize > source_len)
                    .then_some(SessionSourceRange {
                        first_session_seq: 1,
                        last_session_seq: source_len as u64,
                    });
                assert_eq!(first, expected);

                if let Some(source) = first {
                    source.validate().unwrap();
                    let source_records = &records[..source.last_session_seq as usize];
                    assert!(source_records.len() >= minimum_source_records);
                    assert!(source_records
                        .iter()
                        .all(|record| record.run_id != current_run_id));
                    assert!(source.last_session_seq as usize <= record_count - keep_recent_records);
                    assert!(session_range_digest(&records, &source).unwrap().is_sha256());
                    selected += 1;
                } else {
                    deferred += 1;
                }
            }
        }

        assert_eq!(selected + deferred, HISTORIES * POLICIES_PER_HISTORY);
        assert!(selected > 0);
        assert!(deferred > 0);
    }

    #[tokio::test]
    async fn ten_thousand_generated_context_budgets_never_overflow_or_evict_a_fittable_recent_prefix(
    ) {
        const HISTORIES: usize = 100;
        const CONFIGS_PER_HISTORY: usize = 100;

        let store = Arc::new(InMemoryAgentSessionJournalStore::default());
        let meter = Arc::new(JsonSizeTokenMeter::new(1).unwrap());
        let engine = AgentSessionContextEngine::new(store.clone(), meter.clone());
        let system = ModelMessage::text(ModelRole::System, "stable system policy");
        let tools = vec![ModelToolDefinition {
            name: "inspect".to_owned(),
            description: "Inspect one generated value".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["value"],
                "properties": { "value": { "type": "string" } },
                "additionalProperties": false
            }),
        }];
        let mut seed = 0xA6E7_5E55_D15C_A11Du64;
        let mut successful = 0usize;
        let mut overflowed = 0usize;

        for history_index in 0..HISTORIES {
            let session_id = AgentSessionId::new(format!("budget-session-{history_index}"));
            let old_run_id = RunId::new(format!("budget-old-{history_index}"));
            let current_run_id = RunId::new(format!("budget-current-{history_index}"));
            for sequence in 1..=12u64 {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let width = 12 + (seed % 96) as usize;
                let payload = if sequence % 3 == 0 {
                    let call_id =
                        ModelToolCallId::new(format!("budget-call-{history_index}-{sequence}"));
                    AgentSessionEvent::ToolExchangeCommitted {
                        request_id: ModelRequestId::new(format!(
                            "budget-request-{history_index}-{sequence}"
                        )),
                        assistant: ModelMessage {
                            role: ModelRole::Assistant,
                            content: vec![ModelContent::ToolCall {
                                call_id: call_id.clone(),
                                name: "inspect".to_owned(),
                                arguments: json!({ "value": "x".repeat(width) }),
                            }],
                        },
                        tool: ModelMessage {
                            role: ModelRole::Tool,
                            content: vec![ModelContent::ToolResult {
                                call_id,
                                result: json!({ "observed": "y".repeat(width / 2 + 1) }),
                                is_error: false,
                            }],
                        },
                        usage: None,
                    }
                } else {
                    AgentSessionEvent::RunInputCommitted {
                        message: ModelMessage::text(
                            ModelRole::User,
                            format!("history-{history_index}-{sequence}-{}", "z".repeat(width)),
                        ),
                    }
                };
                append_session_payload(
                    &store,
                    &session_id,
                    &old_run_id,
                    format!("budget-event-{history_index}-{sequence}"),
                    payload,
                )
                .await;
            }
            append_session_payload(
                &store,
                &session_id,
                &current_run_id,
                format!("budget-current-event-{history_index}"),
                AgentSessionEvent::RunInputCommitted {
                    message: ModelMessage::text(
                        ModelRole::User,
                        format!("current-task-{history_index}"),
                    ),
                },
            )
            .await;

            let records = store.load_session(&session_id).await.unwrap();
            let groups = replay_groups(&records, &current_run_id, &BTreeMap::new()).unwrap();
            let pinned = groups
                .values()
                .filter(|group| group.pinned)
                .map(|group| group.key)
                .collect::<BTreeSet<_>>();
            let pinned_tokens = meter
                .count_request_input(
                    &assemble_messages(&Some(system.clone()), &groups, &pinned),
                    &tools,
                )
                .unwrap();
            let old_keys = groups
                .values()
                .rev()
                .filter(|group| !group.pinned)
                .map(|group| group.key)
                .collect::<Vec<_>>();
            let mut recent = pinned.clone();
            let mut recent_prefixes = Vec::new();
            for key in &old_keys {
                recent.insert(*key);
                recent_prefixes.push((
                    recent.clone(),
                    meter
                        .count_request_input(
                            &assemble_messages(&Some(system.clone()), &groups, &recent),
                            &tools,
                        )
                        .unwrap(),
                ));
            }

            for config_index in 0..CONFIGS_PER_HISTORY {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let max_context_tokens = 320 + seed % 4_681;
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let reserved_output_tokens = 1 + seed % (max_context_tokens - 1);
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let history_limit = 1 + (seed % 12) as usize;
                let input_budget = max_context_tokens - reserved_output_tokens;
                let config_digest =
                    Digest::sha256(format!("budget-config-{history_index}-{config_index}"));
                let projected = engine
                    .project(SessionContextRequest {
                        session_id: session_id.clone(),
                        current_run_id: current_run_id.clone(),
                        through_session_seq: None,
                        system_message: Some(system.clone()),
                        tools: tools.clone(),
                        history_limit,
                        max_context_tokens,
                        reserved_output_tokens,
                        config_digest: config_digest.clone(),
                        allowed_skill_digests: BTreeMap::new(),
                    })
                    .await;

                if input_budget < pinned_tokens {
                    assert!(matches!(
                        projected,
                        Err(SessionContextError::ContextOverflow { used, budget })
                            if used == pinned_tokens && budget == input_budget
                    ));
                    overflowed += 1;
                    continue;
                }

                let projection = projected.expect("fittable pinned context projects");
                let actual = meter
                    .count_request_input(&projection.messages, &tools)
                    .unwrap();
                assert_eq!(actual, projection.used_input_tokens);
                assert!(actual <= input_budget);
                assert_eq!(projection.input_budget_tokens, input_budget);
                assert_eq!(projection.config_digest, config_digest);
                assert_eq!(projection.through_session_seq, records.len() as u64);
                let selected = projection
                    .included_ranges
                    .iter()
                    .map(|range| {
                        assert_eq!(range.first_session_seq, range.last_session_seq);
                        range.first_session_seq
                    })
                    .collect::<BTreeSet<_>>();
                assert!(pinned.is_subset(&selected));
                let eligible_history = old_keys
                    .iter()
                    .take(history_limit)
                    .copied()
                    .collect::<BTreeSet<_>>();
                assert!(selected
                    .difference(&pinned)
                    .all(|key| eligible_history.contains(key)));
                if let Some((required, _)) =
                    recent_prefixes.iter().rev().find(|(required, tokens)| {
                        required.len().saturating_sub(pinned.len()) <= history_limit
                            && *tokens <= input_budget
                    })
                {
                    assert!(required.is_subset(&selected));
                }
                let calls = projection
                    .messages
                    .iter()
                    .flat_map(|message| message.content.iter())
                    .filter_map(|content| match content {
                        ModelContent::ToolCall { call_id, .. } => Some(call_id.clone()),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                let results = projection
                    .messages
                    .iter()
                    .flat_map(|message| message.content.iter())
                    .filter_map(|content| match content {
                        ModelContent::ToolResult { call_id, .. } => Some(call_id.clone()),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(calls, results);
                successful += 1;
            }
        }

        assert_eq!(successful + overflowed, HISTORIES * CONFIGS_PER_HISTORY);
        assert!(successful > 0);
        assert!(overflowed > 0);
    }
}
