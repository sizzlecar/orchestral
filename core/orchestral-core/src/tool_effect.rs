//! Durable Tool effect lifecycle and replay contracts.
//!
//! `Prepared` is safe to resume because no executor has been entered.
//! `Invoked` without a later observation is conservatively `UnknownEffect` and
//! must never be executed again without explicit reconciliation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::agent_protocol::wire::{Digest, RunId};
use crate::tool_protocol::{
    ApprovalNonce, EffectScope, ToolCallId, ToolIdempotency, ToolInvocation, ToolOutcome,
};

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

string_id!(ToolEffectEventId);
string_id!(ToolEffectAttemptId);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolEffectKey {
    pub run_id: RunId,
    pub call_id: ToolCallId,
}

impl ToolEffectKey {
    pub fn new(run_id: RunId, call_id: ToolCallId) -> Self {
        Self { run_id, call_id }
    }

    pub fn validate(&self) -> Result<(), ToolEffectError> {
        if self.run_id.is_empty() || self.call_id.is_empty() {
            return Err(ToolEffectError::InvalidEvent(
                "Tool effect key identities must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedToolEffect {
    pub invocation: ToolInvocation,
    pub args_digest: Digest,
    pub policy_digest: Digest,
    pub descriptor_digest: Digest,
    pub idempotency: ToolIdempotency,
    #[serde(default)]
    pub effect_scopes: BTreeSet<EffectScope>,
}

impl PreparedToolEffect {
    pub fn key(&self) -> ToolEffectKey {
        ToolEffectKey::new(
            self.invocation.run_id.clone(),
            self.invocation.call_id.clone(),
        )
    }

    pub fn validate(&self) -> Result<(), ToolEffectError> {
        self.invocation
            .validate()
            .map_err(|error| ToolEffectError::InvalidEvent(error.message))?;
        if self
            .invocation
            .args_digest()
            .map_err(|error| ToolEffectError::InvalidEvent(error.message))?
            != self.args_digest
            || !self.policy_digest.is_sha256()
            || !self.descriptor_digest.is_sha256()
        {
            return Err(ToolEffectError::InvalidEvent(
                "Prepared Tool effect digests do not match its invocation".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn identity_digest(&self) -> Result<Digest, ToolEffectError> {
        self.validate()?;
        canonical_digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolAuthorizationEvidence {
    Policy,
    Approval { nonce: ApprovalNonce },
}

impl ToolAuthorizationEvidence {
    fn validate(&self) -> Result<(), ToolEffectError> {
        if matches!(self, Self::Approval { nonce } if nonce.is_empty()) {
            return Err(ToolEffectError::InvalidEvent(
                "approval authorization evidence requires a nonce".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ToolEffectEvent {
    Prepared {
        effect: PreparedToolEffect,
    },
    Invoked {
        attempt_id: ToolEffectAttemptId,
        authorization: ToolAuthorizationEvidence,
    },
    Observed {
        outcome: ToolOutcome,
    },
    Committed {
        outcome_digest: Digest,
    },
    EffectUnknown {
        reason: String,
    },
}

impl ToolEffectEvent {
    fn validate(&self) -> Result<(), ToolEffectError> {
        match self {
            Self::Prepared { effect } => effect.validate(),
            Self::Invoked {
                attempt_id,
                authorization,
            } => {
                if attempt_id.is_empty() {
                    return Err(ToolEffectError::InvalidEvent(
                        "Tool invocation attempt identity must not be empty".to_owned(),
                    ));
                }
                authorization.validate()
            }
            Self::Observed { outcome } => outcome
                .validate_shape()
                .map_err(|error| ToolEffectError::InvalidEvent(error.message)),
            Self::Committed { outcome_digest } if !outcome_digest.is_sha256() => {
                Err(ToolEffectError::InvalidEvent(
                    "committed Tool outcome requires a SHA-256 digest".to_owned(),
                ))
            }
            Self::EffectUnknown { reason } if reason.trim().is_empty() => Err(
                ToolEffectError::InvalidEvent("UnknownEffect requires a reason".to_owned()),
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolEffectEventDraft {
    pub event_id: ToolEffectEventId,
    pub key: ToolEffectKey,
    pub payload: ToolEffectEvent,
}

impl ToolEffectEventDraft {
    pub fn validate(&self) -> Result<(), ToolEffectError> {
        if self.event_id.is_empty() {
            return Err(ToolEffectError::InvalidEvent(
                "Tool effect event_id must not be empty".to_owned(),
            ));
        }
        self.key.validate()?;
        self.payload.validate()?;
        if let ToolEffectEvent::Prepared { effect } = &self.payload {
            if effect.key() != self.key {
                return Err(ToolEffectError::InvalidEvent(
                    "Prepared Tool effect crossed an invocation boundary".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest, ToolEffectError> {
        self.validate()?;
        canonical_digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolEffectJournalRecord {
    pub effect_seq: u64,
    pub draft_digest: Digest,
    pub event_digest: Digest,
    pub event_id: ToolEffectEventId,
    pub key: ToolEffectKey,
    pub payload: ToolEffectEvent,
}

#[derive(Serialize)]
struct RecordDigestView<'a> {
    effect_seq: u64,
    draft_digest: &'a Digest,
    event_id: &'a ToolEffectEventId,
    key: &'a ToolEffectKey,
    payload: &'a ToolEffectEvent,
}

impl ToolEffectJournalRecord {
    pub fn seal(draft: ToolEffectEventDraft, effect_seq: u64) -> Result<Self, ToolEffectError> {
        draft.validate()?;
        if effect_seq == 0 {
            return Err(ToolEffectError::InvalidEvent(
                "effect_seq must be positive".to_owned(),
            ));
        }
        let draft_digest = draft.digest()?;
        let mut record = Self {
            effect_seq,
            draft_digest,
            event_digest: Digest::sha256([]),
            event_id: draft.event_id,
            key: draft.key,
            payload: draft.payload,
        };
        record.event_digest = record.computed_event_digest()?;
        Ok(record)
    }

    pub fn computed_event_digest(&self) -> Result<Digest, ToolEffectError> {
        canonical_digest(&RecordDigestView {
            effect_seq: self.effect_seq,
            draft_digest: &self.draft_digest,
            event_id: &self.event_id,
            key: &self.key,
            payload: &self.payload,
        })
    }

    pub fn validate(&self) -> Result<(), ToolEffectError> {
        if self.effect_seq == 0
            || self.event_id.is_empty()
            || !self.draft_digest.is_sha256()
            || self.computed_event_digest()? != self.event_digest
        {
            return Err(ToolEffectError::Corrupt(
                "Tool effect record identity or digest is invalid".to_owned(),
            ));
        }
        ToolEffectEventDraft {
            event_id: self.event_id.clone(),
            key: self.key.clone(),
            payload: self.payload.clone(),
        }
        .validate()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToolEffectPhase {
    Prepared,
    Invoked {
        attempt_id: ToolEffectAttemptId,
        authorization: ToolAuthorizationEvidence,
    },
    Observed {
        attempt_id: ToolEffectAttemptId,
        authorization: ToolAuthorizationEvidence,
        outcome: ToolOutcome,
    },
    Committed {
        attempt_id: ToolEffectAttemptId,
        authorization: ToolAuthorizationEvidence,
        outcome: ToolOutcome,
    },
    UnknownEffect {
        attempt_id: ToolEffectAttemptId,
        authorization: ToolAuthorizationEvidence,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolEffectProjection {
    pub key: ToolEffectKey,
    pub prepared: PreparedToolEffect,
    pub phase: ToolEffectPhase,
    pub last_effect_seq: u64,
}

pub fn replay_tool_effect(
    key: &ToolEffectKey,
    records: &[ToolEffectJournalRecord],
) -> Result<Option<ToolEffectProjection>, ToolEffectError> {
    key.validate()?;
    if records.is_empty() {
        return Ok(None);
    }
    let mut event_ids = BTreeSet::new();
    let mut projection: Option<ToolEffectProjection> = None;
    for (index, record) in records.iter().enumerate() {
        record.validate()?;
        if record.key != *key
            || record.effect_seq != index as u64 + 1
            || !event_ids.insert(record.event_id.clone())
        {
            return Err(ToolEffectError::Corrupt(format!(
                "Tool effect trace diverged at sequence {}",
                index + 1
            )));
        }
        projection = Some(match (projection, &record.payload) {
            (None, ToolEffectEvent::Prepared { effect }) => ToolEffectProjection {
                key: key.clone(),
                prepared: effect.clone(),
                phase: ToolEffectPhase::Prepared,
                last_effect_seq: record.effect_seq,
            },
            (
                Some(mut current),
                ToolEffectEvent::Invoked {
                    attempt_id,
                    authorization,
                },
            ) if matches!(current.phase, ToolEffectPhase::Prepared) => {
                current.phase = ToolEffectPhase::Invoked {
                    attempt_id: attempt_id.clone(),
                    authorization: authorization.clone(),
                };
                current.last_effect_seq = record.effect_seq;
                current
            }
            (Some(mut current), ToolEffectEvent::Observed { outcome }) => {
                let ToolEffectPhase::Invoked {
                    attempt_id,
                    authorization,
                } = &current.phase
                else {
                    return Err(invalid_transition(record.effect_seq));
                };
                current.phase = ToolEffectPhase::Observed {
                    attempt_id: attempt_id.clone(),
                    authorization: authorization.clone(),
                    outcome: outcome.clone(),
                };
                current.last_effect_seq = record.effect_seq;
                current
            }
            (Some(mut current), ToolEffectEvent::Committed { outcome_digest }) => {
                let ToolEffectPhase::Observed {
                    attempt_id,
                    authorization,
                    outcome,
                } = &current.phase
                else {
                    return Err(invalid_transition(record.effect_seq));
                };
                if outcome
                    .digest()
                    .map_err(|error| ToolEffectError::InvalidEvent(error.message))?
                    != *outcome_digest
                {
                    return Err(ToolEffectError::Corrupt(
                        "committed outcome digest does not match the observation".to_owned(),
                    ));
                }
                current.phase = ToolEffectPhase::Committed {
                    attempt_id: attempt_id.clone(),
                    authorization: authorization.clone(),
                    outcome: outcome.clone(),
                };
                current.last_effect_seq = record.effect_seq;
                current
            }
            (Some(mut current), ToolEffectEvent::EffectUnknown { reason }) => {
                let ToolEffectPhase::Invoked {
                    attempt_id,
                    authorization,
                } = &current.phase
                else {
                    return Err(invalid_transition(record.effect_seq));
                };
                current.phase = ToolEffectPhase::UnknownEffect {
                    attempt_id: attempt_id.clone(),
                    authorization: authorization.clone(),
                    reason: reason.clone(),
                };
                current.last_effect_seq = record.effect_seq;
                current
            }
            _ => return Err(invalid_transition(record.effect_seq)),
        });
    }
    Ok(projection)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolEffectAppend {
    pub record: ToolEffectJournalRecord,
    pub exact_duplicate: bool,
}

#[async_trait]
pub trait ToolEffectJournalStore: Send + Sync {
    async fn load_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Vec<ToolEffectJournalRecord>, ToolEffectError>;

    async fn append(
        &self,
        expected_previous: u64,
        draft: ToolEffectEventDraft,
    ) -> Result<ToolEffectAppend, ToolEffectError>;
}

#[derive(Default)]
pub struct InMemoryToolEffectJournalStore {
    effects: RwLock<BTreeMap<ToolEffectKey, Vec<ToolEffectJournalRecord>>>,
}

#[async_trait]
impl ToolEffectJournalStore for InMemoryToolEffectJournalStore {
    async fn load_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Vec<ToolEffectJournalRecord>, ToolEffectError> {
        let records = self
            .effects
            .read()
            .await
            .get(key)
            .cloned()
            .unwrap_or_default();
        replay_tool_effect(key, &records)?;
        Ok(records)
    }

    async fn append(
        &self,
        expected_previous: u64,
        draft: ToolEffectEventDraft,
    ) -> Result<ToolEffectAppend, ToolEffectError> {
        draft.validate()?;
        let draft_digest = draft.digest()?;
        let mut effects = self.effects.write().await;
        let records = effects.entry(draft.key.clone()).or_default();
        if let Some(existing) = records
            .iter()
            .find(|record| record.event_id == draft.event_id)
        {
            return if existing.draft_digest == draft_digest {
                Ok(ToolEffectAppend {
                    record: existing.clone(),
                    exact_duplicate: true,
                })
            } else {
                Err(ToolEffectError::EventConflict(draft.event_id))
            };
        }
        let actual_previous = records.len() as u64;
        if actual_previous != expected_previous {
            return Err(ToolEffectError::SequenceConflict {
                key: draft.key,
                expected_previous,
                actual_previous,
            });
        }
        let record = ToolEffectJournalRecord::seal(draft, actual_previous + 1)?;
        let mut candidate = records.clone();
        candidate.push(record.clone());
        replay_tool_effect(&record.key, &candidate)?;
        records.push(record.clone());
        Ok(ToolEffectAppend {
            record,
            exact_duplicate: false,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ToolEffectError {
    #[error("invalid Tool effect event: {0}")]
    InvalidEvent(String),
    #[error("Tool effect event identity conflict: {0}")]
    EventConflict(ToolEffectEventId),
    #[error(
        "Tool effect sequence conflict for {key:?}: expected {expected_previous}, actual {actual_previous}"
    )]
    SequenceConflict {
        key: ToolEffectKey,
        expected_previous: u64,
        actual_previous: u64,
    },
    #[error("Tool effect journal is unavailable: {0}")]
    StoreUnavailable(String),
    #[error("Tool effect journal is corrupt: {0}")]
    Corrupt(String),
}

fn invalid_transition(effect_seq: u64) -> ToolEffectError {
    ToolEffectError::Corrupt(format!(
        "invalid Tool effect transition at sequence {effect_seq}"
    ))
}

fn canonical_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, ToolEffectError> {
    let bytes = serde_jcs::to_vec(value)
        .map_err(|error| ToolEffectError::InvalidEvent(error.to_string()))?;
    Ok(Digest::sha256(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_protocol::{ToolId, ToolOutcome};
    use serde_json::json;

    fn prepared() -> PreparedToolEffect {
        let invocation = ToolInvocation {
            run_id: RunId::new("run-1"),
            call_id: ToolCallId::new("call-1"),
            tool_id: ToolId::new("test/write"),
            arguments: json!({ "value": "hello" }),
        };
        PreparedToolEffect {
            args_digest: invocation.args_digest().unwrap(),
            invocation,
            policy_digest: Digest::sha256("policy"),
            descriptor_digest: Digest::sha256("descriptor"),
            idempotency: ToolIdempotency::NonIdempotent,
            effect_scopes: BTreeSet::from([EffectScope::ExternalSideEffect]),
        }
    }

    fn draft(id: &str, payload: ToolEffectEvent) -> ToolEffectEventDraft {
        ToolEffectEventDraft {
            event_id: ToolEffectEventId::new(id),
            key: prepared().key(),
            payload,
        }
    }

    #[tokio::test]
    async fn exact_append_is_deduplicated_and_committed_trace_replays() {
        let store = InMemoryToolEffectJournalStore::default();
        let first = draft("prepared", ToolEffectEvent::Prepared { effect: prepared() });
        assert!(
            !store
                .append(0, first.clone())
                .await
                .unwrap()
                .exact_duplicate
        );
        assert!(store.append(0, first).await.unwrap().exact_duplicate);
        store
            .append(
                1,
                draft(
                    "invoked",
                    ToolEffectEvent::Invoked {
                        attempt_id: ToolEffectAttemptId::new("attempt-1"),
                        authorization: ToolAuthorizationEvidence::Policy,
                    },
                ),
            )
            .await
            .unwrap();
        let outcome = ToolOutcome::Completed {
            output: json!({ "ok": true }).into(),
        };
        store
            .append(
                2,
                draft(
                    "observed",
                    ToolEffectEvent::Observed {
                        outcome: outcome.clone(),
                    },
                ),
            )
            .await
            .unwrap();
        store
            .append(
                3,
                draft(
                    "committed",
                    ToolEffectEvent::Committed {
                        outcome_digest: outcome.digest().unwrap(),
                    },
                ),
            )
            .await
            .unwrap();

        let records = store.load_effect(&prepared().key()).await.unwrap();
        let projection = replay_tool_effect(&prepared().key(), &records)
            .unwrap()
            .unwrap();
        assert!(matches!(
            projection.phase,
            ToolEffectPhase::Committed { outcome: observed, .. } if observed == outcome
        ));
    }

    #[tokio::test]
    async fn observation_without_invocation_is_rejected() {
        let store = InMemoryToolEffectJournalStore::default();
        store
            .append(
                0,
                draft("prepared", ToolEffectEvent::Prepared { effect: prepared() }),
            )
            .await
            .unwrap();
        assert!(store
            .append(
                1,
                draft(
                    "observed",
                    ToolEffectEvent::Observed {
                        outcome: ToolOutcome::Cancelled,
                    },
                ),
            )
            .await
            .is_err());
    }
}
