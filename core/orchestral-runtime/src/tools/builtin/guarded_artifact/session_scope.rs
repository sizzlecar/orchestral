use std::collections::BTreeSet;
use std::sync::Arc;

use orchestral_core::agent_protocol::spi::AgentJournalStore;
use orchestral_core::agent_protocol::wire::{AgentSessionId, ArtifactRef, RunId};
use orchestral_core::agent_session::{
    validate_session_trace, AgentSessionEvent, AgentSessionJournalStore,
};
use orchestral_core::model_protocol::ModelContent;
use orchestral_core::tool_effect::{
    replay_tool_effect, ToolEffectJournalStore, ToolEffectKey, ToolEffectPhase,
};
use orchestral_core::tool_protocol::{ToolArtifact, ToolCallId, ToolOutcome, ToolOutput};
use tokio_util::sync::CancellationToken;

use crate::tool_runtime::artifact_model_output;

#[derive(Clone)]
pub(super) struct SessionArtifactResolver {
    pub runs: Arc<dyn AgentJournalStore>,
    pub sessions: Arc<dyn AgentSessionJournalStore>,
    pub effects: Arc<dyn ToolEffectJournalStore>,
}

impl SessionArtifactResolver {
    async fn session_for_run(&self, run_id: &RunId) -> Result<AgentSessionId, String> {
        let run = self
            .runs
            .load_run(run_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("Artifact access requires a registered Run")?;
        run.validate_shape().map_err(|error| error.to_string())?;
        let spec = &run.registration.run().spec;
        if spec.run_id != *run_id {
            return Err("registered Run identity mismatch".to_owned());
        }
        Ok(spec.session_id.clone())
    }

    pub async fn resolve(
        &self,
        run_id: &RunId,
        reference: &ArtifactRef,
        cancellation: &CancellationToken,
    ) -> Result<ToolArtifact, String> {
        let session_id = self.session_for_run(run_id).await?;
        let records = self
            .sessions
            .load_session(&session_id)
            .await
            .map_err(|error| error.to_string())?;
        validate_session_trace(&session_id, &records).map_err(|error| error.to_string())?;
        let mut verified_runs = BTreeSet::from([run_id.clone()]);
        let mut resolved: Option<ToolArtifact> = None;
        for record in records {
            if cancellation.is_cancelled() {
                return Err("Artifact lookup cancelled".to_owned());
            }
            let AgentSessionEvent::ToolExchangeCommitted {
                tool,
                retained_artifacts,
                ..
            } = record.payload
            else {
                continue;
            };
            if !retained_artifacts
                .iter()
                .any(|artifact| artifact.artifact_ref == *reference)
            {
                continue;
            }
            if !verified_runs.contains(&record.run_id) {
                if self.session_for_run(&record.run_id).await? != session_id {
                    return Err("Artifact producer belongs to another Session".to_owned());
                }
                verified_runs.insert(record.run_id.clone());
            }
            for content in tool.content {
                let ModelContent::ToolResult {
                    call_id,
                    result,
                    is_error: false,
                } = content
                else {
                    continue;
                };
                // A visible JSON envelope alone is not authority. Its exact
                // producer must have durably committed this Artifact outcome.
                let key =
                    ToolEffectKey::new(record.run_id.clone(), ToolCallId::new(call_id.as_str()));
                let effects = self
                    .effects
                    .load_effect(&key)
                    .await
                    .map_err(|error| error.to_string())?;
                let Some(effect) =
                    replay_tool_effect(&key, &effects).map_err(|error| error.to_string())?
                else {
                    continue;
                };
                let ToolEffectPhase::Committed {
                    outcome:
                        ToolOutcome::Completed {
                            output: ToolOutput::Artifact(artifact),
                        },
                    ..
                } = effect.phase
                else {
                    continue;
                };
                if artifact.artifact.artifact_ref != *reference
                    || !retained_artifacts.contains(&artifact.artifact)
                    || artifact_model_output(&artifact) != result
                {
                    continue;
                }
                if resolved.as_ref().is_some_and(|prior| {
                    prior.artifact != artifact.artifact
                        || prior.media_type != artifact.media_type
                        || prior.byte_size != artifact.byte_size
                }) {
                    return Err("Artifact reference has conflicting committed metadata".to_owned());
                }
                resolved = Some(artifact);
            }
        }
        resolved.ok_or_else(|| "Artifact reference is not committed in this Session".to_owned())
    }
}
