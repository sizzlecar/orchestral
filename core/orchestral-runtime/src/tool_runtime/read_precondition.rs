use super::*;
use orchestral_core::model_protocol::{ModelContent, ModelMessage, ModelRole};

/// Successful Tool values present in the exact request sent to a model.
/// Construct this before executing that response's Tool batch: a read from the
/// same batch has not yet been observed by the model and is ineligible.
#[derive(Debug, Clone, Default)]
pub struct ModelToolObservations(Vec<(ToolCallId, serde_json::Value)>);

impl ModelToolObservations {
    pub fn from_messages(messages: &[ModelMessage]) -> Self {
        Self(
            messages
                .iter()
                .filter(|message| message.role == ModelRole::Tool)
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    ModelContent::ToolResult {
                        call_id,
                        result,
                        is_error: false,
                    } => Some((ToolCallId::new(call_id.as_str()), result.clone())),
                    _ => None,
                })
                .collect(),
        )
    }
}

/// Executor-declared evidence of a complete, untruncated UTF-8 file read.
/// This is a version precondition, never filesystem authority.
#[derive(Debug, Clone)]
pub struct CompleteFileRead {
    pub workspace: String,
    pub path: String,
    pub content_digest: Digest,
}

#[derive(Debug, Clone)]
pub struct ObservedFileRead {
    pub version: CompleteFileRead,
    pub source: ToolEffectKey,
    pub source_event_digest: Digest,
}

/// Verified before any Tool in the current response is executed. Its private
/// contents cannot gain a newly committed read during that batch.
#[derive(Debug, Clone, Default)]
pub struct FrozenToolObservations(Vec<ObservedFileRead>);

pub(super) fn execution_invocation(
    original: &ToolInvocation,
    resolution: Option<&ToolArgumentResolution>,
) -> ToolInvocation {
    let mut invocation = original.clone();
    if let Some(resolution) = resolution {
        invocation.arguments = resolution.arguments.clone();
    }
    invocation
}

impl<S: ApprovalCapabilityStore> GuardedToolRuntime<S> {
    pub(super) async fn resolve_invocation_arguments(
        &self,
        invocation: &ToolInvocation,
        registered: &RegisteredTool,
        observations: &FrozenToolObservations,
    ) -> Result<Option<ToolArgumentResolution>, ToolOutcome> {
        if !registered.executor.requires_observed_arguments(invocation) {
            return Ok(None);
        }
        let key = ToolEffectKey::new(invocation.run_id.clone(), invocation.call_id.clone());
        let records = self
            .effect_journal
            .load_effect(&key)
            .await
            .map_err(read_error)?;
        if let Some(prior) = replay_tool_effect(&key, &records).map_err(read_error)? {
            if prior.prepared.invocation != *invocation {
                return Err(ToolOutcome::Rejected {
                    code: "call_identity_conflict".to_owned(),
                    message: "original Tool arguments differ from the prepared invocation"
                        .to_owned(),
                });
            }
            // Never refresh a prepared precondition from newer reads, even
            // when this call is retried after approval or process recovery.
            return Ok(prior
                .prepared
                .argument_resolution
                .map(|resolution| *resolution));
        }
        let reads = observations
            .0
            .iter()
            .filter(|read| {
                read.source.run_id == invocation.run_id && read.source.call_id != invocation.call_id
            })
            .cloned()
            .collect::<Vec<_>>();
        let resolution = registered.executor.resolve_arguments(invocation, &reads)?;
        if let Some(resolution) = &resolution {
            if !reads.iter().any(|read| {
                read.source == resolution.source
                    && read.source_event_digest == resolution.source_event_digest
            }) {
                return Err(ToolOutcome::Rejected {
                    code: "tool_argument_resolution_invalid".to_owned(),
                    message: "resolved arguments refer to an unobserved Tool result".to_owned(),
                });
            }
            registered
                .descriptor
                .model_schema
                .validate_arguments(&resolution.arguments)
                .map_err(|error| ToolOutcome::Rejected {
                    code: "input_schema_violation".to_owned(),
                    message: error.message,
                })?;
        }
        Ok(resolution)
    }

    pub async fn freeze_model_observations(
        &self,
        run_id: &RunId,
        observations: &ModelToolObservations,
        pending_calls: &[ToolCallId],
    ) -> Result<FrozenToolObservations, ToolOutcome> {
        let mut reads = Vec::new();
        for (call_id, visible) in &observations.0 {
            // Call IDs need not be globally unique across Runs. An old
            // message cannot be rebound to a read in this response's batch.
            if pending_calls.contains(call_id) {
                continue;
            }
            let source = ToolEffectKey::new(run_id.clone(), call_id.clone());
            let records = self
                .effect_journal
                .load_effect(&source)
                .await
                .map_err(read_error)?;
            let Some(prior) = replay_tool_effect(&source, &records).map_err(read_error)? else {
                continue;
            };
            let ToolEffectPhase::Committed {
                outcome:
                    ToolOutcome::Completed {
                        output: ToolOutput::Inline(ref output),
                    },
                ..
            } = prior.phase
            else {
                continue;
            };
            if output != visible {
                continue;
            }
            let Some(producer) = self
                .registered_tool(&prior.prepared.invocation.tool_id)
                .map_err(|error| ToolOutcome::Rejected {
                    code: "runtime_unavailable".to_owned(),
                    message: error.to_string(),
                })?
            else {
                continue;
            };
            if producer
                .descriptor
                .digest()
                .map_err(|error| ToolOutcome::Rejected {
                    code: "invalid_descriptor".to_owned(),
                    message: error.message,
                })?
                != prior.prepared.descriptor_digest
            {
                continue;
            }
            let Some(version) = producer
                .executor
                .complete_file_read(&prior.prepared.invocation, output)
            else {
                continue;
            };
            reads.push(ObservedFileRead {
                version,
                source,
                source_event_digest: records
                    .last()
                    .expect("committed effect has records")
                    .event_digest
                    .clone(),
            });
        }
        Ok(FrozenToolObservations(reads))
    }
}

fn read_error(error: ToolEffectError) -> ToolOutcome {
    ToolOutcome::Rejected {
        code: "effect_journal_unavailable".to_owned(),
        message: error.to_string(),
    }
}
