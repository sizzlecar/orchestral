use super::*;

impl InternalGenericAgentProvider {
    pub(super) async fn recover_run(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        request.validate_for(&self.inner.descriptor)?;
        {
            let state = self.state();
            if let Some(run) = state.runs.get(&request.execution.run_id) {
                if run.execution != request.execution || run.request != request.start_request {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::RunIdConflict,
                        "recovery identity does not match the Generic Agent Run",
                    ));
                }
                return Ok(AgentRecovery::reattached(Self::stream_for(run)));
            }
        }

        let stored = self
            .inner
            .checkpoint_store
            .load_run(&request.execution.run_id)
            .map_err(checkpoint_recovery_error)?
            .ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RunNotFound,
                    "Generic Agent private WAL has no matching Run",
                )
            })?;
        let projection = stored.validate().map_err(checkpoint_recovery_error)?;
        if stored.registration.request != request.start_request
            || stored.registration.execution != request.execution
            || stored.registration.config_digest != self.inner.config_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "recovery identity or Generic Agent configuration does not match the private WAL",
            ));
        }
        let recovery_events = checkpoint_recovery_events(&stored)?;

        match projection.phase {
            GenericCheckpointPhase::Terminal => {
                let replay = stream::iter(
                    recovery_events
                        .into_iter()
                        .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft)))),
                )
                .boxed();
                Ok(AgentRecovery::staged(replay, async { Ok(()) }))
            }
            GenericCheckpointPhase::ModelAttemptOpen {
                round, request_id, ..
            } => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Generic Agent recovery is unsafe while a model attempt outcome is unknown",
            )
            .with_details(serde_json::json!({
                "boundary": "model_attempt_open",
                "round": round,
                "request_id": request_id,
            }))),
            GenericCheckpointPhase::ModelAttemptObserved {
                boundary,
                round,
                request_id,
                request_digest,
                observation,
            } => stage_observed_recovery(
                self.inner.clone(),
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                recovery_events,
            ),
            GenericCheckpointPhase::WorkflowAttemptOpen {
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call_id,
                arguments_digest,
            } => stage_started_workflow_recovery(
                self.inner.clone(),
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call_id,
                arguments_digest,
                recovery_events,
            ),
            GenericCheckpointPhase::Stable(boundary) => stage_loop_recovery(
                self.inner.clone(),
                stored,
                boundary,
                recovery_events,
                GenericRecoveryContinuation::ModelLoop {
                    restore_initial_input: false,
                },
            ),
            GenericCheckpointPhase::Prepared => stage_loop_recovery(
                self.inner.clone(),
                stored,
                GenericLoopBoundary {
                    next_model_round: 1,
                    usage: ModelUsage::default(),
                    tool_call_count: 0,
                    last_response: String::new(),
                    supporting_event_ids: Vec::new(),
                },
                recovery_events,
                GenericRecoveryContinuation::ModelLoop {
                    restore_initial_input: true,
                },
            ),
        }
    }
}
