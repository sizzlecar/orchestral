use super::*;

impl InternalGenericAgentProvider {
    pub(super) async fn start_run(
        &self,
        request: AgentStartRequest,
    ) -> Result<AgentStart, AgentStartError> {
        request
            .validate_for_descriptor(&self.inner.descriptor)
            .map_err(|error| {
                Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        let mut compatibility = self
            .inner
            .descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(AgentStartError::Rejected)?;
        if let (Some(limit), Some(policy)) = (
            request.run.spec.limits.max_cost.as_ref(),
            self.inner.config.model_cost_policy.as_ref(),
        ) {
            if limit.currency != policy.currency {
                return Err(Self::rejection(
                    AgentRejectionCode::UnsupportedCapability,
                    format!(
                        "cost limit currency {} does not match configured model pricing currency {}",
                        limit.currency, policy.currency
                    ),
                ));
            }
        }
        let run_skills = resolve_run_skill_binding(
            self.inner.skills.as_ref(),
            &request,
            &mut compatibility.skipped_optional_bindings,
        )?;
        let admission = AgentAdmission {
            skipped_optional_bindings: compatibility.skipped_optional_bindings.clone(),
        };
        admission
            .validate_against(&request.run, &compatibility)
            .map_err(|error| Self::rejection(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        let user_message = agent_input_message(&request)
            .map_err(|error| Self::rejection(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        let execution =
            AgentExecutionRef::for_start(&request, &self.inner.descriptor).map_err(|error| {
                Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;

        let (stream, cancellation, steer_updates) = {
            let mut state = self.state();
            if let Some(existing) = state.runs.get(&request.run.spec.run_id) {
                if existing.execution != execution || existing.request != request {
                    return Err(Self::rejection(
                        AgentRejectionCode::RunIdConflict,
                        "run_id already belongs to another immutable start",
                    ));
                }
                return Ok(AgentStart {
                    execution: existing.execution.clone(),
                    admission: existing.admission.clone(),
                    stream: Self::stream_for(existing),
                });
            }

            let session = state
                .sessions
                .entry(request.run.spec.session_id.clone())
                .or_default();
            if session.active_run.is_some() {
                return Err(Self::rejection(
                    AgentRejectionCode::SessionConflict,
                    "Generic Agent permits one active Run per session",
                ));
            }
            match self
                .inner
                .checkpoint_store
                .create_run(GenericAgentRunRegistration {
                    request: request.clone(),
                    execution: execution.clone(),
                    admission: admission.clone(),
                    config_digest: self.inner.config_digest.clone(),
                }) {
                Ok(CreateGenericRunOutcome::Created) => {}
                Ok(CreateGenericRunOutcome::ExactExisting) => {
                    return Err(AgentStartError::OutcomeUnknown(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidTransition,
                        "Generic Agent private WAL already owns this Run; use recovery",
                    )))
                }
                Err(error) => return Err(checkpoint_start_error(error)),
            }
            session.active_run = Some(request.run.spec.run_id.clone());

            let (sender, _) = broadcast::channel(self.inner.config.stream_buffer);
            let cancellation = CancellationToken::new();
            let stop_cause = Arc::new(AtomicU8::new(RUN_STOP_RUNNING));
            arm_run_deadline(
                request.run.spec.limits.deadline_unix_ms,
                cancellation.clone(),
                stop_cause.clone(),
            );
            let (steer_signal, steer_updates) = watch::channel(0_u64);
            let run = GenericRun {
                request: request.clone(),
                execution: execution.clone(),
                admission: admission.clone(),
                durable_events: Vec::new(),
                sender,
                terminal: false,
                cancellation: cancellation.clone(),
                stop_cause,
                cancel_command: None,
                commands: BTreeMap::new(),
                queued_steers: VecDeque::new(),
                steer_signal: steer_signal.clone(),
                pending_inputs: BTreeMap::new(),
                pending_approvals: BTreeMap::new(),
                checkpoint_seq: 0,
            };
            let stream = Self::stream_for(&run);
            state.runs.insert(request.run.spec.run_id.clone(), run);
            (stream, cancellation, steer_updates)
        };

        let model_definitions = model_definitions_for_run(&self.inner, run_skills.is_some());
        let context_result = project_model_messages(
            &self.inner,
            &request,
            &model_definitions,
            run_skills.as_deref(),
            Some(user_message.clone()),
            None,
            None,
        )
        .await;

        let model_messages = match context_result {
            Ok(messages) => messages,
            Err(error) => {
                let inner = self.inner.clone();
                let failed_request = request.clone();
                let failed_user_message = user_message.clone();
                tokio::spawn(async move {
                    fail_before_model(
                        inner,
                        &failed_request,
                        &failed_user_message,
                        session_failure(error),
                    );
                });
                return Ok(AgentStart {
                    execution,
                    admission,
                    stream,
                });
            }
        };

        if let Err(failure) = commit_loop_boundary(
            &self.inner,
            &request.run.spec.run_id,
            1,
            &ModelUsage::default(),
            0,
            "",
            &[],
        ) {
            let inner = self.inner.clone();
            let failed_request = request.clone();
            let failed_user_message = user_message.clone();
            tokio::spawn(async move {
                fail_before_model(inner, &failed_request, &failed_user_message, failure);
            });
            return Ok(AgentStart {
                execution,
                admission,
                stream,
            });
        }

        let inner = self.inner.clone();
        tokio::spawn(async move {
            execute_model_run(ModelRunExecution {
                inner,
                request,
                user_message,
                model_messages,
                model_tools: model_definitions,
                run_skills,
                seed: GenericExecutionSeed::fresh(),
                cancellation,
                steer_updates,
            })
            .await;
        });
        Ok(AgentStart {
            execution,
            admission,
            stream,
        })
    }
}
