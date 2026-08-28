use super::*;

impl InternalGenericAgentProvider {
    /// Replaces the process-lifetime checkpoint WAL before this Provider is
    /// cloned or bound to a controller.
    pub fn with_checkpoint_store(
        mut self,
        checkpoint_store: Arc<dyn GenericAgentCheckpointStore>,
    ) -> Result<Self, AgentProtocolError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Generic Agent checkpoint store must be bound before the Provider is shared",
            )
        })?;
        inner.checkpoint_store = checkpoint_store;
        Ok(self)
    }

    /// Binds one explicit Session compaction strategy before this Provider is
    /// shared. The policy becomes part of the immutable Generic Agent config
    /// digest; summaries remain durable Journal facts with their own strategy
    /// and version provenance.
    pub fn with_session_compaction(
        mut self,
        summarizer: Arc<dyn AgentSessionSummarizer>,
        policy: SessionCompactionPolicy,
    ) -> Result<Self, AgentProtocolError> {
        policy.validate().map_err(|error| {
            AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, error.to_string())
        })?;
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Session compaction must be bound before the Generic Agent Provider is shared",
            )
        })?;
        if inner.session_compactor.is_some() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Session compaction is already bound",
            ));
        }
        let compactor =
            AgentSessionCompactor::new(inner.session_journal.clone(), summarizer, policy).map_err(
                |error| {
                    AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, error.to_string())
                },
            )?;
        let config_digest = bind_session_compaction_config_digest(
            &inner.config_digest,
            compactor.policy(),
            compactor.summarizer_descriptor(),
        )?;
        inner.config_digest = config_digest;
        inner.session_compactor = Some(Arc::new(compactor));
        Ok(self)
    }

    pub fn new(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            None,
            None,
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
    }

    pub fn new_with_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(backend, config, None, None, session_journal, token_meter)
    }

    pub fn new_with_tools(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(runtime, run_grant, None, None)?),
            None,
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
    }

    pub fn new_with_tools_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(runtime, run_grant, None, None)?),
            None,
            session_journal,
            token_meter,
        )
    }

    /// Enables Host-mediated approval while keeping capability issuance out of
    /// both the model and the Generic Agent implementation.
    pub fn new_with_tools_approval_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        approval_bridge: Arc<dyn AgentApprovalBridge>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(
                runtime,
                run_grant,
                None,
                Some(approval_bridge),
            )?),
            None,
            session_journal,
            token_meter,
        )
    }

    /// Enables explicit complex-workflow selection while retaining one Generic
    /// Agent loop and the same guarded Tool Runtime for direct and DAG calls.
    pub fn new_with_workflow_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        workflow: Arc<WorkflowExecutionStrategy>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        if !workflow.uses_tool_runtime(&runtime) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Generic Agent and Workflow must share one guarded Tool Runtime",
            ));
        }
        Self::build(
            backend,
            config,
            Some(configure_tools(runtime, run_grant, Some(workflow), None)?),
            None,
            session_journal,
            token_meter,
        )
    }

    /// Enables the independent Skill Context Plane. The catalog must still be
    /// bound into each Run before descriptors or loading are visible.
    pub fn new_with_skills_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        skills: Arc<SkillRuntime>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            None,
            Some(skills),
            session_journal,
            token_meter,
        )
    }

    /// Composition-root constructor for the ordinary CLI/API Agent: Skill
    /// context and guarded Tools remain separate runtimes sharing only the
    /// Generic Agent loop.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_tools_approval_skills_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        approval_bridge: Arc<dyn AgentApprovalBridge>,
        skills: Arc<SkillRuntime>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(
                runtime,
                run_grant,
                None,
                Some(approval_bridge),
            )?),
            Some(skills),
            session_journal,
            token_meter,
        )
    }

    fn build(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        tools: Option<GenericTools>,
        skills: Option<Arc<SkillRuntime>>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        let model_descriptor = backend.descriptor();
        model_descriptor.validate().map_err(model_protocol_error)?;
        let token_meter_descriptor = token_meter.meter_descriptor();
        token_meter_descriptor
            .validate()
            .map_err(model_protocol_error)?;
        if (tools.is_some() || skills.is_some()) && !model_descriptor.capabilities.tool_calls {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "configured ModelBackend does not support model function calls",
            ));
        }
        if let Some(conflict) = tools.as_ref().and_then(|tools| {
            tools.model_definitions.iter().find_map(|definition| {
                [SKILL_READ_TOOL_NAME, REQUEST_INPUT_TOOL_NAME]
                    .contains(&definition.name.as_str())
                    .then(|| definition.name.clone())
            })
        }) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                format!("reserved Generic Agent function name is already registered: {conflict}"),
            ));
        }
        if config.stream_buffer == 0
            || config.max_model_rounds == 0
            || config.max_tool_calls == 0
            || config.history_limit == 0
            || config.max_context_tokens == 0
            || config.reserved_output_tokens >= config.max_context_tokens
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Generic Agent buffers and loop limits must be non-zero",
            ));
        }
        if let Some(policy) = &config.model_cost_policy {
            policy.validate()?;
        }
        let has_tools = tools.is_some();
        let has_input_requests = model_descriptor.capabilities.tool_calls;
        let has_approval = tools
            .as_ref()
            .and_then(|tools| tools.approval_bridge.as_ref())
            .is_some();
        let mut supported_limits = BTreeSet::from([
            RunLimitKind::Deadline,
            RunLimitKind::ModelSteps,
            RunLimitKind::InputTokens,
            RunLimitKind::OutputTokens,
        ]);
        if has_tools {
            supported_limits.insert(RunLimitKind::ToolCalls);
        }
        if config.model_cost_policy.is_some() {
            supported_limits.insert(RunLimitKind::Cost);
        }
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: config.provider_id.clone(),
            agent_id: config.agent_id.clone(),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    steer: true,
                    cancel: CancelSupport::Confirmed,
                    recover: true,
                },
                pending_request_kinds: {
                    let mut kinds = BTreeSet::new();
                    if has_input_requests {
                        kinds.insert(PendingRequestKind::Input);
                    }
                    if has_approval {
                        kinds.insert(PendingRequestKind::Approval);
                    }
                    kinds
                },
                supported_limits,
                resources: skills
                    .as_ref()
                    .map(|_| {
                        vec![ResourceCapability {
                            kind: ResourceKind::new(
                                orchestral_core::skill_protocol::SKILL_CATALOG_RESOURCE_KIND_V1,
                            ),
                            modes: BTreeSet::from([ResourceBindingMode::Snapshot]),
                            max_bindings: Some(1),
                        }]
                    })
                    .unwrap_or_default(),
                effect_mediation: if has_tools {
                    EffectMediation::HostMediated
                } else {
                    EffectMediation::None
                },
            },
            extensions: Default::default(),
        })?;
        let config_digest = generic_config_digest(
            &config,
            &model_descriptor,
            &token_meter_descriptor,
            tools.as_ref(),
            skills.as_ref().map(|skills| skills.catalog()),
            has_approval,
            has_input_requests,
        )?;
        let context_engine = AgentSessionContextEngine::new(session_journal.clone(), token_meter);
        Ok(Self {
            inner: Arc::new(GenericInner {
                backend,
                descriptor,
                config,
                tools,
                skills,
                session_journal,
                context_engine,
                session_compactor: None,
                checkpoint_store: Arc::new(InMemoryGenericAgentCheckpointStore::default()),
                config_digest,
                state: Mutex::new(GenericState::default()),
            }),
        })
    }

    pub(super) fn state(&self) -> MutexGuard<'_, GenericState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn stream_for(run: &GenericRun) -> AgentProviderStream {
        let receiver = run.sender.subscribe();
        let replay = run
            .durable_events
            .clone()
            .into_iter()
            .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft))));
        let replay_stream = stream::iter(replay);
        if run.terminal {
            return replay_stream.boxed();
        }
        let live = stream::unfold(receiver, |mut receiver| async move {
            match receiver.recv().await {
                Ok(item) => Some((item, receiver)),
                Err(broadcast::error::RecvError::Lagged(skipped)) => Some((
                    Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::SequenceGap,
                        format!("Generic Agent stream subscriber lagged by {skipped}"),
                    )),
                    receiver,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });
        replay_stream.chain(live).boxed()
    }

    pub(super) fn rejection(
        code: AgentRejectionCode,
        message: impl Into<String>,
    ) -> AgentStartError {
        AgentStartError::Rejected(AgentRejection::new(code, message))
    }
}
