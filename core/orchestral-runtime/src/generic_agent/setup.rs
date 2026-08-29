use super::*;

pub(super) fn model_event_failure(message: impl Into<String>) -> AgentFailure {
    agent_failure("model_protocol", message, false)
}

pub(super) fn resolve_run_skill_binding(
    configured: Option<&Arc<SkillRuntime>>,
    request: &AgentStartRequest,
    skipped: &mut Vec<ResourceBindingSkip>,
) -> Result<Option<Arc<SkillRuntime>>, AgentStartError> {
    let Some(skills) = configured else {
        return Ok(None);
    };
    let skipped_ids = skipped
        .iter()
        .map(|skip| skip.binding_id.clone())
        .collect::<BTreeSet<_>>();
    let mut resolved = None;
    for binding in request.run.spec.resources.iter().filter(|binding| {
        binding.resource.kind.as_str()
            == orchestral_core::skill_protocol::SKILL_CATALOG_RESOURCE_KIND_V1
            && !skipped_ids.contains(&binding.binding_id)
    }) {
        let matches = binding.resource.id == skills.catalog().resource_id
            && binding.resource.revision.as_str() == skills.catalog().revision.as_str();
        if matches {
            resolved = Some(skills.clone());
            continue;
        }
        let reason = format!(
            "Skill catalog binding does not match Host snapshot id={} revision={}",
            skills.catalog().resource_id,
            skills.catalog().revision
        );
        if binding.requirement == BindingRequirement::Required {
            return Err(InternalGenericAgentProvider::rejection(
                AgentRejectionCode::UnsupportedResource,
                reason,
            ));
        }
        skipped.push(ResourceBindingSkip {
            binding_id: binding.binding_id.clone(),
            code: ResourceBindingSkipCode::ResolutionFailed,
            reason,
        });
    }
    Ok(resolved)
}

pub(super) fn model_definitions_for_run(
    inner: &GenericInner,
    skill_catalog_bound: bool,
) -> Vec<ModelToolDefinition> {
    let mut definitions = inner
        .tools
        .as_ref()
        .map(|tools| tools.model_definitions.clone())
        .unwrap_or_default();
    if inner.backend.descriptor().capabilities.tool_calls {
        definitions.push(request_input_definition());
    }
    if skill_catalog_bound {
        definitions.push(skill_read_definition());
    }
    definitions
}

pub(super) fn request_input_definition() -> ModelToolDefinition {
    ModelToolDefinition {
        name: REQUEST_INPUT_TOOL_NAME.to_owned(),
        description: "Ask the user for information that is required before the current Run can continue. Use only when the answer cannot be derived from available context or Tools.".to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["prompt"],
            "properties": {
                "prompt": {
                    "type": "string",
                    "minLength": 1,
                    "description": "A concise question for the user"
                }
            },
            "additionalProperties": false
        }),
    }
}

pub(super) fn system_message_for_run(
    config: &GenericAgentConfig,
    skills: Option<&SkillRuntime>,
) -> Option<ModelMessage> {
    let mut sections = Vec::new();
    if !config.system_prompt.trim().is_empty() {
        sections.push(config.system_prompt.clone());
    }
    if let Some(skills) = skills {
        sections.push(skills.descriptor_context());
    }
    (!sections.is_empty()).then(|| ModelMessage::text(ModelRole::System, sections.join("\n\n")))
}

pub(super) fn configure_tools(
    runtime: Arc<dyn AgentToolRuntime>,
    run_grant: RunToolGrant,
    workflow: Option<Arc<WorkflowExecutionStrategy>>,
    approval_bridge: Option<Arc<dyn AgentApprovalBridge>>,
) -> Result<GenericTools, AgentProtocolError> {
    run_grant.bounds.validate().map_err(|error| {
        AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, error.message)
    })?;
    let runtime_contract_digest = runtime
        .execution_contract_digest()
        .map_err(tool_runtime_error)?;
    let mut model_definitions = runtime
        .model_tool_schemas()
        .map_err(tool_runtime_error)?
        .into_iter()
        .map(|schema| ModelToolDefinition {
            name: schema.name,
            description: schema.description,
            input_schema: schema.input_schema,
        })
        .collect::<Vec<_>>();
    if model_definitions.is_empty() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            "tool-enabled Generic Agent requires at least one registered Tool",
        ));
    }
    if workflow.is_some() {
        if model_definitions
            .iter()
            .any(|definition| definition.name == WORKFLOW_TOOL_NAME)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                format!(
                    "reserved Generic Agent Tool name is already registered: {WORKFLOW_TOOL_NAME}"
                ),
            ));
        }
        model_definitions.push(workflow_tool_definition());
    }
    Ok(GenericTools {
        runtime,
        runtime_contract_digest,
        run_grant,
        model_definitions,
        workflow,
        approval_bridge,
    })
}

pub(super) fn skill_read_definition() -> ModelToolDefinition {
    ModelToolDefinition {
        name: SKILL_READ_TOOL_NAME.to_owned(),
        description: "Read one Host-discovered Skill instruction document into context. Reading instructions does not grant Tool or MCP authority.".to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Exact descriptor name from the bound Skill catalog"
                }
            },
            "additionalProperties": false
        }),
    }
}

pub(super) fn workflow_tool_definition() -> ModelToolDefinition {
    ModelToolDefinition {
        name: WORKFLOW_TOOL_NAME.to_owned(),
        description: "Execute a dependency-aware workflow for a complex task. Prefer a direct answer or one ordinary Tool for simple work.".to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["plan"],
            "properties": {
                "plan": {
                    "type": "object",
                    "required": ["goal", "steps"],
                    "properties": {
                        "goal": { "type": "string", "minLength": 1 },
                        "steps": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "required": ["id", "action"],
                                "properties": {
                                    "id": { "type": "string", "minLength": 1 },
                                    "action": { "type": "string", "minLength": 1 },
                                    "kind": { "type": "string", "enum": ["action", "system"] },
                                    "depends_on": { "type": "array", "items": { "type": "string" } },
                                    "exports": { "type": "array", "items": { "type": "string" } },
                                    "io_bindings": { "type": "array" },
                                    "params": {}
                                },
                                "additionalProperties": false
                            }
                        },
                        "confidence": { "type": ["number", "null"], "minimum": 0, "maximum": 1 },
                        "on_complete": { "type": ["string", "null"] },
                        "on_failure": { "type": ["string", "null"] }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false
        }),
    }
}

pub(super) fn generic_config_digest(
    config: &GenericAgentConfig,
    model_descriptor: &orchestral_core::model_protocol::ModelDescriptor,
    token_meter: &ModelTokenMeterDescriptor,
    tools: Option<&GenericTools>,
    skills: Option<&orchestral_core::skill_protocol::SkillCatalogDescriptor>,
    approval_enabled: bool,
    input_requests_enabled: bool,
) -> Result<Digest, AgentProtocolError> {
    let tool_contract = tools.map(|tools| {
        serde_json::json!({
            "runtime_contract_digest": &tools.runtime_contract_digest,
            "run_grant": &tools.run_grant,
            "model_definitions": &tools.model_definitions,
            "workflow": tools
                .workflow
                .as_ref()
                .map(|workflow| workflow.recovery_contract()),
        })
    });
    let value = serde_json::json!({
        "provider_id": config.provider_id,
        "agent_id": config.agent_id,
        "system_prompt": config.system_prompt,
        "model_descriptor": model_descriptor,
        "token_meter": token_meter,
        "continuation_policy": {
            "max_model_steps": config.continuation.max_model_steps,
            "max_tool_calls": config.continuation.max_tool_calls,
        },
        "history_limit": config.history_limit,
        "max_context_tokens": config.max_context_tokens,
        "reserved_output_tokens": config.reserved_output_tokens,
        "model_cost_policy": config.model_cost_policy.as_ref().map(|policy| serde_json::json!({
            "currency": policy.currency,
            "input_microunits_per_million_tokens": policy.input_microunits_per_million_tokens,
            "output_microunits_per_million_tokens": policy.output_microunits_per_million_tokens,
        })),
        "tool_contract": tool_contract,
        "skill_catalog": skills,
        "approval_enabled": approval_enabled,
        "input_requests_enabled": input_requests_enabled,
        "steer_enabled": true,
    });
    let bytes = serde_jcs::to_vec(&value).map_err(|error| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            format!("could not digest Generic Agent configuration: {error}"),
        )
    })?;
    Ok(Digest::sha256(bytes))
}

pub(super) fn bind_session_compaction_config_digest(
    base_config_digest: &Digest,
    policy: &SessionCompactionPolicy,
    summarizer: &SessionSummarizerDescriptor,
) -> Result<Digest, AgentProtocolError> {
    let value = serde_json::json!({
        "contract": "generic-agent-session-compaction/v1",
        "base_config_digest": base_config_digest,
        "policy": policy,
        "summarizer": summarizer,
    });
    let bytes = serde_jcs::to_vec(&value).map_err(|error| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            format!("could not bind Session compaction configuration: {error}"),
        )
    })?;
    Ok(Digest::sha256(bytes))
}

pub(super) async fn append_session_event(
    inner: &GenericInner,
    draft: AgentSessionEventDraft,
) -> Result<(), AgentFailure> {
    inner
        .session_journal
        .append(draft)
        .await
        .map(|_| ())
        .map_err(session_journal_failure)
}

pub(super) async fn append_effect_uncertainty(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    model_call_id: &ModelToolCallId,
    tool_name: &str,
    message: &str,
) -> Result<(), AgentFailure> {
    append_session_event(
        inner,
        AgentSessionEventDraft {
            event_id: AgentSessionEventId::new(format!(
                "generic-{}-effect-uncertainty-{round}-{}",
                request.run.spec.run_id.as_str(),
                model_call_id.as_str()
            )),
            session_id: request.run.spec.session_id.clone(),
            run_id: request.run.spec.run_id.clone(),
            payload: AgentSessionEvent::EffectUncertaintyCommitted {
                effect_call_id: ToolCallId::new(model_call_id.as_str()),
                model_call_id: model_call_id.clone(),
                tool_name: tool_name.to_owned(),
                message: message.to_owned(),
            },
        },
    )
    .await
}

pub(super) fn session_journal_failure(error: AgentSessionError) -> AgentFailure {
    agent_failure("session_journal", error.to_string(), true)
}

pub(super) fn checkpoint_failure(error: GenericCheckpointError) -> AgentFailure {
    agent_failure("generic_checkpoint", error.to_string(), true)
}

pub(super) fn checkpoint_stream_error(failure: AgentFailure) -> AgentProtocolError {
    AgentProtocolError::new(AgentProtocolErrorCode::ProviderUnavailable, failure.message)
        .with_retryable(failure.retryable)
        .with_details(failure.details)
}

pub(super) fn poison_run_after_checkpoint_failure(
    run: &mut GenericRun,
    failure: AgentFailure,
) -> AgentProtocolError {
    let error = checkpoint_stream_error(failure);
    run.terminal = true;
    run.cancellation.cancel();
    let _ = run.sender.send(Err(error.clone()));
    error
}

pub(super) fn checkpoint_start_error(error: GenericCheckpointError) -> AgentStartError {
    AgentStartError::OutcomeUnknown(
        AgentProtocolError::new(
            AgentProtocolErrorCode::ProviderUnavailable,
            error.to_string(),
        )
        .with_retryable(true),
    )
}

pub(super) fn checkpoint_recovery_error(error: GenericCheckpointError) -> AgentProtocolError {
    match error {
        GenericCheckpointError::Unavailable(message) => AgentProtocolError::new(
            AgentProtocolErrorCode::ProviderUnavailable,
            format!("Generic Agent checkpoint storage is unavailable: {message}"),
        )
        .with_retryable(true),
        GenericCheckpointError::RunNotFound(run_id) => AgentProtocolError::new(
            AgentProtocolErrorCode::RunNotFound,
            format!("Generic Agent checkpoint Run does not exist: {run_id}"),
        ),
        other => AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            format!("Generic Agent private WAL cannot be trusted for recovery: {other}"),
        ),
    }
}

pub(super) fn observed_recovery_error(failure: AgentFailure) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::InvalidDigest,
        format!(
            "observed Generic Agent continuation is invalid: {}",
            failure.message
        ),
    )
    .with_details(failure.details)
}

pub(super) fn recovery_start_error(error: AgentStartError) -> AgentProtocolError {
    match error {
        AgentStartError::Rejected(rejection) => {
            AgentProtocolError::new(AgentProtocolErrorCode::Unsupported, rejection.message)
                .with_retryable(rejection.retryable)
                .with_details(rejection.details)
        }
        AgentStartError::OutcomeUnknown(error) => error,
        _ => AgentProtocolError::new(
            AgentProtocolErrorCode::Internal,
            "Generic Agent recovery encountered an unknown start error",
        ),
    }
}

pub(super) fn session_context_recovery_error(error: SessionContextError) -> AgentProtocolError {
    let failure = session_failure(error);
    let code = if failure.retryable {
        AgentProtocolErrorCode::ProviderUnavailable
    } else {
        AgentProtocolErrorCode::InvalidSpec
    };
    AgentProtocolError::new(code, failure.message)
        .with_retryable(failure.retryable)
        .with_details(failure.details)
}

pub(super) fn session_failure(error: SessionContextError) -> AgentFailure {
    match error {
        SessionContextError::ContextOverflow { used, budget } => AgentFailure {
            code: "context_overflow".to_owned(),
            message: format!("pinned model context uses {used} tokens but budget is {budget}"),
            retryable: false,
            details: serde_json::json!({ "used": used, "budget": budget }),
        },
        other => agent_failure("session_context", other.to_string(), true),
    }
}

pub(super) fn fail_before_model(
    inner: Arc<GenericInner>,
    request: &AgentStartRequest,
    user_message: &ModelMessage,
    failure: AgentFailure,
) {
    let run_id = &request.run.spec.run_id;
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: started_event_id.clone(),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunStarted,
        },
    ) {
        return;
    }
    emit_failure(&inner, request, user_message, failure);
}

pub(super) fn model_protocol_error(error: ModelError) -> AgentProtocolError {
    let code = match error.code {
        ModelErrorCode::InvalidRequest => AgentProtocolErrorCode::InvalidSpec,
        ModelErrorCode::Unsupported => AgentProtocolErrorCode::Unsupported,
        ModelErrorCode::Protocol => AgentProtocolErrorCode::InvalidTransition,
        ModelErrorCode::Unavailable
        | ModelErrorCode::RateLimited
        | ModelErrorCode::Authentication
        | ModelErrorCode::Cancelled
        | ModelErrorCode::Internal => AgentProtocolErrorCode::ProviderUnavailable,
        _ => AgentProtocolErrorCode::ProviderUnavailable,
    };
    AgentProtocolError::new(code, error.message)
        .with_retryable(error.retryable)
        .with_details(error.details)
}

pub(super) fn tool_runtime_error(error: ToolRuntimeError) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::ProviderUnavailable,
        error.to_string(),
    )
}

pub(super) fn model_failure(error: ModelError) -> AgentFailure {
    AgentFailure {
        code: format!("model_{:?}", error.code).to_lowercase(),
        message: error.message,
        retryable: error.retryable,
        details: error.details,
    }
}
