use super::*;

pub(super) struct PendingModelToolCall {
    pub(super) call_id: ModelToolCallId,
    pub(super) name: String,
    pub(super) arguments: String,
    pub(super) extensions: BTreeMap<String, serde_json::Value>,
    pub(super) ended: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowToolArguments {
    plan: Plan,
}

struct GenericWorkflowProgressReporter {
    inner: Arc<GenericInner>,
    run_id: RunId,
    workflow_id: WorkflowId,
    total_steps: u64,
    completed_steps: AtomicU64,
    sequence: AtomicU64,
}

impl GenericWorkflowProgressReporter {
    fn new(
        inner: Arc<GenericInner>,
        run_id: RunId,
        workflow_id: WorkflowId,
        total_steps: usize,
    ) -> Self {
        Self {
            inner,
            run_id,
            workflow_id,
            total_steps: total_steps as u64,
            completed_steps: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl ExecutionProgressReporter for GenericWorkflowProgressReporter {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
        if event.workflow_id != self.workflow_id {
            return Err("workflow progress crossed an Agent Run task boundary".to_owned());
        }
        let completed = match event.phase.as_str() {
            "step_completed" => self.completed_steps.fetch_add(1, Ordering::AcqRel) + 1,
            "workflow_completed" => {
                self.completed_steps
                    .store(self.total_steps, Ordering::Release);
                self.total_steps
            }
            _ => self.completed_steps.load(Ordering::Acquire),
        };
        let fraction = (self.total_steps > 0)
            .then_some((completed.min(self.total_steps) as f64) / (self.total_steps as f64));
        let target = event
            .step_id
            .as_ref()
            .map(|step_id| format!(" [{}]", step_id.as_str()))
            .unwrap_or_default();
        let message = event
            .message
            .unwrap_or_else(|| format!("workflow {}{}", event.phase, target));
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel) + 1;
        publish_telemetry(
            &self.inner,
            &self.run_id,
            AgentTelemetryEnvelope {
                telemetry_id: TelemetryId::new(format!(
                    "generic-{}-workflow-progress-{sequence}",
                    self.run_id.as_str()
                )),
                run_id: self.run_id.clone(),
                provider_seq: None,
                payload: AgentTelemetry::ProgressReported { message, fraction },
            },
        );
        Ok(())
    }
}

pub(super) struct WorkflowCallObservation {
    pub(super) result: serde_json::Value,
    pub(super) is_error: bool,
    pub(super) tool_calls: u64,
}

pub(super) enum WorkflowCallExecution {
    Observed(WorkflowCallObservation),
    Cancelled,
    UnknownEffect(String),
    RecoveryFailed(AgentFailure),
}

pub(super) struct WorkflowCallRequest<'a> {
    pub(super) run_id: &'a RunId,
    pub(super) call_id: &'a ModelToolCallId,
    pub(super) arguments: serde_json::Value,
    pub(super) remaining_tool_calls: Option<u64>,
    pub(super) cancellation: CancellationToken,
    pub(super) recovery_replay: bool,
}

pub(super) async fn execute_workflow_call(
    inner: Arc<GenericInner>,
    tools: &GenericTools,
    call: WorkflowCallRequest<'_>,
) -> WorkflowCallExecution {
    let parsed = match serde_json::from_value::<WorkflowToolArguments>(call.arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            return WorkflowCallExecution::Observed(WorkflowCallObservation {
                result: workflow_error("invalid_workflow", error.to_string()),
                is_error: true,
                tool_calls: 0,
            })
        }
    };
    let Some(workflow) = tools.workflow.as_ref() else {
        return WorkflowCallExecution::Observed(WorkflowCallObservation {
            result: workflow_error(
                "workflow_unavailable",
                "Generic Agent has no configured Workflow execution strategy",
            ),
            is_error: true,
            tool_calls: 0,
        });
    };
    let workflow_id = WorkflowId::new(format!(
        "workflow:{}:{}",
        call.run_id.as_str(),
        call.call_id.as_str()
    ));
    let reporter = Arc::new(GenericWorkflowProgressReporter::new(
        inner,
        call.run_id.clone(),
        workflow_id.clone(),
        parsed.plan.steps.len(),
    ));
    let request = WorkflowExecutionRequest::new(
        call.run_id.clone(),
        workflow_id,
        parsed.plan,
        tools.run_grant.clone(),
    )
    .with_cancellation(call.cancellation.clone())
    .with_progress_reporter(reporter);
    let request = match call.remaining_tool_calls {
        Some(limit) => request.with_max_tool_calls(limit),
        None => request,
    };
    let request = if call.recovery_replay {
        request.with_recovery_replay()
    } else {
        request
    };
    let snapshot = match workflow.execute(request).await {
        Ok(snapshot) => snapshot,
        Err(crate::workflow_strategy::WorkflowExecutionError::UnknownEffect {
            message, ..
        }) => return WorkflowCallExecution::UnknownEffect(message),
        Err(error) if call.recovery_replay => {
            return WorkflowCallExecution::RecoveryFailed(agent_failure(
                "workflow_recovery",
                error.to_string(),
                false,
            ))
        }
        Err(error) => {
            return WorkflowCallExecution::Observed(WorkflowCallObservation {
                result: workflow_error("workflow_rejected", error.to_string()),
                is_error: true,
                tool_calls: 0,
            })
        }
    };
    if call.cancellation.is_cancelled() {
        return WorkflowCallExecution::Cancelled;
    }
    let tool_calls = snapshot.tool_calls;
    let (result, is_error) = snapshot.tool_result();
    WorkflowCallExecution::Observed(WorkflowCallObservation {
        result,
        is_error,
        tool_calls,
    })
}

fn workflow_error(code: &str, message: impl Into<String>) -> serde_json::Value {
    serde_json::json!({
        "status": "rejected",
        "code": code,
        "message": message.into(),
    })
}

pub(super) fn publish_workflow_output(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
    result: serde_json::Value,
) -> Option<AgentEventId> {
    let event_id = workflow_output_event_id(run_id, round, call_id);
    publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: event_id.clone(),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::OutputCommitted {
                output_id: workflow_output_id(run_id, round, call_id),
                content: vec![Content {
                    media_type: "application/json".to_owned(),
                    schema_id: None,
                    body: ContentBody::Inline(result),
                }],
            },
        },
    )
    .then_some(event_id)
}

pub(super) fn workflow_output_event_id(
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
) -> AgentEventId {
    AgentEventId::new(format!(
        "generic-{}-workflow-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    ))
}

pub(super) fn workflow_output_id(
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
) -> OutputId {
    OutputId::new(format!(
        "generic-{}-workflow-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    ))
}

pub(super) fn recovered_workflow_output(
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
    recovery_events: &[AgentEventDraft],
) -> Result<Option<(AgentEventId, WorkflowCallObservation)>, AgentProtocolError> {
    let expected_event_id = workflow_output_event_id(run_id, round, call_id);
    let Some(event) = recovery_events
        .iter()
        .find(|event| event.event_id == expected_event_id)
    else {
        return Ok(None);
    };
    let AgentEvent::OutputCommitted { output_id, content } = &event.payload else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output event has the wrong shape",
        ));
    };
    let [Content {
        media_type,
        schema_id,
        body: ContentBody::Inline(result),
    }] = content.as_slice()
    else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output must contain one inline JSON value",
        ));
    };
    if event.run_id != *run_id
        || output_id != &workflow_output_id(run_id, round, call_id)
        || media_type != "application/json"
        || schema_id.is_some()
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output crossed its Run or output identity",
        ));
    }
    let status = result
        .get("status")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Workflow output has no status",
            )
        })?;
    if !matches!(
        status,
        "completed" | "failed" | "waiting_user" | "waiting_event" | "rejected"
    ) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output has an unknown status",
        ));
    }
    let tool_calls = match result.get("tool_calls") {
        Some(value) => value.as_u64().ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Workflow output has an invalid Tool call count",
            )
        })?,
        None if status == "rejected" => 0,
        None => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Workflow output has no Tool call count",
            ))
        }
    };
    if status == "rejected" && tool_calls != 0 {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "rejected Workflow output cannot report executed Tool calls",
        ));
    }
    Ok(Some((
        expected_event_id,
        WorkflowCallObservation {
            result: result.clone(),
            is_error: status != "completed",
            tool_calls,
        },
    )))
}

pub(super) fn commit_workflow_attempt_started(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    request_id: &ModelRequestId,
    call_id: &ModelToolCallId,
    arguments: &str,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-workflow-started-{round}-{}",
            run_id.as_str(),
            call_id.as_str()
        )),
        GenericCheckpointEvent::WorkflowAttemptStarted {
            round,
            request_id: request_id.clone(),
            call_id: call_id.clone(),
            arguments_digest: Digest::sha256(arguments.as_bytes()),
        },
    )
}

pub(super) fn parse_tool_arguments(
    call: &PendingModelToolCall,
) -> Result<serde_json::Value, AgentFailure> {
    let raw = if call.arguments.trim().is_empty() {
        "{}"
    } else {
        call.arguments.as_str()
    };
    let arguments = serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
        agent_failure(
            "invalid_tool_arguments",
            format!(
                "model emitted invalid JSON arguments for {}: {error}",
                call.name
            ),
            false,
        )
    })?;
    if !arguments.is_object() {
        return Err(agent_failure(
            "invalid_tool_arguments",
            format!(
                "model Tool arguments for {} must be a JSON object",
                call.name
            ),
            false,
        ));
    }
    Ok(arguments)
}
