use super::*;

pub(super) enum DeliveryCommit {
    Committed,
    SteerPending,
    TerminationPending,
    CheckpointFailed,
    AlreadyTerminal,
}

pub(super) fn try_emit_delivery(
    inner: &GenericInner,
    request: &AgentStartRequest,
    response: String,
    usage: Option<ModelUsage>,
    tool_calls: u64,
    mut supporting_event_ids: Vec<AgentEventId>,
) -> DeliveryCommit {
    let run_id = &request.run.spec.run_id;
    let output_event_id = AgentEventId::new(format!("generic-{}-output", run_id.as_str()));
    let output = AgentEventDraft {
        event_id: output_event_id.clone(),
        run_id: run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload: AgentEvent::OutputCommitted {
            output_id: OutputId::new(format!("generic-{}-response", run_id.as_str())),
            content: vec![Content::text(response.clone())],
        },
    };
    supporting_event_ids.push(output_event_id);
    let delivery = AgentEventDraft {
        event_id: AgentEventId::new(format!("generic-{}-delivered", run_id.as_str())),
        run_id: run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload: AgentEvent::DeliveryCommitted {
            delivery: AgentDelivery {
                delivery_id: DeliveryId::new(format!("generic-{}-delivery", run_id.as_str())),
                run_id: run_id.clone(),
                spec_digest: request.run.spec_digest.clone(),
                final_response: Content::text(response),
                outputs: Vec::new(),
                artifacts: Vec::new(),
                unresolved_issues: Vec::new(),
                usage: agent_usage(inner, usage, tool_calls),
                provenance: provenance(inner, supporting_event_ids),
            },
        },
    };

    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(run) = state.runs.get_mut(run_id) else {
        return DeliveryCommit::AlreadyTerminal;
    };
    if run.terminal {
        return DeliveryCommit::AlreadyTerminal;
    }
    if run.stop_cause.load(Ordering::SeqCst) != RUN_STOP_RUNNING {
        return DeliveryCommit::TerminationPending;
    }
    if !run.queued_steers.is_empty() {
        return DeliveryCommit::SteerPending;
    }
    if run
        .stop_cause
        .compare_exchange(
            RUN_STOP_RUNNING,
            RUN_STOP_COMPLETING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_err()
    {
        return DeliveryCommit::TerminationPending;
    }
    if let Err(failure) =
        checkpoint_provider_events(inner, run, run_id, &[output.clone(), delivery.clone()])
    {
        poison_run_after_checkpoint_failure(run, failure);
        return DeliveryCommit::CheckpointFailed;
    }
    run.durable_events.push(output.clone());
    run.durable_events.push(delivery.clone());
    run.terminal = true;
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(output))));
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(delivery))));
    DeliveryCommit::Committed
}

pub(super) struct IncompleteRun {
    pub(super) response: String,
    pub(super) usage: Option<ModelUsage>,
    pub(super) tool_calls: u64,
    pub(super) started_event_id: AgentEventId,
    pub(super) limit: RunLimitKind,
    pub(super) unresolved_issue: &'static str,
}

pub(super) fn emit_limit_reached(
    inner: &GenericInner,
    request: &AgentStartRequest,
    response: String,
    usage: Option<ModelUsage>,
    tool_calls: u64,
    started_event_id: AgentEventId,
    limit: RunLimitKind,
) {
    let unresolved_issue = match limit {
        RunLimitKind::Deadline => "Run deadline reached",
        RunLimitKind::ModelSteps => "model step limit reached",
        RunLimitKind::ToolCalls => "Tool call limit reached",
        RunLimitKind::InputTokens => "model input token limit reached",
        RunLimitKind::OutputTokens => "model output token limit reached",
        RunLimitKind::Cost => "model cost limit reached",
        _ => "Run limit reached",
    };
    emit_incomplete(
        inner,
        request,
        IncompleteRun {
            response,
            usage,
            tool_calls,
            started_event_id,
            limit,
            unresolved_issue,
        },
    );
}

pub(super) fn emit_incomplete(
    inner: &GenericInner,
    request: &AgentStartRequest,
    incomplete: IncompleteRun,
) {
    let IncompleteRun {
        response,
        usage,
        tool_calls,
        started_event_id,
        limit,
        unresolved_issue,
    } = incomplete;
    let run_id = &request.run.spec.run_id;
    let partial_delivery = (!response.is_empty()).then(|| PartialDelivery {
        partial_delivery_id: PartialDeliveryId::new(format!("generic-{}-partial", run_id.as_str())),
        run_id: run_id.clone(),
        spec_digest: request.run.spec_digest.clone(),
        response: Some(Content::text(response)),
        outputs: Vec::new(),
        artifacts: Vec::new(),
        unresolved_issues: vec![unresolved_issue.to_owned()],
        usage: agent_usage(inner, usage, tool_calls),
        provenance: provenance(inner, vec![started_event_id]),
    });
    if publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!("generic-{}-incomplete", run_id.as_str())),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunIncomplete {
                reason: IncompleteReason::LimitReached { limit },
                partial_delivery,
            },
        },
    ) {
        finish_session(inner, request);
    }
}

pub(super) fn emit_failure(
    inner: &GenericInner,
    request: &AgentStartRequest,
    _user_message: &ModelMessage,
    failure: AgentFailure,
) {
    let run_id = &request.run.spec.run_id;
    if publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!("generic-{}-failed", run_id.as_str())),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunFailed { failure },
        },
    ) {
        finish_session(inner, request);
    }
}

pub(super) fn current_unix_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

pub(super) fn deadline_delay_ms(deadline_unix_ms: i64, now_unix_ms: i64) -> Option<u64> {
    let remaining_ms = deadline_unix_ms.saturating_sub(now_unix_ms);
    (remaining_ms > 0).then_some(remaining_ms as u64)
}

pub(super) fn arm_run_deadline(
    deadline_unix_ms: Option<i64>,
    cancellation: CancellationToken,
    stop_cause: Arc<AtomicU8>,
) {
    let Some(deadline_unix_ms) = deadline_unix_ms else {
        return;
    };
    let Some(remaining_ms) = deadline_delay_ms(deadline_unix_ms, current_unix_ms()) else {
        if stop_cause
            .compare_exchange(
                RUN_STOP_RUNNING,
                RUN_STOP_DEADLINE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            cancellation.cancel();
        }
        return;
    };
    tokio::spawn(async move {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(remaining_ms)) => {
                if stop_cause
                    .compare_exchange(
                        RUN_STOP_RUNNING,
                        RUN_STOP_DEADLINE,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    cancellation.cancel();
                }
            }
        }
    });
}

pub(super) fn emit_deadline_incomplete(inner: &GenericInner, request: &AgentStartRequest) {
    let run_id = &request.run.spec.run_id;
    if publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!("generic-{}-deadline", run_id.as_str())),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunIncomplete {
                reason: IncompleteReason::LimitReached {
                    limit: RunLimitKind::Deadline,
                },
                partial_delivery: None,
            },
        },
    ) {
        finish_session(inner, request);
    }
}

pub(super) fn emit_cancel(
    inner: &GenericInner,
    request: &AgentStartRequest,
    user_message: &ModelMessage,
) {
    let run_id = &request.run.spec.run_id;
    let (stop_cause, cancel_command) = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .runs
            .get(run_id)
            .map_or((RUN_STOP_RUNNING, None), |run| {
                (
                    run.stop_cause.load(Ordering::SeqCst),
                    run.cancel_command.clone(),
                )
            })
    };
    if stop_cause == RUN_STOP_DEADLINE {
        emit_deadline_incomplete(inner, request);
        return;
    }
    if stop_cause == RUN_STOP_HOST_CANCEL {
        let Some((command_id, reason)) = cancel_command else {
            emit_failure(
                inner,
                request,
                user_message,
                agent_failure(
                    "cancel_command_missing",
                    "Host cancellation won the termination race without a durable command",
                    true,
                ),
            );
            return;
        };
        if !publish_durable(
            inner,
            run_id,
            AgentEventDraft {
                event_id: AgentEventId::new(format!("generic-{}-stop", run_id.as_str())),
                run_id: run_id.clone(),
                causation_id: Some(command_id),
                source_fingerprint: None,
                payload: AgentEvent::StopRequested {
                    reason: reason.clone(),
                },
            },
        ) {
            return;
        }
        if !publish_durable(
            inner,
            run_id,
            AgentEventDraft {
                event_id: AgentEventId::new(format!("generic-{}-cancelled", run_id.as_str())),
                run_id: run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunCancelled { reason },
            },
        ) {
            return;
        }
    } else {
        emit_failure(
            inner,
            request,
            user_message,
            AgentFailure {
                code: "unexpected_model_cancellation".to_owned(),
                message: "model request cancelled without an Agent Cancel command".to_owned(),
                retryable: true,
                details: serde_json::Value::Null,
            },
        );
        return;
    }
    finish_session(inner, request);
}

pub(super) fn finish_session(inner: &GenericInner, request: &AgentStartRequest) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cancellation = state
        .runs
        .get(&request.run.spec.run_id)
        .map(|run| run.cancellation.clone());
    let session = state
        .sessions
        .entry(request.run.spec.session_id.clone())
        .or_default();
    if session.active_run.as_ref() == Some(&request.run.spec.run_id) {
        session.active_run = None;
    }
    drop(state);
    if let Some(cancellation) = cancellation {
        cancellation.cancel();
    }
}

pub(super) fn provenance(
    inner: &GenericInner,
    supporting_event_ids: Vec<AgentEventId>,
) -> Provenance {
    Provenance {
        provider_id: inner.config.provider_id.clone(),
        agent_id: inner.config.agent_id.clone(),
        supporting_event_ids,
        extensions: Default::default(),
    }
}

pub(super) fn agent_usage(
    inner: &GenericInner,
    usage: Option<ModelUsage>,
    tool_calls: u64,
) -> Option<UsageReport> {
    if usage.is_none() && tool_calls == 0 {
        return None;
    }
    let usage = usage.unwrap_or_default();
    let cost = inner.config.model_cost_policy.as_ref().map(|policy| {
        policy.quote(
            usage.input_tokens.unwrap_or(0),
            usage.output_tokens.unwrap_or(0),
        )
    });
    Some(UsageReport {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        tool_calls: (tool_calls > 0).then_some(tool_calls),
        cost,
    })
}

pub(super) fn agent_failure(
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> AgentFailure {
    AgentFailure {
        code: code.into(),
        message: message.into(),
        retryable,
        details: serde_json::Value::Null,
    }
}
