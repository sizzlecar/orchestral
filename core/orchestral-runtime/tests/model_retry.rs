use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    spi::{AgentProvider, AgentRecoveryRequest},
    wire::{
        AgentEvent, AgentProtocolErrorCode, AgentProviderStreamItem, AgentRunEnvelope,
        AgentSessionId, AgentStartRequest, Content, IncompleteReason, ProviderBindingRef, RunId,
    },
    AGENT_PROTOCOL_V1,
};
use orchestral_core::model_protocol::{
    ModelBackend, ModelDescriptor, ModelError, ModelErrorCode, ModelEvent, ModelEventId,
    ModelFinishReason, ModelRequest, ModelStream, ModelStreamEvent, ModelToolCallId, ModelUsage,
};
use orchestral_core::project_instructions::ProjectInstruction;
use orchestral_runtime::{
    AgentController, GenericAgentCheckpointStore, GenericAgentConfig, GenericCheckpointEvent,
    GenericCheckpointPhase, InMemoryGenericAgentCheckpointStore, InternalGenericAgentProvider,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum FailurePoint {
    Start,
    EmptyStream,
    BeforeFirstEvent,
    AfterUsage,
    AfterUsageUpdates,
    AfterText,
    AfterToolStart,
    InvalidUsageSequence,
    InvalidUsageRequest,
}

struct RetryModel {
    calls: AtomicUsize,
    point: FailurePoint,
    code: ModelErrorCode,
    failures: usize,
}

#[async_trait]
impl ModelBackend for RetryModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "test/retry".to_owned(),
            capabilities: Default::default(),
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        let error = ModelError::new(self.code.clone(), "transient failure").with_retryable(true);
        let first = self.calls.fetch_add(1, Ordering::SeqCst) < self.failures;
        let event = |sequence, payload| {
            Ok(ModelStreamEvent {
                request_id: request.request_id.clone(),
                event_id: ModelEventId::new(format!("event-{sequence}")),
                sequence,
                payload,
            })
        };
        if first {
            match self.point {
                FailurePoint::Start => return Err(error),
                FailurePoint::EmptyStream => return Ok(stream::empty().boxed()),
                FailurePoint::BeforeFirstEvent => return Ok(stream::iter([Err(error)]).boxed()),
                FailurePoint::AfterUsage => {
                    return Ok(stream::iter([
                        event(
                            1,
                            ModelEvent::Usage {
                                usage: ModelUsage {
                                    input_tokens: Some(1),
                                    output_tokens: Some(1),
                                },
                            },
                        ),
                        Err(error),
                    ])
                    .boxed())
                }
                FailurePoint::AfterUsageUpdates => {
                    return Ok(stream::iter([
                        event(
                            1,
                            ModelEvent::Usage {
                                usage: ModelUsage {
                                    input_tokens: Some(1),
                                    output_tokens: Some(1),
                                },
                            },
                        ),
                        event(
                            2,
                            ModelEvent::Usage {
                                usage: ModelUsage {
                                    input_tokens: Some(4),
                                    output_tokens: Some(5),
                                },
                            },
                        ),
                        Err(error),
                    ])
                    .boxed())
                }
                FailurePoint::AfterText | FailurePoint::AfterToolStart => {
                    let payload = if matches!(self.point, FailurePoint::AfterText) {
                        ModelEvent::TextDelta {
                            delta: "partial".to_owned(),
                        }
                    } else {
                        ModelEvent::ToolCallStart {
                            call_id: ModelToolCallId::new("call"),
                            name: "tool".to_owned(),
                            extensions: Default::default(),
                        }
                    };
                    return Ok(stream::iter([
                        event(
                            1,
                            ModelEvent::Usage {
                                usage: ModelUsage::default(),
                            },
                        ),
                        event(2, payload),
                        Err(error),
                    ])
                    .boxed());
                }
                FailurePoint::InvalidUsageSequence | FailurePoint::InvalidUsageRequest => {
                    let mut invalid = event(
                        1,
                        ModelEvent::Usage {
                            usage: ModelUsage::default(),
                        },
                    )
                    .unwrap();
                    if matches!(self.point, FailurePoint::InvalidUsageSequence) {
                        invalid.sequence = 2;
                    } else {
                        invalid.request_id =
                            orchestral_core::model_protocol::ModelRequestId::new("wrong");
                    }
                    return Ok(stream::iter([Ok(invalid), Err(error)]).boxed());
                }
            }
        }
        Ok(stream::iter([
            event(
                1,
                ModelEvent::Usage {
                    usage: ModelUsage {
                        input_tokens: Some(2),
                        output_tokens: Some(3),
                    },
                },
            ),
            event(
                2,
                ModelEvent::TextDelta {
                    delta: "done".to_owned(),
                },
            ),
            event(
                3,
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ),
        ])
        .boxed())
    }
}

fn config() -> GenericAgentConfig {
    let mut config = GenericAgentConfig::new("retry-provider", "retry-agent");
    config.model_retry.base_delay_ms = 1;
    config.model_retry.max_delay_ms = 4;
    config
}

fn run() -> AgentRunEnvelope {
    AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("session"),
        RunId::new("run"),
        vec![Content::text("finish this task")],
    )
    .unwrap()
}

#[tokio::test]
async fn retries_usage_only_failures_but_never_replays_content_or_invalid_events() {
    for point in [
        FailurePoint::Start,
        FailurePoint::EmptyStream,
        FailurePoint::BeforeFirstEvent,
        FailurePoint::AfterUsage,
        FailurePoint::AfterUsageUpdates,
        FailurePoint::AfterText,
        FailurePoint::AfterToolStart,
        FailurePoint::InvalidUsageSequence,
        FailurePoint::InvalidUsageRequest,
    ] {
        let model = Arc::new(RetryModel {
            calls: AtomicUsize::new(0),
            point,
            code: ModelErrorCode::Unavailable,
            failures: 1,
        });
        let provider =
            Arc::new(InternalGenericAgentProvider::new(model.clone(), config()).unwrap());
        let controller =
            Arc::new(AgentController::new(provider, ProviderBindingRef::new("binding")).unwrap());
        controller.start(run()).await.unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&RunId::new("run")),
        )
        .await
        .unwrap()
        .unwrap();
        if matches!(
            point,
            FailurePoint::AfterText
                | FailurePoint::AfterToolStart
                | FailurePoint::InvalidUsageSequence
                | FailurePoint::InvalidUsageRequest
        ) {
            assert_eq!(view.state.status(), AgentRunStatus::Failed);
            assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(view.state.status(), AgentRunStatus::Delivered);
            assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        }
    }
}

#[tokio::test]
async fn usage_only_retries_keep_the_latest_snapshot_once_in_delivery_and_replay() {
    let store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let model = Arc::new(RetryModel {
        calls: AtomicUsize::new(0),
        point: FailurePoint::AfterUsageUpdates,
        code: ModelErrorCode::Unavailable,
        failures: 2,
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new(model.clone(), config())
            .unwrap()
            .with_checkpoint_store(store.clone())
            .unwrap(),
    );
    let controller =
        Arc::new(AgentController::new(provider, ProviderBindingRef::new("binding")).unwrap());
    let mut request = run();
    request.spec.limits.max_model_steps = Some(1);
    controller
        .start(AgentRunEnvelope::seal(request.spec).unwrap())
        .await
        .unwrap();
    let view = tokio::time::timeout(
        Duration::from_secs(2),
        controller.wait_for_terminal(&RunId::new("run")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    let usage = view.delivery.unwrap().usage.unwrap();
    assert_eq!(usage.input_tokens, Some(10));
    assert_eq!(usage.output_tokens, Some(13));

    let mut stored = store.load_run(&RunId::new("run")).unwrap().unwrap();
    let observed_index = stored
        .records
        .iter()
        .position(|record| {
            matches!(
                record.payload,
                GenericCheckpointEvent::ModelAttemptObserved { .. }
            )
        })
        .unwrap();
    stored.records.truncate(observed_index);
    assert!(matches!(stored.validate().unwrap().phase,
        GenericCheckpointPhase::ModelAttemptOpen { boundary, .. }
            if boundary.usage.input_tokens == Some(8) && boundary.usage.output_tokens == Some(10)));
    let encoded = serde_json::to_string(&stored).unwrap();
    let decoded: orchestral_runtime::StoredGenericAgentRun =
        serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.validate().unwrap(), stored.validate().unwrap());
}

#[tokio::test]
async fn usage_only_failures_respect_retry_limits_and_strict_usage_ceilings() {
    for (max_retries, limited, code, expected_calls) in [
        (0, false, ModelErrorCode::Unavailable, 1),
        (2, false, ModelErrorCode::Unavailable, 3),
        (2, false, ModelErrorCode::Authentication, 1),
        (2, true, ModelErrorCode::Unavailable, 1),
        (2, true, ModelErrorCode::RateLimited, 1),
    ] {
        let model = Arc::new(RetryModel {
            calls: AtomicUsize::new(0),
            point: FailurePoint::AfterUsageUpdates,
            code,
            failures: usize::MAX,
        });
        let mut config = config();
        config.model_retry.max_retries = max_retries;
        let provider = Arc::new(InternalGenericAgentProvider::new(model.clone(), config).unwrap());
        let controller =
            Arc::new(AgentController::new(provider, ProviderBindingRef::new("binding")).unwrap());
        let mut request = run();
        if limited {
            request.spec.limits.max_input_tokens = Some(100_000);
        }
        controller
            .start(AgentRunEnvelope::seal(request.spec).unwrap())
            .await
            .unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&RunId::new("run")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Failed);
        assert_eq!(model.calls.load(Ordering::SeqCst), expected_calls);
    }
}

#[tokio::test]
async fn usage_ceiling_prevents_reissuing_an_unaccounted_request_but_allows_rate_limit_retry() {
    for code in [ModelErrorCode::Unavailable, ModelErrorCode::RateLimited] {
        let rate_limited = code == ModelErrorCode::RateLimited;
        let model = Arc::new(RetryModel {
            calls: AtomicUsize::new(0),
            point: FailurePoint::Start,
            code,
            failures: 1,
        });
        let provider =
            Arc::new(InternalGenericAgentProvider::new(model.clone(), config()).unwrap());
        let controller =
            Arc::new(AgentController::new(provider, ProviderBindingRef::new("binding")).unwrap());
        let mut run = run();
        run.spec.limits.max_input_tokens = Some(100_000);
        controller
            .start(AgentRunEnvelope::seal(run.spec).unwrap())
            .await
            .unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&RunId::new("run")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            model.calls.load(Ordering::SeqCst),
            if rate_limited { 2 } else { 1 }
        );
        assert_eq!(
            view.state.status(),
            if rate_limited {
                AgentRunStatus::Delivered
            } else {
                AgentRunStatus::Failed
            }
        );
    }
}

#[tokio::test]
async fn cancellation_interrupts_usage_only_retry_backoff_without_another_request() {
    let store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let model = Arc::new(RetryModel {
        calls: AtomicUsize::new(0),
        point: FailurePoint::AfterUsage,
        code: ModelErrorCode::Unavailable,
        failures: 1,
    });
    let mut config = config();
    config.model_retry.base_delay_ms = 60_000;
    config.model_retry.max_delay_ms = 60_000;
    let provider = Arc::new(
        InternalGenericAgentProvider::new(model.clone(), config)
            .unwrap()
            .with_checkpoint_store(store.clone())
            .unwrap(),
    );
    let controller =
        Arc::new(AgentController::new(provider, ProviderBindingRef::new("binding")).unwrap());
    controller.start(run()).await.unwrap();
    let run_id = RunId::new("run");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stored = store.load_run(&run_id).unwrap().unwrap();
            if stored.records.iter().any(|record| {
                matches!(
                    record.payload,
                    GenericCheckpointEvent::ModelRetryScheduled { .. }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    controller
        .cancel(&run_id, "stop during retry backoff")
        .await
        .unwrap();
    let view = tokio::time::timeout(
        Duration::from_secs(2),
        controller.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Cancelled);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .load_run(&run_id)
            .unwrap()
            .unwrap()
            .validate()
            .unwrap()
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test]
async fn recovery_of_scheduled_retry_closes_interrupted_attempt_and_rejects_changed_instructions() {
    let store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let model = Arc::new(RetryModel {
        calls: AtomicUsize::new(0),
        point: FailurePoint::AfterUsage,
        code: ModelErrorCode::Unavailable,
        failures: 1,
    });
    let mut config = config();
    config.model_retry.base_delay_ms = 60_000;
    config.model_retry.max_delay_ms = 60_000;
    config.project_instructions = vec![ProjectInstruction {
        source: "/project/AGENTS.md".to_owned(),
        scope: "/project".to_owned(),
        content: "Use focused verification.".to_owned(),
    }];
    let first = InternalGenericAgentProvider::new(model.clone(), config.clone())
        .unwrap()
        .with_checkpoint_store(store.clone())
        .unwrap();
    let descriptor = first.describe();
    let request =
        AgentStartRequest::new(run(), ProviderBindingRef::new("binding"), &descriptor).unwrap();
    let started = first.start(request.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stored = store.load_run(&RunId::new("run")).unwrap().unwrap();
            if stored.records.iter().any(|record| {
                matches!(
                    record.payload,
                    GenericCheckpointEvent::ModelRetryScheduled { .. }
                )
            }) {
                assert!(matches!(
                    stored.validate().unwrap().phase,
                    GenericCheckpointPhase::ModelAttemptOpen { boundary, .. }
                        if boundary.usage.input_tokens == Some(1)
                            && boundary.usage.output_tokens == Some(1)
                ));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let recovery_request =
        AgentRecoveryRequest::new(request, started.execution, &descriptor).unwrap();
    let mut changed_config = config.clone();
    changed_config.project_instructions[0].content = "Changed repository policy.".to_owned();
    let changed = InternalGenericAgentProvider::new(model.clone(), changed_config)
        .unwrap()
        .with_checkpoint_store(store.clone())
        .unwrap();
    let Err(error) = changed.recover(recovery_request.clone()).await else {
        panic!("changed instruction snapshot must not recover the old Run");
    };
    assert_eq!(error.code, AgentProtocolErrorCode::RunIdConflict);

    let replacement = InternalGenericAgentProvider::new(model.clone(), config)
        .unwrap()
        .with_checkpoint_store(store.clone())
        .unwrap();
    let recovery = replacement.recover(recovery_request).await.unwrap();
    let (mut events, confirmation) = recovery.into_parts();
    confirmation.await.unwrap();
    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(item) = events.next().await {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if matches!(
                    event.payload,
                    AgentEvent::RunIncomplete {
                        reason: IncompleteReason::Interrupted { .. },
                        ..
                    }
                ) {
                    return;
                }
            }
        }
        panic!("missing interrupted terminal");
    })
    .await;
    assert!(terminal.is_ok());
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store
            .load_run(&RunId::new("run"))
            .unwrap()
            .unwrap()
            .validate()
            .unwrap()
            .phase,
        GenericCheckpointPhase::Terminal
    ));
    // The original task simulates a disappeared process and stays in its long
    // backoff until the isolated Tokio test runtime drops it.
}
