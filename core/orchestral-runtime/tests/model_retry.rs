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
    ModelFinishReason, ModelRequest, ModelStream, ModelStreamEvent, ModelUsage,
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
}

struct RetryModel {
    calls: AtomicUsize,
    point: FailurePoint,
    code: ModelErrorCode,
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
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
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
            }
        }
        Ok(stream::iter([
            event(
                1,
                ModelEvent::TextDelta {
                    delta: "done".to_owned(),
                },
            ),
            event(
                2,
                ModelEvent::Usage {
                    usage: ModelUsage {
                        input_tokens: Some(1),
                        output_tokens: Some(1),
                    },
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
async fn retries_start_and_empty_stream_failures_but_stops_after_usage_is_observed() {
    for point in [
        FailurePoint::Start,
        FailurePoint::EmptyStream,
        FailurePoint::BeforeFirstEvent,
        FailurePoint::AfterUsage,
    ] {
        let model = Arc::new(RetryModel {
            calls: AtomicUsize::new(0),
            point,
            code: ModelErrorCode::Unavailable,
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
        if matches!(point, FailurePoint::AfterUsage) {
            assert_eq!(view.state.status(), AgentRunStatus::Failed);
            assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(view.state.status(), AgentRunStatus::Delivered);
            assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        }
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
async fn recovery_of_scheduled_retry_closes_interrupted_attempt_and_rejects_changed_instructions() {
    let store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let model = Arc::new(RetryModel {
        calls: AtomicUsize::new(0),
        point: FailurePoint::Start,
        code: ModelErrorCode::Unavailable,
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
                    GenericCheckpointPhase::ModelAttemptOpen { .. }
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
