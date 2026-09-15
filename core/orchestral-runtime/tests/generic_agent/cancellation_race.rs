use super::*;
use futures_util::FutureExt;
use orchestral_core::model_protocol::ModelErrorCode;

type CancelOnPoll = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

#[derive(Clone, Copy)]
enum Tail {
    Error,
    Eof,
    Finish,
}

struct CancelRaceModel {
    cancel_on_poll: CancelOnPoll,
    tail: Tail,
}

#[async_trait]
impl ModelBackend for CancelRaceModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "cancel-race".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..Default::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        let cancel = self.cancel_on_poll.clone();
        let tail = self.tail;
        let mut sequence = 0;
        Ok(stream::poll_fn(move |_| {
            sequence += 1;
            let payload = if sequence == 1 {
                ModelEvent::TextDelta {
                    delta: "Partial response".to_owned(),
                }
            } else {
                // Cancellation becomes ready inside the selected stream poll,
                // after the cancellation branch could have returned Pending.
                if let Some(cancel) = cancel.lock().unwrap().take() {
                    cancel();
                }
                match tail {
                    Tail::Error => {
                        return std::task::Poll::Ready(Some(Err(ModelError::new(
                            ModelErrorCode::Cancelled,
                            "model request cancelled",
                        ))));
                    }
                    Tail::Eof => return std::task::Poll::Ready(None),
                    Tail::Finish => ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                }
            };
            std::task::Poll::Ready(Some(Ok(ModelStreamEvent {
                request_id: request.request_id.clone(),
                event_id: ModelEventId::new(format!("cancel-race-{sequence}")),
                sequence,
                payload,
            })))
        })
        .boxed())
    }
}

async fn run_race(tail: Tail, host_cancel: bool) -> Vec<AgentEvent> {
    let callback: CancelOnPoll = Default::default();
    let checkpoints = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(CancelRaceModel {
                cancel_on_poll: callback.clone(),
                tail,
            }),
            GenericAgentConfig::new("cancel-provider", "cancel-agent"),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap(),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("cancel-session"),
        RunId::new("cancel-run"),
        vec![Content::text("Begin work")],
    )
    .unwrap();
    let run_id = run.spec.run_id.clone();
    let request = AgentStartRequest::new(
        run,
        ProviderBindingRef::new("cancel-binding"),
        &provider.describe(),
    )
    .unwrap();
    let mut started = provider.start(request).await.unwrap();
    if host_cancel {
        let execution = started.execution.clone();
        let weak = Arc::downgrade(&provider);
        *callback.lock().unwrap() = Some(Box::new(move || {
            let disposition = weak
                .upgrade()
                .unwrap()
                .command(
                    &execution,
                    AgentCommandEnvelope::new(
                        CommandId::new("cancel-during-stream-poll"),
                        execution.run_id.clone(),
                        None,
                        AgentCommand::Cancel {
                            reason: "user interrupted".to_owned(),
                        },
                    )
                    .unwrap(),
                )
                .now_or_never()
                .expect("in-memory Host cancellation commits in the same poll")
                .unwrap();
            assert_eq!(disposition.outcome, ProviderCommandOutcome::Accepted);
        }));
    }
    let events = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut events = Vec::new();
        while let Some(item) = started.stream.next().await {
            if let AgentProviderStreamItem::Event(draft) = item.unwrap() {
                let terminal = matches!(
                    draft.payload,
                    AgentEvent::RunCancelled { .. }
                        | AgentEvent::RunFailed { .. }
                        | AgentEvent::DeliveryCommitted { .. }
                );
                events.push(draft.payload);
                if terminal {
                    break;
                }
            }
        }
        events
    })
    .await
    .unwrap();
    assert_eq!(
        checkpoints
            .load_run(&run_id)
            .unwrap()
            .unwrap()
            .validate()
            .unwrap()
            .phase,
        GenericCheckpointPhase::Terminal
    );
    events
}

#[tokio::test]
async fn accepted_host_cancel_wins_over_a_ready_model_error_or_eof() {
    for tail in [Tail::Error, Tail::Eof, Tail::Finish] {
        let events = run_race(tail, true).await;
        assert!(events
            .iter()
            .any(|event| matches!(event, AgentEvent::RunCancelled { .. })));
        assert!(!events.iter().any(|event| matches!(
            event,
            AgentEvent::RunFailed { .. } | AgentEvent::DeliveryCommitted { .. }
        )));
    }
}

#[tokio::test]
async fn provider_cancel_error_without_host_cancel_remains_a_failure() {
    let events = run_race(Tail::Error, false).await;
    assert!(events.iter().any(|event| matches!(event,
        AgentEvent::RunFailed { failure } if failure.code == "model_cancelled"
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::RunCancelled { .. })));
}
