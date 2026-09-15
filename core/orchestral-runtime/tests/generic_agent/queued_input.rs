use super::*;
use orchestral_core::agent_protocol::wire::QueuedInputOperation;

#[derive(Default)]
struct PausedQueueModel {
    requests: Mutex<Vec<ModelRequest>>,
    cancellations: Mutex<Vec<CancellationToken>>,
    started: Notify,
    release: Notify,
}

#[async_trait]
impl ModelBackend for PausedQueueModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "queued-input-model".into(),
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
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.requests.lock().unwrap().push(request.clone());
        self.cancellations.lock().unwrap().push(cancellation);
        self.started.notify_one();
        self.release.notified().await;
        let payloads = [
            ModelEvent::Usage {
                usage: ModelUsage {
                    input_tokens: Some(123),
                    output_tokens: Some(1),
                },
            },
            ModelEvent::TextDelta {
                delta: "Current response completed".into(),
            },
            ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            },
        ];
        Ok(Box::pin(stream::iter(
            payloads
                .into_iter()
                .enumerate()
                .map(move |(index, payload)| {
                    Ok(ModelStreamEvent {
                        request_id: request.request_id.clone(),
                        event_id: ModelEventId::new(format!("event-{index}")),
                        sequence: index as u64 + 1,
                        payload,
                    })
                }),
        )))
    }
}

async fn reached_model(model: &PausedQueueModel) {
    tokio::time::timeout(std::time::Duration::from_secs(2), model.started.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn queued_input_edits_wait_for_the_model_boundary_and_consumption_is_final() {
    let model = Arc::new(PausedQueueModel::default());
    let checkpoints = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            model.clone(),
            GenericAgentConfig::new("provider", "agent"),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap(),
    );
    assert!(QueuedInputOperation::is_supported(
        &provider.describe().descriptor
    ));
    let controller =
        Arc::new(AgentController::new(provider, ProviderBindingRef::new("binding")).unwrap());
    let client = AgentClient::new(controller.clone(), AgentSessionId::new("queue-session"));
    let run_id = RunId::new("queue-run");
    let handle = client
        .start_with_run_id(run_id.clone(), vec![Content::text("Initial task")])
        .await
        .unwrap();
    reached_model(&model).await;
    let operations = [
        ("a", QueuedInputOperation::Enqueue, "Original requirement"),
        (
            "b",
            QueuedInputOperation::Enqueue,
            "Withdraw this requirement",
        ),
        (
            "a2",
            QueuedInputOperation::Replace {
                target: CommandId::new("a"),
            },
            "First revision",
        ),
        (
            "a3",
            QueuedInputOperation::Replace {
                target: CommandId::new("a2"),
            },
            "Final requirement",
        ),
        (
            "withdraw",
            QueuedInputOperation::Withdraw {
                target: CommandId::new("b"),
            },
            "Withdraw this requirement",
        ),
        ("c", QueuedInputOperation::Enqueue, "Additional requirement"),
    ];
    for (id, operation, text) in operations {
        let command = operation
            .command(
                CommandId::new(id),
                run_id.clone(),
                vec![Content::text(text)],
            )
            .unwrap();
        for _ in 0..2 {
            let ack = handle.command(command.clone()).await.unwrap();
            assert!(matches!(
                ack.state,
                CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
            ));
        }
    }
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert!(!model.cancellations.lock().unwrap()[0].is_cancelled());
    assert!(!handle
        .events(0)
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(record.event.payload, AgentEvent::InputCommitted { .. })));
    model.release.notify_one();
    reached_model(&model).await;
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    let contents = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| {
            if let ModelContent::Text { text } = content {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    for expected in [
        "Initial task",
        "Current response completed",
        "Final requirement",
        "Additional requirement",
    ] {
        assert_eq!(
            contents.iter().filter(|text| **text == expected).count(),
            1,
            "{contents:?}"
        );
    }
    for absent in [
        "Original requirement",
        "First revision",
        "Withdraw this requirement",
    ] {
        assert!(!contents.contains(&absent), "{contents:?}");
    }
    let late = QueuedInputOperation::Replace {
        target: CommandId::new("a3"),
    }
    .command(
        CommandId::new("too-late"),
        run_id.clone(),
        vec![Content::text("Late edit")],
    )
    .unwrap();
    assert!(matches!(
        handle.command(late).await.unwrap().state,
        CommandAckState::Rejected { .. }
    ));
    let committed = handle
        .events(0)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|record| {
            matches!(record.event.payload, AgentEvent::InputCommitted { .. })
                .then_some(record.event.causation_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        committed,
        vec![Some(CommandId::new("a3")), Some(CommandId::new("c"))]
    );
    model.release.notify_one();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        controller.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.state.status(), AgentRunStatus::Delivered);
    checkpoints
        .load_run(&run_id)
        .unwrap()
        .unwrap()
        .validate()
        .unwrap();
}
