//! Deterministic command and cancellation gates for the public reference API.

use std::collections::BTreeSet;

use orchestral_core::agent_protocol::{
    reference::{AgentRunReducer, AgentRunStatus, ApplyOutcome, SequencedApply},
    wire::{
        AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
        AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
        AgentExecutionRef, AgentId, AgentProtocolError, AgentProtocolErrorCode, AgentProviderId,
        AgentRunEnvelope, AgentSessionId, AgentStartRequest, ApprovalDecision, CancelSupport,
        CommandAckState, CommandId, Content, ControlCapabilities, DeliveryId, OutputId,
        PendingRequest, PendingRequestKind, PendingRequestPayload, Provenance, ProviderBindingRef,
        ProviderCommandOutcome, RequestId, RequestResolution, RunId,
    },
    AGENT_PROTOCOL_V1,
};

struct Fixture {
    run: AgentRunEnvelope,
    reducer: AgentRunReducer,
    event_index: usize,
}

impl Fixture {
    fn running(case: usize) -> Self {
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("testkit.control"),
            agent_id: AgentId::new("reference-agent-v1"),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                controls: ControlCapabilities {
                    steer: true,
                    cancel: CancelSupport::Confirmed,
                    recover: true,
                },
                pending_request_kinds: BTreeSet::from([
                    PendingRequestKind::Input,
                    PendingRequestKind::Approval,
                    PendingRequestKind::ExternalAction,
                ]),
                ..AgentCapabilities::default()
            },
            extensions: Default::default(),
        })
        .expect("fixture descriptor seals");
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new(format!("control-session-{case}")),
            RunId::new(format!("control-run-{case}")),
            vec![Content::text("exercise the control protocol")],
        )
        .expect("fixture run seals");
        let request = AgentStartRequest::new(
            run.clone(),
            ProviderBindingRef::new("testkit-control-binding"),
            &descriptor,
        )
        .expect("start request binds descriptor");
        let execution =
            AgentExecutionRef::for_start(&request, &descriptor).expect("execution binds start");
        let reducer =
            AgentRunReducer::new(execution, &request, &descriptor, AgentAdmission::default())
                .expect("reference reducer initializes");
        let mut fixture = Self {
            run,
            reducer,
            event_index: 0,
        };
        fixture
            .host(
                AgentEvent::RunAccepted {
                    session_id: fixture.run.spec.session_id.clone(),
                    spec_digest: fixture.run.spec_digest.clone(),
                },
                None,
            )
            .expect("Host accepts run");
        fixture
            .provider(AgentEvent::RunStarted, None)
            .expect("Provider starts run");
        assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Running);
        fixture
    }

    fn draft(&mut self, payload: AgentEvent, causation_id: Option<CommandId>) -> AgentEventDraft {
        self.event_index += 1;
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "{}-event-{}",
                self.run.spec.run_id.as_str(),
                self.event_index
            )),
            run_id: self.run.spec.run_id.clone(),
            causation_id,
            source_fingerprint: None,
            payload,
        }
    }

    fn host(
        &mut self,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> Result<SequencedApply, AgentProtocolError> {
        let draft = self.draft(payload, causation_id);
        self.reducer.apply_host_draft(draft)
    }

    fn provider(
        &mut self,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> Result<SequencedApply, AgentProtocolError> {
        let draft = self.draft(payload, causation_id);
        self.reducer.apply_provider_draft(draft)
    }

    fn command(&self, id: &str, payload: AgentCommand) -> AgentCommandEnvelope {
        AgentCommandEnvelope::new(
            CommandId::new(id),
            self.run.spec.run_id.clone(),
            None,
            payload,
        )
        .expect("command seals")
    }

    fn receive_and_accept(&mut self, command: &AgentCommandEnvelope) {
        let received = self
            .host(
                AgentEvent::CommandReceived {
                    command: command.clone(),
                },
                Some(command.command_id.clone()),
            )
            .expect("command received sequences");
        assert!(matches!(received.outcome, ApplyOutcome::Applied));
        let disposition = self
            .provider(
                AgentEvent::CommandDispositionRecorded {
                    command_id: command.command_id.clone(),
                    outcome: ProviderCommandOutcome::Accepted,
                },
                Some(command.command_id.clone()),
            )
            .expect("Provider disposition sequences");
        assert!(matches!(disposition.outcome, ApplyOutcome::Applied));
    }

    fn delivery(&self, suffix: usize) -> AgentDelivery {
        AgentDelivery {
            delivery_id: DeliveryId::new(format!("delivery-{suffix}")),
            run_id: self.run.spec.run_id.clone(),
            spec_digest: self.run.spec_digest.clone(),
            final_response: Content::text("done"),
            outputs: Vec::new(),
            artifacts: Vec::new(),
            unresolved_issues: Vec::new(),
            usage: None,
            provenance: Provenance {
                provider_id: AgentProviderId::new("testkit.control"),
                agent_id: AgentId::new("reference-agent-v1"),
                supporting_event_ids: Vec::new(),
                extensions: Default::default(),
            },
        }
    }
}

fn assert_not_applied(
    result: Result<SequencedApply, AgentProtocolError>,
) -> Option<AgentProtocolErrorCode> {
    match result {
        Err(error) => Some(error.code),
        Ok(sequenced) => match sequenced.outcome {
            ApplyOutcome::Applied => panic!("invalid command entered the normal projection"),
            ApplyOutcome::ProtocolViolation { error } => Some(error.code),
            ApplyOutcome::ExactDuplicate => {
                panic!("a freshly generated invalid command was treated as an exact duplicate")
            }
            _ => panic!("invalid command returned an unknown apply outcome"),
        },
    }
}

#[test]
fn one_hundred_command_replays_forward_and_apply_exactly_once() {
    let mut fixture = Fixture::running(0);
    let command = fixture.command(
        "steer-once",
        AgentCommand::Steer {
            content: vec![Content::text("use the stable route")],
        },
    );
    let mut forwarded = 0;
    let mut applied_effects = 0;

    for replay_index in 0..100 {
        match fixture
            .reducer
            .command_ack(&command.command_id, replay_index > 0)
        {
            Ok(ack) => {
                assert!(ack.duplicate);
                assert!(matches!(ack.state, CommandAckState::Applied { .. }));
            }
            Err(error) if error.code == AgentProtocolErrorCode::CommandNotFound => {
                assert_eq!(replay_index, 0);
                forwarded += 1;
                fixture.receive_and_accept(&command);
                let effect = fixture
                    .provider(
                        AgentEvent::InputCommitted {
                            content: vec![Content::text("use the stable route")],
                        },
                        Some(command.command_id.clone()),
                    )
                    .expect("causal effect sequences");
                assert!(matches!(effect.outcome, ApplyOutcome::Applied));
                applied_effects += 1;
            }
            Err(error) => panic!("unexpected command query failure: {error}"),
        }
    }

    assert_eq!(forwarded, 1);
    assert_eq!(applied_effects, 1);
    assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Running);
}

#[test]
fn one_thousand_wrong_command_mutations_enter_normal_projection_zero_times() {
    let mut rejected = 0;
    for case in 0..1_000 {
        let mut fixture = Fixture::running(case + 1);
        let family = case % 5;
        let result = match family {
            0 => {
                let command = AgentCommandEnvelope::new(
                    CommandId::new(format!("wrong-run-{case}")),
                    RunId::new(format!("foreign-run-{case}")),
                    None,
                    AgentCommand::Cancel {
                        reason: "stop".to_owned(),
                    },
                )
                .expect("foreign command is structurally valid");
                fixture.host(
                    AgentEvent::CommandReceived {
                        command: command.clone(),
                    },
                    Some(command.command_id),
                )
            }
            1 => {
                let resolution = RequestResolution::Input {
                    content: vec![Content::text("answer")],
                };
                let command = AgentCommandEnvelope::new(
                    CommandId::new(format!("missing-request-{case}")),
                    fixture.run.spec.run_id.clone(),
                    Some(RequestId::new(format!("missing-{case}"))),
                    AgentCommand::ResolveRequest {
                        response: resolution,
                    },
                )
                .expect("missing request command is structurally valid");
                fixture.host(
                    AgentEvent::CommandReceived {
                        command: command.clone(),
                    },
                    Some(command.command_id),
                )
            }
            2 => {
                let request_id = RequestId::new(format!("input-request-{case}"));
                let opened = fixture
                    .provider(
                        AgentEvent::RequestOpened {
                            request: PendingRequest {
                                request_id: request_id.clone(),
                                blocking: true,
                                payload: PendingRequestPayload::Input {
                                    prompt: vec![Content::text("provide input")],
                                    input_schema: None,
                                },
                            },
                        },
                        None,
                    )
                    .expect("input request opens");
                assert!(matches!(opened.outcome, ApplyOutcome::Applied));
                let command = AgentCommandEnvelope::new(
                    CommandId::new(format!("wrong-type-{case}")),
                    fixture.run.spec.run_id.clone(),
                    Some(request_id),
                    AgentCommand::ResolveRequest {
                        response: RequestResolution::Approval {
                            decision: ApprovalDecision::Deny,
                            grant_ref: None,
                        },
                    },
                )
                .expect("wrong-type command is structurally valid");
                fixture.host(
                    AgentEvent::CommandReceived {
                        command: command.clone(),
                    },
                    Some(command.command_id),
                )
            }
            3 => {
                let mut command = fixture.command(
                    &format!("bad-digest-{case}"),
                    AgentCommand::Steer {
                        content: vec![Content::text("original")],
                    },
                );
                command.payload = AgentCommand::Steer {
                    content: vec![Content::text("mutated")],
                };
                fixture.host(
                    AgentEvent::CommandReceived {
                        command: command.clone(),
                    },
                    Some(command.command_id),
                )
            }
            _ => {
                let mut command = fixture.command(
                    &format!("bad-shape-{case}"),
                    AgentCommand::Cancel {
                        reason: "stop".to_owned(),
                    },
                );
                command.request_id = Some(RequestId::new("request-not-allowed-on-cancel"));
                fixture.host(
                    AgentEvent::CommandReceived {
                        command: command.clone(),
                    },
                    Some(command.command_id),
                )
            }
        };
        assert!(assert_not_applied(result).is_some());
        rejected += 1;
    }
    assert_eq!(rejected, 1_000);
}

#[test]
fn one_thousand_cancel_complete_races_commit_one_terminal_and_no_tail_effect() {
    let mut delivered = 0;
    let mut cancelled = 0;
    let mut terminal_tail_accepts = 0;
    let mut post_terminal_output_accepts = 0;

    for case in 0..1_000 {
        let mut fixture = Fixture::running(case + 10_000);
        let command = fixture.command(
            &format!("cancel-{case}"),
            AgentCommand::Cancel {
                reason: "user requested cancellation".to_owned(),
            },
        );
        fixture.receive_and_accept(&command);
        assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Running);
        assert!(matches!(
            fixture
                .reducer
                .command_ack(&command.command_id, false)
                .expect("accepted command has an ack")
                .state,
            CommandAckState::Accepted { .. }
        ));

        let stopped = fixture
            .provider(
                AgentEvent::StopRequested {
                    reason: "user requested cancellation".to_owned(),
                },
                Some(command.command_id.clone()),
            )
            .expect("stop request sequences");
        assert!(matches!(stopped.outcome, ApplyOutcome::Applied));
        assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Stopping);
        assert!(matches!(
            fixture
                .reducer
                .command_ack(&command.command_id, false)
                .expect("causal stop updates ack")
                .state,
            CommandAckState::Applied { .. }
        ));

        let (winner, loser) = if case % 2 == 0 {
            delivered += 1;
            (
                AgentEvent::DeliveryCommitted {
                    delivery: fixture.delivery(case),
                },
                AgentEvent::RunCancelled {
                    reason: "native cancellation completed late".to_owned(),
                },
            )
        } else {
            cancelled += 1;
            (
                AgentEvent::RunCancelled {
                    reason: "native cancellation won".to_owned(),
                },
                AgentEvent::DeliveryCommitted {
                    delivery: fixture.delivery(case),
                },
            )
        };
        let terminal = fixture
            .provider(winner, None)
            .expect("first competing terminal commits");
        assert!(matches!(terminal.outcome, ApplyOutcome::Applied));
        assert!(fixture.reducer.state().is_terminal());
        let terminal_view_digest = fixture
            .reducer
            .view()
            .projection_digest()
            .expect("terminal view is valid");

        if fixture.provider(loser, None).is_ok() {
            terminal_tail_accepts += 1;
        }
        if fixture
            .provider(
                AgentEvent::OutputCommitted {
                    output_id: OutputId::new(format!("late-output-{case}")),
                    content: vec![Content::text("late side effect")],
                },
                None,
            )
            .is_ok()
        {
            post_terminal_output_accepts += 1;
        }
        assert_eq!(
            fixture
                .reducer
                .view()
                .projection_digest()
                .expect("terminal view remains valid"),
            terminal_view_digest
        );
    }

    assert_eq!(delivered, 500);
    assert_eq!(cancelled, 500);
    assert_eq!(terminal_tail_accepts, 0);
    assert_eq!(post_terminal_output_accepts, 0);
}
