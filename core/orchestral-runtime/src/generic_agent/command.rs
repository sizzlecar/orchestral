use super::*;

impl InternalGenericAgentProvider {
    pub(super) async fn apply_command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        command.verify_digest()?;
        let approval_bridge = self
            .inner
            .tools
            .as_ref()
            .and_then(|tools| tools.approval_bridge.clone());

        let (request_id, resolution, binding) = {
            let mut state = self.state();
            let run = state.runs.get_mut(&command.run_id).ok_or_else(|| {
                AgentProtocolError::new(AgentProtocolErrorCode::RunNotFound, "run does not exist")
            })?;
            validate_execution_and_duplicate(run, execution, &command)?;
            if let Some(existing) = run.commands.get(&command.command_id) {
                return Ok(ProviderCommandDisposition {
                    command_id: command.command_id,
                    run_id: command.run_id,
                    outcome: existing.outcome.clone(),
                    duplicate: true,
                });
            }
            if !run.terminal && run.stop_cause.load(Ordering::SeqCst) != RUN_STOP_RUNNING {
                return record_command(
                    &self.inner,
                    run,
                    &command,
                    ProviderCommandOutcome::Rejected {
                        code: AgentProtocolErrorCode::InvalidTransition,
                        message: "Run termination is already in progress".to_owned(),
                    },
                );
            }

            match &command.payload {
                AgentCommand::Cancel { .. } if run.terminal => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::TerminalRun,
                            message: "Run is already terminal".to_owned(),
                        },
                    );
                }
                AgentCommand::Cancel { reason } => {
                    if run
                        .stop_cause
                        .compare_exchange(
                            RUN_STOP_RUNNING,
                            RUN_STOP_HOST_CANCEL,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::InvalidTransition,
                                message: "Run termination is already in progress".to_owned(),
                            },
                        );
                    }
                    let disposition = record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Accepted,
                    )?;
                    run.cancel_command = Some((command.command_id.clone(), reason.clone()));
                    run.cancellation.cancel();
                    return Ok(disposition);
                }
                AgentCommand::Steer { .. } if run.terminal => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::TerminalRun,
                            message: "Run is already terminal".to_owned(),
                        },
                    );
                }
                AgentCommand::Steer { .. } if run.cancel_command.is_some() => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::InvalidTransition,
                            message: "Run cancellation is already in progress".to_owned(),
                        },
                    );
                }
                AgentCommand::Steer { content } => {
                    let message = match agent_content_message(content) {
                        Ok(message) => message,
                        Err(error) => {
                            return record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: error.code,
                                    message: error.message,
                                },
                            );
                        }
                    };
                    if run.queued_steers.len() >= self.inner.config.stream_buffer {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::InvalidTransition,
                                message: "Steer input buffer is full".to_owned(),
                            },
                        );
                    }
                    let disposition = record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Accepted,
                    )?;
                    run.queued_steers.push_back(QueuedSteer {
                        command_id: command.command_id.clone(),
                        content: content.clone(),
                        message,
                    });
                    let signal = run.steer_signal.clone();
                    drop(state);
                    signal.send_modify(|generation| {
                        *generation = generation.saturating_add(1);
                    });
                    return Ok(disposition);
                }
                AgentCommand::ResolveRequest { response } => {
                    let Some(request_id) = command.request_id.as_ref() else {
                        unreachable!("validated ResolveRequest always carries request_id")
                    };
                    if run.terminal {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::TerminalRun,
                                message: "Run is already terminal".to_owned(),
                            },
                        );
                    }
                    if let RequestResolution::Input { content } = response {
                        if let Err(error) = agent_content_message(content) {
                            return record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: error.code,
                                    message: error.message,
                                },
                            );
                        }
                        let Some(pending) = run.pending_inputs.get(request_id) else {
                            return record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: AgentProtocolErrorCode::RequestNotFound,
                                    message: "input request is not pending".to_owned(),
                                },
                            );
                        };
                        if pending
                            .responder
                            .as_ref()
                            .is_none_or(|responder| responder.is_closed())
                        {
                            let disposition = record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: AgentProtocolErrorCode::InvalidTransition,
                                    message: "input waiter is no longer active".to_owned(),
                                },
                            )?;
                            run.pending_inputs.remove(request_id);
                            return Ok(disposition);
                        }
                        let disposition = record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Accepted,
                        )?;
                        let mut pending = run
                            .pending_inputs
                            .remove(request_id)
                            .expect("pending input was checked before its command commit");
                        if let Some(responder) = pending.responder.take() {
                            let _ = responder.send(InputResponse {
                                command_id: command.command_id.clone(),
                                resolution: response.clone(),
                            });
                        }
                        return Ok(disposition);
                    }
                    if !matches!(response, RequestResolution::Approval { .. }) {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::RequestTypeMismatch,
                                message:
                                    "request resolution kind is not pending in this Generic Agent"
                                        .to_owned(),
                            },
                        );
                    }
                    let Some(pending) = run.pending_approvals.get(request_id) else {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::RequestNotFound,
                                message: "approval request is not pending".to_owned(),
                            },
                        );
                    };
                    if approval_bridge.is_none() {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Unsupported {
                                feature: "approval".to_owned(),
                            },
                        );
                    }
                    (
                        request_id.clone(),
                        response.clone(),
                        pending.binding.clone(),
                    )
                }
                _ => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Unsupported {
                            feature: "unknown_command".to_owned(),
                        },
                    );
                }
            }
        };

        let bridge = approval_bridge.expect("approval bridge presence was checked");
        let capability = match &resolution {
            RequestResolution::Approval {
                decision: ApprovalDecision::Allow,
                grant_ref: Some(grant_ref),
            } => match bridge.resolve(&request_id, grant_ref, &binding).await {
                Ok(capability) => Some(capability),
                Err(error) => {
                    let mut state = self.state();
                    let run = state.runs.get_mut(&command.run_id).ok_or_else(|| {
                        AgentProtocolError::new(
                            AgentProtocolErrorCode::RunNotFound,
                            "run disappeared while resolving approval",
                        )
                    })?;
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::InvalidSpec,
                            message: error.to_string(),
                        },
                    );
                }
            },
            RequestResolution::Approval {
                decision: ApprovalDecision::Deny,
                grant_ref: None,
            } => None,
            _ => unreachable!("command shape and approval kind were validated"),
        };

        let mut state = self.state();
        let run = state.runs.get_mut(&command.run_id).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::RunNotFound,
                "run disappeared while resolving approval",
            )
        })?;
        validate_execution_and_duplicate(run, execution, &command)?;
        if let Some(existing) = run.commands.get(&command.command_id) {
            return Ok(ProviderCommandDisposition {
                command_id: command.command_id,
                run_id: command.run_id,
                outcome: existing.outcome.clone(),
                duplicate: true,
            });
        }
        let Some(pending) = run.pending_approvals.get(&request_id) else {
            return record_command(
                &self.inner,
                run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::RequestNotFound,
                    message: "approval request is no longer pending".to_owned(),
                },
            );
        };
        if pending.binding != binding {
            return record_command(
                &self.inner,
                run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::InvalidDigest,
                    message: "approval binding changed while resolving request".to_owned(),
                },
            );
        }
        if pending
            .responder
            .as_ref()
            .is_none_or(|responder| responder.is_closed())
        {
            let disposition = record_command(
                &self.inner,
                run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::InvalidTransition,
                    message: "approval waiter is no longer active".to_owned(),
                },
            )?;
            run.pending_approvals.remove(&request_id);
            return Ok(disposition);
        }
        let disposition = record_command_with_approval(
            &self.inner,
            run,
            &command,
            ProviderCommandOutcome::Accepted,
            capability.clone(),
        )?;
        let mut pending = run
            .pending_approvals
            .remove(&request_id)
            .expect("pending approval was checked before its command commit");
        if let Some(responder) = pending.responder.take() {
            let _ = responder.send(ApprovalResponse {
                command_id: command.command_id.clone(),
                resolution,
                capability,
            });
        }
        Ok(disposition)
    }
}
