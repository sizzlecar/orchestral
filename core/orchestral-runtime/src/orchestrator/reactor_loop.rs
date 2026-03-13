use super::*;
use orchestral_core::types::{Plan, ReactorFailureState};

fn should_wait_for_reactor_retry(
    plan: &Plan,
    current_stage: StageKind,
    result: &ExecutionResult,
) -> bool {
    let ExecutionResult::Failed { step_id, .. } = result else {
        return false;
    };
    if matches!(
        current_stage,
        StageKind::Verify | StageKind::WaitUser | StageKind::Done | StageKind::Failed
    ) {
        return false;
    }
    plan.get_step(step_id.as_str())
        .map(|step| matches!(step.kind, StepKind::Action))
        .unwrap_or(false)
}

impl Orchestrator {
    pub(super) async fn run_reactor_pipeline(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
        initial_choice: StageChoice,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let turn_started_at = Instant::now();
        let mut choice = normalize_reactor_stage_choice(initial_choice)?;
        let mut remaining_stages = self.config.reactor_stage_loop_limit;

        let final_result = loop {
            if remaining_stages == 0 {
                break ExecutionResult::Failed {
                    step_id: StepId::from("reactor"),
                    error: "reactor stage loop limit reached".to_string(),
                };
            }
            remaining_stages -= 1;
            validate_reactor_stage_choice(&choice)?;

            let mut reactor_state = task.reactor.clone().unwrap_or(ReactorTaskState {
                skeleton: choice.skeleton,
                artifact_family: choice.artifact_family,
                current_stage: choice.current_stage,
                derivation_policy: choice.derivation_policy,
                last_continuation: None,
                last_verify: None,
                last_failure: None,
                verify_required: true,
            });
            reactor_state.skeleton = choice.skeleton;
            reactor_state.artifact_family = choice.artifact_family;
            reactor_state.current_stage = choice.current_stage;
            reactor_state.derivation_policy = choice.derivation_policy;
            task.set_reactor_state(reactor_state.clone());
            self.task_store.save(&task).await?;

            let stage_plan = match choice.current_stage {
                StageKind::Prepare
                | StageKind::Locate
                | StageKind::Probe
                | StageKind::Run
                | StageKind::Collect
                | StageKind::Derive
                | StageKind::Assess
                | StageKind::Commit
                | StageKind::Export
                | StageKind::Verify => Some(self.lower_reactor_stage_plan(&task, &choice)?),
                StageKind::WaitUser | StageKind::Done | StageKind::Failed => None,
                unsupported_stage => {
                    return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                        "reactor stage {:?} not implemented yet",
                        unsupported_stage
                    ))));
                }
            };

            self.emit_lifecycle_event(
                "planning_completed",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("reactor stage selected"),
                serde_json::json!({
                    "output_type": "stage_choice",
                    "skeleton": choice.skeleton,
                    "artifact_family": choice.artifact_family,
                    "current_stage": choice.current_stage,
                    "stage_goal": choice.stage_goal,
                    "derivation_policy": choice.derivation_policy,
                    "next_stage_hint": choice.next_stage_hint,
                    "reason": choice.reason,
                    "step_count": stage_plan.as_ref().map(|plan| plan.steps.len()).unwrap_or(0),
                    "steps": stage_plan
                        .as_ref()
                        .map(summarize_plan_steps)
                        .unwrap_or_else(|| serde_json::json!({ "items": [], "total_steps": 0 })),
                }),
            )
            .await;

            match choice.current_stage {
                StageKind::WaitUser => {
                    let prompt = choice
                        .reason
                        .clone()
                        .unwrap_or_else(|| "Need additional input".to_string());
                    break ExecutionResult::WaitingUser {
                        step_id: StepId::from("reactor"),
                        prompt,
                        approval: None,
                    };
                }
                StageKind::Done => break ExecutionResult::Completed,
                StageKind::Failed => {
                    break ExecutionResult::Failed {
                        step_id: StepId::from("reactor"),
                        error: choice
                            .reason
                            .clone()
                            .unwrap_or_else(|| "reactor stage failed".to_string()),
                    };
                }
                StageKind::Locate
                | StageKind::Prepare
                | StageKind::Probe
                | StageKind::Run
                | StageKind::Collect
                | StageKind::Derive
                | StageKind::Assess
                | StageKind::Commit
                | StageKind::Export
                | StageKind::Verify => {}
                unsupported_stage => {
                    return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                        "reactor stage {:?} cannot execute without lowering support",
                        unsupported_stage
                    ))));
                }
            }

            let plan = stage_plan.expect("stage plan must exist for executable reactor stages");
            task.set_plan(plan.clone());
            task.start_executing();
            self.task_store.save(&task).await?;
            self.emit_lifecycle_event(
                "execution_started",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("reactor stage execution started"),
                serde_json::json!({
                    "execution_mode": "reactor",
                    "current_stage": choice.current_stage,
                    "step_count": plan.steps.len(),
                }),
            )
            .await;

            let snapshot = self
                .execute_plan_once(&task, interaction_id.as_str(), &plan, None)
                .await?;
            task.set_checkpoint(
                snapshot.completed_step_ids.clone(),
                snapshot.working_set_snapshot.clone(),
            );
            self.task_store.save(&task).await?;
            self.emit_lifecycle_event(
                "execution_completed",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("reactor stage execution completed"),
                serde_json::json!({
                    "execution_mode": "reactor",
                    "current_stage": choice.current_stage,
                    "result": execution_result_metadata(&snapshot.result),
                }),
            )
            .await;

            if !matches!(snapshot.result, ExecutionResult::Failed { .. })
                && reactor_state.last_failure.is_some()
            {
                reactor_state.last_failure = None;
                task.set_reactor_state(reactor_state.clone());
                self.task_store.save(&task).await?;
            }

            if should_wait_for_reactor_retry(&plan, choice.current_stage, &snapshot.result) {
                if let ExecutionResult::Failed { step_id, error } = snapshot.result.clone() {
                    reactor_state.last_failure = Some(ReactorFailureState {
                        step_id: step_id.clone(),
                        error: error.clone(),
                    });
                    task.set_reactor_state(reactor_state.clone());
                    self.task_store.save(&task).await?;
                    break ExecutionResult::WaitingUser {
                        step_id,
                        prompt: format!(
                            "Step failed: {}. Reply 继续/重试 to retry from the failed step, or provide updated instructions.",
                            truncate_for_log(&error, 300)
                        ),
                        approval: None,
                    };
                }
            }

            match snapshot.result.clone() {
                ExecutionResult::Completed => match choice.current_stage {
                    StageKind::Prepare
                    | StageKind::Locate
                    | StageKind::Probe
                    | StageKind::Run
                    | StageKind::Collect
                    | StageKind::Derive
                    | StageKind::Export
                    | StageKind::Commit => {
                        let next_stage = choice
                            .skeleton
                            .next_stage_on_success(choice.current_stage)
                            .ok_or_else(|| {
                                OrchestratorError::Planner(PlanError::Generation(format!(
                                    "reactor skeleton {:?} has no success transition from stage {:?}",
                                    choice.skeleton, choice.current_stage
                                )))
                            })?;
                        choice = build_reactor_stage_choice(
                            choice.skeleton,
                            choice.artifact_family,
                            next_stage,
                            choice.derivation_policy,
                            Some(next_stage),
                            choice.reason.clone(),
                        )?;
                        continue;
                    }
                    StageKind::Assess => {
                        let continuation: ContinuationState = parse_working_set_value(
                            &snapshot.working_set_snapshot,
                            "continuation",
                        )?;
                        reactor_state.last_continuation = Some(continuation.clone());
                        reactor_state.last_failure = None;
                        task.set_reactor_state(reactor_state);
                        self.task_store.save(&task).await?;
                        match continuation.status {
                            ContinuationStatus::CommitReady => {
                                let next_stage =
                                    continuation.next_stage_hint.unwrap_or_else(|| {
                                        choice
                                            .skeleton
                                            .next_stage_on_success(choice.current_stage)
                                            .unwrap_or(StageKind::Commit)
                                    });
                                choice = build_reactor_stage_choice(
                                    choice.skeleton,
                                    choice.artifact_family,
                                    next_stage,
                                    choice.derivation_policy,
                                    continuation.next_stage_hint,
                                    Some(continuation.reason),
                                )?;
                                continue;
                            }
                            ContinuationStatus::WaitUser => {
                                let prompt = continuation.user_message.unwrap_or_else(|| {
                                    format!("Need more input: {}", continuation.reason)
                                });
                                break ExecutionResult::WaitingUser {
                                    step_id: StepId::from("reactor_assess"),
                                    prompt,
                                    approval: None,
                                };
                            }
                            ContinuationStatus::NeedReplan => {
                                choice = self
                                    .build_reactor_replan_choice(
                                        &task,
                                        &choice,
                                        &continuation.reason,
                                        interaction_id.as_str(),
                                    )
                                    .await?;
                                continue;
                            }
                            ContinuationStatus::Done => break ExecutionResult::Completed,
                            ContinuationStatus::Failed => {
                                break ExecutionResult::Failed {
                                    step_id: StepId::from("reactor_assess"),
                                    error: continuation.reason,
                                };
                            }
                        }
                    }
                    StageKind::Verify => {
                        let verify_decision: VerifyDecision = parse_working_set_value(
                            &snapshot.working_set_snapshot,
                            "verify_decision",
                        )?;
                        let mut updated_reactor_state =
                            task.reactor.clone().unwrap_or(ReactorTaskState {
                                skeleton: choice.skeleton,
                                artifact_family: choice.artifact_family,
                                current_stage: choice.current_stage,
                                derivation_policy: choice.derivation_policy,
                                last_continuation: None,
                                last_verify: None,
                                last_failure: None,
                                verify_required: true,
                            });
                        updated_reactor_state.last_verify = Some(verify_decision.clone());
                        updated_reactor_state.last_failure = None;
                        task.set_reactor_state(updated_reactor_state);
                        self.task_store.save(&task).await?;
                        match verify_decision.status {
                            VerifyStatus::Passed => {
                                if let Some(next_stage) =
                                    choice.skeleton.next_stage_on_success(choice.current_stage)
                                {
                                    match next_stage {
                                        StageKind::Done => break ExecutionResult::Completed,
                                        StageKind::Failed => {
                                            break ExecutionResult::Failed {
                                                step_id: StepId::from("reactor_verify"),
                                                error: "reactor verify transitioned to failed"
                                                    .to_string(),
                                            };
                                        }
                                        StageKind::WaitUser => {
                                            break ExecutionResult::WaitingUser {
                                                step_id: StepId::from("reactor_verify"),
                                                prompt: verify_decision.reason,
                                                approval: None,
                                            };
                                        }
                                        _ => {
                                            choice = build_reactor_stage_choice(
                                                choice.skeleton,
                                                choice.artifact_family,
                                                next_stage,
                                                choice.derivation_policy,
                                                Some(next_stage),
                                                Some(verify_decision.reason),
                                            )?;
                                            continue;
                                        }
                                    }
                                }
                                break ExecutionResult::Completed;
                            }
                            VerifyStatus::Failed => {
                                break ExecutionResult::Failed {
                                    step_id: StepId::from("reactor_verify"),
                                    error: verify_decision.reason,
                                };
                            }
                        }
                    }
                    StageKind::WaitUser | StageKind::Done | StageKind::Failed => {
                        break ExecutionResult::Completed;
                    }
                    unsupported_stage => {
                        break ExecutionResult::Failed {
                            step_id: StepId::from("reactor"),
                            error: format!(
                                "reactor stage {:?} completed without runtime completion handler",
                                unsupported_stage
                            ),
                        };
                    }
                },
                ExecutionResult::NeedReplan { prompt, .. } => {
                    choice = self
                        .build_reactor_replan_choice(
                            &task,
                            &choice,
                            &prompt,
                            interaction_id.as_str(),
                        )
                        .await?;
                    continue;
                }
                other => break other,
            }
        };

        let new_state = task_state_from_execution(&final_result);
        task.set_state(new_state.clone());
        self.task_store.save(&task).await?;
        self.thread_runtime
            .update_interaction_state(
                interaction_id.as_str(),
                interaction_state_from_task(&new_state),
            )
            .await?;
        self.emit_interpreted_output(interaction_id.as_str(), &task, &final_result)
            .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
            "orchestrator reactor pipeline finished"
        );

        let response = match started_kind {
            "started" => OrchestratorResult::Started {
                interaction_id,
                task_id: task.id,
                result: final_result,
            },
            _ => OrchestratorResult::Merged {
                interaction_id,
                task_id: task.id,
                result: final_result,
            },
        };
        Ok(response)
    }

    pub(super) async fn resume_reactor_waiting_interaction(
        &self,
        interaction_id: InteractionId,
        mut task: Task,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        match &event {
            Event::UserInput { payload, .. } => {
                task.working_set_snapshot
                    .insert("resume_user_input".to_string(), payload.clone());
            }
            Event::ExternalEvent { payload, .. } => {
                task.working_set_snapshot
                    .insert("resume_external_event".to_string(), payload.clone());
            }
            _ => {}
        }
        self.task_store.save(&task).await?;

        let choice = self
            .build_reactor_resume_choice(&task, &event, interaction_id.as_str())
            .await?;
        self.run_reactor_pipeline(interaction_id, "merged", task, choice)
            .await
    }

    pub(super) async fn build_reactor_resume_choice(
        &self,
        task: &Task,
        event: &Event,
        interaction_id: &str,
    ) -> Result<StageChoice, OrchestratorError> {
        reactor_resume::build_reactor_resume_choice(self, task, event, interaction_id).await
    }

    pub(super) fn lower_reactor_stage_plan(
        &self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<orchestral_core::types::Plan, OrchestratorError> {
        reactor_lowering::lower_reactor_stage_plan(task, choice)
    }

    pub(super) async fn build_reactor_replan_choice(
        &self,
        task: &Task,
        current_choice: &StageChoice,
        replan_prompt: &str,
        interaction_id: &str,
    ) -> Result<StageChoice, OrchestratorError> {
        reactor_resume::build_reactor_replan_choice(
            self,
            task,
            current_choice,
            replan_prompt,
            interaction_id,
        )
        .await
    }
}
