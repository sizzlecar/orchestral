use super::*;

impl Orchestrator {
    /// Handle an event end-to-end (intent -> plan -> normalize -> execute)
    pub async fn handle_event(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        if let Some(interaction_id) = self.thread_runtime.find_resume_interaction(&event).await {
            return self.resume_interaction(interaction_id, event).await;
        }

        self.start_new_interaction(event).await
    }

    async fn start_new_interaction(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let event_clone = event.clone();
        tracing::info!(
            thread_id = %event_clone.thread_id(),
            event_type = %event_type_label(&event_clone),
            "orchestrator received event"
        );
        let decision = self.thread_runtime.handle_event(event).await?;

        let (interaction_id, started_kind) = match &decision {
            HandleEventResult::Started { interaction_id } => (interaction_id.clone(), "started"),
            HandleEventResult::Merged { interaction_id } => {
                // Event is already persisted to the active interaction by thread_runtime.
                // Don't start a new pipeline — the running pipeline's agent loop will
                // see the new message in history on its next iteration.
                self.emit_lifecycle_event(
                    "turn_merged",
                    Some(interaction_id.as_str()),
                    None,
                    Some("event merged into active interaction"),
                    serde_json::json!({
                        "event_type": event_type_label(&event_clone),
                    }),
                )
                .await;
                return Ok(OrchestratorResult::Merged {
                    interaction_id: interaction_id.clone(),
                    task_id: "".into(), // no new task created
                    result: ExecutionResult::Completed,
                });
            }
            HandleEventResult::Rejected { reason } => {
                self.emit_lifecycle_event(
                    "turn_rejected",
                    None,
                    None,
                    Some(reason),
                    serde_json::json!({ "reason": reason }),
                )
                .await;
                return Ok(OrchestratorResult::Rejected {
                    reason: reason.clone(),
                });
            }
            HandleEventResult::Queued => {
                self.emit_lifecycle_event(
                    "turn_queued",
                    None,
                    None,
                    Some("event queued"),
                    Value::Null,
                )
                .await;
                return Ok(OrchestratorResult::Queued);
            }
        };

        let intent = intent_from_event(&event_clone, Some(interaction_id.to_string()))?;
        let task = Task::new(intent);
        self.task_store.save(&task).await?;
        self.thread_runtime
            .add_task_to_interaction(interaction_id.as_str(), task.id.clone())
            .await?;
        self.emit_lifecycle_event(
            "turn_started",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("turn started"),
            serde_json::json!({
                "started_kind": started_kind,
                "event_type": event_type_label(&event_clone),
            }),
        )
        .await;

        self.run_planning_pipeline(interaction_id, started_kind, task)
            .await
    }

    async fn resume_interaction(
        &self,
        interaction_id: InteractionId,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        self.thread_runtime
            .append_event_to_interaction(interaction_id.as_str(), event.clone())
            .await?;
        self.thread_runtime
            .resume_interaction(interaction_id.as_str())
            .await?;

        let interaction = self
            .thread_runtime
            .get_interaction(interaction_id.as_str())
            .await
            .ok_or_else(|| OrchestratorError::ResumeError("interaction missing".to_string()))?;
        let task_id =
            interaction.task_ids.last().cloned().ok_or_else(|| {
                OrchestratorError::ResumeError("interaction has no task".to_string())
            })?;

        let mut task = self
            .task_store
            .load(task_id.as_str())
            .await?
            .ok_or_else(|| OrchestratorError::TaskNotFound(task_id.to_string()))?;
        self.emit_lifecycle_event(
            "turn_resumed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("resuming waiting turn"),
            serde_json::json!({
                "event_type": event_type_label(&event),
            }),
        )
        .await;
        if task.plan.is_none() {
            let intent = intent_from_event(&event, Some(interaction_id.to_string()))?;
            let new_task = Task::new(intent);
            self.task_store.save(&new_task).await?;
            self.thread_runtime
                .add_task_to_interaction(interaction_id.as_str(), new_task.id.clone())
                .await?;
            self.emit_lifecycle_event(
                "turn_started",
                Some(interaction_id.as_str()),
                Some(new_task.id.as_str()),
                Some("turn started from clarification follow-up"),
                serde_json::json!({
                    "started_kind": "merged",
                    "event_type": event_type_label(&event),
                    "resumed_from_task_id": task.id.clone(),
                }),
            )
            .await;
            return self
                .run_planning_pipeline(interaction_id, "merged", new_task)
                .await;
        }
        task.start_executing();
        self.task_store.save(&task).await?;

        self.emit_lifecycle_event(
            "execution_started",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution resumed"),
            serde_json::json!({ "resume": true }),
        )
        .await;
        let snapshot = self
            .execute_existing_task(&mut task, interaction_id.as_str(), Some(&event))
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution completed"),
            execution_result_metadata(&snapshot.result),
        )
        .await;
        self.emit_interpreted_output(interaction_id.as_str(), &task, &snapshot.result)
            .await;

        Ok(OrchestratorResult::Merged {
            interaction_id,
            task_id: task.id.clone(),
            result: snapshot.result,
        })
    }
}
