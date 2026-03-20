use super::*;

pub(super) struct PlanExecutionSnapshot {
    pub(super) result: ExecutionResult,
    pub(super) completed_step_ids: Vec<StepId>,
    pub(super) working_set_snapshot: HashMap<String, Value>,
}

impl Orchestrator {
    pub(super) async fn execute_existing_task(
        &self,
        task: &mut Task,
        interaction_id: &str,
        resume_event: Option<&Event>,
    ) -> Result<ExecutionResult, OrchestratorError> {
        let initial_plan = task
            .plan
            .clone()
            .ok_or_else(|| OrchestratorError::MissingPlan(task.id.to_string()))?;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            step_count = initial_plan.steps.len(),
            "orchestrator normalize/execute started"
        );
        let current_plan = initial_plan.clone();
        let resume = resume_event;
        let execute_started_at = Instant::now();
        let one_run_started_at = Instant::now();
        let run = match self
            .execute_plan_once(task, interaction_id, &current_plan, resume)
            .await
        {
            Ok(run) => run,
            Err(err) => return Err(err),
        };
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            run_elapsed_ms = one_run_started_at.elapsed().as_millis() as u64,
            result = %truncate_debug_for_log(&run.result, MAX_LOG_CHARS),
            "orchestrator execute_plan_once latency"
        );
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            completed_steps = ?run.completed_step_ids,
            ws_keys = ?run.working_set_snapshot.keys().collect::<Vec<_>>(),
            "orchestrator checkpoint snapshot"
        );
        task.set_checkpoint(run.completed_step_ids, run.working_set_snapshot);
        self.task_store.save(task).await?;

        let final_result = match run.result {
            ExecutionResult::NeedReplan { step_id, .. } => ExecutionResult::Failed {
                step_id,
                error: "planner-owned workflow continuation has been removed; return a concrete follow-up plan or terminal planner output instead".to_string(),
            },
            other => other,
        };

        let new_state = task_state_from_execution(&final_result);
        task.set_state(new_state.clone());
        self.task_store.save(task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id, interaction_state_from_task(&new_state))
            .await?;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            execute_existing_task_elapsed_ms = execute_started_at.elapsed().as_millis() as u64,
            "orchestrator execute_existing_task finished"
        );

        Ok(final_result)
    }

    pub(super) async fn execute_plan_once(
        &self,
        task: &Task,
        interaction_id: &str,
        plan: &orchestral_core::types::Plan,
        resume_event: Option<&Event>,
    ) -> Result<PlanExecutionSnapshot, OrchestratorError> {
        let normalized = self.normalizer.normalize(plan.clone())?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                normalized_steps = %truncate_debug_for_log(&normalized.plan.steps, MAX_LOG_CHARS),
                "orchestrator normalized plan detail"
            );
        }
        let mut dag = normalized.dag;
        restore_checkpoint(&mut dag, task);
        let dag_completed_after_restore: Vec<&str> = dag.completed_nodes();
        let dag_ready_after_restore: Vec<String> = dag.ready_nodes.clone();
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            task_completed_step_ids = ?task.completed_step_ids,
            dag_completed = ?dag_completed_after_restore,
            dag_ready = ?dag_ready_after_restore,
            plan_step_count = plan.steps.len(),
            "execute_plan_once DAG state after restore_checkpoint"
        );
        if let Some(event) = resume_event {
            complete_wait_step_for_resume(&mut dag, plan, &task.completed_step_ids, event);
        }

        let mut ws = WorkingSet::new();
        ws.import_task_data(task.working_set_snapshot.clone());
        if let Some(event) = resume_event {
            apply_resume_event_to_working_set(&mut ws, event);
        }
        let working_set = Arc::new(RwLock::new(ws));
        let progress_reporter = Arc::new(RuntimeProgressReporter::new(
            self.thread_runtime.thread_id().await,
            InteractionId::from(interaction_id),
            self.thread_runtime.event_store.clone(),
            self.thread_runtime.event_bus.clone(),
            self.hook_registry.clone(),
        ));
        let runtime_info = PlannerRuntimeInfo::detect();
        let skill_instructions = self.skill_catalog.build_instructions(&task.intent.content);
        let exec_ctx = ExecutorContext::new(task.id.clone(), working_set.clone())
            .with_progress_reporter(progress_reporter)
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions);
        let result = self.executor.execute(&mut dag, &exec_ctx).await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            result = %truncate_debug_for_log(&result, MAX_LOG_CHARS),
            "orchestrator execution completed"
        );
        let working_set_snapshot = {
            let ws_guard = working_set.read().await;
            ws_guard.export_task_data()
        };
        let completed_step_ids = dag
            .completed_nodes()
            .into_iter()
            .map(StepId::from)
            .collect();
        Ok(PlanExecutionSnapshot {
            result,
            completed_step_ids,
            working_set_snapshot,
        })
    }

    pub(super) async fn available_actions(&self) -> Vec<ActionMeta> {
        let mut actions = Vec::new();
        let registry = self.executor.action_registry.read().await;
        for name in registry.names() {
            if let Some(action) = registry.get(&name) {
                actions.push(extract_meta(action.as_ref()));
            }
        }
        actions
    }

    pub(super) async fn history_for_planner(
        &self,
        interaction_id: &str,
        task_id: &str,
    ) -> Result<Vec<HistoryItem>, OrchestratorError> {
        if let Some(builder) = &self.context_builder {
            let request = ContextRequest {
                thread_id: self.thread_runtime.thread_id().await.to_string(),
                task_id: Some(task_id.to_string()),
                interaction_id: Some(interaction_id.to_string()),
                query: None,
                budget: self.config.context_budget.clone(),
                include_history: self.config.include_history,
                tags: Vec::new(),
            };
            let window = builder.build(&request).await?;
            return Ok(context_window_to_history(&window));
        }

        let mut events = self
            .thread_runtime
            .query_history(self.config.history_limit)
            .await?;
        events.sort_by_key(|a| a.timestamp());
        Ok(events.iter().filter_map(event_to_history_item).collect())
    }
}
