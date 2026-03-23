use super::*;
use orchestral_core::executor::render_working_set_template;
use orchestral_core::store::WorkingSet;

impl Orchestrator {
    pub(super) async fn emit_lifecycle_event(
        &self,
        event_type: &str,
        interaction_id: Option<&str>,
        task_id: Option<&str>,
        message: Option<&str>,
        metadata: Value,
    ) {
        let thread_id = self.thread_runtime.thread_id().await;
        let mut payload = serde_json::Map::new();
        payload.insert(
            "category".to_string(),
            Value::String("runtime_lifecycle".to_string()),
        );
        payload.insert(
            "event_type".to_string(),
            Value::String(event_type.to_string()),
        );
        if let Some(interaction_id) = interaction_id {
            payload.insert(
                "interaction_id".to_string(),
                Value::String(interaction_id.to_string()),
            );
        }
        if let Some(task_id) = task_id {
            payload.insert("task_id".to_string(), Value::String(task_id.to_string()));
        }
        if let Some(message) = message {
            payload.insert("message".to_string(), Value::String(message.to_string()));
        }
        if !metadata.is_null() {
            payload.insert("metadata".to_string(), metadata);
        }

        let trace = Event::trace(thread_id, "info", Value::Object(payload));
        if let Err(err) = self.thread_runtime.event_store.append(trace.clone()).await {
            tracing::warn!(
                event_type = %event_type,
                error = %err,
                "failed to persist lifecycle event"
            );
            return;
        }

        if let Err(err) = self.thread_runtime.event_bus.publish(trace).await {
            tracing::warn!(
                event_type = %event_type,
                error = %err,
                "failed to publish lifecycle event"
            );
        }
    }

    pub(super) async fn emit_interpreted_output(
        &self,
        interaction_id: &str,
        task: &Task,
        result: &ExecutionResult,
    ) {
        let started_at = Instant::now();
        let Some(plan) = task.plan.clone() else {
            return;
        };

        let message = resolve_plan_response_template(&plan, result, &task.working_set_snapshot)
            .unwrap_or_else(|| {
                let request = InterpretRequest {
                    intent: task.intent.content.clone(),
                    plan: plan.clone(),
                    execution_result: result.clone(),
                    completed_step_ids: task.completed_step_ids.clone(),
                    working_set_snapshot: task.working_set_snapshot.clone(),
                };
                noop_interpret_sync(&request)
            });

        self.emit_assistant_output_message(
            interaction_id,
            task.id.as_str(),
            message,
            Value::Null,
            Some(execution_result_metadata(result)),
        )
        .await;
        tracing::info!(
            task_id = %task.id,
            interaction_id = %interaction_id,
            emit_interpreted_output_elapsed_ms = started_at.elapsed().as_millis() as u64,
            "orchestrator emit_interpreted_output finished"
        );
    }

    pub(super) async fn emit_assistant_output_message(
        &self,
        interaction_id: &str,
        task_id: &str,
        message: String,
        metadata: Value,
        result: Option<Value>,
    ) {
        let mut payload = serde_json::Map::new();
        payload.insert("task_id".to_string(), Value::String(task_id.to_string()));
        payload.insert("message".to_string(), Value::String(message.clone()));
        if !metadata.is_null() {
            payload.insert("metadata".to_string(), metadata);
        }
        if let Some(result) = result {
            if !result.is_null() {
                payload.insert("result".to_string(), result);
            }
        }
        let assistant_event = Event::assistant_output(
            self.thread_runtime.thread_id().await,
            interaction_id,
            Value::Object(payload),
        );
        if let Err(err) = self
            .thread_runtime
            .event_store
            .append(assistant_event.clone())
            .await
        {
            tracing::warn!(
                task_id = %task_id,
                interaction_id = %interaction_id,
                error = %err,
                "failed to persist assistant output"
            );
            return;
        }
        if let Err(err) = self.thread_runtime.event_bus.publish(assistant_event).await {
            tracing::warn!(
                task_id = %task_id,
                interaction_id = %interaction_id,
                error = %err,
                "failed to publish assistant output"
            );
            return;
        }
        tracing::info!(
            task_id = %task_id,
            interaction_id = %interaction_id,
            message = %truncate_for_log(&message, 400),
            "assistant output emitted"
        );
    }
}

pub(super) fn summarize_plan_steps(plan: &orchestral_core::types::Plan) -> Value {
    const STEP_PREVIEW_LIMIT: usize = 32;
    let steps: Vec<Value> = plan
        .steps
        .iter()
        .take(STEP_PREVIEW_LIMIT)
        .map(|step| {
            serde_json::json!({
                "id": step.id.clone(),
                "action": step.action.clone(),
                "kind": format!("{:?}", step.kind),
                "depends_on": step.depends_on.clone(),
                "io_bindings": step.io_bindings.clone(),
            })
        })
        .collect();

    if plan.steps.len() > STEP_PREVIEW_LIMIT {
        serde_json::json!({
            "items": steps,
            "truncated": true,
            "total_steps": plan.steps.len(),
        })
    } else {
        serde_json::json!({
            "items": steps,
            "truncated": false,
            "total_steps": plan.steps.len(),
        })
    }
}

pub(super) fn execution_result_metadata(result: &ExecutionResult) -> Value {
    match result {
        ExecutionResult::Completed => serde_json::json!({
            "status": "completed"
        }),
        ExecutionResult::Failed { step_id, error } => serde_json::json!({
            "status": "failed",
            "step_id": step_id,
            "error": truncate_for_log(error, 400),
        }),
        ExecutionResult::WaitingUser {
            step_id,
            prompt,
            approval,
        } => {
            let mut meta = serde_json::json!({
                "status": "waiting_user",
                "step_id": step_id,
                "prompt": truncate_for_log(prompt, 400),
                "waiting_kind": "input",
            });
            if let Some(req) = approval {
                meta["waiting_kind"] = Value::String("approval".to_string());
                meta["approval_reason"] = Value::String(truncate_for_log(&req.reason, 400));
                if let Some(command) = &req.command {
                    meta["approval_command"] = Value::String(truncate_for_log(command, 400));
                }
            }
            meta
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => serde_json::json!({
            "status": "waiting_event",
            "step_id": step_id,
            "event_type": event_type,
        }),
        ExecutionResult::NeedReplan { step_id, prompt } => serde_json::json!({
            "status": "need_replan",
            "step_id": step_id,
            "prompt": truncate_for_log(prompt, 400),
        }),
    }
}

pub(super) fn render_output_template<I>(
    template: &str,
    working_set: &HashMap<String, Value>,
    extra_bindings: I,
) -> Result<String, String>
where
    I: IntoIterator<Item = (String, Value)>,
{
    let mut ws = WorkingSet::new();
    ws.import_task_data(working_set.clone());
    for (key, value) in extra_bindings {
        ws.set_task(key, value);
    }
    render_working_set_template(template, &ws)
}

pub(super) fn resolve_plan_response_template(
    plan: &orchestral_core::types::Plan,
    result: &ExecutionResult,
    working_set: &HashMap<String, Value>,
) -> Option<String> {
    let template = match result {
        ExecutionResult::Completed => plan.on_complete.as_deref(),
        ExecutionResult::Failed { .. } => plan.on_failure.as_deref(),
        _ => None,
    }?;

    let is_completed = matches!(result, ExecutionResult::Completed);
    let mut resolved = match result {
        ExecutionResult::Failed { error, .. } => render_output_template(
            template,
            working_set,
            std::iter::once(("error".to_string(), Value::String(error.clone()))),
        )
        .ok()?,
        _ => render_output_template(template, working_set, std::iter::empty::<(String, Value)>())
            .ok()?,
    };
    if is_completed && !template_contains_summary_placeholder(template) {
        if let Some(summary) = best_summary_from_working_set(working_set) {
            if !summary.trim().is_empty() && !resolved.contains(&summary) {
                if !resolved.trim().is_empty() {
                    resolved.push_str("\n\n");
                }
                resolved.push_str(&summary);
            }
        }
    }
    Some(resolved)
}

fn value_to_template_replacement(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn template_contains_summary_placeholder(template: &str) -> bool {
    template.contains("{{summary}}") || template.contains(".summary}}")
}

fn best_summary_from_working_set(working_set: &HashMap<String, Value>) -> Option<String> {
    if let Some(value) = working_set.get("summary") {
        let summary = value_to_template_replacement(value);
        if !summary.trim().is_empty() {
            return Some(summary);
        }
    }

    let mut scoped_keys = working_set
        .keys()
        .filter(|key| key.ends_with(".summary"))
        .cloned()
        .collect::<Vec<_>>();
    scoped_keys.sort();

    for key in scoped_keys {
        if let Some(value) = working_set.get(&key) {
            let summary = value_to_template_replacement(value);
            if !summary.trim().is_empty() {
                return Some(summary);
            }
        }
    }
    None
}

fn noop_interpret_sync(request: &InterpretRequest) -> String {
    let completed = request.completed_step_ids.len();
    let total = request.plan.steps.len();
    match &request.execution_result {
        ExecutionResult::Completed => {
            format!("Done ({}/{} steps).", completed, total)
        }
        ExecutionResult::Failed { step_id, error } => {
            format!(
                "Failed at step '{}' ({}/{} steps): {}",
                step_id,
                completed,
                total,
                truncate_for_log(error, 400)
            )
        }
        ExecutionResult::WaitingUser { prompt, .. } => {
            format!(
                "Need input ({}/{} steps done): {}",
                completed, total, prompt
            )
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => {
            format!(
                "Waiting for '{}' at step '{}' ({}/{} steps done).",
                event_type, step_id, completed, total
            )
        }
        ExecutionResult::NeedReplan { step_id, prompt } => {
            format!(
                "Replanning at step '{}' ({}/{} steps done): {}",
                step_id, completed, total, prompt
            )
        }
    }
}
