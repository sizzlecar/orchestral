use super::*;

pub(super) struct RecoveryDispatch {
    pub(super) inner: Arc<GenericInner>,
    pub(super) request: AgentStartRequest,
    pub(super) user_message: ModelMessage,
    pub(super) model_messages: Vec<ModelMessage>,
    pub(super) model_definitions: Vec<ModelToolDefinition>,
    pub(super) run_skills: Option<Arc<SkillRuntime>>,
    pub(super) seed: GenericExecutionSeed,
    pub(super) cancellation: CancellationToken,
    pub(super) steer_updates: watch::Receiver<u64>,
    pub(super) continuation: GenericRecoveryContinuation,
    pub(super) session_exchange_committed: bool,
}

pub(super) fn spawn_recovered_continuation(dispatch: RecoveryDispatch) {
    let RecoveryDispatch {
        inner,
        request,
        user_message,
        model_messages,
        model_definitions,
        run_skills,
        seed,
        cancellation,
        steer_updates,
        continuation,
        session_exchange_committed,
    } = dispatch;
    tokio::spawn(async move {
        if seed.run_started && cancellation.is_cancelled() {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        match continuation {
            GenericRecoveryContinuation::ModelLoop { .. } => {
                execute_model_run(ModelRunExecution {
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_tools: model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                })
                .await;
            }
            GenericRecoveryContinuation::Input {
                round,
                request_id,
                observation,
                call,
                arguments,
                prompt,
                request_open,
                committed_response,
                resolved_response,
                response,
                ..
            } => {
                resume_observed_input(
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    prompt,
                    request_open,
                    committed_response,
                    resolved_response,
                    session_exchange_committed,
                    response,
                )
                .await;
            }
            GenericRecoveryContinuation::Approval {
                round,
                request_id,
                observation,
                call,
                arguments,
                binding,
                committed_response,
                resolved_response,
                response,
                ..
            } => {
                resume_observed_approval(
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    binding.expect("recovered approval binding was prepared"),
                    committed_response,
                    resolved_response,
                    session_exchange_committed,
                    response,
                )
                .await;
            }
            GenericRecoveryContinuation::Skill {
                round,
                request_id,
                observation,
                call,
                arguments,
                recovered_observation,
                ..
            } => {
                resume_observed_skill(
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    recovered_observation,
                    session_exchange_committed,
                )
                .await;
            }
            GenericRecoveryContinuation::Workflow {
                round,
                request_id,
                observation,
                call,
                arguments,
                recovery_replay,
                ..
            } => {
                resume_observed_workflow(
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    recovery_replay,
                )
                .await;
            }
            GenericRecoveryContinuation::WorkflowOutput {
                round,
                request_id,
                observation,
                call,
                arguments,
                outcome,
                workflow_event_id,
                ..
            } => {
                resume_observed_workflow_output(
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    outcome,
                    workflow_event_id,
                    session_exchange_committed,
                )
                .await;
            }
            GenericRecoveryContinuation::Tool {
                round,
                request_id,
                observation,
                call,
                arguments,
                ..
            } => {
                resume_observed_tool(
                    inner,
                    request,
                    user_message,
                    model_messages,
                    model_definitions,
                    run_skills,
                    seed,
                    cancellation,
                    steer_updates,
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    session_exchange_committed,
                )
                .await;
            }
        }
    });
}
