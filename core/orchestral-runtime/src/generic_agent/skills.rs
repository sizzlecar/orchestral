use super::*;

#[derive(Clone)]
pub(super) struct SkillCallObservation {
    pub(super) result: serde_json::Value,
    pub(super) is_error: bool,
    pub(super) context_message: Option<ModelMessage>,
}

pub(super) struct SkillReadEvaluation {
    pub(super) observation: SkillCallObservation,
    pub(super) load: Option<SkillLoad>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillReadArguments {
    name: String,
}

pub(super) async fn execute_skill_read(
    inner: &GenericInner,
    request: &AgentStartRequest,
    skills: &SkillRuntime,
    round: u64,
    call_id: &ModelToolCallId,
    arguments: serde_json::Value,
) -> Result<SkillCallObservation, AgentFailure> {
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(session_journal_failure)?;
    let loaded = LoadedSkillSet::replay(&records)
        .map_err(|error| agent_failure("skill_session_state", error.to_string(), false))?;
    let evaluation = evaluate_skill_read(skills, arguments, &loaded);
    if let Some(load) = evaluation.load {
        append_session_event(
            inner,
            AgentSessionEventDraft {
                event_id: skill_load_event_id(&request.run.spec.run_id, round, call_id),
                session_id: request.run.spec.session_id.clone(),
                run_id: request.run.spec.run_id.clone(),
                payload: AgentSessionEvent::SkillLoaded {
                    load: Box::new(load),
                },
            },
        )
        .await?;
    }
    Ok(evaluation.observation)
}

pub(super) fn skill_load_event_id(
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
) -> AgentSessionEventId {
    AgentSessionEventId::new(format!(
        "generic-{}-skill-{}-{}",
        run_id.as_str(),
        round,
        call_id.as_str()
    ))
}

pub(super) fn evaluate_skill_read(
    skills: &SkillRuntime,
    arguments: serde_json::Value,
    loaded: &LoadedSkillSet,
) -> SkillReadEvaluation {
    let parsed = match serde_json::from_value::<SkillReadArguments>(arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            return SkillReadEvaluation {
                observation: SkillCallObservation {
                    result: serde_json::json!({
                        "code": "skill_read_arguments_invalid",
                        "message": error.to_string(),
                    }),
                    is_error: true,
                    context_message: None,
                },
                load: None,
            }
        }
    };
    match skills.read_for_context(&parsed.name, loaded) {
        Ok(SkillLoadOutcome::Loaded(load)) => {
            let descriptor = &load.package.descriptor;
            let resource_base = crate::session_context::skill_resource_base(&descriptor.source);
            SkillReadEvaluation {
                observation: SkillCallObservation {
                    result: serde_json::json!({
                        "status": "loaded",
                        "name": descriptor.name,
                        "skill_id": descriptor.skill_id,
                        "version": descriptor.version,
                        "digest": descriptor.digest,
                        "source": descriptor.source,
                        "resource_base": resource_base,
                    }),
                    is_error: false,
                    context_message: Some(crate::session_context::skill_load_message(&load)),
                },
                load: Some(load),
            }
        }
        Ok(SkillLoadOutcome::AlreadyLoaded(descriptor)) => SkillReadEvaluation {
            observation: SkillCallObservation {
                result: serde_json::json!({
                    "status": "already_loaded",
                    "name": descriptor.name,
                    "skill_id": descriptor.skill_id,
                    "digest": descriptor.digest,
                }),
                is_error: false,
                context_message: None,
            },
            load: None,
        },
        Err(error) => SkillReadEvaluation {
            observation: SkillCallObservation {
                result: serde_json::json!({
                    "code": "skill_read_failed",
                    "message": error.to_string(),
                }),
                is_error: true,
                context_message: None,
            },
            load: None,
        },
    }
}
