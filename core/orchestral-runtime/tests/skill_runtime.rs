use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream;
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    wire::{
        AgentRunEnvelope, AgentSessionId, BindingRequirement, Content, ProviderBindingRef,
        ResourceBinding, ResourceBindingId, ResourceBindingMode, ResourceId, ResourceKind,
        ResourceRef, ResourceRevision, RunId,
    },
    AGENT_PROTOCOL_V1,
};
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionJournalStore, InMemoryAgentSessionJournalStore,
};
use orchestral_core::model_protocol::{
    ModelBackend, ModelCapabilities, ModelContent, ModelDescriptor, ModelError, ModelEvent,
    ModelEventId, ModelFinishReason, ModelMessage, ModelRequest, ModelRole, ModelStream,
    ModelStreamEvent, ModelToolCallId,
};
use orchestral_core::skill_protocol::{
    SkillCompatibility, SkillDependencies, SkillId, SkillPackage, SkillSource, SkillSourceKind,
    SKILL_CATALOG_RESOURCE_KIND_V1,
};
use orchestral_runtime::{
    AgentController, GenericAgentConfig, InternalGenericAgentProvider, JsonSizeTokenMeter,
    SkillRuntime,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const SKILL_FUNCTION: &str = "skill_read";
const SECRET_INSTRUCTIONS: &str = "SECRET WORKFLOW: calculate twice, then verify the artifact.";

struct SkillLoadModel {
    rounds: AtomicUsize,
    digest: String,
}

struct RecoveredSkillModel;

struct UnboundCatalogModel;

struct SkillVisibilityModel {
    starts: AtomicUsize,
    digest: String,
    violations: Mutex<Vec<String>>,
}

#[async_trait]
impl ModelBackend for SkillLoadModel {
    fn descriptor(&self) -> ModelDescriptor {
        function_capable_descriptor("skill-load-model")
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request.tools.iter().any(|tool| tool.name == SKILL_FUNCTION));
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.name == "orchestral_request_input"));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let system = system_text(&request.messages);
        let request_id = request.request_id;
        match round {
            0 => {
                assert!(system.contains("Spreadsheet workflow"));
                assert!(system.contains(&self.digest));
                assert!(!system.contains(SECRET_INSTRUCTIONS));
                skill_read_stream(request_id, "read-xlsx")
            }
            1 => {
                assert!(system.contains(SECRET_INSTRUCTIONS));
                assert!(request.messages.iter().any(|message| {
                    message.role == ModelRole::Tool
                        && message.content.iter().any(|content| {
                            matches!(
                                content,
                                ModelContent::ToolResult { result, is_error: false, .. }
                                    if result.get("status") == Some(&json!("loaded"))
                            )
                        })
                }));
                skill_read_stream(request_id, "read-xlsx-again")
            }
            2 => {
                assert!(system.contains(SECRET_INSTRUCTIONS));
                assert!(request.messages.iter().any(|message| {
                    message.role == ModelRole::Tool
                        && message.content.iter().any(|content| {
                            matches!(
                                content,
                                ModelContent::ToolResult { result, is_error: false, .. }
                                    if result.get("status") == Some(&json!("already_loaded"))
                            )
                        })
                }));
                answer_stream(request_id, "skill context applied")
            }
            _ => panic!("Skill load scenario dispatched an unexpected model round"),
        }
    }
}

#[async_trait]
impl ModelBackend for RecoveredSkillModel {
    fn descriptor(&self) -> ModelDescriptor {
        function_capable_descriptor("recovered-skill-model")
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(system_text(&request.messages).contains(SECRET_INSTRUCTIONS));
        answer_stream(request.request_id, "recovered skill context")
    }
}

#[async_trait]
impl ModelBackend for UnboundCatalogModel {
    fn descriptor(&self) -> ModelDescriptor {
        function_capable_descriptor("unbound-catalog-model")
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, "orchestral_request_input");
        let system = system_text(&request.messages);
        assert!(!system.contains("Spreadsheet workflow"));
        assert!(!system.contains(SECRET_INSTRUCTIONS));
        answer_stream(request.request_id, "no catalog bound")
    }
}

#[async_trait]
impl ModelBackend for SkillVisibilityModel {
    fn descriptor(&self) -> ModelDescriptor {
        function_capable_descriptor("skill-visibility-model")
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let input = user_text(&request.messages);
        let expect_bound = match input.as_str() {
            "catalog-bound" => true,
            "catalog-unbound" => false,
            other => panic!("unexpected visibility probe input: {other}"),
        };
        let system = system_text(&request.messages);
        let descriptor_visible = system.contains("Spreadsheet workflow");
        let digest_visible = system.contains(&self.digest);
        let load_visible = request.tools.iter().any(|tool| tool.name == SKILL_FUNCTION);
        let instructions_visible = system.contains(SECRET_INSTRUCTIONS);
        if descriptor_visible != expect_bound
            || digest_visible != expect_bound
            || load_visible != expect_bound
            || instructions_visible
        {
            self.violations.lock().unwrap().push(format!(
                "input={input} descriptor={descriptor_visible} digest={digest_visible} load={load_visible} instructions={instructions_visible}"
            ));
        }
        self.starts.fetch_add(1, Ordering::SeqCst);
        answer_stream(request.request_id, "visibility checked")
    }
}

#[tokio::test]
async fn bound_skill_loads_into_context_and_replays_after_provider_restart() {
    let skills = Arc::new(skill_runtime());
    let digest = skills.catalog().skills[0].digest.clone();
    let journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let session_id = AgentSessionId::new("skill-session");

    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            Arc::new(SkillLoadModel {
                rounds: AtomicUsize::new(0),
                digest: digest.to_string(),
            }),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            skills.clone(),
            journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let first_controller = Arc::new(
        AgentController::new(first_provider, ProviderBindingRef::new("skill-binding")).unwrap(),
    );
    let first = bound_run(
        &skills,
        session_id.clone(),
        RunId::new("skill-run-1"),
        "use the spreadsheet skill",
    );
    let execution = first_controller.start(first).await.unwrap();
    let view = first_controller
        .wait_for_terminal(&execution.run_id)
        .await
        .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);

    let records = journal.load_session(&session_id).await.unwrap();
    let loads = records
        .iter()
        .filter_map(|record| match &record.payload {
            AgentSessionEvent::SkillLoaded { load } => Some(load),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(loads.len(), 1);
    let load = loads[0];
    assert_eq!(load.package.descriptor.source.locator, "builtin:xlsx");
    assert_eq!(load.package.descriptor.version.as_deref(), Some("1.0.0"));
    assert_eq!(load.package.descriptor.digest, digest);

    drop(first_controller);
    let unbound_provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            Arc::new(UnboundCatalogModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            skills.clone(),
            journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let unbound_controller = Arc::new(
        AgentController::new(unbound_provider, ProviderBindingRef::new("skill-binding")).unwrap(),
    );
    let unbound = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id.clone(),
        RunId::new("skill-run-unbound"),
        vec![Content::text("do not bind the catalog")],
    )
    .unwrap();
    let execution = unbound_controller.start(unbound).await.unwrap();
    let view = unbound_controller
        .wait_for_terminal(&execution.run_id)
        .await
        .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    drop(unbound_controller);

    let restarted_provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            Arc::new(RecoveredSkillModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            skills.clone(),
            journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let restarted_controller = Arc::new(
        AgentController::new(restarted_provider, ProviderBindingRef::new("skill-binding")).unwrap(),
    );
    let second = bound_run(&skills, session_id, RunId::new("skill-run-2"), "continue");
    let execution = restarted_controller.start(second).await.unwrap();
    let view = restarted_controller
        .wait_for_terminal(&execution.run_id)
        .await
        .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
}

#[tokio::test]
async fn configured_but_unbound_skill_catalog_is_invisible_to_the_model() {
    let skills = Arc::new(skill_runtime());
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            Arc::new(UnboundCatalogModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            skills,
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let controller =
        Arc::new(AgentController::new(provider, ProviderBindingRef::new("skill-binding")).unwrap());
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("unbound-session"),
        RunId::new("unbound-run"),
        vec![Content::text("hello")],
    )
    .unwrap();
    let execution = controller.start(run).await.unwrap();
    let view = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
}

#[tokio::test]
async fn one_thousand_run_bindings_never_leak_unloaded_skill_instructions() {
    const CASES: usize = 1_000;

    let skills = Arc::new(skill_runtime());
    let model = Arc::new(SkillVisibilityModel {
        starts: AtomicUsize::new(0),
        digest: skills.catalog().skills[0].digest.to_string(),
        violations: Mutex::new(Vec::new()),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            skills.clone(),
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let controller =
        Arc::new(AgentController::new(provider, ProviderBindingRef::new("skill-binding")).unwrap());
    let mut expected_model_starts = 0;

    for index in 0..CASES {
        let mode = index % 4;
        let mut run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new(format!("visibility-session-{index}")),
            RunId::new(format!("visibility-run-{index}")),
            vec![Content::text(if mode == 0 {
                "catalog-bound"
            } else {
                "catalog-unbound"
            })],
        )
        .unwrap();
        if mode != 1 {
            run.spec.resources = vec![ResourceBinding {
                binding_id: ResourceBindingId::new("skills"),
                resource: ResourceRef {
                    kind: ResourceKind::new(SKILL_CATALOG_RESOURCE_KIND_V1),
                    id: skills.catalog().resource_id.clone(),
                    revision: if mode == 0 {
                        ResourceRevision::new(skills.catalog().revision.as_str())
                    } else {
                        ResourceRevision::new(format!("{:064x}", index + 1))
                    },
                },
                requirement: if mode == 3 {
                    BindingRequirement::Required
                } else {
                    BindingRequirement::Optional
                },
                mode: ResourceBindingMode::Snapshot,
            }];
            run = AgentRunEnvelope::seal(run.spec).unwrap();
        }

        let started = controller.start(run).await;
        if mode == 3 {
            assert!(started.is_err(), "required mismatched binding was accepted");
            continue;
        }
        expected_model_starts += 1;
        let execution = started.unwrap();
        let view = match controller.wait_for_terminal(&execution.run_id).await {
            Ok(view) => view,
            Err(error) => panic!(
                "visibility run failed: {error:?}; events={:?}",
                controller.events(&execution.run_id, 0).await.unwrap()
            ),
        };
        assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    }

    assert_eq!(model.starts.load(Ordering::SeqCst), expected_model_starts);
    assert_eq!(*model.violations.lock().unwrap(), Vec::<String>::new());
}

fn skill_runtime() -> SkillRuntime {
    let package = SkillPackage::seal(
        SkillId::new("xlsx"),
        "xlsx",
        "Spreadsheet workflow",
        Some("1.0.0".to_owned()),
        SkillSource {
            kind: SkillSourceKind::BuiltIn,
            locator: "builtin:xlsx".to_owned(),
        },
        SkillCompatibility::default(),
        SkillDependencies::default(),
        SECRET_INSTRUCTIONS,
    )
    .unwrap();
    SkillRuntime::from_packages(ResourceId::new("default-skills"), vec![package]).unwrap()
}

fn bound_run(
    skills: &SkillRuntime,
    session_id: AgentSessionId,
    run_id: RunId,
    input: &str,
) -> AgentRunEnvelope {
    let mut run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id,
        run_id,
        vec![Content::text(input)],
    )
    .unwrap();
    run.spec.resources = vec![ResourceBinding {
        binding_id: ResourceBindingId::new("skills"),
        resource: ResourceRef {
            kind: ResourceKind::new(SKILL_CATALOG_RESOURCE_KIND_V1),
            id: skills.catalog().resource_id.clone(),
            revision: ResourceRevision::new(skills.catalog().revision.as_str()),
        },
        requirement: BindingRequirement::Required,
        mode: ResourceBindingMode::Snapshot,
    }];
    AgentRunEnvelope::seal(run.spec).unwrap()
}

fn function_capable_descriptor(backend_id: &str) -> ModelDescriptor {
    ModelDescriptor {
        backend_id: backend_id.to_owned(),
        capabilities: ModelCapabilities {
            streaming: true,
            tool_calls: true,
            ..ModelCapabilities::default()
        },
        extensions: Default::default(),
    }
}

fn system_text(messages: &[ModelMessage]) -> String {
    messages
        .iter()
        .filter(|message| message.role == ModelRole::System)
        .flat_map(|message| message.content.iter())
        .filter_map(|content| match content {
            ModelContent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn user_text(messages: &[ModelMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| message.role == ModelRole::User)
        .into_iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|content| match content {
            ModelContent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn answer_stream(
    request_id: orchestral_core::model_protocol::ModelRequestId,
    text: &str,
) -> Result<ModelStream, ModelError> {
    Ok(Box::pin(stream::iter([
        Ok(ModelStreamEvent {
            request_id: request_id.clone(),
            event_id: ModelEventId::new("answer-delta"),
            sequence: 1,
            payload: ModelEvent::TextDelta {
                delta: text.to_owned(),
            },
        }),
        Ok(ModelStreamEvent {
            request_id,
            event_id: ModelEventId::new("answer-finish"),
            sequence: 2,
            payload: ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            },
        }),
    ])))
}

fn skill_read_stream(
    request_id: orchestral_core::model_protocol::ModelRequestId,
    call_id: &str,
) -> Result<ModelStream, ModelError> {
    let arguments = json!({ "name": "xlsx" }).to_string();
    Ok(Box::pin(stream::iter([
        Ok(ModelStreamEvent {
            request_id: request_id.clone(),
            event_id: ModelEventId::new(format!("{call_id}-start")),
            sequence: 1,
            payload: ModelEvent::ToolCallStart {
                call_id: ModelToolCallId::new(call_id),
                name: SKILL_FUNCTION.to_owned(),
                extensions: Default::default(),
            },
        }),
        Ok(ModelStreamEvent {
            request_id: request_id.clone(),
            event_id: ModelEventId::new(format!("{call_id}-arguments")),
            sequence: 2,
            payload: ModelEvent::ToolCallArgumentsDelta {
                call_id: ModelToolCallId::new(call_id),
                delta: arguments,
            },
        }),
        Ok(ModelStreamEvent {
            request_id: request_id.clone(),
            event_id: ModelEventId::new(format!("{call_id}-end")),
            sequence: 3,
            payload: ModelEvent::ToolCallEnd {
                call_id: ModelToolCallId::new(call_id),
            },
        }),
        Ok(ModelStreamEvent {
            request_id,
            event_id: ModelEventId::new(format!("{call_id}-finish")),
            sequence: 4,
            payload: ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls,
            },
        }),
    ])))
}
