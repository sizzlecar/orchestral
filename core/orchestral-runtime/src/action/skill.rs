use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::factory::ActionBuildError;

#[derive(Debug, Clone, Deserialize)]
struct SkillActionConfig {
    skill_name: String,
    source_path: String,
    content: String,
}

pub fn build_skill_action(spec: &ActionSpec) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    match spec.kind.as_str() {
        "skill_prompt" => {
            let action = SkillPromptAction::from_spec(spec)?;
            Ok(Some(Box::new(action)))
        }
        _ => Ok(None),
    }
}

struct SkillPromptAction {
    name: String,
    description: String,
    config: SkillActionConfig,
}

impl SkillPromptAction {
    fn from_spec(spec: &ActionSpec) -> Result<Self, ActionBuildError> {
        let config: SkillActionConfig =
            serde_json::from_value(spec.config.clone()).map_err(|err| {
                ActionBuildError::InvalidConfig(format!(
                    "action '{}' invalid skill_prompt config: {}",
                    spec.name, err
                ))
            })?;

        Ok(Self {
            name: spec.name.clone(),
            description: spec.description_or("Expose a local skill prompt as an action"),
            config,
        })
    }
}

#[async_trait]
impl Action for SkillPromptAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_capabilities(["instruction_only", "skill_prompt"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Optional question to contextualize the returned skill instructions"
                    }
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "skill": {"type": "string"},
                    "source_path": {"type": "string"},
                    "instructions": {"type": "string"},
                    "query": {"type": "string"}
                },
                "required": ["skill", "source_path", "instructions"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let mut exports = HashMap::new();
        exports.insert(
            "skill".to_string(),
            Value::String(self.config.skill_name.clone()),
        );
        exports.insert(
            "source_path".to_string(),
            Value::String(self.config.source_path.clone()),
        );
        exports.insert(
            "instructions".to_string(),
            Value::String(self.config.content.clone()),
        );
        if let Some(query) = input.params.get("query").and_then(Value::as_str) {
            exports.insert("query".to_string(), Value::String(query.to_string()));
        }
        ActionResult::success_with(exports)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use orchestral_core::store::{
        Reference, ReferenceStore, ReferenceType, StoreError, WorkingSet,
    };
    use tokio::sync::RwLock;

    struct NoopReferenceStore;

    #[async_trait]
    impl ReferenceStore for NoopReferenceStore {
        async fn add(&self, _reference: Reference) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get(&self, _id: &str) -> Result<Option<Reference>, StoreError> {
            Ok(None)
        }

        async fn query_by_type(
            &self,
            _ref_type: &ReferenceType,
        ) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn query_recent(&self, _limit: usize) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn delete(&self, _id: &str) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    fn test_ctx() -> ActionContext {
        ActionContext::new(
            "task-1",
            "s1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
            Arc::new(NoopReferenceStore),
        )
    }

    #[test]
    fn build_skill_action_rejects_invalid_config() {
        let spec = ActionSpec {
            name: "skill__demo".to_string(),
            kind: "skill_prompt".to_string(),
            description: None,
            config: json!({
                "skill_name": "demo"
            }),
            interface: None,
        };
        match SkillPromptAction::from_spec(&spec) {
            Ok(_) => panic!("should fail"),
            Err(err) => assert!(err.to_string().contains("invalid skill_prompt config")),
        }
    }

    #[test]
    fn run_skill_action_returns_instruction_payload() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "skill__demo".to_string(),
                kind: "skill_prompt".to_string(),
                description: Some("demo".to_string()),
                config: json!({
                    "skill_name": "demo",
                    "source_path": "/tmp/demo/SKILL.md",
                    "content": "Use tool x."
                }),
                interface: None,
            };
            let action = SkillPromptAction::from_spec(&spec).expect("build action");
            let result = action
                .run(
                    ActionInput::with_params(json!({"query":"how?"})),
                    test_ctx(),
                )
                .await;

            match result {
                ActionResult::Success { exports } => {
                    assert_eq!(exports.get("skill"), Some(&json!("demo")));
                    assert_eq!(
                        exports.get("source_path"),
                        Some(&json!("/tmp/demo/SKILL.md"))
                    );
                    assert_eq!(exports.get("instructions"), Some(&json!("Use tool x.")));
                    assert_eq!(exports.get("query"), Some(&json!("how?")));
                }
                other => panic!("expected success result, got {:?}", other),
            }
        });
    }
}
