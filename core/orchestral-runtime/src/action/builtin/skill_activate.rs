use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

use crate::skill::SkillCatalog;

/// Action that loads full instructions for a named skill.
/// The planner calls this when it sees a relevant skill in the catalog
/// but the skill wasn't automatically injected via keyword matching.
pub(crate) struct SkillActivateAction {
    skill_catalog: Arc<RwLock<SkillCatalog>>,
}

impl SkillActivateAction {
    pub(crate) fn new(skill_catalog: Arc<RwLock<SkillCatalog>>) -> Self {
        Self { skill_catalog }
    }
}

#[async_trait]
impl Action for SkillActivateAction {
    fn name(&self) -> &str {
        "skill_activate"
    }

    fn description(&self) -> &str {
        "Load full instructions for a named skill"
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("utility")
            .with_capabilities(["pure", "read_only"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Exact skill name from the catalog (e.g. xlsx)"
                    }
                },
                "required": ["name"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "skill_name": { "type": "string" },
                    "instructions": { "type": "string" },
                    "skill_path": { "type": "string" },
                    "scripts_dir": { "type": "string" },
                    "venv_python": { "type": "string" }
                },
                "required": ["skill_name", "instructions"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(name) = input.params.get("name").and_then(Value::as_str) else {
            return ActionResult::error("Missing required parameter: name");
        };

        let catalog = self.skill_catalog.read().await;
        let Some(instruction) = catalog.get_instructions(name) else {
            return ActionResult::error(format!("Skill '{}' not found in catalog", name));
        };

        let mut exports = std::collections::HashMap::new();
        exports.insert(
            "skill_name".to_string(),
            Value::String(instruction.skill_name),
        );
        exports.insert(
            "instructions".to_string(),
            Value::String(instruction.instructions),
        );
        if let Some(path) = instruction.skill_path {
            exports.insert("skill_path".to_string(), Value::String(path));
        }
        if let Some(dir) = instruction.scripts_dir {
            exports.insert("scripts_dir".to_string(), Value::String(dir));
        }
        if let Some(python) = instruction.venv_python {
            exports.insert("venv_python".to_string(), Value::String(python));
        }

        ActionResult::success_with(exports)
    }
}

impl std::fmt::Debug for SkillActivateAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkillActivateAction").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skill::SkillEntry;
    use orchestral_core::store::WorkingSet;
    use std::path::PathBuf;
    use tokio::sync::RwLock;

    fn test_ctx() -> ActionContext {
        ActionContext::new(
            "task-1",
            "s1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
        )
    }

    #[tokio::test]
    async fn skill_activate_returns_full_instructions() {
        let catalog = Arc::new(RwLock::new(SkillCatalog::new(
            vec![SkillEntry {
                name: "xlsx".to_string(),
                description: "Spreadsheet automation".to_string(),
                instructions: "# Workflow\n- Use openpyxl\n- Recalculate\n".to_string(),
                source_path: PathBuf::from("/tmp/xlsx/SKILL.md"),
                scripts_dir: Some(PathBuf::from("/tmp/xlsx/scripts")),
                venv_python: Some(PathBuf::from("/tmp/xlsx/.venv/bin/python3")),
            }],
            3,
        )));

        let action = SkillActivateAction::new(catalog);
        let result = action
            .run(
                ActionInput::with_params(json!({ "name": "xlsx" })),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Success { exports } => {
                assert_eq!(
                    exports.get("skill_name"),
                    Some(&Value::String("xlsx".to_string()))
                );
                assert!(exports
                    .get("instructions")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .contains("openpyxl"));
                assert!(exports.contains_key("scripts_dir"));
                assert!(exports.contains_key("venv_python"));
            }
            other => panic!("expected success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn skill_activate_returns_error_for_unknown_skill() {
        let catalog = Arc::new(RwLock::new(SkillCatalog::new(vec![], 3)));
        let action = SkillActivateAction::new(catalog);
        let result = action
            .run(
                ActionInput::with_params(json!({ "name": "nonexistent" })),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Error { message } => {
                assert!(message.contains("not found"));
            }
            other => panic!("expected error, got {:?}", other),
        }
    }
}
