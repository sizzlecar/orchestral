use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::executor::ActionRegistry;

/// Action that looks up metadata (including full input/output schema) for any
/// registered action. Designed to let the planner discover MCP tool schemas
/// before invoking them.
pub(crate) struct ToolLookupAction {
    registry: Arc<RwLock<ActionRegistry>>,
}

impl ToolLookupAction {
    pub(crate) fn new(registry: Arc<RwLock<ActionRegistry>>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl Action for ToolLookupAction {
    fn name(&self) -> &str {
        "tool_lookup"
    }

    fn description(&self) -> &str {
        "Look up the full input/output schema of a registered action by name"
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
                        "description": "Exact action name to look up (e.g. mcp__github__create_issue)"
                    }
                },
                "required": ["name"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "description": { "type": "string" },
                    "input_schema": { "type": "object" },
                    "output_schema": { "type": "object" }
                },
                "required": ["name", "description", "input_schema", "output_schema"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(name) = input.params.get("name").and_then(Value::as_str) else {
            return ActionResult::error("Missing required parameter: name");
        };

        let registry = self.registry.read().await;
        let Some(action) = registry.get(name) else {
            return ActionResult::error(format!("Action '{}' not found in registry", name));
        };

        let meta = action.metadata();
        ActionResult::success_with(
            [
                ("name".to_string(), Value::String(meta.name.clone())),
                (
                    "description".to_string(),
                    Value::String(meta.description.clone()),
                ),
                ("input_schema".to_string(), meta.input_schema.clone()),
                ("output_schema".to_string(), meta.output_schema.clone()),
            ]
            .into_iter()
            .collect(),
        )
    }
}

impl std::fmt::Debug for ToolLookupAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolLookupAction").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::store::WorkingSet;

    fn test_ctx() -> ActionContext {
        ActionContext::new(
            "task-1",
            "s1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
        )
    }

    #[tokio::test]
    async fn tool_lookup_returns_schema_for_registered_action() {
        let registry = Arc::new(RwLock::new(ActionRegistry::new()));

        // Register a mock action in the registry.
        let mock = Arc::new(MockAction {
            name: "mcp__github__create_issue".to_string(),
            description: "Create a GitHub issue".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": { "repo": { "type": "string" } },
                "required": ["repo"]
            }),
        });
        registry.write().await.register(mock);

        let lookup = ToolLookupAction::new(registry);
        let result = lookup
            .run(
                ActionInput::with_params(json!({ "name": "mcp__github__create_issue" })),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Success { exports } => {
                assert_eq!(
                    exports.get("name"),
                    Some(&Value::String("mcp__github__create_issue".to_string()))
                );
                assert_eq!(
                    exports.get("description"),
                    Some(&Value::String("Create a GitHub issue".to_string()))
                );
                let schema = exports.get("input_schema").expect("input_schema");
                assert!(schema.get("properties").is_some());
            }
            other => panic!("expected success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn tool_lookup_returns_error_for_unknown_action() {
        let registry = Arc::new(RwLock::new(ActionRegistry::new()));
        let lookup = ToolLookupAction::new(registry);
        let result = lookup
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

    struct MockAction {
        name: String,
        description: String,
        input_schema: Value,
    }

    #[async_trait]
    impl Action for MockAction {
        fn name(&self) -> &str {
            &self.name
        }
        fn description(&self) -> &str {
            &self.description
        }
        fn metadata(&self) -> ActionMeta {
            ActionMeta::new(&self.name, &self.description)
                .with_input_schema(self.input_schema.clone())
        }
        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            ActionResult::error("mock not callable")
        }
    }
}
