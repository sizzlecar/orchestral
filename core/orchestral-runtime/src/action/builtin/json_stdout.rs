use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

pub(crate) struct JsonStdoutAction {
    name: String,
    description: String,
}

impl JsonStdoutAction {
    pub(crate) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Serializes payload to stdout as one JSON object without side effects",
            ),
        }
    }

    pub(crate) fn internal() -> Self {
        Self {
            name: "json_stdout".to_string(),
            description: "Serializes payload to stdout as one JSON object without side effects"
                .to_string(),
        }
    }
}

#[async_trait]
impl Action for JsonStdoutAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_capabilities(["pure", "structured_output"])
            .with_roles(["emit"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured", "text"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "payload": {
                        "description": "Arbitrary JSON object to serialize to stdout."
                    }
                },
                "required": ["payload"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "stdout": { "type": "string" },
                    "stderr": { "type": "string" },
                    "status": { "type": "integer" }
                },
                "required": ["stdout", "stderr", "status"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let payload = input
            .params
            .get("payload")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));
        let stdout = match serde_json::to_string(&payload) {
            Ok(value) => value,
            Err(error) => {
                return ActionResult::error(format!("Failed to serialize payload: {}", error))
            }
        };
        let mut exports = HashMap::new();
        exports.insert("stdout".to_string(), Value::String(stdout));
        exports.insert("stderr".to_string(), Value::String(String::new()));
        exports.insert("status".to_string(), Value::Number(0.into()));
        ActionResult::success_with(exports)
    }
}
