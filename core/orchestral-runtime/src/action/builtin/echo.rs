use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::support::config_string;

pub(super) struct EchoAction {
    name: String,
    description: String,
    prefix: String,
}

impl EchoAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        let prefix = config_string(&spec.config, "prefix").unwrap_or_default();
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Echoes the input back as output"),
            prefix,
        }
    }
}

#[async_trait]
impl Action for EchoAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("utility")
            .with_capability("pure")
            .with_roles(["emit"])
            .with_input_kinds(["text"])
            .with_output_kinds(["text"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "Text to echo back."
                    }
                },
                "required": ["message"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "result": {
                        "type": "string",
                        "description": "Echoed text result."
                    }
                },
                "required": ["result"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let message = input
            .params
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("No message provided");
        let result = format!("{}{}", self.prefix, message);
        ActionResult::success_with_one("result", Value::String(result))
    }
}
