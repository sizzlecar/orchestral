//! SDK Quickstart: Define a custom action and run it through Orchestral's planner.
//!
//! ```bash
//! export OPENROUTER_API_KEY="sk-or-..."
//! cargo run --example sdk_quickstart
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral::core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral::{Orchestral, SdkError};

/// A custom action that reverses a string.
struct ReverseAction;

#[async_trait]
impl Action for ReverseAction {
    fn name(&self) -> &str {
        "reverse"
    }

    fn description(&self) -> &str {
        "Reverse a string"
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new("reverse", "Reverse a string")
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string", "description": "Text to reverse" }
                },
                "required": ["text"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "result": { "type": "string" }
                },
                "required": ["result"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let text = input
            .params
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("");
        let reversed: String = text.chars().rev().collect();
        ActionResult::success_with(HashMap::from([(
            "result".to_string(),
            Value::String(reversed),
        )]))
    }
}

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    let app = Orchestral::builder()
        .action(ReverseAction)
        .planner_backend("openrouter")
        .planner_model("anthropic/claude-sonnet-4.5")
        .build()
        .await?;

    let result = app.run("Reverse the text 'Hello Orchestral'").await?;
    println!("Status: {}", result.status);
    println!("Message: {}", result.message);

    Ok(())
}
