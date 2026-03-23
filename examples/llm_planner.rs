//! LLM planner example (mock client)

use orchestral_core::prelude::*;
use orchestral_runtime::planner::{LlmPlanner, LlmPlannerConfig, MockLlmClient};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let intent = Intent::new("Echo a message");

    let available_actions = vec![ActionMeta::new("echo", "Echoes the input back as output")];
    let context = PlannerContext::new(available_actions);

    let response =
        r#"{"type":"SINGLE_ACTION","action":"echo","params":{"input":"hello"},"reason":"example"}"#;

    let planner = LlmPlanner::new(
        MockLlmClient {
            response: response.to_string(),
        },
        LlmPlannerConfig::default(),
    );

    let output = planner.plan(&intent, &context).await?;
    match output {
        PlannerOutput::SingleAction(call) => {
            info!(action = %call.action, reason = ?call.reason, "single action");
        }
        PlannerOutput::MiniPlan(plan) => {
            info!(goal = %plan.goal, step_count = plan.steps.len(), "mini plan");
        }
        PlannerOutput::Done(message) => {
            info!(%message, "done");
        }
        PlannerOutput::NeedInput(question) => {
            info!(%question, "need input");
        }
    }

    Ok(())
}
