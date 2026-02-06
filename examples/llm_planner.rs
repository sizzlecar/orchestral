//! LLM planner example (mock client)

use orchestral_core::prelude::*;
use orchestral_planners::{LlmPlanner, LlmPlannerConfig, MockLlmClient};
use orchestral_stores::InMemoryReferenceStore;
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
    let reference_store = std::sync::Arc::new(InMemoryReferenceStore::new());
    let context = PlannerContext::new(available_actions, reference_store);

    let response = r#"{"type":"WORKFLOW","goal":"Echo message","steps":[{"id":"s1","action":"echo","kind":"action","depends_on":[],"imports":[],"exports":[],"params":{"message":"Hello"}}],"confidence":0.9}"#;

    let planner = LlmPlanner::new(
        MockLlmClient {
            response: response.to_string(),
        },
        LlmPlannerConfig::default(),
    );

    let output = planner.plan(&intent, &context).await?;
    match output {
        PlannerOutput::Workflow(plan) => {
            info!(goal = %plan.goal, steps = plan.steps.len(), "workflow planned");
        }
        PlannerOutput::DirectResponse(message) => {
            info!(%message, "direct response");
        }
        PlannerOutput::Clarification(question) => {
            info!(%question, "clarification required");
        }
    }

    Ok(())
}
