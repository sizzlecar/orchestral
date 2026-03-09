//! LLM planner example (mock client)

use orchestral_core::prelude::*;
use orchestral_core::store::InMemoryReferenceStore;
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
    let reference_store = std::sync::Arc::new(InMemoryReferenceStore::new());
    let context = PlannerContext::new(available_actions, reference_store);

    let response = r#"{"type":"WORKFLOW","goal":"Echo message","steps":[{"id":"s1","action":"echo","kind":"action","depends_on":[],"exports":[],"params":{"message":"Hello"}}],"confidence":0.9}"#;

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
        PlannerOutput::StageChoice(choice) => {
            info!(
                recipe_family = ?choice.recipe_family,
                artifact_family = ?choice.artifact_family,
                current_stage = ?choice.current_stage,
                "reactor stage choice"
            );
        }
    }

    Ok(())
}
