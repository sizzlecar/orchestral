//! LLM planner example (mock client)

use orchestral_core::prelude::*;
use orchestral_planners::{LlmPlanner, LlmPlannerConfig, MockLlmClient};
use orchestral_stores::InMemoryReferenceStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let intent = Intent::new("Echo a message");

    let available_actions = vec![ActionMeta::new("echo", "Echoes the input back as output")];
    let reference_store = std::sync::Arc::new(InMemoryReferenceStore::new());
    let context = PlannerContext::new(available_actions, reference_store);

    let response = r#"{"goal":"Echo message","steps":[{"id":"s1","action":"echo","kind":"action","depends_on":[],"imports":[],"exports":[],"params":{"message":"Hello"}}],"confidence":0.9}"#;

    let planner = LlmPlanner::new(
        MockLlmClient {
            response: response.to_string(),
        },
        LlmPlannerConfig::default(),
    );

    let plan = planner.plan(&intent, &context).await?;
    println!("Plan goal: {}", plan.goal);
    println!("Steps: {}", plan.steps.len());

    Ok(())
}
