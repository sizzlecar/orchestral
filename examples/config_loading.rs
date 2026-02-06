//! Example: Loading configuration and creating LLM clients from config
//!
//! This example demonstrates:
//! - Loading the unified orchestral.yaml config
//! - Creating LLM clients from provider specifications
//! - Using the factory pattern to build clients
//!
//! Run with: cargo run --example config_loading
//!
//! Note: To test with real providers, set the appropriate environment variables:
//! - OPENAI_API_KEY for OpenAI
//! - GEMINI_API_KEY for Gemini

use std::path::Path;
use std::sync::Arc;

use orchestral_actions::{ActionRegistryManager, DefaultActionFactory};
use orchestral_config::{load_config, OrchestralConfig};
use orchestral_planners::{DefaultLlmClientFactory, LlmClientFactory, LlmInvocationConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the unified configuration
    let config_path = Path::new("configs/orchestral.yaml");
    let config: OrchestralConfig = load_config(config_path)?;

    println!("=== Configuration Loaded ===\n");

    // Display providers
    let providers = config.providers();
    println!("LLM Backends:");
    for name in providers.backend_names() {
        println!("  - {}", name);
    }
    if let Some(default) = &providers.default_backend {
        println!("  Default Backend: {}", default);
    }
    if let Some(default) = &providers.default_model {
        println!("  Default Model Profile: {}", default);
    }
    println!();

    // Display actions
    let actions = config.actions();
    println!("Actions:");
    for name in actions.names() {
        println!("  - {}", name);
    }
    println!();

    // Demo: Create an LLM client from config
    println!("=== LLM Client Factory Demo ===\n");

    let factory = DefaultLlmClientFactory::new();

    // Try to get default backend + model profile
    if let (Some(backend), Some(profile)) = (
        providers.get_default_backend(),
        providers.get_default_model(),
    ) {
        println!("Attempting to create client:");
        println!("  Backend: {} ({})", backend.name, backend.kind);
        println!("  Model Profile: {}", profile.name);
        println!("  Model: {}", profile.model);
        println!("  Temperature: {}", profile.temperature.unwrap_or(0.2));

        let invocation = LlmInvocationConfig {
            model: profile.model.clone(),
            temperature: profile.temperature.unwrap_or(0.2),
            normalize_response: true,
        };

        // Note: This will fail if API key env var is not set
        match factory.build(&backend, &invocation) {
            Ok(_client) => {
                println!("  Status: Client created successfully!");
                println!("  (Skipping actual API call to avoid costs)\n");
            }
            Err(e) => {
                println!("  Status: Failed to create client");
                println!("  Error: {}", e);
                println!(
                    "  Hint: Set {} environment variable\n",
                    backend.api_key_env.as_deref().unwrap_or("API_KEY")
                );
            }
        }
    }

    // Demo: Load actions from config
    println!("=== Action Registry Demo ===\n");

    let action_factory = Arc::new(DefaultActionFactory::new());
    let registry_manager = ActionRegistryManager::new("configs/orchestral.yaml", action_factory);

    // Load from the unified config instead
    let registry = registry_manager.registry();
    let mut reg = registry.write().await;

    // Build actions manually from config
    let factory = DefaultActionFactory::new();
    for spec in &actions.actions {
        match orchestral_actions::ActionFactory::build(&factory, spec) {
            Ok(action) => {
                println!("Registered action: {}", action.name());
                reg.register(action);
            }
            Err(e) => {
                println!("Failed to build action {}: {}", spec.name, e);
            }
        }
    }

    println!("\n=== Done ===");
    Ok(())
}
