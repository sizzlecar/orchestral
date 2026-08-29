use std::time::Duration;

use orchestral_model_openai::{OpenAiCompatibleBackend, OpenAiCompatibleConfig};
use orchestral_model_protocol_testkit::run_live_text_smoke;

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for the opt-in live smoke"))
}

#[tokio::test]
#[ignore = "requires an explicit live endpoint, model, and API key"]
async fn openai_compatible_adapter_live_smoke() {
    if std::env::var("ORCHESTRAL_LIVE_MODEL_SMOKE").as_deref() != Ok("1") {
        panic!("ORCHESTRAL_LIVE_MODEL_SMOKE=1 is required for the opt-in live smoke");
    }
    let backend = OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
        backend_id: "live/openai-compatible".to_owned(),
        endpoint: required("ORCHESTRAL_OPENAI_LIVE_ENDPOINT"),
        api_key: required("OPENAI_API_KEY"),
        model: required("ORCHESTRAL_OPENAI_LIVE_MODEL"),
        temperature: 0.0,
        default_max_output_tokens: 64,
        max_context_tokens: None,
        timeout: Duration::from_secs(60),
        structured_output: false,
        max_buffered_events: 128,
    })
    .expect("live OpenAI-compatible config is valid");

    let report = tokio::time::timeout(Duration::from_secs(75), run_live_text_smoke(&backend))
        .await
        .expect("live OpenAI-compatible smoke timed out")
        .expect("live OpenAI-compatible protocol/wiring smoke failed");
    assert!(report.event_count >= 2);
}
