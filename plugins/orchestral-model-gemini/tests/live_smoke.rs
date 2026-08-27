use std::time::Duration;

use orchestral_model_gemini::{GeminiModelBackend, GeminiModelConfig};
use orchestral_model_protocol_testkit::run_live_text_smoke;

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for the opt-in live smoke"))
}

#[tokio::test]
#[ignore = "requires an explicit live endpoint, model, and API key"]
async fn gemini_native_adapter_live_smoke() {
    if std::env::var("ORCHESTRAL_LIVE_MODEL_SMOKE").as_deref() != Ok("1") {
        panic!("ORCHESTRAL_LIVE_MODEL_SMOKE=1 is required for the opt-in live smoke");
    }
    let api_key = std::env::var("GOOGLE_API_KEY")
        .or_else(|_| std::env::var("GEMINI_API_KEY"))
        .expect("GOOGLE_API_KEY or GEMINI_API_KEY is required for the opt-in live smoke");
    let backend = GeminiModelBackend::new(GeminiModelConfig {
        backend_id: "live/gemini-native".to_owned(),
        endpoint: required("ORCHESTRAL_GEMINI_LIVE_ENDPOINT"),
        api_key,
        model: required("ORCHESTRAL_GEMINI_LIVE_MODEL"),
        temperature: 0.0,
        default_max_output_tokens: 64,
        max_context_tokens: None,
        timeout: Duration::from_secs(60),
        max_buffered_events: 128,
    })
    .expect("live Gemini config is valid");

    let report = tokio::time::timeout(Duration::from_secs(75), run_live_text_smoke(&backend))
        .await
        .expect("live Gemini smoke timed out")
        .expect("live Gemini protocol/wiring smoke failed");
    assert!(report.event_count >= 2);
}
