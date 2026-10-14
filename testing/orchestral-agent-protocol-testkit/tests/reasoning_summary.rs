use orchestral_core::agent_protocol::wire::{AgentTelemetry, OutputId, ReasoningSummaryDelta};
use serde_json::json;

#[test]
fn reasoning_summaries_use_the_existing_v1_extension_envelope() {
    let summary = ReasoningSummaryDelta {
        summary_id: OutputId::new("request-summary"),
        delta: "Checking the interface.".into(),
    };
    let telemetry = summary.clone().into_telemetry();
    telemetry.validate_integrity().unwrap();
    let wire = serde_json::to_value(&telemetry).unwrap();
    assert_eq!(wire["type"], "extension");
    assert_eq!(wire["namespace"], "orchestral/reasoning-summary.v1");
    let decoded: AgentTelemetry = serde_json::from_value(wire).unwrap();
    assert_eq!(
        ReasoningSummaryDelta::from_telemetry(&decoded),
        Some(summary)
    );
    for value in [
        json!({"summary_id":"", "delta":"text"}),
        json!({"summary_id":"id","delta":""}),
        json!({"summary_id":"id","delta":7}),
    ] {
        assert!(
            ReasoningSummaryDelta::from_telemetry(&AgentTelemetry::Extension {
                namespace: "orchestral/reasoning-summary.v1".into(),
                value,
            })
            .is_none()
        );
    }
    assert!(
        ReasoningSummaryDelta::from_telemetry(&AgentTelemetry::ProgressReported {
            message: "working".into(),
            fraction: None
        })
        .is_none()
    );
}
