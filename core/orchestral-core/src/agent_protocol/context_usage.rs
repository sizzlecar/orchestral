//! Optional context observations carried by the v1 telemetry extension point.
use serde::{Deserialize, Serialize};

use super::types::AgentTelemetry;

const NAMESPACE: &str = "orchestral/context-usage.v1";

/// Input size for one model request, never cumulative Run usage or live KV
/// occupancy. An absent serving capacity means unreported. Older v1 clients
/// can ignore this namespaced extension without rejecting core telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextUsageReport {
    pub request_id: String,
    pub input_tokens: u64,
    pub input_tokens_estimated: bool,
    #[serde(default)]
    pub max_context_tokens: Option<u64>,
}

impl ContextUsageReport {
    pub fn into_telemetry(self) -> AgentTelemetry {
        AgentTelemetry::Extension {
            namespace: NAMESPACE.to_owned(),
            value: serde_json::json!(self),
        }
    }

    /// Ignore unrelated, malformed, or unsupported context observations.
    pub fn from_telemetry(telemetry: &AgentTelemetry) -> Option<Self> {
        let AgentTelemetry::Extension { namespace, value } = telemetry else {
            return None;
        };
        if namespace != NAMESPACE {
            return None;
        }
        let report: Self = serde_json::from_value(value.clone()).ok()?;
        if report.request_id.trim().is_empty() || report.max_context_tokens == Some(0) {
            return None;
        }
        Some(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_reports_keep_estimates_and_unknown_limits_through_v1_telemetry() {
        let report = ContextUsageReport {
            request_id: "model-request".into(),
            input_tokens: 8194,
            input_tokens_estimated: true,
            max_context_tokens: None,
        };
        let telemetry = report.clone().into_telemetry();
        telemetry.validate_integrity().unwrap();
        let wire = serde_json::to_value(&telemetry).unwrap();
        assert_eq!(wire["type"], "extension");
        let decoded: AgentTelemetry = serde_json::from_value(wire).unwrap();
        assert_eq!(ContextUsageReport::from_telemetry(&decoded), Some(report));
    }

    #[test]
    fn malformed_or_unrelated_reports_do_not_become_context_facts() {
        for value in [
            serde_json::json!({"request_id":"request", "input_tokens":-1, "input_tokens_estimated":false}),
            serde_json::json!({"request_id":"request", "input_tokens":12, "input_tokens_estimated":false, "max_context_tokens":0}),
            serde_json::json!({"request_id":"", "input_tokens":12, "input_tokens_estimated":false}),
        ] {
            assert!(
                ContextUsageReport::from_telemetry(&AgentTelemetry::Extension {
                    namespace: NAMESPACE.into(),
                    value
                })
                .is_none()
            );
        }
        assert!(
            ContextUsageReport::from_telemetry(&AgentTelemetry::ProgressReported {
                message: "working".into(),
                fraction: None
            })
            .is_none()
        );
    }
}
