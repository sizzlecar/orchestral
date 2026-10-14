//! Optional, provider-neutral reasoning summaries on the v1 telemetry extension point.
use serde::{Deserialize, Serialize};

use super::types::{AgentTelemetry, OutputId};

const NAMESPACE: &str = "orchestral/reasoning-summary.v1";

/// An incremental summary explicitly exposed by the model provider. It is
/// ephemeral presentation, never deliverable output, opaque continuation state,
/// or a new message in the model's conversation history. Clients deduplicate by
/// the enclosing telemetry ID and append deltas sharing a summary ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningSummaryDelta {
    pub summary_id: OutputId,
    pub delta: String,
}

impl ReasoningSummaryDelta {
    pub fn into_telemetry(self) -> AgentTelemetry {
        AgentTelemetry::Extension {
            namespace: NAMESPACE.to_owned(),
            value: serde_json::json!(self),
        }
    }

    /// Older clients ignore the extension; malformed summaries are ignored too.
    pub fn from_telemetry(telemetry: &AgentTelemetry) -> Option<Self> {
        let AgentTelemetry::Extension { namespace, value } = telemetry else {
            return None;
        };
        if namespace != NAMESPACE {
            return None;
        }
        let report: Self = serde_json::from_value(value.clone()).ok()?;
        if report.summary_id.is_empty() || report.delta.is_empty() {
            return None;
        }
        Some(report)
    }
}
