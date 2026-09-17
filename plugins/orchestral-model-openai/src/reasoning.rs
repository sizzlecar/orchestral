//! Explicit reasoning requests and optional model-declared controls. A known
//! wire vocabulary is not a claim that every endpoint/model supports its values.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenAiReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}

impl OpenAiReasoningEffort {
    /// Recognized request values, independently of a model's advertised support.
    pub const ALL: [Self; 7] = [
        Self::None,
        Self::Minimal,
        Self::Low,
        Self::Medium,
        Self::High,
        Self::XHigh,
        Self::Max,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}

impl fmt::Display for OpenAiReasoningEffort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for OpenAiReasoningEffort {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|effort| effort.as_str() == value)
            .ok_or_else(|| {
                "expected reasoning effort none, minimal, low, medium, high, xhigh, or max"
                    .to_owned()
            })
    }
}

/// Select one wire control, preventing contradictory standard/extension fields.
/// Absence of this enum means the service's default; `Effort(None)` is an
/// explicit request to disable reasoning and must not be omitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OpenAiReasoningControl {
    Effort(OpenAiReasoningEffort),
    Thinking(bool),
}

/// Optional extension returned alongside an OpenAI-compatible model ID.
/// Unknown fields remain compatible with other endpoint extensions.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenAiReasoningCapabilities {
    /// None means undeclared; an empty list explicitly declares no effort levels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_efforts: Option<Vec<OpenAiReasoningEffort>>,
    /// Presence advertises an actual enable/disable control, not effort levels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<OpenAiThinkingCapability>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenAiThinkingCapability {
    /// The service's effective default, including any server override.
    pub default_enabled: bool,
}

#[cfg(test)]
mod tests;
