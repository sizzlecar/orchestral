//! Explicit reasoning requests and optional model-declared controls. Provider
//! effort names are extensible and preserve their exact spelling on the wire.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum OpenAiReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Custom(String),
}

impl OpenAiReasoningEffort {
    /// Common spellings, not an exhaustive capability list.
    pub const ALL: [Self; 7] = [
        Self::None,
        Self::Minimal,
        Self::Low,
        Self::Medium,
        Self::High,
        Self::XHigh,
        Self::Max,
    ];

    pub fn as_str(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
            Self::Custom(value) => value,
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
        if value.trim().is_empty() {
            return Err("reasoning effort must be a non-empty string".to_owned());
        }
        Ok(Self::ALL
            .into_iter()
            .find(|effort| effort.as_str() == value)
            .unwrap_or_else(|| Self::Custom(value.to_owned())))
    }
}

impl TryFrom<String> for OpenAiReasoningEffort {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<OpenAiReasoningEffort> for String {
    fn from(value: OpenAiReasoningEffort) -> Self {
        value.to_string()
    }
}

/// Select one wire control, preventing contradictory standard/extension fields.
/// Absence of this enum means the service's default; `Effort(None)` is an
/// explicit request to disable reasoning and must not be omitted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
