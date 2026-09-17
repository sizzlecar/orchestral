//! Provider-neutral reasoning preferences. Adapters decide how to encode them.

use serde::{Deserialize, Serialize};

/// An explicit reasoning preference, distinct from an absent configuration override.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningPreference {
    /// Leave the provider's default unchanged; omit reasoning controls from requests.
    #[default]
    Default,
    /// Explicitly request the protocol's `none` effort.
    None,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    /// Enable thinking when the provider exposes a binary thinking control.
    On,
    /// Disable thinking when the provider exposes a binary thinking control.
    Off,
}

impl ReasoningPreference {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

impl std::fmt::Display for ReasoningPreference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ReasoningPreference {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|_| {
            "expected default, none, minimal, low, medium, high, xhigh, max, on, or off".to_owned()
        })
    }
}
