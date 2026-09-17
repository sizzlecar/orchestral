//! Provider-neutral reasoning preferences. Adapters decide how to encode them.

use serde::{Deserialize, Serialize};

/// An explicit reasoning preference, distinct from an absent configuration override.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
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
    /// A provider-defined effort name, preserved without normalization.
    Custom(String),
    /// Enable thinking when the provider exposes a binary thinking control.
    On,
    /// Disable thinking when the provider exposes a binary thinking control.
    Off,
}

impl ReasoningPreference {
    pub fn as_str(&self) -> std::borrow::Cow<'_, str> {
        if let Self::Custom(value) = self {
            return if matches!(value.as_str(), "default" | "on" | "off")
                || value.starts_with("effort:")
            {
                format!("effort:{value}").into()
            } else {
                value.as_str().into()
            };
        }
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
            Self::Custom(_) => unreachable!(),
        }
        .into()
    }

    /// Interpret a service-declared name as an effort, never as a local toggle/default.
    pub fn from_effort(value: &str) -> Result<Self, String> {
        if value.trim().is_empty() {
            return Err("reasoning effort must be a non-empty string".to_owned());
        }
        Ok(match value {
            "none" => Self::None,
            "minimal" => Self::Minimal,
            "low" => Self::Low,
            "medium" => Self::Medium,
            "high" => Self::High,
            "xhigh" => Self::XHigh,
            "max" => Self::Max,
            _ => Self::Custom(value.to_owned()),
        })
    }
}

impl std::fmt::Display for ReasoningPreference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_str())
    }
}

impl std::str::FromStr for ReasoningPreference {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "default" => Ok(Self::Default),
            "on" => Ok(Self::On),
            "off" => Ok(Self::Off),
            _ => Self::from_effort(value.strip_prefix("effort:").unwrap_or(value)),
        }
    }
}

impl TryFrom<String> for ReasoningPreference {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ReasoningPreference> for String {
    fn from(value: ReasoningPreference) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_efforts_round_trip_without_becoming_local_controls() {
        for name in [
            "ultra",
            "future-next",
            "HIGH",
            "default",
            "on",
            "off",
            "effort:on",
            "effort:",
        ] {
            let effort = ReasoningPreference::from_effort(name).unwrap();
            assert!(matches!(effort, ReasoningPreference::Custom(_)));
            let json = serde_json::to_value(&effort).unwrap();
            assert_eq!(
                serde_json::from_value::<ReasoningPreference>(json).unwrap(),
                effort
            );
            assert_eq!(
                effort.to_string().parse::<ReasoningPreference>().unwrap(),
                effort
            );
        }
        assert_eq!(
            "default".parse::<ReasoningPreference>().unwrap(),
            ReasoningPreference::Default
        );
        assert_eq!(
            "on".parse::<ReasoningPreference>().unwrap(),
            ReasoningPreference::On
        );
        assert_eq!(
            "off".parse::<ReasoningPreference>().unwrap(),
            ReasoningPreference::Off
        );
        assert_eq!(
            "none".parse::<ReasoningPreference>().unwrap(),
            ReasoningPreference::None
        );
        for invalid in ["", " ", "effort:", "effort: "] {
            assert!(invalid.parse::<ReasoningPreference>().is_err());
        }
    }
}
