//! Provider-neutral inspection of optional model reasoning controls.

use serde::{Deserialize, Serialize};

use crate::config::ReasoningPreference;

use super::ModelError;

/// Reasoning controls declared by a model service or its adapter.
/// This metadata is separate from the immutable execution descriptor: refreshing
/// a menu must not change an active Run's recovery identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelReasoningCapabilities {
    /// Explicit controls, excluding `Default` (which always omits controls).
    /// `None` means unknown; an empty list declares no explicit controls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_preferences: Option<Vec<ReasoningPreference>>,
    /// Whether thinking is enabled when controls are omitted. None is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_thinking_enabled: Option<bool>,
}

impl ModelReasoningCapabilities {
    pub fn validate(&self) -> Result<(), ModelError> {
        if let Some(preferences) = &self.supported_preferences {
            for (index, preference) in preferences.iter().enumerate() {
                if *preference == ReasoningPreference::Default
                    || preferences[..index].contains(preference)
                    || matches!(preference, ReasoningPreference::Custom(value) if value.trim().is_empty())
                {
                    return Err(ModelError::invalid_request(
                        "reasoning capabilities require distinct, non-empty explicit preferences; default is implicit",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Selectable preferences, including the always-available service default.
    pub fn choices(&self) -> Vec<ReasoningPreference> {
        std::iter::once(ReasoningPreference::Default)
            .chain(self.supported_preferences.iter().flatten().cloned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasoning_metadata_preserves_unknown_empty_and_literal_provider_efforts() {
        let unknown: ModelReasoningCapabilities = serde_json::from_str("{}").unwrap();
        assert_eq!(unknown.supported_preferences, None);
        assert_eq!(unknown.choices(), [ReasoningPreference::Default]);
        let known_empty = ModelReasoningCapabilities {
            supported_preferences: Some(Vec::new()),
            ..Default::default()
        };
        assert_ne!(unknown, known_empty);
        for name in ["on", "default", "future-effort"] {
            let literal = ReasoningPreference::from_effort(name).unwrap();
            let capabilities = ModelReasoningCapabilities {
                supported_preferences: Some(vec![literal.clone()]),
                ..Default::default()
            };
            capabilities.validate().unwrap();
            assert_eq!(
                capabilities.choices(),
                [ReasoningPreference::Default, literal]
            );
            assert_eq!(
                serde_json::from_value::<ModelReasoningCapabilities>(
                    serde_json::to_value(&capabilities).unwrap()
                )
                .unwrap(),
                capabilities
            );
        }
        for preferences in [
            vec![ReasoningPreference::Default],
            vec![ReasoningPreference::Low, ReasoningPreference::Low],
            vec![ReasoningPreference::Custom(" ".into())],
        ] {
            assert!(ModelReasoningCapabilities {
                supported_preferences: Some(preferences),
                ..Default::default()
            }
            .validate()
            .is_err());
        }
    }
}
