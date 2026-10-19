//! Native generateContent thinking controls, independent of authentication.

use orchestral_core::config::ReasoningPreference;

use super::{GeminiThinkingLevel, ModelError};

/// Documented controls for an exact Google model ID. Unknown IDs remain unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiReasoningCapabilities {
    pub preferences: Vec<ReasoningPreference>,
    pub default_enabled: bool,
}

impl GeminiReasoningCapabilities {
    /// Controls supported by Google's generateContent API, including Vertex AI.
    ///
    /// Source: <https://docs.cloud.google.com/vertex-ai/generative-ai/docs/thinking>.
    /// Do not infer capabilities from model-name prefixes or moving aliases.
    pub fn for_model(model: &str) -> Option<Self> {
        use ReasoningPreference::{Default, High, Low, Medium, Minimal, Off, On};
        let model = model.strip_prefix("models/").unwrap_or(model);
        let (preferences, default_enabled) = match model {
            "gemini-2.5-pro" => (vec![Default, On], true),
            "gemini-2.5-flash" => (vec![Default, On, Off], true),
            "gemini-2.5-flash-lite" => (vec![Default, On, Off], false),
            "gemini-3-pro-preview" => (vec![Default, Low, High], true),
            "gemini-3.1-pro-preview"
            | "gemini-3.1-pro-preview-customtools"
            | "gemini-3.7-flash"
            | "gemini-3.8-flash"
            | "gemini-4-argon" => (vec![Default, Low, Medium, High], true),
            "gemini-3-flash-preview"
            | "gemini-3.1-flash-lite"
            | "gemini-3.1-flash-lite-preview"
            | "gemini-3.5-flash"
            | "gemini-3.5-flash-lite"
            | "gemini-3.6-flash" => (vec![Default, Minimal, Low, Medium, High], true),
            "gemini-3.1-flash-image"
            | "gemini-3.1-flash-image-preview"
            | "gemini-3.1-flash-lite-image" => (vec![Default, Minimal, High], true),
            "gemini-3-pro-image" | "gemini-3-pro-image-preview" => (vec![Default, High], true),
            _ => return None,
        };
        Some(Self {
            preferences,
            default_enabled,
        })
    }
}

/// A single native control, so levels and budgets cannot be sent together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeminiThinkingControl {
    Level(GeminiThinkingLevel),
    /// `-1` requests dynamic thinking; `0` explicitly disables thinking.
    Budget(i32),
}

impl GeminiThinkingControl {
    /// Resolve a neutral preference without credentials or network access.
    /// Unknown model IDs allow explicit native controls for service validation.
    pub fn from_preference(
        model: &str,
        preference: &ReasoningPreference,
    ) -> Result<Option<Self>, ModelError> {
        use ReasoningPreference as Preference;
        if let Some(capabilities) = GeminiReasoningCapabilities::for_model(model) {
            if !capabilities.preferences.contains(preference) {
                return Err(ModelError::invalid_request(format!(
                    "reasoning preference '{preference}' is not supported by Gemini model '{model}'; supported values: {}",
                    capabilities.preferences.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
                )));
            }
        }
        Ok(match preference {
            Preference::Default => None,
            Preference::Minimal => Some(Self::Level(GeminiThinkingLevel::Minimal)),
            Preference::Low => Some(Self::Level(GeminiThinkingLevel::Low)),
            Preference::Medium => Some(Self::Level(GeminiThinkingLevel::Medium)),
            Preference::High => Some(Self::Level(GeminiThinkingLevel::High)),
            Preference::On => Some(Self::Budget(-1)),
            Preference::Off => Some(Self::Budget(0)),
            _ => return Err(ModelError::invalid_request(format!(
                "reasoning preference '{preference}' has no Gemini thinking control; use default, minimal, low, medium, high, on, or off as supported by the model"
            ))),
        })
    }
}
