//! Composition of neutral model preferences with concrete provider controls.

use anyhow::{bail, Context};
use orchestral_core::config::{BackendSpec, ModelProfile, ReasoningPreference};
use orchestral_model_openai::{OpenAiReasoningControl, OpenAiReasoningEffort};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredModel {
    pub id: String,
    /// None means the service did not declare capabilities, not that it supports every effort.
    pub reasoning: Option<Vec<ReasoningPreference>>,
    pub thinking_default_enabled: Option<bool>,
}

pub(crate) fn resolve_reasoning(
    configured: Option<ReasoningPreference>,
    profile: Option<&ModelProfile>,
) -> anyhow::Result<ReasoningPreference> {
    if let Some(value) = configured {
        return Ok(value);
    }
    profile
        .and_then(|profile| profile.config.get("reasoning"))
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .context("parse model profile config.reasoning")
        .map(Option::unwrap_or_default)
}

pub(crate) fn openai_reasoning(
    backend: &BackendSpec,
    preference: ReasoningPreference,
) -> anyhow::Result<Option<OpenAiReasoningControl>> {
    use OpenAiReasoningEffort as Effort;
    use ReasoningPreference as Preference;
    if preference == Preference::Default {
        return Ok(None);
    }
    if !crate::openai_connection::supports_discovery(backend) {
        bail!("reasoning preference '{preference}' is not supported by backend protocol '{}'; use its native configuration", backend.kind);
    }
    Ok(Some(match preference {
        Preference::On => OpenAiReasoningControl::Thinking(true),
        Preference::Off => OpenAiReasoningControl::Thinking(false),
        Preference::None => OpenAiReasoningControl::Effort(Effort::None),
        Preference::Minimal => OpenAiReasoningControl::Effort(Effort::Minimal),
        Preference::Low => OpenAiReasoningControl::Effort(Effort::Low),
        Preference::Medium => OpenAiReasoningControl::Effort(Effort::Medium),
        Preference::High => OpenAiReasoningControl::Effort(Effort::High),
        Preference::XHigh => OpenAiReasoningControl::Effort(Effort::XHigh),
        Preference::Max => OpenAiReasoningControl::Effort(Effort::Max),
        Preference::Custom(value) => {
            OpenAiReasoningControl::Effort(value.parse().map_err(anyhow::Error::msg)?)
        }
        Preference::Default => unreachable!(),
    }))
}

pub(crate) fn discovered_model(model: orchestral_model_openai::DiscoveredModel) -> DiscoveredModel {
    use OpenAiReasoningEffort as Effort;
    use ReasoningPreference as Preference;
    let thinking_default_enabled = model
        .reasoning
        .as_ref()
        .and_then(|caps| caps.thinking.as_ref())
        .map(|thinking| thinking.default_enabled);
    let reasoning = model
        .reasoning
        .filter(|caps| caps.supported_efforts.is_some() || caps.thinking.is_some())
        .map(|caps| {
            let mut values = vec![Preference::Default];
            if let Some(efforts) = caps.supported_efforts {
                values.extend(efforts.into_iter().map(|effort| match effort {
                    Effort::None => Preference::None,
                    Effort::Minimal => Preference::Minimal,
                    Effort::Low => Preference::Low,
                    Effort::Medium => Preference::Medium,
                    Effort::High => Preference::High,
                    Effort::XHigh => Preference::XHigh,
                    Effort::Max => Preference::Max,
                    Effort::Custom(value) => Preference::Custom(value),
                }));
            }
            if caps.thinking.is_some() {
                values.extend([Preference::On, Preference::Off]);
            }
            values
        });
    DiscoveredModel {
        id: model.id,
        reasoning,
        thinking_default_enabled,
    }
}

#[cfg(test)]
mod tests;
