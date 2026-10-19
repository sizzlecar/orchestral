//! Resolution of provider-neutral model preferences.

use anyhow::Context;
use orchestral_core::config::{ModelProfile, ReasoningPreference};

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

#[cfg(test)]
mod tests;
