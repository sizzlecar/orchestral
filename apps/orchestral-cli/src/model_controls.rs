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

use crate::agent::HostMetadata;
use crate::runtime::ModelOverrides;
use orchestral_core::config::PermissionPreset;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum Selection {
    ApiModel {
        backend: String,
        id: String,
    },
    Profile {
        name: String,
    },
    Profiles,
    Reasoning {
        backend: String,
        model: String,
        value: ReasoningPreference,
    },
    Permissions {
        value: PermissionPreset,
    },
}

/// Keep the effective connection and current session's explicit choices when selecting an API ID.
pub(crate) fn apply_selection(
    metadata: &HostMetadata,
    current: &str,
    overrides: &mut ModelOverrides,
    selection: Selection,
) -> anyhow::Result<()> {
    match selection {
        Selection::ApiModel { backend, id } => {
            if backend != metadata.model_backend.name || id.trim().is_empty() {
                anyhow::bail!("Model selection is stale; refresh /model");
            }
            // In particular, do not copy the synthesized cli-openai backend into
            // overrides: it need not exist in the user's original configuration.
            overrides.model = Some(id);
        }
        Selection::Profile { name } => {
            let profile = metadata
                .models
                .iter()
                .find(|profile| profile.name == name)
                .context("Model profile is no longer available")?;
            overrides.backend = Some(profile.backend.clone());
            overrides.model = Some(profile.model.clone());
            overrides.model_profile = Some(profile.name.clone());
            // A profile is an explicit selection of its configured connection.
            overrides.base_url = None;
            overrides.api_key_env = None;
            overrides.no_auth = false;
        }
        Selection::Reasoning {
            backend,
            model,
            value,
        } => {
            if backend != metadata.model_backend.name || model != current {
                anyhow::bail!("Reasoning selection is stale; refresh /reasoning");
            }
            overrides.reasoning = Some(value);
            // Reconfiguration must not rediscover a different ID or revert to the profile's ID.
            overrides.model = Some(current.to_owned());
        }
        Selection::Permissions { value } => {
            overrides.permissions = Some(value);
            overrides.model = Some(current.to_owned());
        }
        Selection::Profiles => anyhow::bail!("Profile navigation is not a model selection"),
    }
    Ok(())
}
