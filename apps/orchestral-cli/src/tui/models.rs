//! Live model discovery and explicit model/profile/reasoning selections.

use super::menu::{Choice, Menu, MenuKind};
use crate::agent::HostMetadata;
use crate::runtime::ModelOverrides;
use anyhow::{bail, Context, Result};
use orchestral_core::config::{PermissionPreset, ReasoningPreference};
use orchestral_core::model_protocol::ModelReasoningCapabilities;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(super) enum Selection {
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

fn choice(label: &str, selection: Selection, description: impl Into<String>) -> Choice {
    Choice::new(
        label,
        serde_json::to_string(&selection).expect("model selection serializes"),
        description,
    )
}

pub(super) fn profiles(metadata: &HostMetadata, current: &str) -> Menu {
    Menu::new(
        MenuKind::Models,
        "Models · configured profiles",
        metadata
            .models
            .iter()
            .map(|profile| {
                choice(
                    &profile.name,
                    Selection::Profile {
                        name: profile.name.clone(),
                    },
                    format!(
                        "{} / {}{}",
                        profile.backend,
                        profile.model,
                        if metadata.model_backend.name == profile.backend
                            && current == profile.model
                        {
                            " · current"
                        } else {
                            ""
                        }
                    ),
                )
            })
            .collect(),
    )
}

pub(super) async fn discover(metadata: &HostMetadata, current: &str) -> Result<Menu> {
    let models = metadata.model_controls.list_models().await
        .context("Could not refresh the current service's models; retry /model or use /model profiles explicitly")?;
    let Some(models) = models else {
        let mut menu = profiles(metadata, current);
        menu.title = "Models · configured profiles · protocol has no discovery".to_owned();
        return Ok(menu);
    };
    discovered_menu(metadata, current, models)
}

fn discovered_menu(metadata: &HostMetadata, current: &str, models: Vec<String>) -> Result<Menu> {
    if models.is_empty() {
        bail!("The current service returned no models; load a model and retry /model, or use /model profiles explicitly");
    }
    let mut choices = models
        .into_iter()
        .map(|model| {
            choice(
                &model,
                Selection::ApiModel {
                    backend: metadata.model_backend.name.clone(),
                    id: model.clone(),
                },
                if model == current {
                    "Service model · current"
                } else {
                    "Service model · keep current endpoint and credentials"
                },
            )
        })
        .collect::<Vec<_>>();
    if !metadata.models.is_empty() {
        choices.push(choice(
            "Configured profiles…",
            Selection::Profiles,
            "Select a profile and its configured connection",
        ));
    }
    Ok(Menu::new(
        MenuKind::Models,
        format!(
            "Models · {} · refreshed from service",
            metadata.model_backend.name
        ),
        choices,
    ))
}

pub(super) async fn reasoning(metadata: &HostMetadata, current: &str) -> Result<Menu> {
    let capabilities = metadata.model_controls.resolve_reasoning_capabilities().await
        .context("Could not inspect reasoning capabilities; retry /reasoning, or set --reasoning explicitly for the service to validate")?;
    capabilities.validate()?;
    Ok(reasoning_menu(metadata, current, &capabilities))
}

pub(super) fn reasoning_argument(command: &str) -> Option<&str> {
    let rest = command.strip_prefix("/reasoning")?;
    rest.starts_with(char::is_whitespace).then(|| rest.trim())
}

pub(super) fn permissions_argument(command: &str) -> Option<&str> {
    let rest = command.strip_prefix("/permissions")?;
    rest.starts_with(char::is_whitespace).then(|| rest.trim())
}

pub(super) fn explicit_permissions(value: &str) -> Result<Selection> {
    let value = value
        .parse::<PermissionPreset>()
        .map_err(anyhow::Error::msg)?;
    Ok(Selection::Permissions { value })
}

pub(super) fn permissions(metadata: &HostMetadata) -> Menu {
    Menu::new(
        MenuKind::Models,
        format!(
            "Permissions · current: {} ({})",
            metadata.permissions.label(),
            metadata.permissions
        ),
        PermissionPreset::ALL
            .into_iter()
            .map(|value| {
                let description = if value == metadata.permissions {
                    format!("Current · {}", value.description())
                } else {
                    value.description().to_owned()
                };
                choice(
                    &format!("{} ({})", value.label(), value.as_str()),
                    Selection::Permissions { value },
                    description,
                )
            })
            .collect(),
    )
}

pub(super) fn explicit_reasoning(
    metadata: &HostMetadata,
    current: &str,
    value: &str,
) -> Result<Selection> {
    let value = value
        .parse::<ReasoningPreference>()
        .map_err(anyhow::Error::msg)?;
    // Validate protocol support without requiring optional model discovery.
    metadata
        .model_controls
        .validate_reasoning_preference(&value)?;
    Ok(Selection::Reasoning {
        backend: metadata.model_backend.name.clone(),
        model: current.to_owned(),
        value,
    })
}

fn reasoning_menu(
    metadata: &HostMetadata,
    model: &str,
    capabilities: &ModelReasoningCapabilities,
) -> Menu {
    let declared = capabilities.supported_preferences.is_some();
    let mut choices = capabilities.choices();
    if !declared {
        for value in metadata
            .model_controls
            .reasoning_preference_candidates()
            .into_iter()
            .chain(std::iter::once(metadata.reasoning.clone()))
        {
            if !choices.contains(&value)
                && metadata
                    .model_controls
                    .validate_reasoning_preference(&value)
                    .is_ok()
            {
                choices.push(value);
            }
        }
    }
    Menu::new(
        MenuKind::Models,
        format!("Reasoning · current: {} · {}", metadata.reasoning, model),
        choices
            .into_iter()
            .map(|value| {
                let description = match &value {
                    ReasoningPreference::Default
                        if capabilities.default_thinking_enabled == Some(true) =>
                    {
                        "Use service default (thinking on); omit controls"
                    }
                    ReasoningPreference::Default
                        if capabilities.default_thinking_enabled == Some(false) =>
                    {
                        "Use service default (thinking off); omit controls"
                    }
                    ReasoningPreference::Default if !declared => {
                        "Service default; model capabilities were not declared"
                    }
                    ReasoningPreference::Default => "Use service default; omit reasoning controls",
                    _ if !declared => {
                        "Unverified: the adapter can send this value; the service validates model support"
                    }
                    ReasoningPreference::On => {
                        "Enable thinking; this is a toggle, not an effort level"
                    }
                    ReasoningPreference::Off => "Disable thinking explicitly",
                    _ => "Control supported by the current model",
                };
                choice(
                    &value.to_string(),
                    Selection::Reasoning {
                        backend: metadata.model_backend.name.clone(),
                        model: model.to_owned(),
                        value,
                    },
                    description,
                )
            })
            .collect(),
    )
}

/// Keep the effective connection and current session's explicit choices when selecting an API ID.
pub(super) fn apply_selection(
    metadata: &HostMetadata,
    current: &str,
    overrides: &mut ModelOverrides,
    selection: Selection,
) -> Result<()> {
    match selection {
        Selection::ApiModel { backend, id } => {
            if backend != metadata.model_backend.name || id.trim().is_empty() {
                bail!("Model selection is stale; refresh /model");
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
                bail!("Reasoning selection is stale; refresh /reasoning");
            }
            overrides.reasoning = Some(value);
            // Reconfiguration must not rediscover a different ID or revert to the profile's ID.
            overrides.model = Some(current.to_owned());
        }
        Selection::Permissions { value } => {
            overrides.permissions = Some(value);
            overrides.model = Some(current.to_owned());
        }
        Selection::Profiles => bail!("Profile navigation is not a model selection"),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
