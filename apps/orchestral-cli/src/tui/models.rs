//! Live model discovery and explicit model/profile/reasoning selections.

use super::menu::{Choice, Menu, MenuKind};
use crate::agent::HostMetadata;
use crate::model_controls::DiscoveredModel;
use crate::runtime::ModelOverrides;
use anyhow::{bail, Context, Result};
use orchestral_core::config::ReasoningPreference;
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
    if !crate::openai_connection::supports_discovery(&metadata.model_backend) {
        let mut menu = profiles(metadata, current);
        menu.title = "Models · configured profiles · protocol has no discovery".to_owned();
        return Ok(menu);
    }
    let models = crate::openai_connection::discover_backend_models(&metadata.model_backend).await
        .context("Could not refresh the current service's models; retry /model or use /model profiles explicitly")?;
    discovered_menu(metadata, current, models)
}

fn discovered_menu(
    metadata: &HostMetadata,
    current: &str,
    models: Vec<DiscoveredModel>,
) -> Result<Menu> {
    if models.is_empty() {
        bail!("The current service returned no models; load a model and retry /model, or use /model profiles explicitly");
    }
    let mut choices = models
        .into_iter()
        .map(|model| {
            choice(
                &model.id,
                Selection::ApiModel {
                    backend: metadata.model_backend.name.clone(),
                    id: model.id.clone(),
                },
                if model.id == current {
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
    let models = crate::openai_connection::discover_backend_models(&metadata.model_backend).await
        .context("Could not inspect reasoning capabilities; retry /reasoning, or set --reasoning explicitly for the service to validate")?;
    let model = models
        .into_iter()
        .find(|model| model.id == current)
        .context(
            "The selected model is absent from the service's model list; refresh /model first",
        )?;
    Ok(reasoning_menu(metadata, &model))
}

fn reasoning_choices(model: &DiscoveredModel) -> Vec<ReasoningPreference> {
    use ReasoningPreference as R;
    model.reasoning.clone().unwrap_or_else(|| {
        vec![
            R::Default,
            R::None,
            R::Minimal,
            R::Low,
            R::Medium,
            R::High,
            R::XHigh,
            R::Max,
        ]
    })
}

fn reasoning_menu(metadata: &HostMetadata, model: &DiscoveredModel) -> Menu {
    let declared = model.reasoning.is_some();
    Menu::new(
        MenuKind::Models,
        format!("Reasoning · current: {} · {}", metadata.reasoning, model.id),
        reasoning_choices(model)
            .into_iter()
            .map(|value| {
                let description = match value {
                    ReasoningPreference::Default
                        if model.thinking_default_enabled == Some(true) =>
                    {
                        "Use service default (thinking on); omit controls"
                    }
                    ReasoningPreference::Default
                        if model.thinking_default_enabled == Some(false) =>
                    {
                        "Use service default (thinking off); omit controls"
                    }
                    ReasoningPreference::Default if !declared => {
                        "Capabilities not declared; omit controls by default"
                    }
                    ReasoningPreference::Default => "Use service default; omit reasoning controls",
                    ReasoningPreference::On => {
                        "Enable thinking; this is a toggle, not an effort level"
                    }
                    ReasoningPreference::Off => "Disable thinking explicitly",
                    _ if declared => "Effort declared by the current service",
                    _ => {
                        "Capability not declared; explicit request will be validated by the service"
                    }
                };
                choice(
                    value.as_str(),
                    Selection::Reasoning {
                        backend: metadata.model_backend.name.clone(),
                        model: model.id.clone(),
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
        Selection::Profiles => bail!("Profile navigation is not a model selection"),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
