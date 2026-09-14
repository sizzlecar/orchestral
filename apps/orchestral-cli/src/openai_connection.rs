//! Application policy for selecting OpenAI-compatible credentials and models.

use anyhow::{bail, Context};
use orchestral_core::config::BackendSpec;
use orchestral_model_openai::{discover_models, OpenAiEndpoint};

pub(crate) fn api_key(backend: &BackendSpec) -> anyhow::Result<String> {
    match backend
        .get_config::<String>("auth")
        .as_deref()
        .unwrap_or("api_key")
    {
        "none" => {
            if backend.endpoint.is_none() {
                bail!("auth: none requires an explicit endpoint");
            }
            Ok(String::new())
        }
        "api_key" => {
            let key = match &backend.api_key_env {
                Some(name) => std::env::var(name)
                    .ok()
                    .filter(|key| !key.trim().is_empty()),
                None => backend.resolve_api_key().ok(),
            };
            key.with_context(|| format!(
                "no API key configured for backend '{}'; set {} for a cloud service, or use --base-url http://127.0.0.1:8000/v1 for a local OpenAI-compatible server (use --api-key-env NAME if it requires authentication)",
                backend.name, backend.api_key_env.as_deref().unwrap_or("the provider key environment variable")
            ))
        }
        _ => bail!("OpenAI-compatible auth must be api_key or none"),
    }
}

pub(crate) async fn discover_single_model(backend: &BackendSpec) -> anyhow::Result<String> {
    if !matches!(
        backend.kind.to_ascii_lowercase().as_str(),
        "openai" | "openrouter" | "deepseek" | "groq" | "xai" | "mistral"
    ) {
        bail!("no model configured; use --model MODEL or --model-profile PROFILE");
    }
    let endpoint = OpenAiEndpoint::parse(
        backend
            .endpoint
            .as_deref()
            .context("model discovery requires an endpoint; use --model MODEL")?,
    )?;
    let models = discover_models(&endpoint, &api_key(backend)?)
        .await
        .map_err(anyhow::Error::msg)?;
    match models.as_slice() {
        [model] => Ok(model.clone()),
        [] => {
            bail!("the server has no models loaded; load a model, then retry or pass --model MODEL")
        }
        _ => bail!(
            "the server offers multiple models; select one with --model MODEL. Available: {}",
            models.join(", ")
        ),
    }
}
