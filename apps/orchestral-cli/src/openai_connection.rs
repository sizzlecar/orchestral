//! Application policy for selecting OpenAI-compatible credentials and models.

use anyhow::{bail, Context};
use orchestral_core::config::BackendSpec;
use orchestral_model_openai::{discover_model_metadata, DiscoveredModel, OpenAiEndpoint};

pub(crate) fn supports_discovery(backend: &BackendSpec) -> bool {
    matches!(
        backend.kind.trim().to_ascii_lowercase().as_str(),
        "openai" | "openrouter" | "deepseek" | "groq" | "xai" | "mistral"
    )
}

pub(crate) fn endpoint(backend: &BackendSpec) -> anyhow::Result<OpenAiEndpoint> {
    let endpoint = backend
        .endpoint
        .as_deref()
        .or_else(|| match backend.kind.trim().to_ascii_lowercase().as_str() {
            "openai" => Some("https://api.openai.com/v1"),
            "deepseek" => Some("https://api.deepseek.com"),
            _ => None,
        })
        .with_context(|| {
            format!(
                "OpenAI-compatible backend '{}' requires an endpoint",
                backend.name
            )
        })?;
    OpenAiEndpoint::parse(endpoint).map_err(anyhow::Error::msg)
}

pub(crate) async fn discover_backend_models(
    backend: &BackendSpec,
) -> anyhow::Result<Vec<crate::model_controls::DiscoveredModel>> {
    if !supports_discovery(backend) {
        bail!(
            "backend protocol '{}' does not expose OpenAI-compatible model discovery",
            backend.kind
        );
    }
    orchestral_model_openai::discover_model_metadata(&endpoint(backend)?, &api_key(backend)?)
        .await
        .map_err(anyhow::Error::msg)
        .map(|models| {
            models
                .into_iter()
                .map(crate::model_controls::discovered_model)
                .collect()
        })
}

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

pub(crate) async fn resolve_model(
    mut backend: BackendSpec,
    configured_model: Option<String>,
) -> anyhow::Result<(BackendSpec, String)> {
    if !supports_discovery(&backend) {
        return Ok((
            backend,
            configured_model
                .context("no model configured; use --model MODEL or --model-profile PROFILE")?,
        ));
    }
    let enabled = backend
        .get_config::<bool>("discover_model_capabilities")
        .unwrap_or(true);
    if let Some(model) = configured_model
        .as_ref()
        .filter(|_| backend.endpoint.is_none() || !enabled)
    {
        return Ok((backend, model.clone()));
    }
    let endpoint = OpenAiEndpoint::parse(
        backend
            .endpoint
            .as_deref()
            .context("model discovery requires an endpoint; use --model MODEL")?,
    )?;
    let key = api_key(&backend)?;
    // Selecting an explicit model must remain usable with servers that only
    // implement completions. Optional discovery has a short, bounded wait.
    let discovery = if configured_model.is_some() {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            discover_model_metadata(&endpoint, &key),
        )
        .await
        .unwrap_or_else(|_| Err("optional model capacity discovery timed out".to_owned()))
    } else {
        discover_model_metadata(&endpoint, &key).await
    };
    let models = match discovery {
        Ok(models) => models,
        Err(error) if configured_model.is_some() => {
            tracing::debug!(%error, "model capacity was not discovered; retaining configured budget");
            return Ok((backend, configured_model.expect("model checked")));
        }
        Err(error) => return Err(anyhow::Error::msg(error)),
    };
    let model = match configured_model {
        Some(id) => models
            .iter()
            .find(|model| model.id == id)
            .cloned()
            .unwrap_or(DiscoveredModel {
                id,
                max_context_tokens: None,
                reasoning: None,
            }),
        None => select_single_model(&models)?,
    };
    apply_capacity(&mut backend, model.max_context_tokens);
    Ok((backend, model.id))
}

fn select_single_model(models: &[DiscoveredModel]) -> anyhow::Result<DiscoveredModel> {
    match models {
        [model] => Ok(model.clone()),
        [] => {
            bail!("the server has no models loaded; load a model, then retry or pass --model MODEL")
        }
        _ => bail!(
            "the server offers multiple models; select one with --model MODEL. Available: {}",
            models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn apply_capacity(backend: &mut BackendSpec, discovered: Option<u64>) {
    let Some(capacity) = discovered else {
        return;
    };
    let effective = backend
        .get_config::<u64>("max_context_tokens")
        .map_or(capacity, |configured| configured.min(capacity));
    if backend.config.is_null() {
        backend.config = serde_json::json!({});
    }
    if let Some(config) = backend.config.as_object_mut() {
        config.insert("max_context_tokens".to_owned(), effective.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn both_explicit_and_automatic_model_selection_use_served_capacity() {
        let app = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"data":[{"id":"served","max_model_len":12288}]}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        for configured in [None, Some("served".to_owned())] {
            let backend = BackendSpec {
                name: "local".into(),
                kind: "openai".into(),
                endpoint: Some(endpoint.clone()),
                api_key_env: None,
                config: serde_json::json!({"auth":"none"}),
            };
            let (backend, model) = resolve_model(backend, configured).await.unwrap();
            assert_eq!(model, "served");
            assert_eq!(backend.get_config::<u64>("max_context_tokens"), Some(12288));
        }
        server.abort();
    }

    #[tokio::test]
    async fn explicit_model_survives_an_endpoint_without_discovery() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, axum::Router::new()).await.unwrap() });
        let backend = BackendSpec {
            name: "local".into(),
            kind: "openai".into(),
            endpoint: Some(endpoint),
            api_key_env: None,
            config: serde_json::json!({"auth":"none"}),
        };
        let (backend, model) = resolve_model(backend, Some("explicit".into()))
            .await
            .unwrap();
        assert_eq!(model, "explicit");
        assert_eq!(backend.get_config::<u64>("max_context_tokens"), None);
        server.abort();
    }

    #[test]
    fn discovery_can_only_tighten_a_configured_capacity() {
        let mut backend = BackendSpec {
            name: "test".to_owned(),
            kind: "openai".to_owned(),
            endpoint: None,
            api_key_env: None,
            config: serde_json::Value::Null,
        };
        apply_capacity(&mut backend, None);
        assert_eq!(backend.get_config::<u64>("max_context_tokens"), None);
        apply_capacity(&mut backend, Some(8192));
        assert_eq!(backend.get_config::<u64>("max_context_tokens"), Some(8192));
        apply_capacity(&mut backend, Some(16384));
        assert_eq!(backend.get_config::<u64>("max_context_tokens"), Some(8192));
        apply_capacity(&mut backend, Some(4096));
        assert_eq!(backend.get_config::<u64>("max_context_tokens"), Some(4096));
    }
}
