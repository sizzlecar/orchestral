use std::collections::BTreeSet;
use std::time::Duration;

use orchestral_core::model_protocol::ModelError;
use reqwest::{Client, Url};

/// Validated OpenAI-compatible API base, without credentials or query parameters.
#[derive(Debug, Clone)]
pub struct OpenAiEndpoint(Url);

impl OpenAiEndpoint {
    /// Accept a server root, API base, or full Chat Completions endpoint.
    pub fn parse(value: &str) -> Result<Self, ModelError> {
        let mut url = Url::parse(value.trim()).map_err(|_| {
            ModelError::invalid_request("invalid API URL; use http(s)://HOST[:PORT]/v1")
        })?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ModelError::invalid_request(
                "API URL must use HTTP(S) without credentials, query parameters, or fragments; configure authentication separately",
            ));
        }
        let path = url.path().trim_end_matches('/');
        let path = path.strip_suffix("/chat/completions").unwrap_or(path);
        let base = if path.is_empty() { "/v1" } else { path }.to_owned();
        url.set_path(&base);
        Ok(Self(url))
    }

    pub fn base_url(&self) -> &str {
        self.0.as_str()
    }

    pub fn completions_url(&self) -> String {
        format!("{}/chat/completions", self.base_url())
    }

    pub fn models_url(&self) -> String {
        format!("{}/models", self.base_url())
    }
}

/// Discover model IDs without sending a generation request. Empty keys omit auth.
pub async fn discover_models(
    endpoint: &OpenAiEndpoint,
    api_key: &str,
) -> Result<Vec<String>, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "could not initialize HTTP client".to_owned())?;
    let mut request = client.get(endpoint.models_url());
    if !api_key.is_empty() {
        request = request.bearer_auth(api_key);
    }
    let mut response = request.send().await.map_err(|error| {
        if error.is_timeout() {
            "model discovery timed out; check the server or specify --model".to_owned()
        } else if error.is_connect() {
            "could not connect to model server; check the URL, server, proxy and TLS certificate"
                .to_owned()
        } else {
            "model discovery request failed; check the server or specify --model".to_owned()
        }
    })?;
    if !response.status().is_success() {
        return Err(format!(
            "model discovery returned HTTP {}; check authentication and API base, or specify --model if /models is unavailable",
            response.status().as_u16()
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "model discovery response was interrupted")?
    {
        if body.len().saturating_add(chunk.len()) > 1024 * 1024 {
            return Err("model discovery response exceeds 1 MiB; specify --model".to_owned());
        }
        body.extend_from_slice(&chunk);
    }
    let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| {
        "model discovery did not return JSON; check the API base or specify --model"
    })?;
    let data = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or("model discovery requires an OpenAI-compatible data array; specify --model")?;
    Ok(data
        .iter()
        .filter_map(|model| model.get("id").and_then(serde_json::Value::as_str))
        .filter(|id| !id.trim().is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_variants_preserve_api_prefix_and_use_one_resource_suffix() {
        for value in [
            "http://localhost:8000",
            "http://localhost:8000/",
            "http://localhost:8000/v1/",
            "http://localhost:8000/v1/chat/completions",
        ] {
            let endpoint = OpenAiEndpoint::parse(value).unwrap();
            assert_eq!(
                endpoint.completions_url(),
                "http://localhost:8000/v1/chat/completions"
            );
            assert_eq!(endpoint.models_url(), "http://localhost:8000/v1/models");
        }
        assert_eq!(
            OpenAiEndpoint::parse("https://gateway.example/proxy/openai/v2/")
                .unwrap()
                .models_url(),
            "https://gateway.example/proxy/openai/v2/models"
        );
    }

    #[test]
    fn credential_bearing_urls_are_rejected_without_echoing_secrets() {
        for value in [
            "https://user:secret@example.com/v1",
            "https://example.com/v1?key=secret",
            "file:///secret",
            "https://example.com/v1#secret",
        ] {
            let error = OpenAiEndpoint::parse(value).unwrap_err().to_string();
            assert!(!error.contains("secret"));
        }
    }
}
