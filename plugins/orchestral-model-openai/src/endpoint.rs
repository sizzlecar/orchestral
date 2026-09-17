use std::collections::BTreeMap;
use std::time::Duration;

use orchestral_core::model_protocol::ModelError;
use reqwest::{Client, Url};

use crate::OpenAiReasoningCapabilities;

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
    Ok(discover_model_metadata(endpoint, api_key)
        .await?
        .into_iter()
        .map(|model| model.id)
        .collect())
}

/// Model identity and optional serving capacity declared by an API endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredModel {
    pub id: String,
    /// Input plus output tokens. Absent when the server does not declare it.
    pub max_context_tokens: Option<u64>,
    /// None preserves unknown support rather than claiming all/no controls.
    pub reasoning: Option<OpenAiReasoningCapabilities>,
}

/// Discover IDs, capacities and optional controls without a generation request.
/// Unknown extension fields are ignored; malformed known controls fail visibly.
pub async fn discover_model_metadata(
    endpoint: &OpenAiEndpoint,
    api_key: &str,
) -> Result<Vec<DiscoveredModel>, String> {
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
    parse_model_metadata(&value)
}

fn parse_model_metadata(value: &serde_json::Value) -> Result<Vec<DiscoveredModel>, String> {
    let data = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or("model discovery requires an OpenAI-compatible data array; specify --model")?;
    let mut models = BTreeMap::<String, DiscoveredModel>::new();
    for (index, model) in data.iter().enumerate() {
        let Some(id) = model
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.trim().is_empty())
        else {
            continue;
        };
        // Optional OpenAI-compatible serving metadata, not a nominal model
        // window inferred from its name. Older endpoints may omit this field.
        let capacity = model
            .get("max_model_len")
            .and_then(serde_json::Value::as_u64)
            .filter(|tokens| *tokens > 0);
        let reasoning = parse_reasoning_metadata(model, index)?;
        match models.entry(id.to_owned()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                if existing.reasoning != reasoning {
                    return Err("model discovery returned conflicting reasoning metadata for a duplicate model ID".to_owned());
                }
                existing.max_context_tokens = match (existing.max_context_tokens, capacity) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(DiscoveredModel {
                    id: id.to_owned(),
                    max_context_tokens: capacity,
                    reasoning,
                });
            }
        }
    }
    Ok(models.into_values().collect())
}

fn parse_reasoning_metadata(
    model: &serde_json::Value,
    index: usize,
) -> Result<Option<OpenAiReasoningCapabilities>, String> {
    let invalid_metadata = || {
        format!(
        "model discovery entry {index} has invalid reasoning metadata; expected supported_efforts and/or thinking.default_enabled with valid types"
    )
    };
    let mut reasoning: Option<OpenAiReasoningCapabilities> = match model.get("reasoning") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => {
            // Serde structs may also deserialize positional arrays; this
            // extension deliberately requires named JSON object fields.
            if !value.is_object()
                || value
                    .get("thinking")
                    .is_some_and(|thinking| !thinking.is_null() && !thinking.is_object())
            {
                return Err(invalid_metadata());
            }
            Some(serde_json::from_value(value.clone()).map_err(|_| invalid_metadata())?)
        }
    };
    if let Some(capabilities) = &mut reasoning {
        if let Some(efforts) = &mut capabilities.supported_efforts {
            efforts.sort();
            efforts.dedup();
        }
        if capabilities.supported_efforts.is_none() && capabilities.thinking.is_none() {
            reasoning = None;
        }
    }
    Ok(reasoning)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OpenAiReasoningEffort;
    use serde_json::json;

    #[test]
    fn discovered_reasoning_preserves_unknown_empty_efforts_and_toggle_only_support() {
        let models = parse_model_metadata(&json!({"data": [
            json!({"id": "unknown"}),
            json!({"id": "future", "reasoning": {"future_extension": true}}),
            json!({"id": "empty", "reasoning": {"supported_efforts": []}}),
            json!({"id": "toggle", "reasoning": {"thinking": {"default_enabled": false, "future": 1}}}),
            json!({"id": "levels", "reasoning": {"supported_efforts": ["high", "low", "high"]}}),
            json!({"id": "unknown"}),
        ]}))
        .unwrap();
        assert_eq!(models.len(), 5);
        let find = |id| models.iter().find(|model| model.id == id).unwrap();
        assert!(find("unknown").reasoning.is_none());
        assert!(find("future").reasoning.is_none());
        assert_eq!(
            find("empty").reasoning.as_ref().unwrap().supported_efforts,
            Some(vec![])
        );
        let toggle = find("toggle").reasoning.as_ref().unwrap();
        assert!(toggle.supported_efforts.is_none());
        assert!(!toggle.thinking.unwrap().default_enabled);
        assert_eq!(
            find("levels").reasoning.as_ref().unwrap().supported_efforts,
            Some(vec![
                OpenAiReasoningEffort::Low,
                OpenAiReasoningEffort::High
            ])
        );
    }

    #[test]
    fn malformed_known_reasoning_metadata_is_visible_without_echoing_untrusted_values() {
        for reasoning in [
            json!(true),
            json!([]),
            json!([null, null]),
            json!({"thinking": [false]}),
            json!({"thinking": {}}),
            json!({"thinking": {"default_enabled": "secret"}}),
            json!({"supported_efforts": "secret"}),
            json!({"supported_efforts": [""]}),
            json!({"supported_efforts": [" "]}),
            json!({"supported_efforts": [false]}),
        ] {
            let error =
                parse_model_metadata(&json!({"data": [{"id": "model", "reasoning": reasoning}]}))
                    .unwrap_err();
            assert!(error.contains("reasoning metadata"));
            assert!(!error.contains("secret"));
        }
        assert!(parse_model_metadata(&json!({"data": [
            json!({"id": "same", "reasoning": {"supported_efforts": ["low"]}}),
            json!({"id": "same", "reasoning": {"supported_efforts": ["high"]}}),
        ]}))
        .is_err());
    }

    #[test]
    fn discovered_efforts_preserve_future_names_case_and_reserved_local_words() {
        let names = [
            "ultra",
            "future-next",
            "HIGH",
            "on",
            "off",
            "default",
            "effort:high",
        ];
        let models = parse_model_metadata(&json!({"data": [{"id": "future", "max_model_len": 8192,
            "reasoning": {"supported_efforts": names}}]}))
        .unwrap();
        assert_eq!(models[0].max_context_tokens, Some(8192));
        let efforts = models[0]
            .reasoning
            .as_ref()
            .unwrap()
            .supported_efforts
            .as_ref()
            .unwrap();
        let actual = efforts
            .iter()
            .map(OpenAiReasoningEffort::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, names.into_iter().collect());
        assert_eq!(
            serde_json::to_value(efforts).unwrap(),
            json!(efforts
                .iter()
                .map(OpenAiReasoningEffort::as_str)
                .collect::<Vec<_>>())
        );
    }

    #[test]
    fn duplicate_capacity_minimum_and_reasoning_agreement_are_independent() {
        let models = parse_model_metadata(&json!({"data": [
            {"id": "known", "max_model_len": 8192, "reasoning": {"supported_efforts": ["high", "low"]}},
            {"id": "known", "max_model_len": 4096, "reasoning": {"supported_efforts": ["low", "high", "low"]}},
            {"id": "known", "reasoning": {"supported_efforts": ["low", "high"]}},
            {"id": "unknown", "max_model_len": 16384},
            {"id": "unknown", "max_model_len": 8192, "reasoning": {}},
            {"id": "unknown", "reasoning": null}
        ]})).unwrap();
        assert_eq!(models[0].id, "known");
        assert_eq!(models[0].max_context_tokens, Some(4096));
        assert_eq!(
            models[0].reasoning.as_ref().unwrap().supported_efforts,
            Some(vec![
                OpenAiReasoningEffort::Low,
                OpenAiReasoningEffort::High
            ])
        );
        assert_eq!(models[1].max_context_tokens, Some(8192));
        assert!(models[1].reasoning.is_none());
        for conflicting in [json!({"thinking": {"default_enabled": false}}), json!(null)] {
            assert!(parse_model_metadata(&json!({"data": [
                {"id": "same", "max_model_len": 8192, "reasoning": {"thinking": {"default_enabled": true}}},
                {"id": "same", "max_model_len": 4096, "reasoning": conflicting}
            ]})).is_err());
        }
    }

    #[test]
    fn discovery_preserves_unknown_capacity_and_uses_smallest_duplicate_declaration() {
        let models = parse_model_metadata(&serde_json::json!({"data": [
            {"id": "known", "max_model_len": 8192},
            {"id": "known", "max_model_len": 4096},
            {"id": "known"},
            {"id": "unknown"},
            {"id": "zero", "max_model_len": 0},
            {"id": "invalid", "max_model_len": "131072"},
            {"id": "negative", "max_model_len": -1},
            {"id": "", "max_model_len": 4096}
        ]}))
        .unwrap();
        assert_eq!(models.len(), 5);
        assert_eq!(
            models
                .iter()
                .find(|m| m.id == "known")
                .unwrap()
                .max_context_tokens,
            Some(4096)
        );
        assert!(models
            .iter()
            .filter(|m| m.id != "known")
            .all(|m| m.max_context_tokens.is_none()));
    }

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
