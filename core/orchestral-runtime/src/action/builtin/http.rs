use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Map, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::support::{config_string, config_u64, params_get_string};

pub(super) struct HttpAction {
    name: String,
    description: String,
    default_method: String,
    default_url: Option<String>,
    default_headers: HashMap<String, String>,
    client: reqwest::Client,
}

impl HttpAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        let default_method =
            config_string(&spec.config, "default_method").unwrap_or_else(|| "GET".to_string());
        let default_url = config_string(&spec.config, "default_url");
        let default_headers =
            headers_from_value(spec.config.get("headers").unwrap_or(&Value::Null));
        let timeout_ms = config_u64(&spec.config, "timeout_ms");

        let client = {
            let builder = reqwest::Client::builder();
            let builder = if let Some(ms) = timeout_ms {
                builder.timeout(Duration::from_millis(ms))
            } else {
                builder
            };
            builder.build().unwrap_or_else(|_| reqwest::Client::new())
        };

        Self {
            name: spec.name.clone(),
            description: spec.description_or("Performs an HTTP request"),
            default_method,
            default_url,
            default_headers,
            client,
        }
    }
}

#[async_trait]
impl Action for HttpAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("direct")
            .with_capabilities(["network_io", "side_effect"])
            .with_input_kinds(["network.request"])
            .with_output_kinds(["network.response", "text"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "description": "HTTP method, such as GET/POST/PUT.",
                        "default": self.default_method
                    },
                    "url": {
                        "type": "string",
                        "description": "Request URL. Required when no default_url is configured.",
                        "default": self.default_url
                    },
                    "headers": {
                        "type": "object",
                        "description": "Extra request headers merged with action defaults."
                    },
                    "body": {
                        "description": "Raw request body. If not a string, it is sent as JSON."
                    },
                    "json": {
                        "description": "JSON payload. Takes precedence over body when both are provided."
                    }
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "integer",
                        "description": "HTTP status code."
                    },
                    "url": {
                        "type": "string",
                        "description": "Resolved request URL."
                    },
                    "headers": {
                        "type": "object",
                        "description": "Response headers."
                    },
                    "body": {
                        "type": "string",
                        "description": "Response body as text."
                    }
                },
                "required": ["status", "url", "headers", "body"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let method =
            params_get_string(params, "method").unwrap_or_else(|| self.default_method.clone());
        let url = params_get_string(params, "url").or_else(|| self.default_url.clone());

        let url = match url {
            Some(u) => u,
            None => return ActionResult::error("Missing url for http action"),
        };

        let override_headers = params
            .get("headers")
            .map(headers_from_value)
            .unwrap_or_default();
        let headers = merge_headers(&self.default_headers, &override_headers);

        let request = match method.parse::<reqwest::Method>() {
            Ok(m) => self.client.request(m, url.clone()).headers(headers),
            Err(_) => return ActionResult::error(format!("Invalid HTTP method: {}", method)),
        };

        let request = if let Some(json_value) = params.get("json") {
            request.json(json_value)
        } else if let Some(body) = params.get("body") {
            if body.is_string() {
                request.body(body.as_str().unwrap_or_default().to_string())
            } else {
                request.json(body)
            }
        } else {
            request
        };

        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => return ActionResult::error(format!("HTTP request failed: {}", e)),
        };

        let status = response.status().as_u16();
        let headers_map = response
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|s| (k.to_string(), Value::String(s.to_string())))
            })
            .collect::<Map<String, Value>>();
        let body = match response.text().await {
            Ok(text) => text,
            Err(e) => return ActionResult::error(format!("HTTP response read failed: {}", e)),
        };

        let mut exports = Map::new();
        exports.insert("status".to_string(), Value::Number(status.into()));
        exports.insert("url".to_string(), Value::String(url));
        exports.insert("headers".to_string(), Value::Object(headers_map));
        exports.insert("body".to_string(), Value::String(body));

        ActionResult::success_with(exports.into_iter().collect())
    }
}

fn headers_from_value(value: &Value) -> HashMap<String, String> {
    value
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn merge_headers(
    defaults: &HashMap<String, String>,
    overrides: &HashMap<String, String>,
) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (k, v) in defaults.iter().chain(overrides.iter()) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            map.insert(name, value);
        }
    }
    map
}
