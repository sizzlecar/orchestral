use std::collections::HashMap;
use std::path::{Component, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

use orchestral_config::ActionSpec;
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

fn config_string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn config_bool(config: &Value, key: &str) -> Option<bool> {
    config.get(key).and_then(|v| v.as_bool())
}

fn config_u64(config: &Value, key: &str) -> Option<u64> {
    config.get(key).and_then(|v| v.as_u64())
}

fn params_get_string(params: &Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn params_get_bool(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|v| v.as_bool())
}

fn params_get_array(params: &Value, key: &str) -> Option<Vec<String>> {
    params.get(key).and_then(|v| {
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
    })
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

fn resolve_path(root: &Option<PathBuf>, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    match root {
        Some(root_dir) => root_dir.join(path),
        None => path,
    }
}

fn has_parent_dir(path: &str) -> bool {
    PathBuf::from(path)
        .components()
        .any(|c| matches!(c, Component::ParentDir))
}

/// Echo action
pub struct EchoAction {
    name: String,
    description: String,
    prefix: String,
}

impl EchoAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let prefix = config_string(&spec.config, "prefix").unwrap_or_default();
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Echoes the input back as output"),
            prefix,
        }
    }
}

#[async_trait]
impl Action for EchoAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description()).with_exports(vec!["result".to_string()])
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let message = input
            .params
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("No message provided");
        let result = format!("{}{}", self.prefix, message);
        ActionResult::success_with_one("result", Value::String(result))
    }
}

/// HTTP action
pub struct HttpAction {
    name: String,
    description: String,
    default_method: String,
    default_url: Option<String>,
    default_headers: HashMap<String, String>,
    client: reqwest::Client,
}

impl HttpAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
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
        ActionMeta::new(self.name(), self.description()).with_exports(vec![
            "status".to_string(),
            "url".to_string(),
            "headers".to_string(),
            "body".to_string(),
        ])
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

/// Shell action
pub struct ShellAction {
    name: String,
    description: String,
    working_dir: Option<PathBuf>,
    timeout_ms: Option<u64>,
}

impl ShellAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let working_dir = config_string(&spec.config, "working_dir").map(PathBuf::from);
        let timeout_ms = config_u64(&spec.config, "timeout_ms");
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Runs a shell command"),
            working_dir,
            timeout_ms,
        }
    }
}

#[async_trait]
impl Action for ShellAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description()).with_exports(vec![
            "stdout".to_string(),
            "stderr".to_string(),
            "status".to_string(),
        ])
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let command = match params_get_string(params, "command") {
            Some(cmd) => cmd,
            None => return ActionResult::error("Missing command for shell action"),
        };

        let mut cmd = if let Some(args) = params_get_array(params, "args") {
            let mut c = Command::new(&command);
            c.args(args);
            c
        } else {
            let mut c = Command::new("sh");
            c.arg("-c").arg(command);
            c
        };

        if let Some(dir) = &self.working_dir {
            cmd.current_dir(dir);
        }

        let output = if let Some(ms) = self.timeout_ms {
            match timeout(Duration::from_millis(ms), cmd.output()).await {
                Ok(result) => result,
                Err(_) => return ActionResult::error("Shell command timed out"),
            }
        } else {
            cmd.output().await
        };

        let output = match output {
            Ok(o) => o,
            Err(e) => return ActionResult::error(format!("Shell execution failed: {}", e)),
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let status = output.status.code().unwrap_or(-1);

        let mut exports = Map::new();
        exports.insert("stdout".to_string(), Value::String(stdout));
        exports.insert("stderr".to_string(), Value::String(stderr));
        exports.insert("status".to_string(), Value::Number(status.into()));
        ActionResult::success_with(exports.into_iter().collect())
    }
}

/// File read action
pub struct FileReadAction {
    name: String,
    description: String,
    root_dir: Option<PathBuf>,
}

impl FileReadAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let root_dir = config_string(&spec.config, "root_dir").map(PathBuf::from);
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Reads a file from disk"),
            root_dir,
        }
    }
}

#[async_trait]
impl Action for FileReadAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_exports(vec!["content".to_string(), "path".to_string()])
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let path = match params_get_string(params, "path") {
            Some(p) => p,
            None => return ActionResult::error("Missing path for file_read"),
        };

        if self.root_dir.is_some() && has_parent_dir(&path) {
            return ActionResult::error("Path escapes root_dir");
        }
        let full_path = resolve_path(&self.root_dir, &path);
        let full_path = if let Some(root) = &self.root_dir {
            let root = match tokio::fs::canonicalize(root).await {
                Ok(r) => r,
                Err(e) => return ActionResult::error(format!("Invalid root_dir: {}", e)),
            };
            let full = match tokio::fs::canonicalize(&full_path).await {
                Ok(p) => p,
                Err(e) => return ActionResult::error(format!("Invalid path: {}", e)),
            };
            if !full.starts_with(&root) {
                return ActionResult::error("Path escapes root_dir");
            }
            full
        } else {
            full_path
        };

        let content = match tokio::fs::read_to_string(&full_path).await {
            Ok(c) => c,
            Err(e) => return ActionResult::error(format!("Read failed: {}", e)),
        };

        let mut exports = Map::new();
        exports.insert("content".to_string(), Value::String(content));
        exports.insert(
            "path".to_string(),
            Value::String(full_path.to_string_lossy().to_string()),
        );
        ActionResult::success_with(exports.into_iter().collect())
    }
}

/// File write action
pub struct FileWriteAction {
    name: String,
    description: String,
    root_dir: Option<PathBuf>,
    create_dirs: bool,
    default_append: bool,
}

impl FileWriteAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let root_dir = config_string(&spec.config, "root_dir").map(PathBuf::from);
        let create_dirs = config_bool(&spec.config, "create_dirs").unwrap_or(true);
        let default_append = config_bool(&spec.config, "append").unwrap_or(false);
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Writes a file to disk"),
            root_dir,
            create_dirs,
            default_append,
        }
    }
}

#[async_trait]
impl Action for FileWriteAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_exports(vec!["path".to_string(), "bytes".to_string()])
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let path = match params_get_string(params, "path") {
            Some(p) => p,
            None => return ActionResult::error("Missing path for file_write"),
        };
        let content = match params_get_string(params, "content") {
            Some(c) => c,
            None => return ActionResult::error("Missing content for file_write"),
        };

        if self.root_dir.is_some() && has_parent_dir(&path) {
            return ActionResult::error("Path escapes root_dir");
        }
        let full_path = resolve_path(&self.root_dir, &path);
        let full_path = if let Some(root) = &self.root_dir {
            let root = match tokio::fs::canonicalize(root).await {
                Ok(r) => r,
                Err(e) => return ActionResult::error(format!("Invalid root_dir: {}", e)),
            };
            let full = match tokio::fs::canonicalize(&full_path).await {
                Ok(p) => p,
                Err(_) => full_path.clone(),
            };
            if !full.starts_with(&root) {
                return ActionResult::error("Path escapes root_dir");
            }
            full
        } else {
            full_path
        };

        if self.create_dirs {
            if let Some(parent) = full_path.parent() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    return ActionResult::error(format!("Create dirs failed: {}", e));
                }
            }
        }

        let append = params_get_bool(params, "append").unwrap_or(self.default_append);
        if append {
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full_path)
                .await
            {
                Ok(f) => f,
                Err(e) => return ActionResult::error(format!("Write failed: {}", e)),
            };
            if let Err(e) = file.write_all(content.as_bytes()).await {
                return ActionResult::error(format!("Write failed: {}", e));
            }
        } else if let Err(e) = tokio::fs::write(&full_path, content.as_bytes()).await {
            return ActionResult::error(format!("Write failed: {}", e));
        }

        let mut exports = Map::new();
        exports.insert(
            "path".to_string(),
            Value::String(full_path.to_string_lossy().to_string()),
        );
        exports.insert("bytes".to_string(), Value::Number(content.len().into()));
        ActionResult::success_with(exports.into_iter().collect())
    }
}

pub fn build_builtin_action(spec: &ActionSpec) -> Option<Box<dyn Action>> {
    match spec.kind.as_str() {
        "echo" => Some(Box::new(EchoAction::from_spec(spec))),
        "http" => Some(Box::new(HttpAction::from_spec(spec))),
        "shell" => Some(Box::new(ShellAction::from_spec(spec))),
        "file_read" => Some(Box::new(FileReadAction::from_spec(spec))),
        "file_write" => Some(Box::new(FileWriteAction::from_spec(spec))),
        _ => None,
    }
}
