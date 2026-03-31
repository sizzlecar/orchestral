use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::factory::ActionBuildError;

#[derive(Debug, Clone, Deserialize)]
struct McpServerActionConfig {
    server_name: String,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    bearer_token_env_var: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    startup_timeout_ms: Option<u64>,
    #[serde(default)]
    tool_timeout_ms: Option<u64>,
    #[serde(default)]
    enabled_tools: Vec<String>,
    #[serde(default)]
    disabled_tools: Vec<String>,
}

pub fn build_mcp_action(spec: &ActionSpec) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    match spec.kind.as_str() {
        "mcp_server" => {
            let action = McpServerAction::from_spec(spec)?;
            Ok(Some(Box::new(action)))
        }
        "mcp_tool" => {
            let action = McpToolAction::from_spec(spec)?;
            Ok(Some(Box::new(action)))
        }
        _ => Ok(None),
    }
}

/// Probe an MCP server for its tool list. Returns (name, description, inputSchema) per tool.
/// Used at startup to register per-tool actions.
pub async fn probe_mcp_server_tools(config: &ActionSpec) -> Result<Vec<McpToolDescriptor>, String> {
    let server = McpServerAction::from_spec(config)
        .map_err(|e| format!("failed to build MCP server for probing: {}", e))?;

    let result = if server.config.command.is_some() {
        let mut session = StdioMcpSession::connect(
            server
                .config
                .command
                .as_deref()
                .ok_or_else(|| "missing command".to_string())?,
            &server.config.args,
            &server.config.env,
            server.startup_timeout(),
        )
        .await?;
        session.initialize().await?;
        let result = session.request("tools/list", json!({})).await?;
        let _ = session.shutdown().await;
        result
    } else {
        server
            .http_request("tools/list", json!({}), server.io_timeout())
            .await?
    };

    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut descriptors = Vec::new();
    for tool in tools {
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        let name = name.trim().to_string();
        if name.is_empty() {
            continue;
        }
        if !server.allows_tool(&name) {
            continue;
        }
        let description = tool
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let input_schema = tool
            .get("inputSchema")
            .cloned()
            .unwrap_or(json!({"type": "object"}));
        descriptors.push(McpToolDescriptor {
            name,
            description,
            input_schema,
        });
    }
    descriptors.sort_by(|a, b| a.name.cmp(&b.name));
    descriptors.dedup_by(|a, b| a.name == b.name);
    Ok(descriptors)
}

/// Descriptor for a single MCP tool discovered via tools/list.
#[derive(Debug, Clone)]
pub struct McpToolDescriptor {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

struct McpServerAction {
    name: String,
    description: String,
    config: McpServerActionConfig,
    enabled_tools: HashSet<String>,
    disabled_tools: HashSet<String>,
}

impl McpServerAction {
    fn from_spec(spec: &ActionSpec) -> Result<Self, ActionBuildError> {
        let config: McpServerActionConfig =
            serde_json::from_value(spec.config.clone()).map_err(|err| {
                ActionBuildError::InvalidConfig(format!(
                    "action '{}' invalid mcp_server config: {}",
                    spec.name, err
                ))
            })?;

        if config.command.is_none() && config.url.is_none() {
            return Err(ActionBuildError::InvalidConfig(format!(
                "action '{}' mcp_server requires command or url",
                spec.name
            )));
        }

        let enabled_tools = config
            .enabled_tools
            .iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<HashSet<_>>();
        let disabled_tools = config
            .disabled_tools
            .iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<HashSet<_>>();

        Ok(Self {
            name: spec.name.clone(),
            description: spec.description_or("Invoke MCP tools on a configured server"),
            config,
            enabled_tools,
            disabled_tools,
        })
    }

    fn io_timeout(&self) -> Duration {
        Duration::from_millis(self.config.tool_timeout_ms.unwrap_or(20_000))
    }

    fn startup_timeout(&self) -> Duration {
        Duration::from_millis(self.config.startup_timeout_ms.unwrap_or(15_000))
    }

    fn allows_tool(&self, tool: &str) -> bool {
        let name = tool.trim();
        if name.is_empty() {
            return false;
        }
        if !self.enabled_tools.is_empty() && !self.enabled_tools.contains(name) {
            return false;
        }
        !self.disabled_tools.contains(name)
    }

    async fn invoke_list_tools(&self) -> Result<Vec<String>, String> {
        if self.config.command.is_some() {
            self.invoke_stdio_list_tools().await
        } else {
            self.invoke_http_list_tools().await
        }
    }

    async fn invoke_call_tool(&self, tool: &str, arguments: Value) -> Result<Value, String> {
        if self.config.command.is_some() {
            self.invoke_stdio_call_tool(tool, arguments).await
        } else {
            self.invoke_http_call_tool(tool, arguments).await
        }
    }

    async fn invoke_stdio_list_tools(&self) -> Result<Vec<String>, String> {
        let mut session = StdioMcpSession::connect(
            self.config
                .command
                .as_deref()
                .ok_or_else(|| "missing command".to_string())?,
            &self.config.args,
            &self.config.env,
            self.startup_timeout(),
        )
        .await?;

        session.initialize().await?;
        let result = session.request("tools/list", json!({})).await?;
        let names = extract_tool_names(&result);
        let _ = session.shutdown().await;
        Ok(names)
    }

    async fn invoke_stdio_call_tool(&self, tool: &str, arguments: Value) -> Result<Value, String> {
        let mut session = StdioMcpSession::connect(
            self.config
                .command
                .as_deref()
                .ok_or_else(|| "missing command".to_string())?,
            &self.config.args,
            &self.config.env,
            self.startup_timeout(),
        )
        .await?;

        session.initialize().await?;
        let params = json!({
            "name": tool,
            "arguments": arguments,
        });
        let result = timeout(self.io_timeout(), session.request("tools/call", params))
            .await
            .map_err(|_| {
                format!(
                    "mcp call timed out for server '{}' tool '{}'",
                    self.config.server_name, tool
                )
            })??;
        let _ = session.shutdown().await;
        Ok(result)
    }

    async fn invoke_http_list_tools(&self) -> Result<Vec<String>, String> {
        let response = self
            .http_request("tools/list", json!({}), self.io_timeout())
            .await?;
        Ok(extract_tool_names(&response))
    }

    async fn invoke_http_call_tool(&self, tool: &str, arguments: Value) -> Result<Value, String> {
        let params = json!({
            "name": tool,
            "arguments": arguments,
        });
        self.http_request("tools/call", params, self.io_timeout())
            .await
    }

    async fn http_request(
        &self,
        method: &str,
        params: Value,
        timeout_dur: Duration,
    ) -> Result<Value, String> {
        let url = self
            .config
            .url
            .as_deref()
            .ok_or_else(|| "missing url".to_string())?;
        let client = reqwest::Client::new();
        let mut headers = HeaderMap::new();
        for (key, value) in &self.config.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(key.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                headers.insert(name, value);
            }
        }
        if let Some(env_var) = &self.config.bearer_token_env_var {
            if let Ok(token) = std::env::var(env_var) {
                let auth = format!("Bearer {}", token);
                if let Ok(value) = HeaderValue::from_str(&auth) {
                    headers.insert(AUTHORIZATION, value);
                }
            }
        }

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let send = client.post(url).headers(headers).json(&payload).send();
        let response = timeout(timeout_dur, send)
            .await
            .map_err(|_| format!("mcp http request timed out to {}", url))?
            .map_err(|err| format!("mcp http request failed: {}", err))?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            return Err(format!(
                "mcp http request failed with status {}: {}",
                status,
                truncate_text(&body, 800)
            ));
        }

        let value: Value = response
            .json()
            .await
            .map_err(|err| format!("mcp http response is not JSON: {}", err))?;

        if let Some(error) = value.get("error") {
            return Err(format!("mcp rpc error: {}", error));
        }

        Ok(value.get("result").cloned().unwrap_or(Value::Null))
    }
}

#[async_trait]
impl Action for McpServerAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_capabilities(["mcp", "side_effect", "tool_invocation"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "operation": {
                        "type": "string",
                        "enum": ["list_tools", "call"],
                        "default": "call",
                        "description": "MCP operation to execute"
                    },
                    "tool": {
                        "type": "string",
                        "description": "Tool name for operation=call"
                    },
                    "arguments": {
                        "type": "object",
                        "description": "Tool arguments for operation=call",
                        "default": {}
                    }
                },
                "required": ["operation"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "server": {"type": "string"},
                    "operation": {"type": "string"},
                    "tools": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "tool": {"type": "string"},
                    "result": {}
                },
                "required": ["server", "operation"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = input.params;
        let operation = params
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or("call")
            .to_string();

        if operation == "list_tools" {
            match self.invoke_list_tools().await {
                Ok(tools) => {
                    let mut exports = HashMap::new();
                    exports.insert(
                        "server".to_string(),
                        Value::String(self.config.server_name.clone()),
                    );
                    exports.insert("operation".to_string(), Value::String(operation));
                    exports.insert(
                        "tools".to_string(),
                        Value::Array(tools.into_iter().map(Value::String).collect()),
                    );
                    exports.insert("required".to_string(), Value::Bool(self.config.required));
                    return ActionResult::success_with(exports);
                }
                Err(err) => return ActionResult::error(err),
            }
        }

        if operation != "call" {
            return ActionResult::error(format!(
                "unsupported mcp operation '{}'; expected 'list_tools' or 'call'",
                operation
            ));
        }

        let Some(tool) = params.get("tool").and_then(Value::as_str) else {
            return ActionResult::error("mcp call requires params.tool");
        };
        if !self.allows_tool(tool) {
            return ActionResult::error(format!(
                "mcp tool '{}' is disabled by config for server '{}'",
                tool, self.config.server_name
            ));
        }

        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let arguments = if arguments.is_null() {
            json!({})
        } else {
            arguments
        };

        match self.invoke_call_tool(tool, arguments).await {
            Ok(result) => {
                let mut exports = HashMap::new();
                exports.insert(
                    "server".to_string(),
                    Value::String(self.config.server_name.clone()),
                );
                exports.insert("operation".to_string(), Value::String(operation));
                exports.insert("tool".to_string(), Value::String(tool.to_string()));
                exports.insert("result".to_string(), result);
                exports.insert("required".to_string(), Value::Bool(self.config.required));
                ActionResult::success_with(exports)
            }
            Err(err) => ActionResult::error(err),
        }
    }
}

// ---------------------------------------------------------------------------
// McpToolAction — single-tool action (one per MCP tool, registered at startup)
// ---------------------------------------------------------------------------

struct McpToolAction {
    /// Action name: mcp__<server>__<tool>
    name: String,
    /// Human-readable description from MCP tools/list
    description: String,
    /// Server connection config (reused from McpServerAction)
    config: McpServerActionConfig,
    /// The specific MCP tool name to invoke
    tool_name: String,
    /// Input schema from MCP tools/list
    input_schema: Value,
}

impl McpToolAction {
    fn from_spec(spec: &ActionSpec) -> Result<Self, ActionBuildError> {
        let config: McpServerActionConfig =
            serde_json::from_value(spec.config.clone()).map_err(|err| {
                ActionBuildError::InvalidConfig(format!(
                    "action '{}' invalid mcp_tool config: {}",
                    spec.name, err
                ))
            })?;

        let tool_name = spec
            .config
            .get("tool_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let input_schema = spec
            .config
            .get("tool_input_schema")
            .cloned()
            .unwrap_or(json!({"type": "object"}));

        Ok(Self {
            name: spec.name.clone(),
            description: spec.description_or("Invoke an MCP tool"),
            config,
            tool_name,
            input_schema,
        })
    }

    fn io_timeout(&self) -> Duration {
        Duration::from_millis(self.config.tool_timeout_ms.unwrap_or(20_000))
    }

    fn startup_timeout(&self) -> Duration {
        Duration::from_millis(self.config.startup_timeout_ms.unwrap_or(15_000))
    }

    async fn invoke(&self, arguments: Value) -> Result<Value, String> {
        if self.config.command.is_some() {
            let mut session = StdioMcpSession::connect(
                self.config
                    .command
                    .as_deref()
                    .ok_or_else(|| "missing command".to_string())?,
                &self.config.args,
                &self.config.env,
                self.startup_timeout(),
            )
            .await?;
            session.initialize().await?;
            let params = json!({ "name": self.tool_name, "arguments": arguments });
            let result = timeout(self.io_timeout(), session.request("tools/call", params))
                .await
                .map_err(|_| {
                    format!(
                        "mcp call timed out for server '{}' tool '{}'",
                        self.config.server_name, self.tool_name
                    )
                })??;
            let _ = session.shutdown().await;
            Ok(result)
        } else {
            let url = self
                .config
                .url
                .as_deref()
                .ok_or_else(|| "missing url".to_string())?;
            let client = reqwest::Client::new();
            let mut headers = HeaderMap::new();
            for (key, value) in &self.config.headers {
                if let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(key.as_bytes()),
                    HeaderValue::from_str(value),
                ) {
                    headers.insert(name, value);
                }
            }
            if let Some(env_var) = &self.config.bearer_token_env_var {
                if let Ok(token) = std::env::var(env_var) {
                    let auth = format!("Bearer {}", token);
                    if let Ok(value) = HeaderValue::from_str(&auth) {
                        headers.insert(AUTHORIZATION, value);
                    }
                }
            }
            let payload = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": { "name": self.tool_name, "arguments": arguments },
            });
            let send = client.post(url).headers(headers).json(&payload).send();
            let response = timeout(self.io_timeout(), send)
                .await
                .map_err(|_| format!("mcp http request timed out to {}", url))?
                .map_err(|err| format!("mcp http request failed: {}", err))?;

            let status = response.status();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<unavailable>".to_string());
                return Err(format!(
                    "mcp http request failed with status {}: {}",
                    status,
                    truncate_text(&body, 800)
                ));
            }
            let value: Value = response
                .json()
                .await
                .map_err(|err| format!("mcp http response is not JSON: {}", err))?;
            if let Some(error) = value.get("error") {
                return Err(format!("mcp rpc error: {}", error));
            }
            Ok(value.get("result").cloned().unwrap_or(Value::Null))
        }
    }
}

#[async_trait]
impl Action for McpToolAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_capabilities(["mcp", "side_effect", "tool_invocation"])
            .with_input_schema(self.input_schema.clone())
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "result": {}
                },
                "required": ["result"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        // The input params ARE the tool arguments (direct pass-through).
        let arguments = input.params;
        match self.invoke(arguments).await {
            Ok(result) => {
                let mut exports = HashMap::new();
                exports.insert("result".to_string(), result);
                ActionResult::success_with(exports)
            }
            Err(err) => ActionResult::error(err),
        }
    }
}

struct StdioMcpSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    startup_timeout: Duration,
}

impl StdioMcpSession {
    async fn connect(
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
        startup_timeout: Duration,
    ) -> Result<Self, String> {
        let mut cmd = Command::new(command);
        cmd.args(args);
        if !env.is_empty() {
            cmd.envs(env);
        }
        cmd.kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());

        let mut child = cmd
            .spawn()
            .map_err(|err| format!("spawn mcp process failed: {}", err))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "mcp stdio missing stdin pipe".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "mcp stdio missing stdout pipe".to_string())?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
            startup_timeout,
        })
    }

    async fn initialize(&mut self) -> Result<(), String> {
        let params = json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "tools": {}
            },
            "clientInfo": {
                "name": "orchestral",
                "version": env!("CARGO_PKG_VERSION")
            }
        });
        let _ = timeout(self.startup_timeout, self.request("initialize", params))
            .await
            .map_err(|_| "mcp initialize timed out".to_string())??;

        let _ = self
            .notification("notifications/initialized", json!({}))
            .await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), String> {
        let _ = self.child.kill().await;
        Ok(())
    }

    async fn notification(&mut self, method: &str, params: Value) -> Result<(), String> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.write_frame(&payload).await
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.write_frame(&payload).await?;

        loop {
            let msg = self.read_frame().await?;
            let matched = msg
                .get("id")
                .and_then(Value::as_u64)
                .map(|value| value == id)
                .unwrap_or(false);
            if !matched {
                continue;
            }

            if let Some(error) = msg.get("error") {
                return Err(format!("mcp rpc error: {}", error));
            }
            return Ok(msg.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    async fn write_frame(&mut self, payload: &Value) -> Result<(), String> {
        // Use NDJSON (newline-delimited JSON) — compatible with all MCP servers.
        let body = serde_json::to_vec(payload)
            .map_err(|err| format!("serialize mcp payload failed: {}", err))?;
        self.stdin
            .write_all(&body)
            .await
            .map_err(|err| format!("write mcp payload failed: {}", err))?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(|err| format!("write mcp newline failed: {}", err))?;
        self.stdin
            .flush()
            .await
            .map_err(|err| format!("flush mcp payload failed: {}", err))
    }

    async fn read_frame(&mut self) -> Result<Value, String> {
        // Auto-detect: NDJSON (line = JSON) or LSP (Content-Length header).
        loop {
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .await
                .map_err(|err| format!("read mcp frame failed: {}", err))?;
            if read == 0 {
                return Err("mcp process closed stdout".to_string());
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            // If line starts with '{', it's NDJSON
            if trimmed.starts_with('{') {
                return serde_json::from_str::<Value>(trimmed)
                    .map_err(|err| format!("parse mcp NDJSON failed: {}", err));
            }
            // Otherwise treat as Content-Length header (LSP style)
            if let Some((key, value)) = trimmed.split_once(':') {
                if key.trim().eq_ignore_ascii_case("content-length") {
                    if let Ok(len) = value.trim().parse::<usize>() {
                        // Read blank line after headers
                        let mut blank = String::new();
                        let _ = self.stdout.read_line(&mut blank).await;
                        // Read exact body
                        let mut body = vec![0_u8; len];
                        self.stdout
                            .read_exact(&mut body)
                            .await
                            .map_err(|err| format!("read mcp payload failed: {}", err))?;
                        return serde_json::from_slice::<Value>(&body)
                            .map_err(|err| format!("parse mcp payload failed: {}", err));
                    }
                }
            }
        }
    }
}

fn extract_tool_names(result: &Value) -> Vec<String> {
    let mut names = result
        .get("tools")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("name").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    names.sort();
    names.dedup();
    names
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return text.to_string();
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str(&format!("... [truncated total_chars={}]", char_count));
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use orchestral_core::config::ActionSpec;
    use orchestral_core::store::WorkingSet;
    use serde_json::json;
    use tokio::sync::RwLock;

    fn test_ctx() -> ActionContext {
        ActionContext::new(
            "task-1",
            "s1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
        )
    }

    fn build_mock_mcp_stdio_script(second_result: Value) -> String {
        let init_payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "serverInfo": {
                    "name": "mock",
                    "version": "1.0.0"
                }
            }
        })
        .to_string();
        let second_payload = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": second_result
        })
        .to_string();
        format!(
            "printf 'Content-Length: {}\\r\\n\\r\\n{}'; printf 'Content-Length: {}\\r\\n\\r\\n{}'; cat >/dev/null",
            init_payload.len(),
            init_payload,
            second_payload.len(),
            second_payload
        )
    }

    #[test]
    fn from_spec_requires_command_or_url() {
        let spec = ActionSpec {
            name: "mcp__bad".to_string(),
            kind: "mcp_server".to_string(),
            description: None,
            category: None,
            config: json!({
                "server_name": "bad"
            }),
            interface: None,
        };

        match McpServerAction::from_spec(&spec) {
            Ok(_) => panic!("should fail"),
            Err(err) => assert!(err.to_string().contains("requires command or url")),
        }
    }

    #[test]
    fn allows_tool_respects_enabled_and_disabled_lists() {
        let spec = ActionSpec {
            name: "mcp__alpha".to_string(),
            kind: "mcp_server".to_string(),
            description: None,
            category: None,
            config: json!({
                "server_name": "alpha",
                "command": "node",
                "args": ["server.js"],
                "enabled_tools": ["allowed", "blocked"],
                "disabled_tools": ["blocked"]
            }),
            interface: None,
        };

        let action = McpServerAction::from_spec(&spec).expect("parse action");
        assert!(action.allows_tool("allowed"));
        assert!(!action.allows_tool("blocked"));
        assert!(!action.allows_tool("unknown"));
    }

    #[test]
    fn extract_tool_names_sorts_and_deduplicates() {
        let result = json!({
            "tools": [
                {"name": "zeta"},
                {"name": "alpha"},
                {"name": "alpha"}
            ]
        });

        let names = extract_tool_names(&result);
        assert_eq!(names, vec!["alpha".to_string(), "zeta".to_string()]);
    }

    #[tokio::test]
    async fn run_list_tools_over_stdio_returns_success_exports() {
        let script = build_mock_mcp_stdio_script(json!({
            "tools": [
                {"name":"tool_a"},
                {"name":"tool_b"}
            ]
        }));

        let spec = ActionSpec {
            name: "mcp__alpha".to_string(),
            kind: "mcp_server".to_string(),
            description: None,
            category: None,
            config: json!({
                "server_name": "alpha",
                "command": "sh",
                "args": ["-c", script],
            }),
            interface: None,
        };
        let action = McpServerAction::from_spec(&spec).expect("build action");
        let result = action
            .run(
                ActionInput::with_params(json!({"operation":"list_tools"})),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Success { exports } => {
                assert_eq!(exports.get("server"), Some(&json!("alpha")));
                assert_eq!(exports.get("operation"), Some(&json!("list_tools")));
                assert_eq!(exports.get("tools"), Some(&json!(["tool_a", "tool_b"])));
            }
            other => panic!("expected success result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn probe_mcp_server_tools_returns_descriptors_with_schemas() {
        let script = build_mock_mcp_stdio_script(json!({
            "tools": [
                {
                    "name": "create_issue",
                    "description": "Create a GitHub issue",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "repo": { "type": "string" },
                            "title": { "type": "string" }
                        },
                        "required": ["repo", "title"]
                    }
                },
                {
                    "name": "list_repos",
                    "description": "List repositories"
                }
            ]
        }));

        let spec = ActionSpec {
            name: "mcp__github".to_string(),
            kind: "mcp_server".to_string(),
            description: None,
            category: None,
            config: json!({
                "server_name": "github",
                "command": "sh",
                "args": ["-c", script],
            }),
            interface: None,
        };

        let tools = probe_mcp_server_tools(&spec).await.expect("probe tools");
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name, "create_issue");
        assert_eq!(tools[0].description, "Create a GitHub issue");
        assert!(tools[0].input_schema.get("properties").is_some());
        assert_eq!(tools[1].name, "list_repos");
        // list_repos has no inputSchema, should default to {"type": "object"}
        assert_eq!(tools[1].input_schema, json!({"type": "object"}));
    }

    #[tokio::test]
    async fn probe_mcp_server_tools_respects_enabled_disabled_filter() {
        let script = build_mock_mcp_stdio_script(json!({
            "tools": [
                { "name": "allowed_tool", "description": "ok" },
                { "name": "blocked_tool", "description": "nope" }
            ]
        }));

        let spec = ActionSpec {
            name: "mcp__filtered".to_string(),
            kind: "mcp_server".to_string(),
            description: None,
            category: None,
            config: json!({
                "server_name": "filtered",
                "command": "sh",
                "args": ["-c", script],
                "disabled_tools": ["blocked_tool"]
            }),
            interface: None,
        };

        let tools = probe_mcp_server_tools(&spec).await.expect("probe tools");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "allowed_tool");
    }

    #[tokio::test]
    async fn mcp_tool_action_calls_tool_directly() {
        let script = build_mock_mcp_stdio_script(json!({
            "content": [{"type": "text", "text": "issue created"}],
            "is_error": false
        }));

        let spec = ActionSpec {
            name: "mcp__github__create_issue".to_string(),
            kind: "mcp_tool".to_string(),
            description: Some("Create a GitHub issue".to_string()),
            category: None,
            config: json!({
                "server_name": "github",
                "command": "sh",
                "args": ["-c", script],
                "tool_name": "create_issue",
                "tool_input_schema": {
                    "type": "object",
                    "properties": {
                        "repo": { "type": "string" },
                        "title": { "type": "string" }
                    },
                    "required": ["repo", "title"]
                }
            }),
            interface: None,
        };

        let action = McpToolAction::from_spec(&spec).expect("build mcp_tool action");
        assert_eq!(action.name(), "mcp__github__create_issue");
        assert_eq!(action.tool_name, "create_issue");

        // Verify metadata uses the tool's input schema
        let meta = action.metadata();
        assert!(meta.input_schema.get("properties").is_some());
        assert!(meta.has_capability("mcp"));

        let result = action
            .run(
                ActionInput::with_params(json!({
                    "repo": "foo/bar",
                    "title": "Bug report"
                })),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Success { exports } => {
                let result_val = exports.get("result").expect("result export");
                assert!(result_val.get("content").is_some());
            }
            other => panic!("expected success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn run_call_over_stdio_invokes_tool_and_returns_payload() {
        let script = build_mock_mcp_stdio_script(json!({
            "content":[{"type":"text","text":"pong"}],
            "is_error": false
        }));

        let spec = ActionSpec {
            name: "mcp__alpha".to_string(),
            kind: "mcp_server".to_string(),
            description: None,
            category: None,
            config: json!({
                "server_name": "alpha",
                "command": "sh",
                "args": ["-c", script],
                "enabled_tools": ["ping"]
            }),
            interface: None,
        };
        let action = McpServerAction::from_spec(&spec).expect("build action");
        let result = action
            .run(
                ActionInput::with_params(json!({
                    "operation":"call",
                    "tool":"ping",
                    "arguments":{"x":1}
                })),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Success { exports } => {
                assert_eq!(exports.get("server"), Some(&json!("alpha")));
                assert_eq!(exports.get("operation"), Some(&json!("call")));
                assert_eq!(exports.get("tool"), Some(&json!("ping")));
                assert_eq!(
                    exports
                        .get("result")
                        .and_then(|v| v.get("content"))
                        .and_then(|v| v.as_array())
                        .map(|v| !v.is_empty()),
                    Some(true)
                );
            }
            other => panic!("expected success result, got {:?}", other),
        }
    }
}
