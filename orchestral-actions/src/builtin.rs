use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;

use crate::shell_sandbox::{
    resolve_root_path, sandbox_command, ShellSandboxBackendKind, ShellSandboxMode,
    ShellSandboxPolicy,
};
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

fn config_string_array(config: &Value, key: &str) -> Vec<String> {
    config
        .get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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

fn params_get_u64(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(|v| v.as_u64())
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

fn has_parent_dir(path: &str) -> bool {
    PathBuf::from(path)
        .components()
        .any(|c| matches!(c, Component::ParentDir))
}

fn contains_shell_metacharacters(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(
            c,
            '|' | '&' | ';' | '<' | '>' | '$' | '`' | '(' | ')' | '{' | '}' | '*' | '?' | '\n'
        )
    })
}

fn normalize_command_name(command: &str) -> String {
    Path::new(command)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(command)
        .to_ascii_lowercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShellEnvPolicy {
    Inherit,
    Minimal,
    Allowlist,
}

impl ShellEnvPolicy {
    fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "inherit" => Some(Self::Inherit),
            "minimal" => Some(Self::Minimal),
            "allowlist" | "whitelist" => Some(Self::Allowlist),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::Minimal => "minimal",
            Self::Allowlist => "allowlist",
        }
    }
}

fn normalize_env_key(key: &str) -> String {
    key.trim().to_ascii_uppercase()
}

fn build_shell_env(
    policy: ShellEnvPolicy,
    allowlist: &HashSet<String>,
    denylist: &HashSet<String>,
    extra: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let insert_if_allowed = |map: &mut HashMap<String, String>, key: &str, value: String| {
        let normalized = normalize_env_key(key);
        if !denylist.contains(&normalized) {
            map.insert(key.to_string(), value);
        }
    };

    match policy {
        ShellEnvPolicy::Inherit => {
            for (k, v) in std::env::vars() {
                insert_if_allowed(&mut out, &k, v);
            }
        }
        ShellEnvPolicy::Minimal => {
            for key in [
                "PATH", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "HOME", "USER", "TMPDIR", "SHELL",
            ] {
                if let Ok(value) = std::env::var(key) {
                    insert_if_allowed(&mut out, key, value);
                }
            }
        }
        ShellEnvPolicy::Allowlist => {
            for key in allowlist {
                if let Ok(value) = std::env::var(key) {
                    insert_if_allowed(&mut out, key, value);
                }
            }
            if !out.contains_key("PATH") {
                if let Ok(path) = std::env::var("PATH") {
                    insert_if_allowed(&mut out, "PATH", path);
                }
            }
        }
    }

    for (k, v) in extra {
        out.insert(k.clone(), v.clone());
    }
    out
}

fn first_token(input: &str) -> Option<&str> {
    input.split_whitespace().next()
}

fn expression_command_tokens(input: &str) -> HashSet<String> {
    input
        .split_whitespace()
        .map(|token| {
            token
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '-' && c != '.')
                .to_ascii_lowercase()
        })
        .filter(|token| !token.is_empty())
        .collect()
}

fn expression_command_names(input: &str) -> HashSet<String> {
    input
        .split(|c: char| matches!(c, '|' | '&' | ';' | '\n'))
        .filter_map(first_command_name_from_segment)
        .collect()
}

fn first_command_name_from_segment(segment: &str) -> Option<String> {
    for raw in segment.split_whitespace() {
        let token =
            raw.trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '-' && c != '.');
        if token.is_empty() {
            continue;
        }
        // Skip environment assignments like FOO=bar CMD ...
        if token.contains('=')
            && !token.starts_with('=')
            && token
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '=')
        {
            continue;
        }
        return Some(normalize_command_name(token));
    }
    None
}

fn requires_destructive_approval(
    use_shell: bool,
    command_name: &str,
    command: &str,
    args: &[String],
) -> bool {
    let destructive = [
        "rm", "rmdir", "mv", "chmod", "chown", "truncate", "dd", "mkfs", "fdisk", "git",
    ];

    if use_shell {
        let tokens = expression_command_tokens(command);
        if tokens.contains("git") && tokens.contains("reset") {
            return true;
        }
        return destructive.iter().any(|cmd| tokens.contains(*cmd));
    }

    if command_name == "git" {
        return args
            .first()
            .map(|s| s.eq_ignore_ascii_case("reset"))
            .unwrap_or(false);
    }
    destructive.contains(&command_name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApprovalDecision {
    Approve,
    Deny,
}

fn parse_approval_decision(message: &str) -> Option<ApprovalDecision> {
    let normalized = message.trim().to_ascii_lowercase();
    let approve_tokens = [
        "/approve", "approve", "approved", "yes", "y", "ok", "同意", "确认", "批准",
    ];
    if approve_tokens.contains(&normalized.as_str()) {
        return Some(ApprovalDecision::Approve);
    }

    let deny_tokens = [
        "/deny",
        "deny",
        "denied",
        "no",
        "n",
        "拒绝",
        "不同意",
        "取消",
    ];
    if deny_tokens.contains(&normalized.as_str()) {
        return Some(ApprovalDecision::Deny);
    }
    None
}

async fn approval_decision_from_ctx(ctx: &ActionContext) -> Option<ApprovalDecision> {
    let mut ws = ctx.working_set.write().await;
    let payload = ws.get_task("resume_user_input")?.clone();
    if let Some(decision) = payload
        .get("approval")
        .and_then(|v| v.get("decision"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_lowercase())
    {
        let mapped = match decision.as_str() {
            "approve" => Some(ApprovalDecision::Approve),
            "deny" => Some(ApprovalDecision::Deny),
            _ => None,
        };
        if mapped.is_some() {
            ws.remove_task("resume_user_input");
        }
        if let Some(mapped) = mapped {
            return Some(mapped);
        }
    }
    let message = payload
        .get("message")
        .and_then(|v| v.as_str())
        .map(str::to_string)?;
    let decision = parse_approval_decision(&message)?;
    // Consume the resume input once interpreted as an approval decision.
    ws.remove_task("resume_user_input");
    Some(decision)
}

fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> (String, bool, usize) {
    let total = bytes.len();
    if total <= max_bytes {
        return (String::from_utf8_lossy(bytes).to_string(), false, total);
    }
    (
        String::from_utf8_lossy(&bytes[..max_bytes]).to_string(),
        true,
        total,
    )
}

fn bounded_u64(value: Option<u64>, default: usize, hard_max: usize) -> usize {
    value
        .and_then(|v| usize::try_from(v).ok())
        .unwrap_or(default)
        .clamp(1, hard_max)
}

async fn read_stream_limited<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buf = [0_u8; 8192];
    let mut kept = Vec::new();
    let mut truncated = false;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        if kept.len() < max_bytes {
            let remaining = max_bytes - kept.len();
            let to_copy = remaining.min(n);
            kept.extend_from_slice(&buf[..to_copy]);
            if to_copy < n {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }
    Ok((kept, truncated))
}

async fn resolve_safe_path_with_policy(
    policy: &ShellSandboxPolicy,
    path: &str,
    allow_nonexistent_target: bool,
) -> Result<PathBuf, String> {
    if has_parent_dir(path) {
        return Err("Path escapes sandbox roots".to_string());
    }

    let cwd = std::env::current_dir().map_err(|e| format!("Resolve current dir failed: {}", e))?;
    let full_path = resolve_root_path(&cwd, Path::new(path));

    let candidate = match tokio::fs::canonicalize(&full_path).await {
        Ok(p) => p,
        Err(_e) if allow_nonexistent_target => full_path.clone(),
        Err(e) => return Err(format!("Invalid path: {}", e)),
    };

    if matches!(policy.mode, ShellSandboxMode::None) {
        return Ok(candidate);
    }

    let mut roots = policy.writable_roots.clone();
    if roots.is_empty() {
        roots.push(PathBuf::from("."));
    }
    for root in roots {
        let resolved_root = resolve_root_path(&cwd, &root);
        let canonical_root = tokio::fs::canonicalize(&resolved_root)
            .await
            .map_err(|e| format!("Invalid sandbox root '{}': {}", resolved_root.display(), e))?;
        if candidate.starts_with(&canonical_root) {
            return Ok(candidate);
        }
    }
    Err("Path escapes sandbox roots".to_string())
}

fn build_file_sandbox_policy(spec: &ActionSpec) -> ShellSandboxPolicy {
    let mode = config_string(&spec.config, "sandbox_mode")
        .as_deref()
        .and_then(ShellSandboxMode::from_str)
        .unwrap_or(ShellSandboxMode::WorkspaceWrite);
    let writable_roots = config_string_array(&spec.config, "sandbox_writable_roots")
        .into_iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    ShellSandboxPolicy {
        mode,
        backend: ShellSandboxBackendKind::Auto,
        allow_network: false,
        writable_roots,
        linux_bwrap_path: None,
    }
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
        ActionMeta::new(self.name(), self.description())
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "Text to echo back."
                    }
                },
                "required": ["message"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "result": {
                        "type": "string",
                        "description": "Echoed text result."
                    }
                },
                "required": ["result"]
            }))
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
        ActionMeta::new(self.name(), self.description())
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

/// Shell action
pub struct ShellAction {
    name: String,
    description: String,
    working_dir: Option<PathBuf>,
    timeout_ms: Option<u64>,
    allow_shell_expression: bool,
    allowed_commands: Option<HashSet<String>>,
    blocked_commands: HashSet<String>,
    max_output_bytes: usize,
    sandbox_policy: ShellSandboxPolicy,
    env_policy: ShellEnvPolicy,
    env_allowlist: HashSet<String>,
    env_denylist: HashSet<String>,
}

impl ShellAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let working_dir = config_string(&spec.config, "working_dir").map(PathBuf::from);
        let timeout_ms = config_u64(&spec.config, "timeout_ms");
        let allow_shell_expression =
            config_bool(&spec.config, "allow_shell_expression").unwrap_or(false);
        let allowed_commands_vec = config_string_array(&spec.config, "allowed_commands");
        let allowed_commands = if allowed_commands_vec.is_empty() {
            None
        } else {
            Some(
                allowed_commands_vec
                    .into_iter()
                    .map(|v| normalize_command_name(&v))
                    .collect(),
            )
        };
        let blocked_commands: HashSet<String> =
            config_string_array(&spec.config, "blocked_commands")
                .into_iter()
                .map(|v| normalize_command_name(&v))
                .collect();
        let max_output_bytes = bounded_u64(
            config_u64(&spec.config, "max_output_bytes"),
            64 * 1024,
            1024 * 1024,
        );
        let sandbox_mode = config_string(&spec.config, "sandbox_mode")
            .as_deref()
            .and_then(ShellSandboxMode::from_str)
            .unwrap_or(ShellSandboxMode::None);
        let sandbox_backend = config_string(&spec.config, "sandbox_backend")
            .as_deref()
            .and_then(ShellSandboxBackendKind::from_str)
            .unwrap_or(ShellSandboxBackendKind::Auto);
        let sandbox_allow_network =
            config_bool(&spec.config, "sandbox_allow_network").unwrap_or(false);
        let sandbox_writable_roots = config_string_array(&spec.config, "sandbox_writable_roots")
            .into_iter()
            .map(PathBuf::from)
            .collect();
        let sandbox_linux_bwrap_path =
            config_string(&spec.config, "sandbox_linux_bwrap_path").map(PathBuf::from);
        let sandbox_policy = ShellSandboxPolicy {
            mode: sandbox_mode,
            backend: sandbox_backend,
            allow_network: sandbox_allow_network,
            writable_roots: sandbox_writable_roots,
            linux_bwrap_path: sandbox_linux_bwrap_path,
        };
        let env_policy = config_string(&spec.config, "env_policy")
            .as_deref()
            .and_then(ShellEnvPolicy::from_str)
            .unwrap_or(ShellEnvPolicy::Minimal);
        let env_allowlist = config_string_array(&spec.config, "env_allowlist")
            .into_iter()
            .map(|s| normalize_env_key(&s))
            .collect::<HashSet<_>>();
        let env_denylist = config_string_array(&spec.config, "env_denylist")
            .into_iter()
            .map(|s| normalize_env_key(&s))
            .collect::<HashSet<_>>();
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Runs a shell command"),
            working_dir,
            timeout_ms,
            allow_shell_expression,
            allowed_commands,
            blocked_commands,
            max_output_bytes,
            sandbox_policy,
            env_policy,
            env_allowlist,
            env_denylist,
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
        ActionMeta::new(self.name(), self.description())
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Executable name or shell expression."
                    },
                    "args": {
                        "type": "array",
                        "description": "Optional command arguments. Preferred mode for safe execution.",
                        "items": {
                            "type": "string"
                        }
                    },
                    "shell": {
                        "type": "boolean",
                        "description": "Set true to force shell expression mode (`sh -c command`)."
                    },
                    "sandbox_mode": {
                        "type": "string",
                        "description": "Optional override for this step: none/read_only/workspace_write."
                    },
                    "sandbox_backend": {
                        "type": "string",
                        "description": "Optional override: auto/macos_seatbelt/linux_seccomp/windows_restricted."
                    },
                    "approved": {
                        "type": "boolean",
                        "description": "Set true only after explicit user approval for destructive commands."
                    }
                },
                "required": ["command"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "stdout": {
                        "type": "string",
                        "description": "Captured standard output."
                    },
                    "stderr": {
                        "type": "string",
                        "description": "Captured standard error."
                    },
                    "status": {
                        "type": "integer",
                        "description": "Process exit code (-1 when unavailable)."
                    },
                    "timed_out": {
                        "type": "boolean",
                        "description": "Whether process execution timed out and was killed."
                    },
                    "stdout_truncated": {
                        "type": "boolean",
                        "description": "Whether stdout exceeded max_output_bytes."
                    },
                    "stderr_truncated": {
                        "type": "boolean",
                        "description": "Whether stderr exceeded max_output_bytes."
                    },
                    "sandbox_mode": {
                        "type": "string",
                        "description": "Applied sandbox mode."
                    },
                    "sandboxed": {
                        "type": "boolean",
                        "description": "Whether command ran with OS sandbox wrapper."
                    },
                    "sandbox_backend": {
                        "type": "string",
                        "description": "Resolved sandbox backend name."
                    },
                    "env_policy": {
                        "type": "string",
                        "description": "Applied environment policy."
                    }
                },
                "required": ["stdout", "stderr", "status", "timed_out", "stdout_truncated", "stderr_truncated", "sandbox_mode", "sandboxed", "sandbox_backend", "env_policy"]
            }))
    }

    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let command = match params_get_string(params, "command") {
            Some(cmd) => cmd,
            None => return ActionResult::error("Missing command for shell action"),
        };
        let args = params_get_array(params, "args");
        let mut use_shell = params_get_bool(params, "shell").unwrap_or(self.allow_shell_expression);
        let approved = params_get_bool(params, "approved").unwrap_or(false);
        let looks_like_expression =
            contains_shell_metacharacters(&command) || command.contains(' ');

        let cwd = if let Some(dir) = &self.working_dir {
            dir.clone()
        } else {
            match std::env::current_dir() {
                Ok(dir) => dir,
                Err(e) => return ActionResult::error(format!("Resolve current dir failed: {}", e)),
            }
        };

        let mut sandbox_policy = self.sandbox_policy.clone();
        if let Some(mode_override) = params_get_string(params, "sandbox_mode")
            .as_deref()
            .and_then(ShellSandboxMode::from_str)
        {
            sandbox_policy.mode = mode_override;
        }
        if let Some(backend_override) = params_get_string(params, "sandbox_backend")
            .as_deref()
            .and_then(ShellSandboxBackendKind::from_str)
        {
            sandbox_policy.backend = backend_override;
        }

        // In sandboxed mode, allow shell expressions by auto-promoting to `sh -c`.
        // Keep strict validation only when sandbox is disabled.
        if args.is_none() && !use_shell && looks_like_expression {
            if matches!(sandbox_policy.mode, ShellSandboxMode::None) {
                return ActionResult::error(
                    "Unsafe shell expression detected. Provide args[] or set shell=true explicitly.",
                );
            }
            use_shell = true;
        }

        let command_name = if use_shell {
            first_token(&command)
                .map(normalize_command_name)
                .unwrap_or_else(|| "sh".to_string())
        } else {
            normalize_command_name(&command)
        };

        if self.blocked_commands.contains(&command_name) {
            return ActionResult::error(format!("Command '{}' is blocked by policy", command_name));
        }
        if use_shell {
            let tokens = expression_command_tokens(&command);
            if let Some(blocked) = self
                .blocked_commands
                .iter()
                .find(|cmd| tokens.contains(*cmd))
            {
                return ActionResult::error(format!(
                    "Shell expression contains blocked command '{}'",
                    blocked
                ));
            }
        }
        if let Some(allowed) = &self.allowed_commands {
            if use_shell {
                let mut names = expression_command_names(&command);
                if names.is_empty() {
                    names.insert(command_name.clone());
                }
                if let Some(disallowed) = names.iter().find(|name| !allowed.contains(*name)) {
                    return ActionResult::error(format!(
                        "Shell expression command '{}' is not in allowed_commands policy",
                        disallowed
                    ));
                }
            } else if !allowed.contains(&command_name) {
                return ActionResult::error(format!(
                    "Command '{}' is not in allowed_commands policy",
                    command_name
                ));
            }
        }

        let args_for_check = args.clone().unwrap_or_default();
        if requires_destructive_approval(use_shell, &command_name, &command, &args_for_check) {
            if !approved {
                if let Some(decision) = approval_decision_from_ctx(&ctx).await {
                    if matches!(decision, ApprovalDecision::Deny) {
                        return ActionResult::error("Destructive command denied by user");
                    }
                } else {
                    let approval_command = if use_shell {
                        command.clone()
                    } else if args_for_check.is_empty() {
                        command.clone()
                    } else {
                        format!("{} {}", command, args_for_check.join(" "))
                    };
                    return ActionResult::need_approval(
                        "This command is destructive and requires approval.",
                        Some(approval_command),
                    );
                }
            }
        }

        let (base_program, base_args) = if use_shell {
            ("sh".to_string(), vec!["-c".to_string(), command])
        } else {
            (command, args.unwrap_or_default())
        };

        let sandboxed = match sandbox_command(base_program, base_args, &cwd, &sandbox_policy) {
            Ok(cmd) => cmd,
            Err(e) => return ActionResult::error(format!("Sandbox setup failed: {}", e)),
        };

        let mut cmd = Command::new(&sandboxed.program);
        cmd.args(&sandboxed.args);
        let exec_env = build_shell_env(
            self.env_policy,
            &self.env_allowlist,
            &self.env_denylist,
            &sandboxed.env,
        );
        cmd.env_clear();
        cmd.envs(exec_env);
        cmd.current_dir(&cwd);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return ActionResult::error(format!("Shell execution failed: {}", e)),
        };
        let stdout_pipe = match child.stdout.take() {
            Some(stdout) => stdout,
            None => return ActionResult::error("Shell stdout capture not available"),
        };
        let stderr_pipe = match child.stderr.take() {
            Some(stderr) => stderr,
            None => return ActionResult::error("Shell stderr capture not available"),
        };

        let max_output_bytes = self.max_output_bytes;
        let stdout_task =
            tokio::spawn(async move { read_stream_limited(stdout_pipe, max_output_bytes).await });
        let stderr_task =
            tokio::spawn(async move { read_stream_limited(stderr_pipe, max_output_bytes).await });

        let wait_result: (Option<std::process::ExitStatus>, bool) =
            if let Some(ms) = self.timeout_ms {
                match timeout(Duration::from_millis(ms), child.wait()).await {
                    Ok(status) => match status {
                        Ok(exit) => (Some(exit), false),
                        Err(e) => return ActionResult::error(format!("Shell wait failed: {}", e)),
                    },
                    Err(_) => {
                        let _ = child.kill().await;
                        let status = child.wait().await.ok();
                        (status, true)
                    }
                }
            } else {
                match child.wait().await {
                    Ok(exit) => (Some(exit), false),
                    Err(e) => return ActionResult::error(format!("Shell wait failed: {}", e)),
                }
            };

        let (stdout_raw, stdout_stream_truncated) = match stdout_task.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return ActionResult::error(format!("Read stdout failed: {}", e)),
            Err(e) => return ActionResult::error(format!("Stdout join failed: {}", e)),
        };
        let (stderr_raw, stderr_stream_truncated) = match stderr_task.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return ActionResult::error(format!("Read stderr failed: {}", e)),
            Err(e) => return ActionResult::error(format!("Stderr join failed: {}", e)),
        };

        let (stdout, stdout_truncated, _) = truncate_utf8_lossy(&stdout_raw, self.max_output_bytes);
        let (stderr, stderr_truncated, _) = truncate_utf8_lossy(&stderr_raw, self.max_output_bytes);
        let stdout_truncated = stdout_truncated || stdout_stream_truncated;
        let stderr_truncated = stderr_truncated || stderr_stream_truncated;
        let status = wait_result.0.as_ref().and_then(|s| s.code()).unwrap_or(-1);
        let timed_out = wait_result.1;

        let mut exports = Map::new();
        exports.insert("stdout".to_string(), Value::String(stdout));
        exports.insert("stderr".to_string(), Value::String(stderr));
        exports.insert("status".to_string(), Value::Number(status.into()));
        exports.insert("timed_out".to_string(), Value::Bool(timed_out));
        exports.insert(
            "stdout_truncated".to_string(),
            Value::Bool(stdout_truncated),
        );
        exports.insert(
            "stderr_truncated".to_string(),
            Value::Bool(stderr_truncated),
        );
        exports.insert(
            "sandbox_mode".to_string(),
            Value::String(sandbox_policy.mode.as_str().to_string()),
        );
        exports.insert(
            "sandboxed".to_string(),
            Value::Bool(!matches!(sandbox_policy.mode, ShellSandboxMode::None)),
        );
        exports.insert(
            "sandbox_backend".to_string(),
            Value::String(sandboxed.backend.to_string()),
        );
        exports.insert(
            "env_policy".to_string(),
            Value::String(self.env_policy.as_str().to_string()),
        );
        ActionResult::success_with(exports.into_iter().collect())
    }
}

/// File read action
pub struct FileReadAction {
    name: String,
    description: String,
    sandbox_policy: ShellSandboxPolicy,
    max_read_bytes: usize,
}

impl FileReadAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let sandbox_policy = build_file_sandbox_policy(spec);
        let max_read_bytes = bounded_u64(
            config_u64(&spec.config, "max_read_bytes"),
            512 * 1024,
            10 * 1024 * 1024,
        );
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Reads a file from disk"),
            sandbox_policy,
            max_read_bytes,
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
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File path to read. Must stay under sandbox_writable_roots when sandbox is enabled."
                    },
                    "max_bytes": {
                        "type": "integer",
                        "description": "Read at most this many bytes (clamped by action config max_read_bytes)."
                    },
                    "truncate": {
                        "type": "boolean",
                        "description": "When true, oversize files are truncated instead of returning an error."
                    }
                },
                "required": ["path"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "File content as UTF-8 text."
                    },
                    "path": {
                        "type": "string",
                        "description": "Resolved absolute file path."
                    },
                    "bytes": {
                        "type": "integer",
                        "description": "Number of bytes returned in content."
                    },
                    "truncated": {
                        "type": "boolean",
                        "description": "Whether content was truncated by max_bytes."
                    }
                },
                "required": ["content", "path", "bytes", "truncated"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let path = match params_get_string(params, "path") {
            Some(p) => p,
            None => return ActionResult::error("Missing path for file_read"),
        };

        let full_path =
            match resolve_safe_path_with_policy(&self.sandbox_policy, &path, false).await {
                Ok(path) => path,
                Err(e) => return ActionResult::error(e),
            };

        let max_bytes = bounded_u64(
            params_get_u64(params, "max_bytes"),
            self.max_read_bytes,
            self.max_read_bytes,
        );
        let allow_truncate = params_get_bool(params, "truncate").unwrap_or(false);

        let mut raw = match tokio::fs::read(&full_path).await {
            Ok(v) => v,
            Err(e) => return ActionResult::error(format!("Read failed: {}", e)),
        };
        let mut truncated = false;
        if raw.len() > max_bytes {
            if !allow_truncate {
                return ActionResult::error(format!(
                    "File too large: {} bytes exceeds read limit {}",
                    raw.len(),
                    max_bytes
                ));
            }
            raw.truncate(max_bytes);
            truncated = true;
        }

        let bytes_len = raw.len() as u64;
        let content = match String::from_utf8(raw) {
            Ok(c) => c,
            Err(e) => return ActionResult::error(format!("File is not valid UTF-8 text: {}", e)),
        };

        let mut exports = Map::new();
        exports.insert("content".to_string(), Value::String(content));
        exports.insert(
            "path".to_string(),
            Value::String(full_path.to_string_lossy().to_string()),
        );
        exports.insert("bytes".to_string(), Value::Number(bytes_len.into()));
        exports.insert("truncated".to_string(), Value::Bool(truncated));
        ActionResult::success_with(exports.into_iter().collect())
    }
}

/// File write action
pub struct FileWriteAction {
    name: String,
    description: String,
    sandbox_policy: ShellSandboxPolicy,
    create_dirs: bool,
    default_append: bool,
    max_write_bytes: usize,
}

impl FileWriteAction {
    pub fn from_spec(spec: &ActionSpec) -> Self {
        let sandbox_policy = build_file_sandbox_policy(spec);
        let create_dirs = config_bool(&spec.config, "create_dirs").unwrap_or(true);
        let default_append = config_bool(&spec.config, "append").unwrap_or(false);
        let max_write_bytes = bounded_u64(
            config_u64(&spec.config, "max_write_bytes"),
            512 * 1024,
            10 * 1024 * 1024,
        );
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Writes a file to disk"),
            sandbox_policy,
            create_dirs,
            default_append,
            max_write_bytes,
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
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Target file path to write. Must stay under sandbox_writable_roots when sandbox is enabled."
                    },
                    "content": {
                        "type": "string",
                        "description": "Text content to write."
                    },
                    "append": {
                        "type": "boolean",
                        "description": "Append content instead of overwrite when true.",
                        "default": self.default_append
                    },
                    "max_bytes": {
                        "type": "integer",
                        "description": "Maximum allowed content bytes for this call (clamped by action config max_write_bytes)."
                    }
                },
                "required": ["path", "content"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Resolved absolute file path."
                    },
                    "bytes": {
                        "type": "integer",
                        "description": "Number of bytes written."
                    }
                },
                "required": ["path", "bytes"]
            }))
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
        let content_bytes = content.as_bytes();
        let max_bytes = bounded_u64(
            params_get_u64(params, "max_bytes"),
            self.max_write_bytes,
            self.max_write_bytes,
        );
        if content_bytes.len() > max_bytes {
            return ActionResult::error(format!(
                "Content too large: {} bytes exceeds write limit {}",
                content_bytes.len(),
                max_bytes
            ));
        }

        if matches!(self.sandbox_policy.mode, ShellSandboxMode::ReadOnly) {
            return ActionResult::error(
                "file_write is not allowed when sandbox_mode=read_only".to_string(),
            );
        }
        let full_path = match resolve_safe_path_with_policy(&self.sandbox_policy, &path, true).await
        {
            Ok(path) => path,
            Err(e) => return ActionResult::error(e),
        };

        if self.create_dirs {
            if let Some(parent) = full_path.parent() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    return ActionResult::error(format!("Create dirs failed: {}", e));
                }
            }
        }

        let append = params_get_bool(params, "append").unwrap_or(self.default_append);
        if let Ok(meta) = tokio::fs::symlink_metadata(&full_path).await {
            if meta.file_type().is_symlink() {
                return ActionResult::error("Refusing to write through symlink path");
            }
        }
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
        } else {
            let tmp_name = format!(
                ".orchestral-tmp-{}-{}.tmp",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let tmp_path = full_path.with_file_name(tmp_name);
            if let Err(e) = tokio::fs::write(&tmp_path, content.as_bytes()).await {
                return ActionResult::error(format!("Write temp file failed: {}", e));
            }
            if let Err(e) = tokio::fs::rename(&tmp_path, &full_path).await {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return ActionResult::error(format!("Atomic rename failed: {}", e));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use orchestral_core::store::{
        Reference, ReferenceStore, ReferenceType, StoreError, WorkingSet,
    };
    use serde_json::json;
    use tokio::sync::RwLock;

    struct NoopReferenceStore;

    #[async_trait]
    impl ReferenceStore for NoopReferenceStore {
        async fn add(&self, _reference: Reference) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get(&self, _id: &str) -> Result<Option<Reference>, StoreError> {
            Ok(None)
        }

        async fn query_by_type(
            &self,
            _ref_type: &ReferenceType,
        ) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn query_recent(&self, _limit: usize) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn delete(&self, _id: &str) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    fn test_ctx() -> ActionContext {
        ActionContext::new(
            "task-1",
            "s1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
            Arc::new(NoopReferenceStore),
        )
    }

    #[test]
    fn test_file_write_allows_new_file_under_workspace_roots() {
        tokio_test::block_on(async {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            let path = format!("target/orchestral_file_write_{}.txt", unique);
            let spec = ActionSpec {
                name: "file_write".to_string(),
                kind: "file_write".to_string(),
                description: None,
                config: json!({
                    "sandbox_mode": "workspace_write",
                    "sandbox_writable_roots": ["."],
                    "create_dirs": true
                }),
                interface: None,
            };
            let action = FileWriteAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "path": path,
                "content": "hello"
            }));
            let ctx = test_ctx();

            let result = action.run(input, ctx).await;
            match result {
                ActionResult::Success { exports } => {
                    assert_eq!(
                        exports.get("bytes").and_then(|v| v.as_u64()),
                        Some("hello".len() as u64)
                    );
                    let written_path = exports
                        .get("path")
                        .and_then(|v| v.as_str())
                        .expect("path export");
                    let content = tokio::fs::read_to_string(written_path)
                        .await
                        .expect("read back");
                    assert_eq!(content, "hello");
                    let _ = tokio::fs::remove_file(written_path).await;
                }
                other => panic!("expected success, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_shell_rejects_expression_without_explicit_shell_mode() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "shell".to_string(),
                kind: "shell".to_string(),
                description: None,
                config: json!({}),
                interface: None,
            };
            let action = ShellAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "command": "echo hello"
            }));
            let result = action.run(input, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("Unsafe shell expression detected"));
                }
                other => panic!("expected error, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_shell_blocks_command_by_policy() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "shell".to_string(),
                kind: "shell".to_string(),
                description: None,
                config: json!({
                    "blocked_commands": ["echo"]
                }),
                interface: None,
            };
            let action = ShellAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "command": "echo",
                "args": ["hello"]
            }));
            let result = action.run(input, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("blocked by policy"));
                }
                other => panic!("expected error, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_shell_blocks_expression_when_contains_blocked_command() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "shell".to_string(),
                kind: "shell".to_string(),
                description: None,
                config: json!({
                    "allow_shell_expression": true,
                    "blocked_commands": ["rm"]
                }),
                interface: None,
            };
            let action = ShellAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "command": "echo ok && rm demo.txt",
                "shell": true
            }));
            let result = action.run(input, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("contains blocked command"));
                }
                other => panic!("expected error, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_shell_blocks_expression_when_contains_disallowed_command() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "shell".to_string(),
                kind: "shell".to_string(),
                description: None,
                config: json!({
                    "allow_shell_expression": true,
                    "allowed_commands": ["echo"]
                }),
                interface: None,
            };
            let action = ShellAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "command": "echo ok && rm demo.txt",
                "shell": true
            }));
            let result = action.run(input, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("allowed_commands"));
                    assert!(message.contains("rm"));
                }
                other => panic!("expected error, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_shell_minimal_env_hides_secret_vars() {
        tokio_test::block_on(async {
            let secret_key = format!("ORCHESTRAL_TEST_SECRET_{}", std::process::id());
            std::env::set_var(&secret_key, "top-secret-value");

            let spec = ActionSpec {
                name: "shell".to_string(),
                kind: "shell".to_string(),
                description: None,
                config: json!({
                    "sandbox_mode": "none",
                    "env_policy": "minimal"
                }),
                interface: None,
            };
            let action = ShellAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "command": "env"
            }));
            let result = action.run(input, test_ctx()).await;
            std::env::remove_var(&secret_key);

            match result {
                ActionResult::Success { exports } => {
                    let stdout = exports
                        .get("stdout")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    assert!(!stdout.contains(&secret_key));
                    assert!(!stdout.contains("top-secret-value"));
                    assert_eq!(
                        exports.get("env_policy").and_then(|v| v.as_str()),
                        Some("minimal")
                    );
                }
                other => panic!("expected success, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_file_read_respects_size_limit_and_truncate() {
        tokio_test::block_on(async {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            let path = format!("target/orchestral_file_read_{}.txt", unique);
            tokio::fs::write(&path, "abcdefghij")
                .await
                .expect("seed file");

            let spec = ActionSpec {
                name: "file_read".to_string(),
                kind: "file_read".to_string(),
                description: None,
                config: json!({
                    "sandbox_mode": "workspace_write",
                    "sandbox_writable_roots": ["."],
                    "max_read_bytes": 8
                }),
                interface: None,
            };
            let action = FileReadAction::from_spec(&spec);

            let too_large = ActionInput::with_params(json!({
                "path": path.clone(),
                "max_bytes": 4
            }));
            let result = action.run(too_large, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("File too large"));
                }
                other => panic!("expected error, got {:?}", other),
            }

            let truncated = ActionInput::with_params(json!({
                "path": path.clone(),
                "max_bytes": 4,
                "truncate": true
            }));
            let result = action.run(truncated, test_ctx()).await;
            match result {
                ActionResult::Success { exports } => {
                    assert_eq!(
                        exports.get("content").and_then(|v| v.as_str()),
                        Some("abcd")
                    );
                    assert_eq!(exports.get("bytes").and_then(|v| v.as_u64()), Some(4));
                    assert_eq!(
                        exports.get("truncated").and_then(|v| v.as_bool()),
                        Some(true)
                    );
                }
                other => panic!("expected success, got {:?}", other),
            }

            let _ = tokio::fs::remove_file(&path).await;
        });
    }

    #[test]
    fn test_file_write_rejects_in_read_only_mode() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "file_write".to_string(),
                kind: "file_write".to_string(),
                description: None,
                config: json!({
                    "sandbox_mode": "read_only",
                    "sandbox_writable_roots": ["."]
                }),
                interface: None,
            };
            let action = FileWriteAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "path": "target/readonly.txt",
                "content": "hello"
            }));
            let result = action.run(input, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("read_only"));
                }
                other => panic!("expected error, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_file_read_rejects_path_outside_workspace_roots() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "file_read".to_string(),
                kind: "file_read".to_string(),
                description: None,
                config: json!({
                    "sandbox_mode": "workspace_write",
                    "sandbox_writable_roots": ["target"]
                }),
                interface: None,
            };
            let action = FileReadAction::from_spec(&spec);
            let input = ActionInput::with_params(json!({
                "path": "Cargo.toml"
            }));
            let result = action.run(input, test_ctx()).await;
            match result {
                ActionResult::Error { message } => {
                    assert!(message.contains("sandbox roots"));
                }
                other => panic!("expected error, got {:?}", other),
            }
        });
    }
}
