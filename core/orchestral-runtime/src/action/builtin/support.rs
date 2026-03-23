use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::AtomicU64;
use tokio::io::AsyncReadExt;

use super::super::shell_sandbox::{
    resolve_root_path, ShellSandboxBackendKind, ShellSandboxMode, ShellSandboxPolicy,
};
use orchestral_core::action::ActionContext;

pub(super) static FILE_WRITE_TMP_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(super) fn config_string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub(super) fn config_bool(config: &Value, key: &str) -> Option<bool> {
    config.get(key).and_then(|v| v.as_bool())
}

pub(super) fn config_u64(config: &Value, key: &str) -> Option<u64> {
    config.get(key).and_then(|v| v.as_u64())
}

pub(super) fn config_string_array(config: &Value, key: &str) -> Vec<String> {
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

pub(super) fn params_get_string(params: &Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub(super) fn params_get_bool(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|v| v.as_bool())
}

pub(super) fn params_get_array(params: &Value, key: &str) -> Option<Vec<String>> {
    params.get(key).and_then(|v| {
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
    })
}

pub(super) fn params_get_u64(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(|v| v.as_u64())
}

fn has_parent_dir(path: &str) -> bool {
    PathBuf::from(path)
        .components()
        .any(|c| matches!(c, Component::ParentDir))
}

pub(super) fn contains_shell_metacharacters(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(
            c,
            '|' | '&' | ';' | '<' | '>' | '$' | '`' | '(' | ')' | '{' | '}' | '*' | '?' | '\n'
        )
    })
}

pub(super) fn normalize_command_name(command: &str) -> String {
    Path::new(command)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(command)
        .to_ascii_lowercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ShellEnvPolicy {
    Inherit,
    Minimal,
    Allowlist,
}

impl ShellEnvPolicy {
    pub(super) fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "inherit" => Some(Self::Inherit),
            "minimal" => Some(Self::Minimal),
            "allowlist" | "whitelist" => Some(Self::Allowlist),
            _ => None,
        }
    }

    pub(super) fn as_str(&self) -> &'static str {
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

pub(super) fn build_shell_env(
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

pub(super) fn first_token(input: &str) -> Option<&str> {
    input.split_whitespace().next()
}

pub(super) fn expression_command_tokens(input: &str) -> HashSet<String> {
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

pub(super) fn expression_command_names(input: &str) -> HashSet<String> {
    input
        .split(['|', '&', ';', '\n'])
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

pub(super) fn requires_destructive_approval(
    use_shell: bool,
    command_name: &str,
    command: &str,
    args: &[String],
) -> bool {
    let destructive = [
        "rm", "rmdir", "mv", "chmod", "chown", "truncate", "dd", "mkfs", "fdisk",
    ];

    if use_shell {
        let tokens = expression_command_tokens(command);
        if tokens.contains("find")
            && (tokens.contains("-delete")
                || tokens.contains("--delete")
                || tokens.contains("delete"))
        {
            return true;
        }
        if tokens.contains("git") && tokens.contains("reset") {
            return true;
        }
        if tokens.contains("git")
            && (tokens.contains("clean")
                || tokens.contains("checkout")
                || tokens.contains("restore"))
        {
            return true;
        }
        return destructive.iter().any(|cmd| tokens.contains(*cmd));
    }

    if command_name == "git" {
        return args
            .first()
            .map(|s| {
                s.eq_ignore_ascii_case("reset")
                    || s.eq_ignore_ascii_case("clean")
                    || s.eq_ignore_ascii_case("checkout")
                    || s.eq_ignore_ascii_case("restore")
            })
            .unwrap_or(false);
    }
    if command_name == "find" {
        return args
            .iter()
            .any(|arg| matches!(arg.to_ascii_lowercase().as_str(), "-delete" | "--delete"));
    }
    destructive.contains(&command_name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ApprovalDecision {
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

pub(super) async fn approval_decision_from_ctx(ctx: &ActionContext) -> Option<ApprovalDecision> {
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
        .as_str()
        .map(str::to_string)
        .or_else(|| {
            payload
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            payload
                .get("text")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })?;
    let decision = parse_approval_decision(&message)?;
    ws.remove_task("resume_user_input");
    Some(decision)
}

pub(super) fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> (String, bool, usize) {
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

pub(super) fn stderr_preview(stderr: &str, max_chars: usize) -> String {
    let compact = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return "<empty>".to_string();
    }
    if compact.chars().count() <= max_chars {
        return compact;
    }
    let mut out = compact.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    out
}

pub(super) fn extract_inline_script_body(use_shell: bool, args: &[String]) -> Option<&str> {
    if use_shell {
        return args
            .first()
            .filter(|flag| flag.as_str() == "-c" || flag.as_str() == "-lc")
            .and_then(|_| args.get(1))
            .map(String::as_str);
    }
    args.windows(2).find_map(|pair| {
        let flag = pair.first()?.as_str();
        let body = pair.get(1)?;
        match flag {
            "-c" | "-lc" => Some(body.as_str()),
            _ => None,
        }
    })
}

pub(super) fn bounded_u64(value: Option<u64>, default: usize, hard_max: usize) -> usize {
    value
        .and_then(|v| usize::try_from(v).ok())
        .unwrap_or(default)
        .clamp(1, hard_max)
}

pub(super) async fn read_stream_limited<R: tokio::io::AsyncRead + Unpin>(
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

pub(super) async fn resolve_safe_path_with_policy(
    policy: &ShellSandboxPolicy,
    path: &str,
    allow_nonexistent_target: bool,
) -> Result<PathBuf, String> {
    let path = path.trim();
    if path.is_empty() {
        return Err("Invalid path: empty after trimming whitespace".to_string());
    }
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

pub(super) fn build_file_sandbox_policy(
    spec: &orchestral_core::config::ActionSpec,
) -> ShellSandboxPolicy {
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
