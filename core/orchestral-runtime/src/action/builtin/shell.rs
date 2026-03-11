use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::process::Command;
use tokio::time::timeout;
use tracing::debug;

use super::super::shell_sandbox::{
    sandbox_command, ShellSandboxBackendKind, ShellSandboxMode, ShellSandboxPolicy,
};
use super::support::{
    approval_decision_from_ctx, build_shell_env, config_bool, config_string,
    config_string_array, config_u64, contains_shell_metacharacters, extract_inline_script_body,
    expression_command_names, expression_command_tokens, first_token, normalize_command_name,
    params_get_array, params_get_bool, params_get_string, read_stream_limited,
    requires_destructive_approval, stderr_preview, truncate_utf8_lossy, bounded_u64,
    ShellEnvPolicy,
};
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

pub(super) struct ShellAction {
    name: String,
    description: String,
    working_dir: Option<PathBuf>,
    timeout_ms: Option<u64>,
    fail_on_non_zero: bool,
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
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        let working_dir = config_string(&spec.config, "working_dir").map(PathBuf::from);
        let timeout_ms = config_u64(&spec.config, "timeout_ms");
        let fail_on_non_zero = config_bool(&spec.config, "fail_on_non_zero").unwrap_or(true);
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
            .map(|s| s.trim().to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let env_denylist = config_string_array(&spec.config, "env_denylist")
            .into_iter()
            .map(|s| s.trim().to_ascii_uppercase())
            .collect::<HashSet<_>>();
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Runs a shell command"),
            working_dir,
            timeout_ms,
            fail_on_non_zero,
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
            .with_capabilities([
                "filesystem_write",
                "shell",
                "side_effect",
                "verification",
                "fallback",
            ])
            .with_roles(["inspect", "apply", "execute", "verify"])
            .with_input_kinds(["command", "path", "text"])
            .with_output_kinds(["process_result", "text"])
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
                        "items": { "type": "string" }
                    },
                    "shell": {
                        "type": "boolean",
                        "description": "Set true to force shell expression mode (`sh -c command`)."
                    },
                    "fail_on_non_zero": {
                        "type": "boolean",
                        "description": "When true, non-zero exit status returns an action error. Defaults to true."
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
                    "stdout": { "type": "string" },
                    "stderr": { "type": "string" },
                    "status": { "type": "integer" },
                    "timed_out": { "type": "boolean" },
                    "stdout_truncated": { "type": "boolean" },
                    "stderr_truncated": { "type": "boolean" },
                    "sandbox_mode": { "type": "string" },
                    "sandboxed": { "type": "boolean" },
                    "sandbox_backend": { "type": "string" },
                    "env_policy": { "type": "string" }
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
        let fail_on_non_zero =
            params_get_bool(params, "fail_on_non_zero").unwrap_or(self.fail_on_non_zero);
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
        if requires_destructive_approval(use_shell, &command_name, &command, &args_for_check)
            && !approved
        {
            if let Some(decision) = approval_decision_from_ctx(&ctx).await {
                if matches!(decision, super::support::ApprovalDecision::Deny) {
                    return ActionResult::error("Destructive command denied by user");
                }
            } else {
                let approval_command = if use_shell || args_for_check.is_empty() {
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

        let (base_program, base_args) = if use_shell {
            ("sh".to_string(), vec!["-c".to_string(), command])
        } else {
            (command, args.unwrap_or_default())
        };
        let inline_script = extract_inline_script_body(use_shell, &base_args);
        debug!(
            action = %self.name,
            command_name = %command_name,
            use_shell = use_shell,
            cwd = %cwd.display(),
            program = %base_program,
            args = ?base_args,
            "shell action resolved command"
        );
        if let Some(script) = inline_script {
            debug!(
                action = %self.name,
                command_name = %command_name,
                inline_script = %script,
                "shell action inline script body"
            );
        }

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
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
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
        let stderr_summary = stderr_preview(&stderr, 280);

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

        if timed_out {
            return ActionResult::error(match self.timeout_ms {
                Some(ms) => format!("Shell command timed out after {} ms", ms),
                None => "Shell command timed out".to_string(),
            });
        }
        if fail_on_non_zero && status != 0 {
            return ActionResult::error(format!(
                "Shell command exited with status {}. stderr={}",
                status, stderr_summary,
            ));
        }
        ActionResult::success_with(exports.into_iter().collect())
    }
}
