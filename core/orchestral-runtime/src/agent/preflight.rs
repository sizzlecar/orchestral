use std::sync::Arc;

use orchestral_core::executor::ActionPreflightHook;
use serde_json::Value;

pub(crate) fn default_action_preflight_hook() -> ActionPreflightHook {
    Arc::new(|action_name: &str, params: &Value| action_preflight_error(action_name, params))
}

pub(super) fn action_preflight_error(action_name: &str, params: &Value) -> Option<String> {
    if action_name != "shell" {
        return None;
    }
    let obj = params.as_object()?;
    let command = obj.get("command")?.as_str()?.trim();
    if command.is_empty() {
        return None;
    }
    let command_token = shell_command_first_token(command);
    let command_for_path_check = command_token.unwrap_or(command);

    if command_for_path_check.contains('/')
        && !std::path::Path::new(command_for_path_check).exists()
    {
        return Some(format!(
            "command path '{}' does not exist",
            command_for_path_check
        ));
    }

    let command_name = std::path::Path::new(command_for_path_check)
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or(command_for_path_check);
    let is_python = command_name.starts_with("python");
    if !is_python {
        return None;
    }

    let args = obj.get("args").and_then(|v| v.as_array())?;
    let script = args.iter().find_map(|value| {
        value
            .as_str()
            .map(str::trim)
            .filter(|arg| !arg.is_empty() && arg.ends_with(".py"))
    })?;

    let path = std::path::Path::new(script);
    let base_dir = resolve_preflight_base_dir(obj);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    if abs.exists() {
        None
    } else {
        Some(format!("python script '{}' does not exist", abs.display()))
    }
}

fn shell_command_first_token(command: &str) -> Option<&str> {
    command.split_whitespace().next().map(str::trim)
}

fn resolve_preflight_base_dir(obj: &serde_json::Map<String, Value>) -> std::path::PathBuf {
    if let Some(workdir) = obj.get("workdir").and_then(|v| v.as_str()) {
        let workdir = workdir.trim();
        if !workdir.is_empty() {
            let workdir_path = std::path::Path::new(workdir);
            if workdir_path.is_absolute() {
                return workdir_path.to_path_buf();
            }
            if let Ok(cwd) = std::env::current_dir() {
                return cwd.join(workdir_path);
            }
        }
    }
    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
}

pub(super) fn deterministic_action_error_reason(message: &str) -> Option<&'static str> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("failed preflight") {
        return Some("preflight_rejected");
    }
    if lower.contains("no such file or directory") || lower.contains("can't open file") {
        return Some("missing_file_or_path");
    }
    if lower.contains("does not exist") {
        return Some("missing_file_or_path");
    }
    if lower.contains("command not found") {
        return Some("missing_command");
    }
    None
}
