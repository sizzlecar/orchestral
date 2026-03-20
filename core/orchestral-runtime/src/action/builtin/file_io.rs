use std::sync::atomic::Ordering;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::io::AsyncWriteExt;

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::super::shell_sandbox::{ShellSandboxMode, ShellSandboxPolicy};
use super::support::{
    bounded_u64, build_file_sandbox_policy, config_bool, config_u64, params_get_bool,
    params_get_string, params_get_u64, resolve_safe_path_with_policy, FILE_WRITE_TMP_COUNTER,
};

pub(super) struct FileReadAction {
    name: String,
    description: String,
    sandbox_policy: ShellSandboxPolicy,
    max_read_bytes: usize,
}

impl FileReadAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
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
            .with_category("direct")
            .with_capabilities(["filesystem_read", "read_only", "verification"])
            .with_roles(["inspect", "verify"])
            .with_input_kinds(["path"])
            .with_output_kinds(["text", "path"])
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

pub(super) struct FileWriteAction {
    name: String,
    description: String,
    sandbox_policy: ShellSandboxPolicy,
    create_dirs: bool,
    default_append: bool,
    max_write_bytes: usize,
}

impl FileWriteAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
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
            .with_category("direct")
            .with_capabilities(["filesystem_write", "side_effect"])
            .with_roles(["apply", "emit"])
            .with_input_kinds(["path", "text"])
            .with_output_kinds(["path"])
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
                    "allow_empty": {
                        "type": "boolean",
                        "description": "Allow empty content writes. Defaults to false to avoid accidental truncation."
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
        let allow_empty = params_get_bool(params, "allow_empty").unwrap_or(false);
        if content.is_empty() && !allow_empty {
            return ActionResult::error(
                "Refusing to write empty content; set allow_empty=true if intentional",
            );
        }
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
            let tmp_counter = FILE_WRITE_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let tmp_name = format!(
                ".orchestral-tmp-{}-{}-{}.tmp",
                std::process::id(),
                tmp_counter,
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
