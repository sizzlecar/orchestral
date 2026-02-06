use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

use orchestral_config::ActionSpec;
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

use crate::factory::ActionBuildError;

const STDOUT_PREVIEW_CHARS: usize = 4_000;
const STDERR_PREVIEW_CHARS: usize = 2_000;

#[derive(Debug)]
pub struct ExternalProcessAction {
    name: String,
    description: String,
    command: String,
    args: Vec<String>,
    entrypoint: Option<String>,
    env: HashMap<String, String>,
    working_dir: Option<PathBuf>,
    timeout_ms: Option<u64>,
}

impl ExternalProcessAction {
    pub fn from_spec(spec: &ActionSpec) -> Result<Self, ActionBuildError> {
        let command = config_string(&spec.config, "command")
            .ok_or_else(|| ActionBuildError::InvalidConfig(missing_config_msg(spec, "command")))?;
        let args = config_string_array(&spec.config, "args");
        let entrypoint = config_string(&spec.config, "entrypoint");
        let env = config_string_map(&spec.config, "env");
        let working_dir = config_string(&spec.config, "working_dir").map(PathBuf::from);
        let timeout_ms = config_u64(&spec.config, "timeout_ms");

        Ok(Self {
            name: spec.name.clone(),
            description: spec
                .description_or("Runs an external process and exchanges JSON over stdin/stdout"),
            command,
            args,
            entrypoint,
            env,
            working_dir,
            timeout_ms,
        })
    }
}

#[async_trait]
impl Action for ExternalProcessAction {
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
                "description": "Plugin-defined input. Declare interface.input_schema in YAML for planner/runtime validation."
            }))
            .with_output_schema(json!({
                "type": "object",
                "description": "Plugin-defined exports. Declare interface.output_schema in YAML for planner/runtime validation."
            }))
    }

    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
        let request = ExternalInvokeRequest::from_action(self, &input, &ctx);
        let request_json = match serde_json::to_string(&request) {
            Ok(v) => v,
            Err(e) => {
                return ActionResult::error(format!("Serialize external request failed: {}", e))
            }
        };

        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args);
        if let Some(cwd) = &self.working_dir {
            cmd.current_dir(cwd);
        }
        if !self.env.is_empty() {
            cmd.envs(self.env.clone());
        }
        cmd.kill_on_drop(true);
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                return ActionResult::error(format!(
                    "Failed to spawn external process '{}': {}",
                    self.command, e
                ))
            }
        };

        if let Some(mut stdin) = child.stdin.take() {
            if let Err(e) = stdin.write_all(request_json.as_bytes()).await {
                return ActionResult::error(format!(
                    "Write request to external process failed: {}",
                    e
                ));
            }
            if let Err(e) = stdin.write_all(b"\n").await {
                return ActionResult::error(format!("Write request newline failed: {}", e));
            }
            if let Err(e) = stdin.shutdown().await {
                return ActionResult::error(format!("Shutdown external stdin failed: {}", e));
            }
        }

        let output = if let Some(ms) = self.timeout_ms {
            match timeout(Duration::from_millis(ms), child.wait_with_output()).await {
                Ok(result) => match result {
                    Ok(output) => output,
                    Err(e) => {
                        return ActionResult::error(format!(
                            "Wait external process output failed: {}",
                            e
                        ))
                    }
                },
                Err(_) => return ActionResult::error("External process timed out"),
            }
        } else {
            match child.wait_with_output().await {
                Ok(output) => output,
                Err(e) => {
                    return ActionResult::error(format!(
                        "Wait external process output failed: {}",
                        e
                    ))
                }
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if !output.status.success() {
            return ActionResult::error(format!(
                "External process exited with status {}. stderr={}",
                output.status,
                preview_text(&stderr, STDERR_PREVIEW_CHARS)
            ));
        }

        match parse_external_response(&stdout) {
            Ok(result) => result,
            Err(e) => ActionResult::error(format!(
                "Invalid external response: {}. stdout={}",
                e,
                preview_text(&stdout, STDOUT_PREVIEW_CHARS)
            )),
        }
    }
}

pub fn build_external_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    match spec.kind.as_str() {
        "external_process" | "external" => {
            let action = ExternalProcessAction::from_spec(spec)?;
            Ok(Some(Box::new(action)))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Serialize)]
struct ExternalInvokeRequest<'a> {
    protocol: &'static str,
    action: ExternalActionDescriptor<'a>,
    input: &'a ActionInput,
    context: ExternalContext<'a>,
}

impl<'a> ExternalInvokeRequest<'a> {
    fn from_action(
        action: &'a ExternalProcessAction,
        input: &'a ActionInput,
        ctx: &'a ActionContext,
    ) -> Self {
        Self {
            protocol: "orchestral.external_action.v1",
            action: ExternalActionDescriptor {
                name: &action.name,
                entrypoint: action.entrypoint.as_deref(),
            },
            input,
            context: ExternalContext {
                task_id: &ctx.task_id,
                step_id: &ctx.step_id,
                execution_id: &ctx.execution_id,
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct ExternalActionDescriptor<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    entrypoint: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct ExternalContext<'a> {
    task_id: &'a str,
    step_id: &'a str,
    execution_id: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExternalResponse {
    Tagged(ActionResult),
    Success {
        exports: Option<HashMap<String, Value>>,
    },
    Clarification {
        question: String,
    },
    Retryable {
        message: String,
        retry_after_ms: Option<u64>,
        attempt: Option<u32>,
    },
    Error {
        message: String,
    },
}

fn parse_external_response(stdout: &str) -> Result<ActionResult, String> {
    let response: ExternalResponse =
        serde_json::from_str(stdout).map_err(|e| format!("response is not valid JSON: {}", e))?;
    let result = match response {
        ExternalResponse::Tagged(result) => result,
        ExternalResponse::Success { exports } => {
            ActionResult::success_with(exports.unwrap_or_default())
        }
        ExternalResponse::Clarification { question } => ActionResult::need_clarification(question),
        ExternalResponse::Retryable {
            message,
            retry_after_ms,
            attempt,
        } => ActionResult::retryable(
            message,
            retry_after_ms.map(Duration::from_millis),
            attempt.unwrap_or(0),
        ),
        ExternalResponse::Error { message } => ActionResult::error(message),
    };
    Ok(result)
}

fn missing_config_msg(spec: &ActionSpec, field: &str) -> String {
    format!(
        "action '{}' kind '{}' requires config.{}",
        spec.name, spec.kind, field
    )
}

fn config_string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
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
                .collect()
        })
        .unwrap_or_default()
}

fn config_string_map(config: &Value, key: &str) -> HashMap<String, String> {
    config
        .get(key)
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|value| (k.clone(), value.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn preview_text(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use orchestral_core::store::{
        Reference, ReferenceStore, ReferenceType, StoreError, WorkingSet,
    };
    use serde_json::json;
    use tokio::sync::RwLock;

    use super::*;

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

    #[test]
    fn test_external_process_action_success() {
        tokio_test::block_on(async {
            let spec = ActionSpec {
                name: "external_demo".to_string(),
                kind: "external_process".to_string(),
                description: Some("demo".to_string()),
                interface: None,
                config: json!({
                    "command": "sh",
                    "args": [
                        "-c",
                        "cat >/dev/null; printf '{\"type\":\"success\",\"exports\":{\"result\":\"ok\"}}'"
                    ],
                    "timeout_ms": 2000
                }),
            };
            let action = ExternalProcessAction::from_spec(&spec).expect("build action");
            let input = ActionInput::with_params(json!({"message":"hello"}));
            let ctx = ActionContext::new(
                "task-1",
                "s1",
                "exec-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = action.run(input, ctx).await;
            match result {
                ActionResult::Success { exports } => {
                    assert_eq!(
                        exports.get("result"),
                        Some(&Value::String("ok".to_string()))
                    );
                }
                other => panic!("expected success, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_external_process_action_missing_command() {
        let spec = ActionSpec {
            name: "external_demo".to_string(),
            kind: "external_process".to_string(),
            description: Some("demo".to_string()),
            interface: None,
            config: json!({}),
        };
        let err = ExternalProcessAction::from_spec(&spec).expect_err("must fail");
        assert!(err.to_string().contains("requires config.command"));
    }
}
