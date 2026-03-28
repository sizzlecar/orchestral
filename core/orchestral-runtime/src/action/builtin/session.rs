use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use super::support::params_get_string;
use crate::session::SessionManager;
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

pub(crate) struct SessionAction {
    name: String,
    description: String,
    session_manager: Arc<SessionManager>,
}

impl SessionAction {
    pub(crate) fn new(session_manager: Arc<SessionManager>) -> Self {
        Self {
            name: "session".to_string(),
            description: "Manage persistent interactive sessions (codex, claude, etc.)".to_string(),
            session_manager,
        }
    }
}

#[async_trait]
impl Action for SessionAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("direct")
            .with_capabilities(["shell", "side_effect", "long_running", "interactive"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "op": {
                        "type": "string",
                        "enum": ["create", "send", "read", "send_and_read", "close", "list"],
                        "description": "Operation: create starts a new session, send writes to stdin, read gets stdout, send_and_read does both, close terminates, list shows active sessions."
                    },
                    "name": {
                        "type": "string",
                        "description": "Session name (e.g. 'codex'). Required for create/send/read/close."
                    },
                    "command": {
                        "type": "string",
                        "description": "Command to run (for create op only)."
                    },
                    "cwd": {
                        "type": "string",
                        "description": "Working directory (for create op only)."
                    },
                    "input": {
                        "type": "string",
                        "description": "Text to send to the session's stdin (for send/send_and_read ops)."
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Max seconds to wait for output (for read/send_and_read). Default 30."
                    }
                },
                "required": ["op"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "output": { "type": "string", "description": "Session stdout output." },
                    "session": { "type": "string", "description": "Session name." },
                    "sessions": { "type": "array", "items": { "type": "string" }, "description": "Active session names (for list op)." },
                    "alive": { "type": "boolean", "description": "Whether the session process is still running." }
                }
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let op = match params_get_string(params, "op") {
            Some(op) => op,
            None => return ActionResult::error("Missing 'op' parameter"),
        };

        match op.as_str() {
            "create" => {
                let name = match params_get_string(params, "name") {
                    Some(n) => n,
                    None => return ActionResult::error("Missing 'name' for create"),
                };
                let command = match params_get_string(params, "command") {
                    Some(c) => c,
                    None => return ActionResult::error("Missing 'command' for create"),
                };
                let cwd = params_get_string(params, "cwd");

                match self.session_manager.create(
                    &name,
                    &command,
                    &[],
                    cwd.as_deref(),
                ) {
                    Ok(session_name) => {
                        // Give the process a moment to start
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        let alive = self.session_manager.is_alive(&session_name);
                        ActionResult::success_with(HashMap::from([
                            ("session".to_string(), Value::String(session_name)),
                            ("alive".to_string(), Value::Bool(alive)),
                            ("output".to_string(), Value::String("Session created".to_string())),
                        ]))
                    }
                    Err(e) => ActionResult::error(format!("Create session failed: {}", e)),
                }
            }

            "send" => {
                let name = match params_get_string(params, "name") {
                    Some(n) => n,
                    None => return ActionResult::error("Missing 'name' for send"),
                };
                let text = match params_get_string(params, "input") {
                    Some(t) => t,
                    None => return ActionResult::error("Missing 'input' for send"),
                };
                match self.session_manager.send(&name, &text) {
                    Ok(()) => ActionResult::success_with(HashMap::from([
                        ("session".to_string(), Value::String(name)),
                        ("output".to_string(), Value::String("Sent".to_string())),
                    ])),
                    Err(e) => ActionResult::error(format!("Send failed: {}", e)),
                }
            }

            "read" => {
                let name = match params_get_string(params, "name") {
                    Some(n) => n,
                    None => return ActionResult::error("Missing 'name' for read"),
                };
                let timeout = params
                    .get("timeout_secs")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(30);
                // Run the blocking read in a spawn_blocking to not block tokio
                let sm = self.session_manager.clone();
                let name_clone = name.clone();
                let result = tokio::task::spawn_blocking(move || sm.read(&name_clone, timeout))
                    .await
                    .map_err(|e| format!("Read task failed: {}", e))
                    .and_then(|r| r);
                match result {
                    Ok(output) => ActionResult::success_with(HashMap::from([
                        ("session".to_string(), Value::String(name)),
                        ("output".to_string(), Value::String(output)),
                    ])),
                    Err(e) => ActionResult::error(format!("Read failed: {}", e)),
                }
            }

            "send_and_read" => {
                let name = match params_get_string(params, "name") {
                    Some(n) => n,
                    None => return ActionResult::error("Missing 'name' for send_and_read"),
                };
                let text = match params_get_string(params, "input") {
                    Some(t) => t,
                    None => return ActionResult::error("Missing 'input' for send_and_read"),
                };
                let timeout = params
                    .get("timeout_secs")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(30);
                let sm = self.session_manager.clone();
                let name_clone = name.clone();
                let result =
                    tokio::task::spawn_blocking(move || sm.send_and_read(&name_clone, &text, timeout))
                        .await
                        .map_err(|e| format!("Send+read task failed: {}", e))
                        .and_then(|r| r);
                match result {
                    Ok(output) => ActionResult::success_with(HashMap::from([
                        ("session".to_string(), Value::String(name)),
                        ("output".to_string(), Value::String(output)),
                    ])),
                    Err(e) => ActionResult::error(format!("Send+read failed: {}", e)),
                }
            }

            "close" => {
                let name = match params_get_string(params, "name") {
                    Some(n) => n,
                    None => return ActionResult::error("Missing 'name' for close"),
                };
                match self.session_manager.close(&name) {
                    Ok(()) => ActionResult::success_with(HashMap::from([
                        ("session".to_string(), Value::String(name)),
                        ("output".to_string(), Value::String("Session closed".to_string())),
                    ])),
                    Err(e) => ActionResult::error(format!("Close failed: {}", e)),
                }
            }

            "list" => {
                let names = self.session_manager.list();
                ActionResult::success_with(HashMap::from([
                    ("sessions".to_string(), Value::Array(
                        names.iter().map(|n| Value::String(n.clone())).collect(),
                    )),
                    ("output".to_string(), Value::String(
                        if names.is_empty() {
                            "No active sessions".to_string()
                        } else {
                            format!("Active sessions: {}", names.join(", "))
                        },
                    )),
                ]))
            }

            _ => ActionResult::error(format!("Unknown op: {}", op)),
        }
    }
}
