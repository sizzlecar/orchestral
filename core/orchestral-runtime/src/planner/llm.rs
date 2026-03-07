use std::sync::Arc;
use std::{collections::HashSet, fmt::Write};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};

use crate::system_prompts::render_planner_prompt;
use orchestral_core::planner::{
    HistoryItem, PlanError, Planner, PlannerContext, PlannerOutput, SkillInstruction,
};
use orchestral_core::types::{Intent, Plan, Step};

const MAX_PROMPT_LOG_CHARS: usize = 4_000;
const MAX_LLM_OUTPUT_LOG_CHARS: usize = 8_000;
const MAX_DISCOVERED_SKILL_SCRIPTS: usize = 12;

/// LLM request payload
#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub system: String,
    pub user: String,
    pub model: String,
    pub temperature: f32,
}

/// Tool definition for LLM function calling.
#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    /// JSON Schema describing the tool parameters.
    pub parameters: serde_json::Value,
}

/// LLM response that may contain a structured tool call instead of text.
#[derive(Debug, Clone)]
pub enum LlmResponse {
    Text(String),
    ToolCall {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
}

pub type StreamChunkCallback = Arc<dyn Fn(String) + Send + Sync>;

/// LLM client trait
#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError>;

    /// Chat with tool definitions. LLM must call one of the provided tools.
    /// Default implementation falls back to text-only `complete`.
    async fn complete_with_tools(
        &self,
        request: LlmRequest,
        tools: &[ToolDefinition],
    ) -> Result<LlmResponse, LlmError> {
        let _ = tools;
        let text = self.complete(request).await?;
        Ok(LlmResponse::Text(text))
    }

    async fn complete_stream(
        &self,
        request: LlmRequest,
        on_chunk: StreamChunkCallback,
    ) -> Result<String, LlmError> {
        let full = self.complete(request).await?;
        for token in full.split_inclusive(char::is_whitespace) {
            if !token.is_empty() {
                on_chunk(token.to_string());
            }
        }
        Ok(full)
    }
}

#[async_trait]
impl LlmClient for Arc<dyn LlmClient> {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        (**self).complete(request).await
    }

    async fn complete_with_tools(
        &self,
        request: LlmRequest,
        tools: &[ToolDefinition],
    ) -> Result<LlmResponse, LlmError> {
        (**self).complete_with_tools(request, tools).await
    }

    async fn complete_stream(
        &self,
        request: LlmRequest,
        on_chunk: StreamChunkCallback,
    ) -> Result<String, LlmError> {
        (**self).complete_stream(request, on_chunk).await
    }
}

/// LLM errors
#[derive(Debug, Error)]
pub enum LlmError {
    #[error("http error: {0}")]
    Http(String),
    #[error("response error: {0}")]
    Response(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Planner config for LLM
#[derive(Debug, Clone)]
pub struct LlmPlannerConfig {
    pub model: String,
    pub temperature: f32,
    pub max_history: usize,
    pub system_prompt: String,
    pub log_full_prompts: bool,
}

impl Default for LlmPlannerConfig {
    fn default() -> Self {
        Self {
            model: "anthropic/claude-sonnet-4.5".to_string(),
            temperature: 0.2,
            max_history: 20,
            system_prompt: String::new(),
            log_full_prompts: false,
        }
    }
}

/// LLM-based planner
pub struct LlmPlanner<C: LlmClient> {
    pub client: C,
    pub config: LlmPlannerConfig,
}

impl<C: LlmClient> LlmPlanner<C> {
    pub fn new(client: C, config: LlmPlannerConfig) -> Self {
        Self { client, config }
    }

    fn build_prompt(&self, intent: &Intent, context: &PlannerContext) -> (String, String) {
        let system = build_system_prompt(&self.config.system_prompt, context);
        let mut user = String::new();
        user.push_str(&format!("Intent:\n{}\n\n", intent.content));

        if !context.history.is_empty() {
            user.push_str("History:\n");
            for item in select_history_for_prompt(&context.history, self.config.max_history) {
                user.push_str(&format!("- {}: {}\n", item.role, item.content));
            }
            user.push('\n');
        }

        user.push_str("Return ONE JSON object in one of these shapes:\n");
        user.push_str(
            r#"{"type":"WORKFLOW","goal":"...","steps":[{"id":"s1","kind":"action","action":"action_name","params":{}}],"confidence":0.0,"on_complete":"...","on_failure":"..."}"#,
        );
        user.push('\n');
        user.push_str(
            r#"{"type":"WORKFLOW","goal":"...","steps":[{"id":"s1","kind":"action","action":"shell","params":{"command":"find","args":["docs","-type","f"]}},{"id":"s2","kind":"action","action":"file_read","depends_on":["s1"],"io_bindings":[{"from":"s1.stdout","to":"path","required":true}],"params":{"path":"{{s1.stdout}}"}}],"confidence":0.0,"on_complete":"...","on_failure":"..."}"#,
        );
        user.push('\n');
        user.push_str(
            r#"{"type":"WORKFLOW","goal":"...","steps":[{"id":"s1","kind":"action","action":"shell","params":{"command":"find","args":["docs","-type","f"]}},{"id":"s2","kind":"agent","depends_on":["s1"],"io_bindings":[{"from":"s1.stdout","to":"input_candidates","required":true}],"params":{"goal":"inspect and update local artifacts","allowed_actions":["shell","file_read","file_write"],"max_iterations":6,"output_keys":["updated_file_path","summary"],"output_rules":{"updated_file_path":{"candidates":[{"slot":"fill_result","path":"updated_file_path","requires":{"action":"file_write"}}]},"summary":{"candidates":[{"slot":"fill_result","path":"summary"}]}}}}],"confidence":0.0,"on_complete":"...","on_failure":"..."}"#,
        );
        user.push('\n');
        user.push_str(r#"{"type":"DIRECT_RESPONSE","message":"..."}"#);
        user.push('\n');
        user.push_str(r#"{"type":"CLARIFICATION","question":"..."}"#);
        user.push_str(
            "\nUse only action names listed in Action Catalog when type is WORKFLOW. Prefer explicit action steps first. If a fixed sequence of actions can solve the task with depends_on + io_bindings, do not use kind=agent. Use kind=agent only when runtime observations must determine subsequent actions, target selection, or key parameters cannot be fixed reliably at planning time. Add io_bindings only when step inputs depend on previous step outputs (including kind=agent). If kind=agent consumes upstream outputs, provide depends_on + io_bindings explicitly. For kind=agent, params.max_iterations MUST be an integer in [1,10]. For kind=agent, include params.output_keys and prefer params.output_rules (slot/path candidates) so runtime can materialize exports from evidence. For side-effect-sensitive keys (for example file paths produced by file_write), add requires.action in output_rules candidates. Do not rely on return_final.exports as ground truth. io_bindings MUST be an array (never a map). Return JSON only.\n",
        );
        user.push('\n');

        (system, user)
    }
}

fn select_history_for_prompt(history: &[HistoryItem], max_history: usize) -> Vec<&HistoryItem> {
    if max_history == 0 {
        return Vec::new();
    }

    let dialog: Vec<&HistoryItem> = history
        .iter()
        .filter(|h| h.role == "user" || h.role == "assistant")
        .collect();

    if dialog.len() <= max_history {
        return dialog;
    }

    let head_keep = if max_history >= 8 { 2 } else { 1 };
    let tail_keep = max_history.saturating_sub(head_keep);
    let split = dialog.len().saturating_sub(tail_keep);

    let mut selected = Vec::with_capacity(max_history);
    selected.extend(dialog.iter().take(head_keep).copied());
    selected.extend(dialog.iter().skip(split).copied());
    selected
}

fn build_system_prompt(base: &str, context: &PlannerContext) -> String {
    let capabilities = detect_prompt_capabilities(context);
    let execution_environment = build_execution_environment_block(context);
    let action_catalog = build_action_catalog(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let conditional_rules = build_conditional_rules(context, capabilities);
    render_planner_prompt(
        base,
        &execution_environment,
        &action_catalog,
        &skill_knowledge,
        &conditional_rules,
    )
}

#[derive(Debug, Clone, Copy, Default)]
struct PromptCapabilities {
    has_shell: bool,
    has_http: bool,
    has_file_read: bool,
    has_file_write: bool,
    has_python_venv: bool,
}

fn detect_prompt_capabilities(context: &PlannerContext) -> PromptCapabilities {
    let has_shell = context
        .available_actions
        .iter()
        .any(|action| action.name == "shell" || action.has_capability("shell"));
    let has_http = context
        .available_actions
        .iter()
        .any(|action| action.name == "http" || action.has_capability("network_io"));
    let has_file_read = context
        .available_actions
        .iter()
        .any(|action| action.name == "file_read" || action.has_capability("filesystem_read"));
    let has_file_write = context
        .available_actions
        .iter()
        .any(|action| action.name == "file_write" || action.has_capability("filesystem_write"));
    let has_python_venv = std::path::Path::new(".venv/bin/python3").exists();

    PromptCapabilities {
        has_shell,
        has_http,
        has_file_read,
        has_file_write,
        has_python_venv,
    }
}

fn build_conditional_rules(context: &PlannerContext, caps: PromptCapabilities) -> String {
    let mut lines: Vec<&str> = Vec::new();

    if !context.skill_instructions.is_empty() {
        lines.push("- Skill mode: treat Skill Knowledge as summary-first guidance.");
        lines.push("- If skill summary is insufficient for concrete params, add an early file_read step for the provided skill file path.");
        lines.push("- If skill lists scripts, invoke them through shell using the provided scripts directory and verify the script path exists.");
    }

    if caps.has_http {
        lines.push("- For headers-only requests, use http action with method HEAD.");
    }

    if caps.has_shell {
        lines.push(
            "- Shell mode: commands must be host-platform compatible and prefer args array style.",
        );
        lines
            .push("- Use find -name for file discovery instead of fragile glob-based ls patterns.");
    }

    if caps.has_shell && caps.has_python_venv {
        lines.push(
            "- Python mode: use .venv/bin/python3 for all Python execution; avoid bare python3.",
        );
    }

    if caps.has_file_read && (caps.has_file_write || caps.has_shell) {
        lines.push("- Prefer explicit action workflows first; use depends_on + io_bindings when upstream outputs only provide later step parameters.");
        lines.push("- Use kind=agent only when runtime observations must determine subsequent actions, target selection, or key parameters.");
        lines.push("- Do not use kind=agent for simple discovery steps whose outputs can flow into fixed downstream actions.");
        lines.push("- If kind=agent consumes upstream step outputs, include both depends_on and io_bindings for those consumed keys.");
        lines.push("- For kind=agent, params.max_iterations must be an integer in [1,10]. Keep the agent scoped to a local subproblem.");
        lines.push("- For kind=agent, include params.output_keys and prefer params.output_rules (slot/path candidates) so runtime materializes exports from evidence.");
        lines.push("- For side-effect-sensitive outputs, annotate candidates with requires.action to bind evidence provenance to the producing action.");
        lines.push("- Do not depend on return_final.exports values as the final truth; runtime evidence materialization is authoritative.");
        lines.push("- `file_write` is for concrete text writes; do not emit file_write with empty content as a path marker.");
        lines.push("- Use kind=replan only when the remaining global plan topology depends on intermediate outputs.");
        lines.push("- Replan continuation plans must not include another replan step.");
    }

    if caps.has_file_read {
        lines.push("- Be context-budget aware: prefer existing working_set summaries (stdout/content/stderr) before adding large reads.");
    }

    if lines.is_empty() {
        "none".to_string()
    } else {
        lines.join("\n")
    }
}

fn build_skill_knowledge_block(skills: &[SkillInstruction]) -> String {
    if skills.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    out.push_str("Activated Skills:\n");
    out.push_str(
        "Only execute scripts that are listed under scripts discovered or verified at runtime; never invent script filenames.\n",
    );
    for skill in skills {
        let _ = writeln!(out, "- {}: {}", skill.skill_name, skill.instructions.trim());
        if let Some(path) = &skill.skill_path {
            let _ = writeln!(out, "  [skill file: {}]", path);
        }
        if let Some(dir) = &skill.scripts_dir {
            let _ = writeln!(out, "  [scripts: {}]", dir);
            let discovered = discover_skill_scripts(dir, MAX_DISCOVERED_SKILL_SCRIPTS);
            if discovered.is_empty() {
                let _ = writeln!(out, "  [scripts discovered: none]");
            } else {
                let _ = writeln!(out, "  [scripts discovered: {}]", discovered.join(", "));
            }
        }
    }
    out
}

fn discover_skill_scripts(dir: &str, limit: usize) -> Vec<String> {
    let root = std::path::Path::new(dir);
    if !root.is_dir() || limit == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(read_dir) = std::fs::read_dir(&current) else {
            continue;
        };
        let mut entries = read_dir
            .filter_map(|entry| entry.ok().map(|v| v.path()))
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if !path.is_file() {
                continue;
            }
            let extension = path
                .extension()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if !matches!(extension.as_str(), "py" | "sh" | "js" | "ts" | "rb") {
                continue;
            }
            let display = path
                .strip_prefix(root)
                .unwrap_or(path.as_path())
                .display()
                .to_string();
            out.push(display);
            if out.len() >= limit {
                out.sort_unstable();
                return out;
            }
        }
    }

    out.sort_unstable();
    out
}

fn build_execution_environment_block(context: &PlannerContext) -> String {
    if context.runtime_info.os.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    out.push_str("Execution Environment:\n");
    out.push_str(&format!(
        "- os: {}\n- os_family: {}\n- arch: {}\n",
        context.runtime_info.os, context.runtime_info.os_family, context.runtime_info.arch
    ));
    if let Some(shell) = &context.runtime_info.shell {
        out.push_str(&format!("- shell: {}\n", shell));
    }
    if let Some(python) = resolve_python_path(&context.skill_instructions) {
        out.push_str(&format!(
            "- python: {} (MUST use this, system python3 lacks packages)\n",
            python
        ));
    }
    out
}

/// Resolve the best python path: skill venv > project root venv > None.
fn resolve_python_path(skills: &[SkillInstruction]) -> Option<String> {
    for skill in skills {
        if let Some(venv) = &skill.venv_python {
            if std::path::Path::new(venv).exists() {
                return Some(venv.clone());
            }
        }
    }
    let project_venv = std::path::Path::new(".venv/bin/python3");
    if project_venv.exists() {
        return Some(".venv/bin/python3".to_string());
    }
    None
}

fn build_action_catalog(context: &PlannerContext) -> String {
    let mut out = String::new();
    for action in &context.available_actions {
        append_action_catalog_entry(
            &mut out,
            &action.name,
            &action.description,
            &action.input_schema,
            &action.output_schema,
            &action.capabilities,
        );
    }
    out
}

fn append_action_catalog_entry(
    buf: &mut String,
    name: &str,
    description: &str,
    input_schema: &serde_json::Value,
    output_schema: &serde_json::Value,
    capabilities: &[String],
) {
    let _ = writeln!(buf, "- name: {}", name);
    let _ = writeln!(buf, "  description: {}", description);
    if capabilities.is_empty() {
        let _ = writeln!(buf, "  capabilities: []");
    } else {
        let _ = writeln!(buf, "  capabilities: [{}]", capabilities.join(", "));
    }
    append_schema_fields(buf, "input_fields", input_schema);
    append_schema_fields(buf, "output_fields", output_schema);
}

fn append_schema_fields(buf: &mut String, label: &str, schema: &serde_json::Value) {
    if schema.is_null() {
        let _ = writeln!(buf, "  {}: []", label);
        return;
    }

    let Some(properties) = schema.get("properties").and_then(|v| v.as_object()) else {
        let _ = writeln!(buf, "  {}: []", label);
        return;
    };

    let required = schema_required_fields(schema);
    let mut keys: Vec<&str> = properties.keys().map(String::as_str).collect();
    keys.sort_unstable();

    let _ = writeln!(buf, "  {}:", label);
    for key in keys {
        let Some(field_schema) = properties.get(key) else {
            continue;
        };
        let type_hint = schema_type_hint(field_schema);
        let required_label = if required.contains(key) {
            "required"
        } else {
            "optional"
        };
        let mut extras = Vec::new();
        if let Some(desc) = field_schema.get("description").and_then(|v| v.as_str()) {
            extras.push(format!("desc={}", desc));
        }
        if let Some(default) = field_schema.get("default") {
            extras.push(format!(
                "default={}",
                truncate_for_log(&default.to_string(), 120)
            ));
        }
        if let Some(example) = field_schema.get("example").or_else(|| {
            field_schema
                .get("examples")
                .and_then(|v| v.as_array()?.first())
        }) {
            extras.push(format!(
                "example={}",
                truncate_for_log(&example.to_string(), 120)
            ));
        }

        if extras.is_empty() {
            let _ = writeln!(buf, "    - {} ({}, {})", key, type_hint, required_label);
        } else {
            let _ = writeln!(
                buf,
                "    - {} ({}, {}): {}",
                key,
                type_hint,
                required_label,
                extras.join("; ")
            );
        }
    }
}

fn schema_required_fields(schema: &serde_json::Value) -> HashSet<String> {
    schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

fn schema_type_hint(schema: &serde_json::Value) -> String {
    if let Some(t) = schema.get("type") {
        if let Some(text) = t.as_str() {
            return text.to_string();
        }
        if let Some(list) = t.as_array() {
            let mut items = list
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<Vec<_>>();
            if !items.is_empty() {
                items.sort_unstable();
                return items.join("|");
            }
        }
    }
    if schema.get("enum").is_some() {
        return "enum".to_string();
    }
    if schema.get("oneOf").is_some() {
        return "oneOf".to_string();
    }
    if schema.get("anyOf").is_some() {
        return "anyOf".to_string();
    }
    if schema.is_object() {
        return "object".to_string();
    }
    "any".to_string()
}

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

#[async_trait]
impl<C: LlmClient> Planner for LlmPlanner<C> {
    async fn plan(
        &self,
        intent: &Intent,
        context: &PlannerContext,
    ) -> Result<PlannerOutput, PlanError> {
        let (system, user) = self.build_prompt(intent, context);
        info!(
            model = %self.config.model,
            temperature = self.config.temperature,
            intent_len = intent.content.len(),
            action_count = context.available_actions.len(),
            history_count = context.history.len(),
            "planner request prepared"
        );
        if tracing::enabled!(tracing::Level::DEBUG) {
            if self.config.log_full_prompts {
                debug!(
                    system_prompt = %system,
                    user_prompt = %user,
                    system_chars = system.chars().count(),
                    user_chars = user.chars().count(),
                    "planner prompts (full)"
                );
            } else {
                let system_preview = truncate_for_log(&system, MAX_PROMPT_LOG_CHARS);
                let user_preview = truncate_for_log(&user, MAX_PROMPT_LOG_CHARS);
                debug!(
                    system_prompt = %system_preview,
                    user_prompt = %user_preview,
                    system_chars = system.chars().count(),
                    user_chars = user.chars().count(),
                    "planner prompts"
                );
            }
        }
        let request = LlmRequest {
            system,
            user,
            model: self.config.model.clone(),
            temperature: self.config.temperature,
        };
        let output = self
            .client
            .complete(request)
            .await
            .map_err(|e| PlanError::LlmError(e.to_string()))?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                llm_output = %truncate_for_log(&output, MAX_LLM_OUTPUT_LOG_CHARS),
                "planner raw llm output"
            );
        }

        let json_str = extract_json(&output)
            .ok_or_else(|| PlanError::Generation("LLM output did not contain JSON".to_string()))?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                plan_json = %truncate_for_log(&json_str, MAX_LLM_OUTPUT_LOG_CHARS),
                "planner extracted json"
            );
        }

        let output = parse_planner_output(&json_str)?;
        match &output {
            PlannerOutput::Workflow(plan) => {
                info!(
                    output_type = "workflow",
                    step_count = plan.steps.len(),
                    confidence = plan.confidence.unwrap_or_default(),
                    "planner parsed output"
                );
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        goal = %truncate_for_log(&plan.goal, MAX_PROMPT_LOG_CHARS),
                        plan = %truncate_for_log(&format!("{:?}", plan.steps), MAX_LLM_OUTPUT_LOG_CHARS),
                        "planner workflow detail"
                    );
                }
            }
            PlannerOutput::DirectResponse(message) => {
                info!(
                    output_type = "direct_response",
                    message = %truncate_for_log(message, MAX_PROMPT_LOG_CHARS),
                    "planner parsed output"
                );
            }
            PlannerOutput::Clarification(question) => {
                info!(
                    output_type = "clarification",
                    question = %truncate_for_log(question, MAX_PROMPT_LOG_CHARS),
                    "planner parsed output"
                );
            }
        }
        Ok(output)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
enum PlannerJsonOutput {
    Workflow {
        goal: String,
        #[serde(default)]
        steps: Vec<Step>,
        #[serde(default)]
        confidence: Option<f32>,
        #[serde(default)]
        on_complete: Option<String>,
        #[serde(default)]
        on_failure: Option<String>,
    },
    DirectResponse {
        message: String,
    },
    Clarification {
        question: String,
    },
}

fn parse_planner_output(json: &str) -> Result<PlannerOutput, PlanError> {
    let parsed = serde_json::from_str::<PlannerJsonOutput>(json)
        .map_err(|e| PlanError::Generation(format!("Invalid planner output JSON: {}", e)))?;

    match parsed {
        PlannerJsonOutput::Workflow {
            goal,
            steps,
            confidence,
            on_complete,
            on_failure,
        } => {
            let mut plan = Plan::new(goal, steps);
            plan.confidence = confidence.map(|c| c.clamp(0.0, 1.0));
            plan.on_complete = on_complete;
            plan.on_failure = on_failure;
            Ok(PlannerOutput::Workflow(plan))
        }
        PlannerJsonOutput::DirectResponse { message } => Ok(PlannerOutput::DirectResponse(message)),
        PlannerJsonOutput::Clarification { question } => Ok(PlannerOutput::Clarification(question)),
    }
}

/// Mock LLM client for tests/examples
pub struct MockLlmClient {
    pub response: String,
}

#[async_trait]
impl LlmClient for MockLlmClient {
    async fn complete(&self, _request: LlmRequest) -> Result<String, LlmError> {
        Ok(self.response.clone())
    }
}

/// HTTP client config (OpenAI-compatible)
#[derive(Debug, Clone)]
pub struct HttpLlmClientConfig {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub model: String,
    pub temperature: f32,
    pub timeout_secs: u64,
    pub extra_headers: HeaderMap,
}

impl Default for HttpLlmClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://api.openai.com/v1/chat/completions".to_string(),
            api_key: None,
            model: "gpt-4o-mini".to_string(),
            temperature: 0.2,
            timeout_secs: 30,
            extra_headers: HeaderMap::new(),
        }
    }
}

/// HTTP LLM client using an OpenAI-compatible API
pub struct HttpLlmClient {
    client: reqwest::Client,
    config: HttpLlmClientConfig,
}

impl HttpLlmClient {
    pub fn new(config: HttpLlmClientConfig) -> Result<Self, LlmError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| LlmError::Http(e.to_string()))?;
        Ok(Self { client, config })
    }
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

#[derive(Debug, Deserialize)]
struct ChatMessageResponse {
    content: String,
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        let mut headers = self.config.extra_headers.clone();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(key) = &self.config.api_key {
            let value = format!("Bearer {}", key);
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&value).map_err(|e| LlmError::Http(e.to_string()))?,
            );
        }

        let body = ChatRequest {
            model: request.model,
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: request.system,
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: request.user,
                },
            ],
            temperature: request.temperature,
        };

        let response = self
            .client
            .post(&self.config.endpoint)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(LlmError::Response(format!("HTTP {}: {}", status, text)));
        }

        let text = response
            .text()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;
        let parsed: ChatResponse =
            serde_json::from_str(&text).map_err(|e| LlmError::Serialization(e.to_string()))?;

        let content = parsed
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| LlmError::Response("Missing choices".to_string()))?;

        Ok(content)
    }
}

fn extract_json(text: &str) -> Option<String> {
    for (start, ch) in text.char_indices() {
        if ch != '{' {
            continue;
        }
        if let Some(end) = find_json_object_end(text, start) {
            let candidate = &text[start..=end];
            if serde_json::from_str::<serde_json::Value>(candidate)
                .map(|v| v.is_object())
                .unwrap_or(false)
            {
                return Some(candidate.to_string());
            }
        }
    }
    None
}

fn find_json_object_end(text: &str, start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in text[start..].char_indices() {
        let abs = start + idx;
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(abs);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use orchestral_core::action::ActionMeta;
    use orchestral_core::planner::PlannerContext;
    use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError};
    use orchestral_core::types::Intent;
    use serde_json::json;
    use std::sync::Arc;

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
    fn test_system_prompt_contains_compact_action_catalog_and_core_rules() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig {
                system_prompt: "Base prompt.".to_string(),
                ..LlmPlannerConfig::default()
            },
        );

        let actions = vec![ActionMeta::new("write_doc", "Write markdown to file")
            .with_capabilities(["filesystem_write", "side_effect"])
            .with_input_schema(json!({
                "type":"object",
                "properties":{
                    "path":{"type":"string","description":"Target markdown path","example":"guide.md"},
                    "content":{"type":"string","description":"Markdown content"}
                },
                "required":["path","content"]
            }))
            .with_output_schema(
                json!({
                    "type":"object",
                    "properties":{
                        "path":{"type":"string","description":"Resolved path"},
                        "bytes":{"type":"integer","description":"Written bytes"}
                    }
                }),
            )];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("generate a guide");
        let (system, _user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("Action Catalog"));
        assert!(system.contains("Orchestral Planner"));
        assert!(system.contains("Intent -> Plan -> Normalize -> Execute"));
        assert!(system.contains("Core Rules"));
        assert!(system.contains("write_doc"));
        assert!(system.contains("capabilities: [filesystem_write, side_effect]"));
        assert!(system.contains("input_fields"));
        assert!(system.contains("path (string, required)"));
        assert!(system.contains("desc=Target markdown path"));
        assert!(system.contains("example=\"guide.md\""));
        assert!(!system.contains("input_schema:"));
        assert!(!system.contains("output_schema:"));
        assert!(system.contains("Conditional Rules"));
        assert!(system.contains("none"));
    }

    #[test]
    fn test_system_prompt_contains_mcp_and_skill_knowledge() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig::default(),
        );

        let actions = vec![ActionMeta::new("mcp__alpha", "Call MCP server alpha")
            .with_capabilities(["mcp", "side_effect"])
            .with_input_schema(json!({
                "type":"object",
                "properties":{
                    "operation":{"type":"string"},
                    "tool":{"type":"string"},
                    "arguments":{"type":"object"}
                }
            }))
            .with_output_schema(json!({
                "type":"object",
                "properties":{
                    "server":{"type":"string"},
                    "result":{}
                }
            }))];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore))
            .with_skill_instructions(vec![SkillInstruction {
                skill_name: "demo".to_string(),
                instructions: "Always write then verify.".to_string(),
                skill_path: Some("skills/demo/SKILL.md".to_string()),
                scripts_dir: Some(".claude/skills/demo/scripts".to_string()),
                venv_python: None,
            }]);
        let intent = Intent::new("need tools and skills");
        let (system, _user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("- name: mcp__alpha"));
        assert!(!system.contains("skill__demo"));
        assert!(system.contains("Activated Skills:"));
        assert!(system.contains("never invent script filenames"));
        assert!(system.contains("- demo: Always write then verify."));
        assert!(system.contains("[skill file: skills/demo/SKILL.md]"));
        assert!(system.contains("[scripts: .claude/skills/demo/scripts]"));
        assert!(system.contains("file_read step for the provided skill file path"));
    }

    #[test]
    fn test_system_prompt_injects_shell_http_and_exploration_rules() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig::default(),
        );

        let actions = vec![
            ActionMeta::new("shell", "Run shell command")
                .with_capabilities(["shell", "filesystem_write"]),
            ActionMeta::new("http", "HTTP request").with_capabilities(["network_io"]),
            ActionMeta::new("file_read", "Read file").with_capabilities(["filesystem_read"]),
        ];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("inspect and modify a file");
        let (system, user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("Shell mode: commands must be host-platform compatible"));
        assert!(system.contains("headers-only requests, use http action with method HEAD"));
        assert!(system.contains("Use kind=agent only when runtime observations"));
        assert!(system.contains("include both depends_on and io_bindings"));
        assert!(system.contains("params.max_iterations must be an integer in [1,10]"));
        assert!(system.contains("prefer params.output_rules"));
        assert!(system.contains("runtime evidence materialization is authoritative"));
        assert!(system.contains("Use kind=replan only when the remaining global plan topology"));
        assert!(system.contains("working_set summaries (stdout/content/stderr)"));
        assert!(user.contains("\"on_complete\":\"...\",\"on_failure\":\"...\""));
        assert!(user.contains("\"depends_on\":[\"s1\"]"));
        assert!(user.contains(
            "\"io_bindings\":[{\"from\":\"s1.stdout\",\"to\":\"path\",\"required\":true}]"
        ));
        assert!(user.contains("\"kind\":\"agent\",\"depends_on\":[\"s1\"],\"io_bindings\":"));
        assert!(user.contains("\"to\":\"input_candidates\""));
        assert!(user.contains("\"output_rules\":"));
        assert!(user.contains("params.max_iterations MUST be an integer in [1,10]"));
        assert!(user.contains("include params.output_keys and prefer params.output_rules"));
        assert!(user.contains("io_bindings MUST be an array (never a map)"));
    }

    #[test]
    fn test_parse_workflow_output() {
        let raw = r#"{
            "type":"WORKFLOW",
            "goal":"echo",
            "steps":[{"id":"s1","action":"echo","params":{"message":"hi"}}],
            "confidence": 1.2
        }"#;
        let parsed = parse_planner_output(raw).expect("parse workflow");
        match parsed {
            PlannerOutput::Workflow(plan) => {
                assert_eq!(plan.goal, "echo");
                assert_eq!(plan.steps.len(), 1);
                assert_eq!(plan.confidence, Some(1.0));
            }
            _ => panic!("expected workflow output"),
        }
    }

    #[test]
    fn test_parse_direct_response_output() {
        let raw = r#"{"type":"DIRECT_RESPONSE","message":"你好"}"#;
        let parsed = parse_planner_output(raw).expect("parse direct response");
        match parsed {
            PlannerOutput::DirectResponse(message) => {
                assert_eq!(message, "你好");
            }
            _ => panic!("expected direct_response output"),
        }
    }

    #[test]
    fn test_parse_clarification_output() {
        let raw = r#"{"type":"CLARIFICATION","question":"请提供文件路径"}"#;
        let parsed = parse_planner_output(raw).expect("parse clarification");
        match parsed {
            PlannerOutput::Clarification(question) => {
                assert_eq!(question, "请提供文件路径");
            }
            _ => panic!("expected clarification output"),
        }
    }

    #[test]
    fn test_extract_json_ignores_non_json_braces() {
        let raw = r#"Preface {not json} -> {"type":"DIRECT_RESPONSE","message":"ok"} trailing"#;
        let json = extract_json(raw).expect("json");
        assert_eq!(json, r#"{"type":"DIRECT_RESPONSE","message":"ok"}"#);
    }

    #[test]
    fn test_extract_json_handles_braces_inside_strings() {
        let raw = r#"noise {"type":"DIRECT_RESPONSE","message":"value with } brace"} end"#;
        let json = extract_json(raw).expect("json");
        assert_eq!(
            json,
            r#"{"type":"DIRECT_RESPONSE","message":"value with } brace"}"#
        );
    }
}
