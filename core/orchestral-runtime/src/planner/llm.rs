use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};

use orchestral_core::planner::{
    HistoryItem, PlanError, Planner, PlannerContext, PlannerOutput, SkillInstruction,
};
use orchestral_core::types::{
    ArtifactFamily, DerivationPolicy, Intent, SkeletonChoice, SkeletonKind, StageChoice, StageKind,
};

const MAX_PROMPT_LOG_CHARS: usize = 4_000;
const MAX_LLM_OUTPUT_LOG_CHARS: usize = 8_000;
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
    pub reactor_enabled: bool,
    pub reactor_default_derivation_policy: DerivationPolicy,
}

impl Default for LlmPlannerConfig {
    fn default() -> Self {
        Self {
            model: "anthropic/claude-sonnet-4.5".to_string(),
            temperature: 0.2,
            max_history: 20,
            system_prompt: String::new(),
            log_full_prompts: false,
            reactor_enabled: false,
            reactor_default_derivation_policy: DerivationPolicy::Strict,
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
        if self.config.reactor_enabled {
            return build_reactor_prompt(
                &self.config.system_prompt,
                intent,
                context,
                self.config.max_history,
                self.config.reactor_default_derivation_policy,
            );
        }

        build_non_reactor_prompt(
            &self.config.system_prompt,
            intent,
            context,
            self.config.max_history,
        )
    }
}

fn build_non_reactor_prompt(
    base: &str,
    intent: &Intent,
    context: &PlannerContext,
    max_history: usize,
) -> (String, String) {
    let system = build_non_reactor_system_prompt(base, context);
    let mut user = String::new();
    user.push_str(&format!("Intent:\n{}\n\n", intent.content));

    if !context.history.is_empty() {
        user.push_str("History:\n");
        for item in select_history_for_prompt(&context.history, max_history) {
            user.push_str(&format!("- {}: {}\n", item.role, item.content));
        }
        user.push('\n');
    }

    user.push_str("Return exactly one JSON object:\n");
    user.push_str(r#"{"type":"DIRECT_RESPONSE","message":"..."}"#);
    user.push('\n');
    user.push_str(r#"{"type":"CLARIFICATION","question":"..."}"#);
    user.push_str(
        "\nRules:\n- JSON only.\n- Reactor execution is disabled for this planner invocation.\n- Do not emit workflow, plan, recipe, or action topology.\n- If execution is required, ask a clarification instead of inventing an execution graph.\n",
    );

    (system, user)
}

#[derive(Debug, Clone, Copy, Default)]
struct ReactorPromptCoverage {
    spreadsheet: bool,
    document: bool,
    structured: bool,
}

fn detect_reactor_prompt_coverage(context: &PlannerContext) -> ReactorPromptCoverage {
    ReactorPromptCoverage {
        spreadsheet: has_reactor_actions(
            context,
            &[
                "reactor_spreadsheet_locate",
                "reactor_spreadsheet_inspect",
                "reactor_spreadsheet_assess_readiness",
                "reactor_spreadsheet_apply_patch",
                "reactor_spreadsheet_verify_patch",
            ],
        ),
        document: has_reactor_actions(
            context,
            &[
                "reactor_document_locate",
                "reactor_document_inspect",
                "reactor_document_assess_readiness",
                "reactor_document_apply_patch",
                "reactor_document_verify_patch",
            ],
        ),
        structured: has_reactor_actions(
            context,
            &[
                "reactor_structured_locate",
                "reactor_structured_inspect",
                "reactor_structured_assess_readiness",
                "reactor_structured_apply_patch",
                "reactor_structured_verify_patch",
            ],
        ),
    }
}

fn has_reactor_actions(context: &PlannerContext, names: &[&str]) -> bool {
    names.iter().all(|name| {
        context
            .available_actions
            .iter()
            .any(|action| action.name == *name)
    })
}

fn build_reactor_prompt(
    base: &str,
    intent: &Intent,
    context: &PlannerContext,
    max_history: usize,
    default_policy: DerivationPolicy,
) -> (String, String) {
    let coverage = detect_reactor_prompt_coverage(context);
    let system = build_reactor_system_prompt(base, context, default_policy, coverage);
    let mut user = String::new();
    user.push_str(&format!("Intent:\n{}\n\n", intent.content));

    if !context.history.is_empty() {
        user.push_str("History:\n");
        for item in select_history_for_prompt(&context.history, max_history) {
            user.push_str(&format!("- {}: {}\n", item.role, item.content));
        }
        user.push('\n');
    }

    let default_policy = match default_policy {
        DerivationPolicy::Strict => "strict",
        DerivationPolicy::Permissive => "permissive",
    };

    user.push_str("Return exactly one JSON object:\n");
    append_reactor_choice_examples(&mut user, coverage);
    user.push_str(r#"{"type":"DIRECT_RESPONSE","message":"..."}"#);
    user.push('\n');
    user.push_str(r#"{"type":"CLARIFICATION","question":"..."}"#);
    user.push_str(
        "\nRules:\n- JSON only.\n- For supported local artifact work, first pass should prefer SKELETON_CHOICE.\n- Use STAGE_CHOICE when the task already belongs to a skeleton and you are choosing the next stage.\n- For SKELETON_CHOICE, initial_stage should follow the skeleton default unless current evidence clearly requires a later stage.\n- skeleton must be one of the supported skeletons.\n- artifact_family must match a supported family when returning STAGE_CHOICE; for SKELETON_CHOICE it may be omitted if not yet certain.\n- derivation_policy must be \"strict\" or \"permissive\" when returning STAGE_CHOICE.\n",
    );
    user.push_str(&format!(
        "- Default derivation_policy is \"{}\" unless the task clearly needs otherwise.\n",
        default_policy
    ));
    user.push_str("- Do not generate a full workflow DAG for supported reactor tasks.\n");
    user.push_str("- Continuation is structural. Prefer explicit wait_user or next_stage_hint over vague narration.\n");
    user.push_str("- Never use assistant narration as a completion signal.\n");

    (system, user)
}

fn append_reactor_choice_examples(buf: &mut String, coverage: ReactorPromptCoverage) {
    if coverage.spreadsheet {
        buf.push_str(
            r#"{"type":"SKELETON_CHOICE","skeleton":"locate_and_patch","artifact_family":"spreadsheet","initial_stage":"locate","confidence":0.9,"reason":"..."}"#,
        );
        buf.push('\n');
        buf.push_str(
            r#"{"type":"STAGE_CHOICE","skeleton":"locate_and_patch","artifact_family":"spreadsheet","current_stage":"probe","stage_goal":"inspect workbook structure","derivation_policy":"permissive","next_stage_hint":"derive","reason":"..."}"#,
        );
        buf.push('\n');
    }
    if coverage.document {
        buf.push_str(
            r#"{"type":"SKELETON_CHOICE","skeleton":"locate_and_patch","artifact_family":"document","initial_stage":"locate","confidence":0.9,"reason":"..."}"#,
        );
        buf.push('\n');
        buf.push_str(
            r#"{"type":"STAGE_CHOICE","skeleton":"locate_and_patch","artifact_family":"document","current_stage":"probe","stage_goal":"inspect document structure","derivation_policy":"permissive","next_stage_hint":"derive","reason":"..."}"#,
        );
        buf.push('\n');
    }
    if coverage.structured {
        buf.push_str(
            r#"{"type":"SKELETON_CHOICE","skeleton":"locate_and_patch","artifact_family":"structured","initial_stage":"locate","confidence":0.9,"reason":"..."}"#,
        );
        buf.push('\n');
        buf.push_str(
            r#"{"type":"STAGE_CHOICE","skeleton":"locate_and_patch","artifact_family":"structured","current_stage":"probe","stage_goal":"inspect structured artifact contents","derivation_policy":"strict","next_stage_hint":"derive","reason":"..."}"#,
        );
        buf.push('\n');
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

fn build_non_reactor_system_prompt(base: &str, context: &PlannerContext) -> String {
    let execution_environment = build_execution_environment_block(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let mut out = String::new();
    if !base.trim().is_empty() {
        out.push_str(base.trim());
        out.push_str("\n\n");
    }
    out.push_str("You are Orchestral Planner.\n");
    out.push_str("Execution planning is disabled in this mode.\n");
    out.push_str("Return only DIRECT_RESPONSE or CLARIFICATION.\n");
    if !execution_environment.trim().is_empty() {
        out.push('\n');
        out.push_str(&execution_environment);
    }
    if !skill_knowledge.trim().is_empty() {
        out.push('\n');
        out.push_str(&skill_knowledge);
    }
    out
}

fn build_reactor_system_prompt(
    base: &str,
    context: &PlannerContext,
    default_policy: DerivationPolicy,
    coverage: ReactorPromptCoverage,
) -> String {
    let execution_environment = build_execution_environment_block(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let mut out = String::new();
    if !base.trim().is_empty() {
        out.push_str(base.trim());
        out.push_str("\n\n");
    }

    out.push_str("You are Orchestral Reactor Planner.\n");
    out.push_str("This prompt is a short constitution, not a full execution manual.\n");
    out.push_str(
        "Planner decides only skeleton selection, current stage, family hint, and derivation posture.\n",
    );
    out.push_str("Runtime owns stage lowering, typed action wiring, continuation handling, and verify gates.\n");
    out.push_str("Continuation must be explicit. Done may only come from verify success.\n");
    out.push_str(
        "Do not design a full end-to-end workflow when skeleton/stage choice is sufficient.\n",
    );

    out.push_str("\nBuilt-in skeleton vocabulary:\n");
    out.push_str("- locate_and_patch\n");
    out.push_str("- inspect_and_extract\n");
    out.push_str("- inspect_and_transform\n");
    out.push_str("- compare_and_sync\n");
    out.push_str("- run_and_verify\n");

    out.push_str("\nCurrent executable coverage:\n");
    if coverage.spreadsheet {
        out.push_str("- skeleton=locate_and_patch, artifact_family=spreadsheet\n");
        out.push_str(
            "  covered stage path: locate -> probe -> derive -> assess -> commit -> verify\n",
        );
        out.push_str("  probe must end with explicit continuation\n");
        out.push_str("  verify is the done gate\n");
    }
    if coverage.document {
        out.push_str("- skeleton=locate_and_patch, artifact_family=document\n");
        out.push_str(
            "  covered stage path: locate -> probe -> derive -> assess -> commit -> verify\n",
        );
        out.push_str("  probe must end with explicit continuation\n");
        out.push_str("  verify is the done gate\n");
    }
    if coverage.structured {
        out.push_str("- skeleton=locate_and_patch, artifact_family=structured\n");
        out.push_str(
            "  covered stage path: locate -> probe -> derive -> assess -> commit -> verify\n",
        );
        out.push_str("  probe must end with explicit continuation\n");
        out.push_str("  verify is the done gate\n");
    }
    if !coverage.spreadsheet && !coverage.document && !coverage.structured {
        out.push_str("- no executable reactor family coverage detected for this request\n");
    }
    out.push_str("- default derivation_policy: ");
    out.push_str(match default_policy {
        DerivationPolicy::Strict => "strict",
        DerivationPolicy::Permissive => "permissive",
    });
    out.push('\n');

    out.push_str("\nHard rules:\n");
    out.push_str("- Return JSON only.\n");
    out.push_str("- Prefer SKELETON_CHOICE or STAGE_CHOICE for supported local artifact tasks.\n");
    out.push_str("- For SKELETON_CHOICE, initial_stage should follow the skeleton default unless current evidence clearly requires a later stage.\n");
    out.push_str("- Choose only from current executable coverage unless the user explicitly asks for a different reactor skeleton experiment.\n");
    out.push_str("- Do not emit a full workflow DAG for supported reactor tasks.\n");
    out.push_str("- Treat artifact_family as an adapter hint, not as task shape.\n");
    out.push_str("- Treat verification as the only done gate.\n");

    if !execution_environment.trim().is_empty() {
        out.push('\n');
        out.push_str(&execution_environment);
    }
    if !skill_knowledge.trim().is_empty() {
        out.push('\n');
        out.push_str(&skill_knowledge);
    }
    out
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
        let _ = writeln!(out, "- {}", skill.skill_name);
        for line in skill.instructions.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let _ = writeln!(out, "  {}", line);
        }
        if let Some(path) = &skill.skill_path {
            let _ = writeln!(out, "  [skill file: {}]", path);
        }
        if let Some(dir) = &skill.scripts_dir {
            let _ = writeln!(out, "  [scripts: {}]", dir);
            let referenced = extract_skill_card_scripts(&skill.instructions);
            if referenced.is_empty() {
                let _ = writeln!(
                    out,
                    "  [scripts referenced: inspect skill file or scripts dir before use]"
                );
            } else {
                let _ = writeln!(out, "  [scripts referenced: {}]", referenced.join(", "));
            }
        }
    }
    out
}

fn extract_skill_card_scripts(instructions: &str) -> Vec<String> {
    instructions
        .lines()
        .find_map(|line| line.trim().strip_prefix("scripts:"))
        .map(|line| {
            line.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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
            PlannerOutput::SkeletonChoice(choice) => {
                info!(
                    output_type = "skeleton_choice",
                    skeleton = ?choice.skeleton,
                    artifact_family = ?choice.artifact_family,
                    initial_stage = ?choice.initial_stage,
                    confidence = choice.confidence,
                    "planner parsed output"
                );
            }
            PlannerOutput::StageChoice(choice) => {
                info!(
                    output_type = "stage_choice",
                    skeleton = ?choice.skeleton,
                    artifact_family = ?choice.artifact_family,
                    current_stage = ?choice.current_stage,
                    derivation_policy = ?choice.derivation_policy,
                    "planner parsed output"
                );
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
    SkeletonChoice {
        skeleton: SkeletonKind,
        #[serde(default)]
        artifact_family: Option<ArtifactFamily>,
        initial_stage: StageKind,
        #[serde(default = "default_confidence")]
        confidence: f32,
        #[serde(default)]
        reason: Option<String>,
    },
    StageChoice {
        skeleton: SkeletonKind,
        artifact_family: ArtifactFamily,
        current_stage: StageKind,
        stage_goal: String,
        derivation_policy: DerivationPolicy,
        #[serde(default)]
        next_stage_hint: Option<StageKind>,
        #[serde(default)]
        reason: Option<String>,
    },
    DirectResponse {
        message: String,
    },
    Clarification {
        question: String,
    },
}

fn default_confidence() -> f32 {
    1.0
}

fn parse_planner_output(json: &str) -> Result<PlannerOutput, PlanError> {
    let parsed = serde_json::from_str::<PlannerJsonOutput>(json)
        .map_err(|e| PlanError::Generation(format!("Invalid planner output JSON: {}", e)))?;

    match parsed {
        PlannerJsonOutput::SkeletonChoice {
            skeleton,
            artifact_family,
            initial_stage,
            confidence,
            reason,
        } => Ok(PlannerOutput::SkeletonChoice(SkeletonChoice {
            skeleton,
            artifact_family,
            initial_stage,
            confidence: confidence.clamp(0.0, 1.0),
            reason,
        })),
        PlannerJsonOutput::StageChoice {
            skeleton,
            artifact_family,
            current_stage,
            stage_goal,
            derivation_policy,
            next_stage_hint,
            reason,
        } => Ok(PlannerOutput::StageChoice(StageChoice {
            skeleton,
            artifact_family,
            current_stage,
            stage_goal,
            derivation_policy,
            next_stage_hint,
            reason,
        })),
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
    fn test_non_reactor_prompt_is_minimal_and_no_workflow() {
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
            .with_roles(["apply", "emit"])
            .with_input_kinds(["path", "text"])
            .with_output_kinds(["path"])
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
        let (system, user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("Orchestral Planner"));
        assert!(system.contains("Execution planning is disabled in this mode."));
        assert!(!system.contains("Action Catalog"));
        assert!(user.contains("\"type\":\"DIRECT_RESPONSE\""));
        assert!(user.contains("\"type\":\"CLARIFICATION\""));
        assert!(!user.contains("\"type\":\"WORKFLOW\""));
        assert!(!user.contains("\"type\":\"STAGE_CHOICE\""));
    }

    #[test]
    fn test_non_reactor_prompt_contains_skill_knowledge() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig::default(),
        );

        let actions = vec![ActionMeta::new("mcp__alpha", "Call MCP server alpha")
            .with_capabilities(["mcp", "side_effect"])
            .with_roles(["execute"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
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

        assert!(system.contains("Activated Skills:"));
        assert!(system.contains("never invent script filenames"));
        assert!(system.contains("- demo"));
        assert!(system.contains("Always write then verify."));
        assert!(system.contains("[skill file: skills/demo/SKILL.md]"));
        assert!(system.contains("[scripts: .claude/skills/demo/scripts]"));
        assert!(!system.contains("Action Catalog"));
    }

    #[test]
    fn test_parse_stage_choice_output() {
        let raw = r#"{
            "type":"STAGE_CHOICE",
            "skeleton":"locate_and_patch",
            "artifact_family":"spreadsheet",
            "current_stage":"probe",
            "stage_goal":"inspect workbook structure and assess readiness",
            "derivation_policy":"permissive",
            "reason":"spreadsheet task"
        }"#;
        let parsed = parse_planner_output(raw).expect("parse stage choice");
        match parsed {
            PlannerOutput::StageChoice(choice) => {
                assert_eq!(choice.skeleton, SkeletonKind::LocateAndPatch);
                assert_eq!(choice.artifact_family, ArtifactFamily::Spreadsheet);
                assert_eq!(choice.current_stage, StageKind::Probe);
                assert_eq!(choice.derivation_policy, DerivationPolicy::Permissive);
                assert_eq!(choice.reason.as_deref(), Some("spreadsheet task"));
            }
            _ => panic!("expected stage choice output"),
        }
    }

    #[test]
    fn test_parse_skeleton_choice_output() {
        let raw = r#"{
            "type":"SKELETON_CHOICE",
            "skeleton":"locate_and_patch",
            "artifact_family":"spreadsheet",
            "initial_stage":"probe",
            "confidence": 1.2,
            "reason":"spreadsheet task"
        }"#;
        let parsed = parse_planner_output(raw).expect("parse skeleton choice");
        match parsed {
            PlannerOutput::SkeletonChoice(choice) => {
                assert_eq!(choice.skeleton, SkeletonKind::LocateAndPatch);
                assert_eq!(choice.artifact_family, Some(ArtifactFamily::Spreadsheet));
                assert_eq!(choice.initial_stage, StageKind::Probe);
                assert_eq!(choice.confidence, 1.0);
                assert_eq!(choice.reason.as_deref(), Some("spreadsheet task"));
            }
            _ => panic!("expected skeleton choice output"),
        }
    }

    #[test]
    fn test_reactor_prompt_is_short_contract_without_action_catalog() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig {
                reactor_enabled: true,
                reactor_default_derivation_policy: DerivationPolicy::Permissive,
                ..LlmPlannerConfig::default()
            },
        );

        let actions = vec![
            ActionMeta::new("shell", "Run shell command")
                .with_capabilities(["shell", "filesystem_write", "fallback"])
                .with_roles(["inspect", "apply", "verify", "execute"]),
            ActionMeta::new("file_read", "Read file")
                .with_capabilities(["filesystem_read"])
                .with_roles(["inspect", "verify"])
                .with_output_kinds(["text"]),
            ActionMeta::new("file_write", "Write file")
                .with_capabilities(["filesystem_write", "side_effect"])
                .with_roles(["apply", "emit"])
                .with_input_kinds(["path", "text"])
                .with_output_kinds(["path"]),
            ActionMeta::new(
                "reactor_spreadsheet_locate",
                "Locate a spreadsheet artifact for the reactor spreadsheet family",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_inspect",
                "Inspect spreadsheet structure for the reactor spreadsheet family",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_assess_readiness",
                "Assess whether spreadsheet probe results are ready for commit",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_apply_patch",
                "Apply a typed spreadsheet patch for the reactor spreadsheet family",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_verify_patch",
                "Verify a spreadsheet patch for the reactor spreadsheet family",
            ),
        ];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("docs 下有个 excel，帮我补全 spreadsheet 内容");
        let (system, user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("Built-in skeleton vocabulary:"));
        assert!(system.contains("skeleton=locate_and_patch, artifact_family=spreadsheet"));
        assert!(!system.contains("Action Catalog:"));
        assert!(!system.contains("Workflow Fallback Rules:"));
        assert!(user.contains("\"type\":\"SKELETON_CHOICE\""));
        assert!(user.contains("\"type\":\"STAGE_CHOICE\""));
        assert!(!user.contains("\"type\":\"WORKFLOW\""));
    }

    #[test]
    fn test_reactor_enabled_always_uses_reactor_prompt() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig {
                reactor_enabled: true,
                reactor_default_derivation_policy: DerivationPolicy::Permissive,
                ..LlmPlannerConfig::default()
            },
        );

        let actions = vec![
            ActionMeta::new("shell", "Run shell command")
                .with_capabilities(["shell", "filesystem_write", "fallback"])
                .with_roles(["inspect", "apply", "verify", "execute"]),
            ActionMeta::new("file_read", "Read file")
                .with_capabilities(["filesystem_read"])
                .with_roles(["inspect", "verify"])
                .with_output_kinds(["text"]),
            ActionMeta::new("file_write", "Write file")
                .with_capabilities(["filesystem_write", "side_effect"])
                .with_roles(["apply", "emit"])
                .with_input_kinds(["path", "text"])
                .with_output_kinds(["path"]),
            ActionMeta::new(
                "reactor_spreadsheet_locate",
                "Locate a spreadsheet artifact for the reactor spreadsheet family",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_inspect",
                "Inspect spreadsheet structure for the reactor spreadsheet family",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_assess_readiness",
                "Assess whether spreadsheet probe results are ready for commit",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_apply_patch",
                "Apply a typed spreadsheet patch for the reactor spreadsheet family",
            ),
            ActionMeta::new(
                "reactor_spreadsheet_verify_patch",
                "Verify a spreadsheet patch for the reactor spreadsheet family",
            ),
        ];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("扫描 markdown 并先给我修改计划");
        let (system, user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("You are Orchestral Reactor Planner"));
        assert!(user.contains("\"type\":\"SKELETON_CHOICE\""));
        assert!(user.contains("\"type\":\"STAGE_CHOICE\""));
        assert!(!user.contains("\"type\":\"WORKFLOW\""));
    }

    #[test]
    fn test_document_intent_uses_reactor_prompt_when_document_family_exists() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig {
                reactor_enabled: true,
                reactor_default_derivation_policy: DerivationPolicy::Permissive,
                ..LlmPlannerConfig::default()
            },
        );

        let actions = vec![
            ActionMeta::new("shell", "Run shell command")
                .with_capabilities(["shell", "filesystem_write", "fallback"])
                .with_roles(["inspect", "apply", "verify", "execute"]),
            ActionMeta::new("file_read", "Read file")
                .with_capabilities(["filesystem_read"])
                .with_roles(["inspect", "verify"])
                .with_output_kinds(["text"]),
            ActionMeta::new("file_write", "Write file")
                .with_capabilities(["filesystem_write", "side_effect"])
                .with_roles(["apply", "emit"])
                .with_input_kinds(["path", "text"])
                .with_output_kinds(["path"]),
            ActionMeta::new(
                "reactor_document_locate",
                "Locate document artifacts for the reactor document family",
            ),
            ActionMeta::new(
                "reactor_document_inspect",
                "Inspect document structure for the reactor document family",
            ),
            ActionMeta::new(
                "reactor_document_assess_readiness",
                "Assess whether document probe results are ready for commit",
            ),
            ActionMeta::new(
                "reactor_document_apply_patch",
                "Apply a typed document patch for the reactor document family",
            ),
            ActionMeta::new(
                "reactor_document_verify_patch",
                "Verify a document patch for the reactor document family",
            ),
        ];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("扫描 docs 下所有 markdown，补全 TODO 和缺失标题");
        let (system, user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("You are Orchestral Reactor Planner"));
        assert!(system.contains("artifact_family=document"));
        assert!(user.contains("\"artifact_family\":\"document\""));
    }

    #[test]
    fn test_structured_intent_uses_reactor_prompt_when_structured_family_exists() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig {
                reactor_enabled: true,
                reactor_default_derivation_policy: DerivationPolicy::Strict,
                ..LlmPlannerConfig::default()
            },
        );

        let actions = vec![
            ActionMeta::new("file_read", "Read file")
                .with_capabilities(["filesystem_read"])
                .with_roles(["inspect", "verify"])
                .with_output_kinds(["text"]),
            ActionMeta::new("file_write", "Write file")
                .with_capabilities(["filesystem_write", "side_effect"])
                .with_roles(["apply", "emit"])
                .with_input_kinds(["path", "text"])
                .with_output_kinds(["path"]),
            ActionMeta::new(
                "reactor_structured_locate",
                "Locate structured artifacts for the reactor structured family",
            ),
            ActionMeta::new(
                "reactor_structured_inspect",
                "Inspect structured artifact contents for the reactor structured family",
            ),
            ActionMeta::new(
                "reactor_structured_assess_readiness",
                "Assess whether structured probe results are ready for commit",
            ),
            ActionMeta::new(
                "reactor_structured_apply_patch",
                "Apply a typed structured patch for the reactor structured family",
            ),
            ActionMeta::new(
                "reactor_structured_verify_patch",
                "Verify a structured patch for the reactor structured family",
            ),
        ];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("修改 config/app.json 里的 service.port 和 owner");
        let (system, user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("artifact_family=structured"));
        assert!(user.contains("\"artifact_family\":\"structured\""));
        assert!(user.contains("\"derivation_policy\":\"strict\""));
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
