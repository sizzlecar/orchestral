mod http;
mod parsing;
mod prompt;

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tracing::{debug, info};

use orchestral_core::planner::{PlanError, Planner, PlannerContext, PlannerOutput};
use orchestral_core::types::{DerivationPolicy, Intent};

pub use self::http::{HttpLlmClient, HttpLlmClientConfig};
use self::parsing::{extract_json, parse_planner_output};
use self::prompt::{build_non_reactor_prompt, build_reactor_prompt, truncate_for_log};

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
