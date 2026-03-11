use std::collections::HashMap;
use std::fmt::Write;

use serde_json::Value;
use tracing::debug;

use super::materialize::render_path_segments;
use super::types::{
    AgentDebugTextPayload, AgentEvidenceStore, AgentMode, AgentOutputRule, AgentStepParams,
};
use super::{
    ExecutorContext, SkillInstruction, ToolDefinition, MAX_AGENT_LOG_TEXT_CHARS,
    MAX_BOUND_INPUT_KEYS, MAX_BOUND_INPUT_VALUE_CHARS, MAX_BOUND_SKILL_VALUE_CHARS,
    MAX_PROMPT_OBSERVATION_CHARS, MAX_RECENT_OBSERVATIONS,
};

pub(super) fn build_agent_system_prompt(params: &AgentStepParams, ctx: &ExecutorContext) -> String {
    let mut actions: Vec<&str> = params.allowed_actions.iter().map(String::as_str).collect();
    actions.sort_unstable();
    let bound_inputs_block = build_bound_inputs_block(params);
    let output_rules_block = build_output_rules_block(&params.output_rules);
    let env_block = build_execution_environment_block(ctx);
    let skill_block = build_skill_knowledge_block(&ctx.skill_instructions);
    let skill_rules = build_skill_execution_rules(&ctx.skill_instructions);
    let mode_rules = build_agent_mode_rules(params);
    format!(
        "You are a constrained execution agent.\n\
Mode: {mode}\n\
Goal: {goal}\n\
Allowed actions: {actions}\n\
Required output keys: {output_keys}.\n\
{sources}{mode_rules}{env}{skill}{skill_rules}{bound}\n\
You have three tools:\n\
1) execute_action — run one of the allowed actions. Optional save_as stores the result in a named slot; capture=json_stdout parses stdout as JSON before storing. Example: execute_action({{\"action\":\"shell\",\"params\":{{\"command\":\"ls\"}},\"save_as\":\"listing\"}})\n\
2) finish — ask runtime to materialize required output keys from saved evidence. Prefer this when saved slots already contain the needed values. Example: finish({{}})\n\
3) return_final — completion signal when you believe runtime can now materialize outputs from evidence. Runtime does not trust explicit exports without evidence. Example: return_final({{\"exports\":{{{output_keys_example}}}}})\n\
\n\
Rules:\n\
- Act first, analyze minimally. Do not spend iterations only reading/analyzing.\n\
- Never call actions outside the allowed list.\n\
- json_stdout is a pure serializer, not a command runner. Never place shell commands, Python snippets, or pseudo-tool calls where final structured data is required.\n\
- Runtime always stores evidence slots: last, last_raw, last_action, action:<name>, action:<name>:raw. If stdout/body is JSON, runtime also stores action:<name>:stdout_json/body_json.\n\
- Save useful structured results with save_as so runtime can materialize outputs deterministically.\n\
- If output_rules specify requires.action, that slot evidence must be produced by the required action.\n\
- Action-named slots (for example slot 'file_write') are provenance-checked against that action.\n\
- If a command prints one JSON object to stdout, use capture=json_stdout.\n\
- Never use file_write with empty content as a marker step. If you only need to expose a path or metadata, save structured evidence with save_as and call finish.\n\
- Never invent script file names. Use script paths discovered in Activated Skills, or inspect the scripts directory first.\n\
- Missing file/command shell errors are treated as deterministic and may abort the step early.\n\
- When the required outputs can be derived from saved evidence, call finish immediately.\n\
- You MUST call finish or return_final before iterations run out.",
        mode = params.mode.as_str(),
        goal = params.goal,
        actions = actions.join(", "),
        output_keys = params.output_keys.join(", "),
        sources = output_rules_block,
        mode_rules = mode_rules,
        env = env_block,
        skill = skill_block,
        skill_rules = skill_rules,
        bound = bound_inputs_block,
        output_keys_example = params
            .output_keys
            .iter()
            .map(|k| format!("\"{}\":\"...\"", k))
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn build_agent_mode_rules(params: &AgentStepParams) -> String {
    match params.mode {
        AgentMode::Explore => String::new(),
        AgentMode::Leaf => {
            let result_slot = params.result_slot.as_deref().unwrap_or("leaf_result");
            let has_change_list_output = params.output_keys.iter().any(|key| {
                matches!(
                    key.to_ascii_lowercase().as_str(),
                    "fills" | "changes" | "patch" | "patches" | "edits" | "change_spec"
                )
            });
            let empty_change_rule = if has_change_list_output {
                "- If the required output is a change list (for example fills/changes/patches), do not return an empty list unless the bound inputs clearly prove no updates are needed.\n\
- Treat common placeholders such as N/A, NA, TBD, '-', '待填写', or similar sentinel values as missing data when the user asked to fill or complete an artifact.\n\
- In tables/forms, when one row has several adjacent placeholder cells under user-entered headers, infer all clearly required fields rather than filling only a single notes column.\n"
            } else {
                ""
            };
            format!(
                "Leaf Mode:\n\
- Solve exactly one local derivation problem; do not explore the filesystem, network, or external tools.\n\
- The only allowed action is json_stdout.\n\
- Emit one JSON object whose top-level keys are the required output keys.\n\
- Empty objects, missing required keys, or payloads that cannot satisfy output_rules are invalid and will be rejected.\n\
{empty_change_rule}\
- Do not emit commands, scripts, code, or tool requests. Emit the final structured answer only.\n\
- Call execute_action with action=json_stdout, capture=json_stdout, save_as={result_slot}.\n\
- After storing that structured payload, call finish immediately.\n",
                empty_change_rule = empty_change_rule,
            )
        }
    }
}

fn build_execution_environment_block(ctx: &ExecutorContext) -> String {
    let Some(info) = &ctx.runtime_info else {
        return String::new();
    };
    if info.os.is_empty() {
        return String::new();
    }
    let mut out = String::from("Execution Environment:\n");
    let _ = writeln!(out, "- os: {}", info.os);
    let _ = writeln!(out, "- os_family: {}", info.os_family);
    let _ = writeln!(out, "- arch: {}", info.arch);
    if let Some(shell) = &info.shell {
        let _ = writeln!(out, "- shell: {}", shell);
    }
    if let Some(python) = resolve_python_path(&ctx.skill_instructions) {
        let _ = writeln!(
            out,
            "- python: {} (preferred managed runtime when Python execution is needed)",
            python
        );
    }
    out
}

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

fn build_skill_knowledge_block(skills: &[SkillInstruction]) -> String {
    if skills.is_empty() {
        return String::new();
    }
    let mut out = String::from("Activated Skills:\n");
    out.push_str(
        "If the skill summary is insufficient for concrete execution details, read its skill file via file_read before acting.\n",
    );
    out.push_str(
        "Only execute scripts that are explicitly listed below or verified at runtime; do not guess script filenames.\n",
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

fn build_skill_execution_rules(skills: &[SkillInstruction]) -> String {
    if !skills_match_keywords(
        skills,
        &["xlsx", "excel", "spreadsheet", "workbook", "worksheet"],
    ) {
        return String::new();
    }

    String::from(
        "Skill-Specific Rules:\n\
- file_read is text-only; never call file_read on .xlsx, .xlsm, .pdf, .docx, .pptx, .zip, or image files.\n\
- For spreadsheet workbooks, inspect or modify the workbook with shell plus the provided Python/runtime or verified skill scripts.\n\
- For plain value fills or template completion, avoid recalc.py unless formulas were added/changed or dependent formula outputs must be refreshed during verification.\n\
- If a workbook must be changed, keep the reasoning local and save structured evidence, but perform the actual read/write/recalc steps with explicit actions.\n",
    )
}

fn skills_match_keywords(skills: &[SkillInstruction], keywords: &[&str]) -> bool {
    skills.iter().any(|skill| {
        let haystack = format!(
            "{} {}",
            skill.skill_name.to_ascii_lowercase(),
            skill.instructions.to_ascii_lowercase()
        );
        keywords.iter().any(|keyword| haystack.contains(keyword))
    })
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

pub(super) fn build_bound_inputs_block(params: &AgentStepParams) -> String {
    if params.bound_inputs.is_empty() {
        return "Bound upstream inputs: none".to_string();
    }

    let mut keys = params.bound_inputs.keys().cloned().collect::<Vec<_>>();
    keys.sort_unstable();
    if params.mode == AgentMode::Leaf {
        let non_skill_keys = keys
            .iter()
            .filter(|key| !key.to_ascii_lowercase().contains("skill"))
            .count();
        if non_skill_keys > 0 {
            keys.retain(|key| !key.to_ascii_lowercase().contains("skill"));
        }
    }
    if keys.is_empty() {
        return "Bound upstream inputs: none".to_string();
    }

    let mut lines = Vec::with_capacity(keys.len() + 2);
    lines.push("Bound upstream inputs (from io_bindings):".to_string());
    for key in keys.iter().take(MAX_BOUND_INPUT_KEYS) {
        if let Some(value) = params.bound_inputs.get(key) {
            let summarized = summarize_bound_input_value(key, value);
            if summarized.contains('\n') {
                lines.push(format!("- {}:", key));
                for line in summarized.lines() {
                    lines.push(format!("  {}", line));
                }
            } else {
                lines.push(format!("- {} = {}", key, summarized));
            }
        }
    }
    if keys.len() > MAX_BOUND_INPUT_KEYS {
        lines.push(format!(
            "- ... {} more bound input key(s) omitted",
            keys.len() - MAX_BOUND_INPUT_KEYS
        ));
    }
    if params.mode == AgentMode::Leaf
        && params
            .bound_inputs
            .keys()
            .any(|key| key.to_ascii_lowercase().contains("skill"))
    {
        lines.push(
            "- skill_* bound inputs omitted in leaf mode to reduce noise; use Activated Skills summary unless directly required."
                .to_string(),
        );
    }
    lines.push(
        "Treat bound upstream inputs as authoritative context; do not rediscover unless verification is necessary."
            .to_string(),
    );
    lines.join("\n")
}

fn build_output_rules_block(output_rules: &HashMap<String, AgentOutputRule>) -> String {
    if output_rules.is_empty() {
        return String::new();
    }

    let mut keys = output_rules.keys().cloned().collect::<Vec<_>>();
    keys.sort_unstable();
    let mut lines = Vec::with_capacity(keys.len() + 1);
    lines.push("Runtime output materialization rules:".to_string());
    for key in keys {
        let Some(rule) = output_rules.get(&key) else {
            continue;
        };
        let mut parts = Vec::new();
        for candidate in &rule.candidates {
            let mut repr = format!("slot '{}'", candidate.slot);
            if !candidate.path.is_empty() {
                repr.push_str(" path '");
                repr.push_str(&render_path_segments(&candidate.path));
                repr.push('\'');
            }
            if let Some(required_action) = &candidate.required_action {
                repr.push_str(" requires.action '");
                repr.push_str(required_action);
                repr.push('\'');
            }
            parts.push(repr);
        }
        if let Some(template) = &rule.template {
            parts.push(format!("template {:?}", truncate_text(template)));
        }
        if !rule.fallback_aliases.is_empty() {
            parts.push(format!("fallback_aliases={:?}", rule.fallback_aliases));
        }
        if let Some(required_action) = &rule.required_action {
            parts.push(format!("requires.action='{}'", required_action));
        }
        if parts.is_empty() {
            lines.push(format!("- {} <= <empty rule>", key));
        } else {
            lines.push(format!("- {} <= {}", key, parts.join(" | ")));
        }
    }
    lines.push(
        "Save evidence into the referenced slots and prefer finish once rules can materialize required outputs."
            .to_string(),
    );
    format!("{}\n", lines.join("\n"))
}

pub(super) fn summarize_bound_input_value(key: &str, value: &Value) -> String {
    let key_lower = key.to_ascii_lowercase();
    match value {
        Value::String(text) => {
            let normalized = if key_lower.contains("skill") {
                summarize_multiline_text(text, 120, "\n")
            } else {
                summarize_multiline_text(text, 4, " | ")
            };
            if key_lower.contains("skill") {
                truncate_with_limit(&normalized, MAX_BOUND_SKILL_VALUE_CHARS)
            } else {
                truncate_with_limit(&normalized, MAX_BOUND_INPUT_VALUE_CHARS)
            }
        }
        _ => truncate_with_limit(&value.to_string(), MAX_BOUND_INPUT_VALUE_CHARS),
    }
}

fn summarize_multiline_text(text: &str, max_lines: usize, separator: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let lines = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(max_lines)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        trimmed.to_string()
    } else {
        lines.join(separator)
    }
}

pub(super) fn build_agent_user_prompt(iteration: u64, observations: &[String]) -> String {
    if observations.is_empty() {
        return format!("iteration={} no prior observations", iteration);
    }
    let tail = observations
        .iter()
        .rev()
        .take(MAX_RECENT_OBSERVATIONS)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    format!(
        "iteration={}\nrecent_observations:\n- {}",
        iteration,
        tail.join("\n- ")
    )
}

pub(super) fn build_agent_finalization_system_prompt(
    params: &AgentStepParams,
    ctx: &ExecutorContext,
) -> String {
    let mut base = build_agent_system_prompt(params, ctx);
    base.push_str(
        "\n\nFinalization Mode:\n- Action iterations are exhausted.\n- execute_action is no longer available.\n- Prefer finish if runtime can materialize outputs from saved evidence.\n- Use return_final only when deterministic materialization is insufficient.\n",
    );
    base
}

pub(super) fn build_agent_finalization_user_prompt(
    observations: &[String],
    last_success_exports: Option<&HashMap<String, Value>>,
    evidence: &AgentEvidenceStore,
) -> String {
    let mut lines = vec![
        "All action iterations are exhausted. No more execute_action calls are allowed."
            .to_string(),
    ];

    if let Some(exports) = last_success_exports {
        let serialized = Value::Object(
            exports
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<serde_json::Map<_, _>>(),
        )
        .to_string();
        lines.push(format!(
            "last_success_exports={}",
            truncate_with_limit(&serialized, MAX_BOUND_INPUT_VALUE_CHARS)
        ));
    }

    let mut slot_names = evidence.slots.keys().cloned().collect::<Vec<_>>();
    slot_names.sort_unstable();
    if slot_names.is_empty() {
        lines.push("saved_slots: none".to_string());
    } else {
        lines.push(format!("saved_slots: {}", slot_names.join(", ")));
    }

    if observations.is_empty() {
        lines.push("recent_observations: none".to_string());
    } else {
        let tail = observations
            .iter()
            .rev()
            .take(MAX_RECENT_OBSERVATIONS)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        lines.push("recent_observations:".to_string());
        for item in tail {
            lines.push(format!("- {}", item));
        }
    }
    lines.push(
        "Call finish now if saved evidence is sufficient; otherwise call return_final.".to_string(),
    );
    lines.join("\n")
}

pub(super) fn agent_tool_definitions(params: &AgentStepParams) -> Vec<ToolDefinition> {
    let execute_action_parameters = if params.mode == AgentMode::Leaf {
        leaf_execute_action_schema(params)
    } else {
        serde_json::json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Action name from the allowed list"
                },
                "params": {
                    "type": "object",
                    "description": "Action parameters"
                },
                "save_as": {
                    "type": "string",
                    "description": "Optional evidence slot name for storing this action result"
                },
                "capture": {
                    "type": "string",
                    "description": "Optional capture mode. Use 'json_stdout' when stdout is exactly one JSON object."
                }
            },
            "required": ["action", "params"]
        })
    };
    vec![
        ToolDefinition {
            name: "execute_action".to_string(),
            description: "Execute one of the allowed actions".to_string(),
            parameters: execute_action_parameters,
        },
        ToolDefinition {
            name: "finish".to_string(),
            description:
                "Finish by asking runtime to materialize required output keys from saved evidence"
                    .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "return_final".to_string(),
            description: "Return final results when the goal is achieved".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "exports": {
                        "type": "object",
                        "description": "Key-value map of required output_keys"
                    }
                },
                "required": ["exports"]
            }),
        },
    ]
}

pub(super) fn leaf_execute_action_schema(params: &AgentStepParams) -> Value {
    let result_slot = params
        .result_slot
        .as_deref()
        .unwrap_or("leaf_result")
        .to_string();
    let payload_properties = params
        .output_keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                serde_json::json!({
                    "description": format!("Required structured value for output key '{}'.", key)
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["json_stdout"],
                "description": "Must be json_stdout in leaf mode."
            },
            "params": {
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "object",
                        "description": "Structured JSON object containing every required output key.",
                        "properties": payload_properties,
                        "required": params.output_keys,
                        "additionalProperties": true
                    }
                },
                "required": ["payload"]
            },
            "save_as": {
                "type": "string",
                "enum": [result_slot],
                "description": "Must save the structured payload into the leaf result slot."
            },
            "capture": {
                "type": "string",
                "enum": ["json_stdout"],
                "description": "Must capture the JSON payload from stdout."
            }
        },
        "required": ["action", "params", "save_as", "capture"]
    })
}

pub(super) fn agent_final_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "finish".to_string(),
            description:
                "Finish by asking runtime to materialize required output keys from saved evidence"
                    .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "return_final".to_string(),
            description: "Return final results when the goal is achieved".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "exports": {
                        "type": "object",
                        "description": "Key-value map of required output_keys"
                    }
                },
                "required": ["exports"]
            }),
        },
    ]
}

pub(super) fn truncate_text(input: &str) -> String {
    truncate_with_limit(input, MAX_PROMPT_OBSERVATION_CHARS)
}

pub(super) fn truncate_log_text(input: &str) -> String {
    let char_count = input.chars().count();
    if char_count <= MAX_AGENT_LOG_TEXT_CHARS {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(MAX_AGENT_LOG_TEXT_CHARS).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn truncate_with_limit(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

pub(super) fn summarize_recent_observations(observations: &[String], take_last: usize) -> String {
    if observations.is_empty() {
        return "none".to_string();
    }
    let tail = observations
        .iter()
        .rev()
        .take(take_last)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join(" | ");
    truncate_log_text(&tail)
}

pub(super) fn log_agent_debug_text_payloads(
    step_id: &str,
    iteration: u64,
    stage: &str,
    value: &Value,
) {
    let payloads = collect_agent_debug_text_payloads(value);
    if payloads.is_empty() {
        return;
    }
    for payload in payloads {
        debug!(
            step_id = %step_id,
            iteration = iteration,
            stage = stage,
            payload_path = %payload.path,
            payload_text = %payload.text,
            "agent text payload"
        );
    }
}

pub(super) fn collect_agent_debug_text_payloads(value: &Value) -> Vec<AgentDebugTextPayload> {
    let mut out = Vec::new();
    collect_agent_debug_text_payloads_inner("$", None, value, &mut out);
    out
}

fn collect_agent_debug_text_payloads_inner(
    path: &str,
    current_key: Option<&str>,
    value: &Value,
    out: &mut Vec<AgentDebugTextPayload>,
) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let child_path = format!("{}.{}", path, key);
                collect_agent_debug_text_payloads_inner(&child_path, Some(key), value, out);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let child_path = format!("{}[{}]", path, index);
                collect_agent_debug_text_payloads_inner(&child_path, current_key, item, out);
            }
        }
        Value::String(text) => {
            if should_log_agent_text_payload(current_key, text) {
                out.push(AgentDebugTextPayload {
                    path: path.to_string(),
                    text: text.clone(),
                });
            }
        }
        _ => {}
    }
}

fn should_log_agent_text_payload(current_key: Option<&str>, text: &str) -> bool {
    let Some(key) = current_key else {
        return false;
    };
    let normalized = key.trim().to_ascii_lowercase();
    let key_suggests_code = matches!(
        normalized.as_str(),
        "script" | "code" | "source" | "program" | "python" | "bash" | "sh" | "cmd"
    );
    key_suggests_code
        || ((text.contains('\n') || text.contains("\r\n")) && text.chars().count() >= 80)
}
