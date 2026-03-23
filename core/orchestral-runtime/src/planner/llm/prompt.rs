use std::collections::BTreeSet;
use std::fmt::Write;

use orchestral_core::action::ActionMeta;
use orchestral_core::planner::{HistoryItem, PlannerContext, PlannerLoopContext, SkillInstruction};
use orchestral_core::types::Intent;

use super::catalog::build_capability_catalog;

const PLANNER_CONSTITUTION: &str = include_str!("../../prompts/planner_constitution.md");
const OUTPUT_CONTRACT: &str = include_str!("../../prompts/output_contract.md");
const PLANNER_EXECUTION_RULES: &str = include_str!("../../prompts/planner_execution_rules.md");
const PLANNER_USER_RULES: &str = include_str!("../../prompts/planner_user_rules.md");
const EXAMPLE_SINGLE_ACTION_SHELL: &str =
    include_str!("../../prompts/examples/single_action_shell.json");
const EXAMPLE_SINGLE_ACTION_FILE_READ: &str =
    include_str!("../../prompts/examples/single_action_file_read.json");
const EXAMPLE_SINGLE_ACTION_FILE_WRITE: &str =
    include_str!("../../prompts/examples/single_action_file_write.json");
const EXAMPLE_SINGLE_ACTION_HTTP: &str =
    include_str!("../../prompts/examples/single_action_http.json");
const EXAMPLE_MINI_PLAN: &str = include_str!("../../prompts/examples/mini_plan.json");
const EXAMPLE_DONE: &str = include_str!("../../prompts/examples/done.json");
const EXAMPLE_NEED_INPUT: &str = include_str!("../../prompts/examples/need_input.json");
const ACTION_SELECTOR_RULES: &str = r#"You are the Orchestral Action Selector.
Choose the smallest safe action subset for the next planner iteration.
Return JSON only with this shape:
{"selected_actions":["file_read"],"blocked_actions":["shell"],"reason":"typed file actions are sufficient"}

Rules:
- Only choose from the listed action names.
- Prefer typed and narrow actions over generic shell commands when both can solve the current step.
- Prefer staying within one typed artifact category when that category already provides collect/inspect/derive/apply/verify coverage.
- Verify coverage from the listed actions and schema contracts; do not assume missing stages exist.
- When a schema field lists allowed enum values, use only those enum values.
- Do not mix derive/build actions from one typed category with apply/verify actions from another unless their `patch_spec` contracts explicitly match.
- Do not select `file_read` for binary artifacts such as `.xlsx`, `.xlsm`, `.docx`, or `.pdf`.
- Keep enough actions for the planner to inspect, apply, and verify in the same iteration if needed.
- Select no more than the requested maximum number of actions.
- Use blocked_actions for actions that should stay out of the planner prompt for this iteration."#;

#[derive(Debug, Clone, Copy, Default)]
struct SingleActionCoverage {
    shell: bool,
    file_read: bool,
    file_write: bool,
    http: bool,
}

pub(super) fn build_planner_prompt(
    base: &str,
    intent: &Intent,
    context: &PlannerContext,
    max_history: usize,
) -> (String, String) {
    let single_action_coverage = detect_single_action_coverage(context);
    let system = build_planner_system_prompt(base, context);
    let mut user = String::new();
    user.push_str(&format!("Intent:\n{}\n\n", intent.content));

    if !context.history.is_empty() {
        user.push_str("History:\n");
        for item in select_history_for_prompt(&context.history, max_history) {
            user.push_str(&format!("- {}: {}\n", item.role, item.content));
        }
        user.push('\n');
    }

    if let Some(loop_context) = &context.loop_context {
        let block = build_loop_context_block(loop_context);
        if !block.is_empty() {
            user.push_str(&block);
            user.push('\n');
        }
    }

    user.push_str(OUTPUT_CONTRACT);
    user.push_str("\n\nExamples:\n");
    append_single_action_examples(&mut user, single_action_coverage);
    append_example_line(&mut user, EXAMPLE_MINI_PLAN);
    append_example_line(&mut user, EXAMPLE_DONE);
    append_example_line(&mut user, EXAMPLE_NEED_INPUT);
    append_rule_block(&mut user, PLANNER_USER_RULES);
    append_rule_lines(&mut user, PLANNER_EXECUTION_RULES);

    (system, user)
}

pub(super) fn build_action_selector_prompt(
    base: &str,
    intent: &Intent,
    context: &PlannerContext,
    max_history: usize,
    max_selected_actions: usize,
) -> (String, String) {
    let system = build_action_selector_system_prompt(base, context);
    let mut user = String::new();
    let _ = writeln!(
        user,
        "Select the smallest safe action subset for the next planner iteration."
    );
    let _ = writeln!(user, "Maximum selected actions: {}", max_selected_actions);
    let _ = writeln!(user, "\nIntent:\n{}\n", intent.content);

    if !context.history.is_empty() {
        user.push_str("History:\n");
        for item in select_history_for_prompt(&context.history, max_history) {
            user.push_str(&format!("- {}: {}\n", item.role, item.content));
        }
        user.push('\n');
    }

    if let Some(loop_context) = &context.loop_context {
        let block = build_loop_context_block(loop_context);
        if !block.is_empty() {
            user.push_str(&block);
            user.push('\n');
        }
    }

    let action_catalog = build_action_selector_catalog(&context.available_actions);
    if !action_catalog.is_empty() {
        user.push_str("Available Actions:\n");
        user.push_str(&action_catalog);
    }
    user.push_str("\nReturn JSON only.\n");

    (system, user)
}

pub(super) fn build_planner_system_prompt(base: &str, context: &PlannerContext) -> String {
    let execution_environment = build_execution_environment_block(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let mut out = String::new();
    if !base.trim().is_empty() {
        out.push_str(base.trim());
        out.push_str("\n\n");
    }
    out.push_str(PLANNER_CONSTITUTION);
    let capability_catalog = build_capability_catalog(&context.available_actions);
    if !capability_catalog.trim().is_empty() {
        out.push('\n');
        out.push_str(&capability_catalog);
    }
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

fn build_action_selector_system_prompt(base: &str, context: &PlannerContext) -> String {
    let execution_environment = build_execution_environment_block(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let mut out = String::new();
    if !base.trim().is_empty() {
        out.push_str(base.trim());
        out.push_str("\n\n");
    }
    out.push_str(ACTION_SELECTOR_RULES.trim());
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

pub(super) fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn detect_single_action_coverage(context: &PlannerContext) -> SingleActionCoverage {
    SingleActionCoverage {
        shell: has_action(context, "shell"),
        file_read: has_action(context, "file_read"),
        file_write: has_action(context, "file_write"),
        http: has_action(context, "http"),
    }
}

fn has_action(context: &PlannerContext, name: &str) -> bool {
    context
        .available_actions
        .iter()
        .any(|action| action.name == name)
}

fn append_single_action_examples(buf: &mut String, coverage: SingleActionCoverage) {
    if coverage.shell {
        append_example_line(buf, EXAMPLE_SINGLE_ACTION_SHELL);
    }
    if coverage.file_read {
        append_example_line(buf, EXAMPLE_SINGLE_ACTION_FILE_READ);
    }
    if coverage.file_write {
        append_example_line(buf, EXAMPLE_SINGLE_ACTION_FILE_WRITE);
    }
    if coverage.http {
        append_example_line(buf, EXAMPLE_SINGLE_ACTION_HTTP);
    }
}

fn append_rule_block(buf: &mut String, block: &str) {
    buf.push('\n');
    buf.push_str(block.trim());
    buf.push('\n');
}

fn append_rule_lines(buf: &mut String, block: &str) {
    for line in block.lines().map(str::trim).filter(|line| !line.is_empty()) {
        buf.push_str("- ");
        buf.push_str(line);
        buf.push('\n');
    }
}

fn append_example_line(buf: &mut String, example: &str) {
    buf.push_str(example.trim());
    buf.push('\n');
}

fn build_action_selector_catalog(actions: &[ActionMeta]) -> String {
    let mut out = String::new();
    for action in actions {
        let _ = writeln!(out, "- {}: {}", action.name, action.description);
        if let Some(category) = action.category.as_deref() {
            let _ = writeln!(out, "  category: {}", category);
        }
        if !action.capabilities.is_empty() {
            let _ = writeln!(out, "  capabilities: {}", action.capabilities.join(", "));
        }
        if !action.input_kinds.is_empty() {
            let _ = writeln!(out, "  consumes: {}", action.input_kinds.join(", "));
        }
        if !action.output_kinds.is_empty() {
            let _ = writeln!(out, "  produces: {}", action.output_kinds.join(", "));
        }

        let input_fields = summarize_schema_fields(&action.input_schema);
        if !input_fields.is_empty() {
            let _ = writeln!(out, "  input_fields: {}", input_fields.join(", "));
        }

        let output_fields = summarize_schema_fields(&action.output_schema);
        if !output_fields.is_empty() {
            let _ = writeln!(out, "  output_fields: {}", output_fields.join(", "));
        }
    }
    out
}

fn summarize_schema_fields(schema: &serde_json::Value) -> Vec<String> {
    let properties = match schema.get("properties").and_then(|value| value.as_object()) {
        Some(properties) => properties,
        None => return Vec::new(),
    };
    let required = schema_required_fields(schema);
    let mut names = properties.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            summarize_schema_field(&name, &properties[&name], required.contains(name.as_str()))
        })
        .collect()
}

fn summarize_schema_field(name: &str, schema: &serde_json::Value, required: bool) -> String {
    let mut label = if required {
        format!("{} (required)", name)
    } else {
        name.to_string()
    };
    let mut hints = Vec::new();

    if let Some(values) = schema.get("enum").and_then(|value| value.as_array()) {
        let items = values
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        if !items.is_empty() {
            hints.push(format!("enum: {}", items.join(" | ")));
        }
    }

    if let Some(shape) = summarize_object_shape(schema) {
        hints.push(format!("shape: {}", shape));
    }

    if let Some(description) = schema.get("description").and_then(|value| value.as_str()) {
        if description.to_ascii_lowercase().contains("utf-8") {
            hints.push("UTF-8 text".to_string());
        }
    }

    if !hints.is_empty() {
        label.push_str(&format!(" [{}]", hints.join("; ")));
    }
    label
}

fn summarize_object_shape(schema: &serde_json::Value) -> Option<String> {
    let properties = schema
        .get("properties")
        .and_then(|value| value.as_object())?;
    if properties.is_empty() {
        return None;
    }
    let required = schema_required_fields(schema);
    let mut names = properties.keys().cloned().collect::<Vec<_>>();
    names.sort();
    if names.len() > 4 {
        names.truncate(4);
        names.push("...".to_string());
    }
    let summary = names
        .into_iter()
        .map(|name| {
            if required.contains(name.as_str()) {
                format!("{}*", name)
            } else {
                name
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("{{{}}}", summary))
}

fn schema_required_fields(schema: &serde_json::Value) -> BTreeSet<&str> {
    schema
        .get("required")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str())
        .collect()
}

fn build_loop_context_block(loop_context: &PlannerLoopContext) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Observed Execution State:\n- iteration: {}/{}",
        loop_context.iteration, loop_context.max_iterations
    );
    if !loop_context.completed_step_ids.is_empty() {
        let _ = writeln!(
            out,
            "- completed_step_ids: {}",
            loop_context.completed_step_ids.join(", ")
        );
    }
    if !loop_context.available_bindings.is_empty() {
        out.push_str("Available Bindings:\n");
        for binding in &loop_context.available_bindings {
            let _ = writeln!(out, "- {}", binding);
        }
    }
    if !loop_context.binding_shapes.is_empty() {
        out.push_str("Binding Shapes:\n");
        for shape in &loop_context.binding_shapes {
            let _ = writeln!(out, "- {}", shape);
        }
    }
    if !loop_context.recent_observations.is_empty() {
        out.push_str("Recent Observations:\n");
        for item in &loop_context.recent_observations {
            let _ = writeln!(out, "- {}", item);
        }
    }
    if let Some(preview) = &loop_context.working_set_preview {
        if !preview.trim().is_empty() {
            out.push_str("Working Set Snapshot:\n");
            out.push_str(preview.trim());
            out.push('\n');
        }
    }
    out
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

fn build_skill_knowledge_block(skills: &[SkillInstruction]) -> String {
    if skills.is_empty() {
        return String::new();
    }

    fn compact_skill_lines(instructions: &str) -> Vec<String> {
        let mut selected = Vec::new();
        for line in instructions
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            if line.starts_with("summary:")
                || line.starts_with("scripts:")
                || (selected.is_empty() && !line.starts_with("keywords:") && !line.starts_with('-'))
            {
                selected.push(line.to_string());
            }
            if selected.len() >= 2 {
                break;
            }
        }
        selected
    }

    let mut out = String::new();
    out.push_str("Activated Skills:\n");
    out.push_str(
        "Treat skills as optional execution hints. Prefer registered actions when they already satisfy the task. Only execute scripts that are listed under scripts discovered or verified at runtime; never invent script filenames.\n",
    );
    for skill in skills {
        let _ = writeln!(out, "- {}", skill.skill_name);
        for line in compact_skill_lines(&skill.instructions) {
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

#[cfg(test)]
mod tests {
    use orchestral_core::action::ActionMeta;
    use orchestral_core::planner::{PlannerContext, PlannerLoopContext};
    use serde_json::json;

    use super::*;

    fn planner_context(actions: Vec<ActionMeta>) -> PlannerContext {
        PlannerContext::new(actions)
    }

    #[test]
    fn test_planner_prompt_uses_template_rules_and_examples() {
        let context = planner_context(vec![
            ActionMeta::new("shell", "shell"),
            ActionMeta::new("file_read", "read"),
        ]);

        let (system, user) = build_planner_prompt("", &Intent::new("列出 docs 文件"), &context, 2);

        assert!(system.contains(PLANNER_CONSTITUTION.trim()));
        assert!(user.contains(PLANNER_USER_RULES.trim()));
        assert!(user.contains(EXAMPLE_SINGLE_ACTION_SHELL.trim()));
        assert!(user.contains(EXAMPLE_SINGLE_ACTION_FILE_READ.trim()));
        assert!(user.contains(EXAMPLE_MINI_PLAN.trim()));
        assert!(user.contains(EXAMPLE_DONE.trim()));
        assert!(user.contains(EXAMPLE_NEED_INPUT.trim()));
    }

    #[test]
    fn test_planner_prompt_includes_observed_execution_state() {
        let context = planner_context(vec![ActionMeta::new("shell", "shell")]).with_loop_context(
            PlannerLoopContext {
                iteration: 2,
                max_iterations: 6,
                recent_observations: vec!["iteration 1 completed shell and captured stdout".into()],
                completed_step_ids: vec!["single_action".into()],
                available_bindings: vec!["{{single_action.stdout}}".into()],
                binding_shapes: vec!["{{single_action.stdout}} -> string".into()],
                working_set_preview: Some("  single_action.stdout: \"README.md\"".into()),
            },
        );

        let (_system, user) =
            build_planner_prompt("", &Intent::new("继续检查结果然后回复"), &context, 2);

        assert!(user.contains("Observed Execution State:"));
        assert!(user.contains("iteration: 2/6"));
        assert!(user.contains("Available Bindings:"));
        assert!(user.contains("{{single_action.stdout}}"));
        assert!(user.contains("Binding Shapes:"));
        assert!(user.contains("{{single_action.stdout}} -> string"));
        assert!(user.contains("iteration 1 completed shell and captured stdout"));
        assert!(user.contains("single_action.stdout"));
    }

    #[test]
    fn test_action_selector_prompt_includes_action_metadata() {
        let context = planner_context(vec![
            ActionMeta::new("file_write", "Write markdown to file")
                .with_category("document")
                .with_capabilities(["filesystem_write", "side_effect"])
                .with_input_kinds(["path", "text"])
                .with_output_kinds(["path"])
                .with_input_schema(json!({
                    "type":"object",
                    "properties":{
                        "content":{"type":"string","description":"File content as UTF-8 text."},
                        "path":{"type":"string"}
                    },
                    "required":["path","content"]
                }))
                .with_output_schema(json!({
                    "type":"object",
                    "properties":{
                        "bytes":{"type":"integer"},
                        "path":{"type":"string"},
                        "mode":{"type":"string","enum":["strict","permissive"]}
                    }
                })),
            ActionMeta::new("shell", "Run a shell command")
                .with_category("direct")
                .with_capabilities(["filesystem_read", "shell"]),
        ])
        .with_loop_context(PlannerLoopContext {
            iteration: 3,
            max_iterations: 6,
            recent_observations: vec!["iteration 2 produced markdown diagnostics".into()],
            completed_step_ids: vec!["inspect_docs".into()],
            available_bindings: vec!["{{diagnostics.count}}".into()],
            binding_shapes: vec!["{{diagnostics.count}} -> number".into()],
            working_set_preview: Some("  diagnostics.count: 4".into()),
        });

        let (system, user) = build_action_selector_prompt(
            "Base policy.",
            &Intent::new("补齐 markdown 标题"),
            &context,
            3,
            4,
        );

        assert!(system.contains("Base policy."));
        assert!(system.contains("Orchestral Action Selector."));
        assert!(user.contains("Maximum selected actions: 4"));
        assert!(user.contains("Observed Execution State:"));
        assert!(user.contains("- file_write: Write markdown to file"));
        assert!(user.contains("category: document"));
        assert!(user.contains("capabilities: filesystem_write, side_effect"));
        assert!(user.contains("consumes: path, text"));
        assert!(user.contains("produces: path"));
        assert!(user.contains("input_fields: content (required) [UTF-8 text], path (required)"));
        assert!(user.contains("output_fields: bytes, mode [enum: strict | permissive], path"));
        assert!(user.contains("Available Bindings:"));
        assert!(user.contains("Binding Shapes:"));
        assert!(user.contains("{{diagnostics.count}}"));
    }
}
