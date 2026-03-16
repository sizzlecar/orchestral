use std::fmt::Write;

use orchestral_core::planner::{HistoryItem, PlannerContext, SkillInstruction};
use orchestral_core::types::{DerivationPolicy, Intent};

use super::catalog::build_capability_catalog;

const REACTOR_CONSTITUTION: &str = include_str!("../../prompts/reactor_constitution.md");
const NON_REACTOR_CONSTITUTION: &str = include_str!("../../prompts/non_reactor_constitution.md");
const OUTPUT_CONTRACT: &str = include_str!("../../prompts/output_contract.md");
const ACTION_CALL_RULES: &str = include_str!("../../prompts/action_call_rules.md");
const REACTOR_SKELETON_VOCABULARY: &str =
    include_str!("../../prompts/reactor_skeleton_vocabulary.md");
const REACTOR_HARD_RULES: &str = include_str!("../../prompts/reactor_hard_rules.md");
const REACTOR_USER_RULES: &str = include_str!("../../prompts/reactor_user_rules.md");
const NON_REACTOR_USER_RULES: &str = include_str!("../../prompts/non_reactor_user_rules.md");
const EXAMPLE_REACTOR_SPREADSHEET: &str =
    include_str!("../../prompts/examples/reactor_spreadsheet.jsonl");
const EXAMPLE_REACTOR_DOCUMENT: &str =
    include_str!("../../prompts/examples/reactor_document.jsonl");
const EXAMPLE_REACTOR_STRUCTURED: &str =
    include_str!("../../prompts/examples/reactor_structured.jsonl");
const EXAMPLE_REACTOR_RUN_AND_VERIFY_CODEBASE: &str =
    include_str!("../../prompts/examples/reactor_run_and_verify_codebase.jsonl");
const EXAMPLE_ACTION_CALL_SHELL: &str =
    include_str!("../../prompts/examples/action_call_shell.json");
const EXAMPLE_ACTION_CALL_FILE_READ: &str =
    include_str!("../../prompts/examples/action_call_file_read.json");
const EXAMPLE_ACTION_CALL_HTTP: &str = include_str!("../../prompts/examples/action_call_http.json");
const EXAMPLE_DIRECT_RESPONSE: &str = include_str!("../../prompts/examples/direct_response.json");
const EXAMPLE_CLARIFICATION: &str = include_str!("../../prompts/examples/clarification.json");

#[derive(Debug, Clone, Copy, Default)]
struct ReactorPromptCoverage {
    spreadsheet: bool,
    document: bool,
    structured: bool,
    codebase: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct DirectActionCoverage {
    shell: bool,
    file_read: bool,
    http: bool,
}

pub(super) fn build_reactor_prompt(
    base: &str,
    intent: &Intent,
    context: &PlannerContext,
    max_history: usize,
    default_policy: DerivationPolicy,
) -> (String, String) {
    let coverage = detect_reactor_prompt_coverage(context);
    let direct_actions = detect_direct_action_coverage(context);
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

    user.push_str(OUTPUT_CONTRACT);
    user.push_str("\n\nExamples:\n");
    append_reactor_choice_examples(&mut user, coverage);
    append_action_call_examples(&mut user, direct_actions);
    append_example_line(&mut user, EXAMPLE_DIRECT_RESPONSE);
    append_example_line(&mut user, EXAMPLE_CLARIFICATION);
    append_rule_block(&mut user, REACTOR_USER_RULES);
    user.push_str(&format!(
        "- Default derivation_policy is \"{}\" unless the task clearly needs otherwise.\n",
        default_policy
    ));
    append_rule_lines(&mut user, ACTION_CALL_RULES);

    (system, user)
}

pub(super) fn build_non_reactor_prompt(
    base: &str,
    intent: &Intent,
    context: &PlannerContext,
    max_history: usize,
) -> (String, String) {
    let direct_actions = detect_direct_action_coverage(context);
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

    user.push_str(OUTPUT_CONTRACT);
    user.push_str("\n\nExamples:\n");
    append_action_call_examples(&mut user, direct_actions);
    append_example_line(&mut user, EXAMPLE_DIRECT_RESPONSE);
    append_example_line(&mut user, EXAMPLE_CLARIFICATION);
    append_rule_block(&mut user, NON_REACTOR_USER_RULES);
    append_rule_lines(&mut user, ACTION_CALL_RULES);

    (system, user)
}

pub(super) fn build_non_reactor_system_prompt(base: &str, context: &PlannerContext) -> String {
    let execution_environment = build_execution_environment_block(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let mut out = String::new();
    if !base.trim().is_empty() {
        out.push_str(base.trim());
        out.push_str("\n\n");
    }
    out.push_str(NON_REACTOR_CONSTITUTION);
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

pub(super) fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
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
        codebase: has_reactor_actions(
            context,
            &[
                "reactor_codebase_collect_targets",
                "reactor_codebase_collect_results",
                "reactor_codebase_aggregate_verify",
                "reactor_codebase_export_summary",
            ],
        ) && has_reactor_actions(
            context,
            &[
                "reactor_document_inspect",
                "reactor_spreadsheet_inspect",
                "reactor_spreadsheet_apply_patch",
                "reactor_spreadsheet_verify_patch",
                "reactor_structured_inspect",
                "reactor_structured_apply_patch",
                "reactor_structured_verify_patch",
            ],
        ),
    }
}

fn detect_direct_action_coverage(context: &PlannerContext) -> DirectActionCoverage {
    DirectActionCoverage {
        shell: has_action(context, "shell"),
        file_read: has_action(context, "file_read"),
        http: has_action(context, "http"),
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

fn has_action(context: &PlannerContext, name: &str) -> bool {
    context
        .available_actions
        .iter()
        .any(|action| action.name == name)
}

fn append_reactor_choice_examples(buf: &mut String, coverage: ReactorPromptCoverage) {
    if coverage.spreadsheet {
        append_example_lines(buf, EXAMPLE_REACTOR_SPREADSHEET);
    }
    if coverage.document {
        append_example_lines(buf, EXAMPLE_REACTOR_DOCUMENT);
    }
    if coverage.structured {
        append_example_lines(buf, EXAMPLE_REACTOR_STRUCTURED);
    }
    if coverage.codebase {
        append_example_line(buf, EXAMPLE_REACTOR_RUN_AND_VERIFY_CODEBASE);
    }
}

fn append_action_call_examples(buf: &mut String, coverage: DirectActionCoverage) {
    if coverage.shell {
        append_example_line(buf, EXAMPLE_ACTION_CALL_SHELL);
    }
    if coverage.file_read {
        append_example_line(buf, EXAMPLE_ACTION_CALL_FILE_READ);
    }
    if coverage.http {
        append_example_line(buf, EXAMPLE_ACTION_CALL_HTTP);
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

fn append_example_lines(buf: &mut String, examples: &str) {
    for line in examples
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        buf.push_str(line);
        buf.push('\n');
    }
}

fn append_reactor_coverage(out: &mut String, artifact_family: &str) {
    let _ = writeln!(
        out,
        "- skeleton=locate_and_patch, artifact_family={}",
        artifact_family
    );
    out.push_str("  covered stage path: locate -> probe -> derive -> assess -> commit -> verify\n");
    out.push_str("  probe must end with explicit continuation\n");
    out.push_str("  verify is the done gate\n");
    let _ = writeln!(
        out,
        "- skeleton=inspect_and_extract, artifact_family={}",
        artifact_family
    );
    out.push_str("  covered stage path: probe -> derive -> export -> verify\n");
    out.push_str("  verify is the done gate\n");
}

fn append_run_and_verify_codebase_coverage(out: &mut String) {
    out.push_str("- skeleton=run_and_verify, artifact_family=codebase\n");
    out.push_str("  covered stage path: prepare -> run -> collect -> verify -> export\n");
    out.push_str("  prepare resolves explicit mixed-artifact targets\n");
    out.push_str("  verify is the done gate\n");
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

fn build_reactor_system_prompt(
    base: &str,
    context: &PlannerContext,
    default_policy: DerivationPolicy,
    coverage: ReactorPromptCoverage,
) -> String {
    let execution_environment = build_execution_environment_block(context);
    let skill_knowledge = build_skill_knowledge_block(&context.skill_instructions);
    let capability_catalog = build_capability_catalog(&context.available_actions);
    let mut out = String::new();
    if !base.trim().is_empty() {
        out.push_str(base.trim());
        out.push_str("\n\n");
    }

    out.push_str(REACTOR_CONSTITUTION);

    out.push('\n');
    out.push_str(REACTOR_SKELETON_VOCABULARY);

    out.push_str("\nCurrent executable coverage:\n");
    if coverage.spreadsheet {
        append_reactor_coverage(&mut out, "spreadsheet");
    }
    if coverage.document {
        append_reactor_coverage(&mut out, "document");
    }
    if coverage.structured {
        append_reactor_coverage(&mut out, "structured");
    }
    if coverage.codebase {
        append_run_and_verify_codebase_coverage(&mut out);
    }
    if !coverage.spreadsheet && !coverage.document && !coverage.structured && !coverage.codebase {
        out.push_str("- no executable reactor family coverage detected for this request\n");
    }
    out.push_str("- default derivation_policy: ");
    out.push_str(match default_policy {
        DerivationPolicy::Strict => "strict",
        DerivationPolicy::Permissive => "permissive",
    });
    out.push('\n');

    out.push('\n');
    out.push_str(REACTOR_HARD_RULES);
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
    use std::sync::Arc;

    use orchestral_core::action::ActionMeta;
    use orchestral_core::planner::PlannerContext;
    use orchestral_core::store::InMemoryReferenceStore;

    use super::*;

    fn planner_context(actions: Vec<ActionMeta>) -> PlannerContext {
        PlannerContext::new(actions, Arc::new(InMemoryReferenceStore::new()))
    }

    #[test]
    fn test_reactor_prompt_uses_template_rules_and_examples() {
        let context = planner_context(vec![
            ActionMeta::new("reactor_document_locate", "locate"),
            ActionMeta::new("reactor_document_inspect", "inspect"),
            ActionMeta::new("reactor_document_assess_readiness", "assess"),
            ActionMeta::new("reactor_document_apply_patch", "apply"),
            ActionMeta::new("reactor_document_verify_patch", "verify"),
            ActionMeta::new("file_read", "read"),
        ]);

        let (system, user) = build_reactor_prompt(
            "",
            &Intent::new("docs 下面有什么文件"),
            &context,
            4,
            DerivationPolicy::Permissive,
        );

        assert!(system.contains(REACTOR_CONSTITUTION.trim()));
        assert!(system.contains(REACTOR_SKELETON_VOCABULARY.trim()));
        assert!(system.contains(REACTOR_HARD_RULES.trim()));
        assert!(system.contains("artifact_family=document"));
        assert!(user.contains(REACTOR_USER_RULES.trim()));
        assert!(user.contains(EXAMPLE_REACTOR_DOCUMENT.lines().next().unwrap_or_default()));
        assert!(user.contains(EXAMPLE_DIRECT_RESPONSE.trim()));
        assert!(user.contains(EXAMPLE_CLARIFICATION.trim()));
        assert!(user.contains(ACTION_CALL_RULES.lines().next().unwrap_or_default()));
    }

    #[test]
    fn test_non_reactor_prompt_uses_template_rules_and_examples() {
        let context = planner_context(vec![
            ActionMeta::new("shell", "shell"),
            ActionMeta::new("file_read", "read"),
        ]);

        let (system, user) =
            build_non_reactor_prompt("", &Intent::new("列出 docs 文件"), &context, 2);

        assert!(system.contains(NON_REACTOR_CONSTITUTION.trim()));
        assert!(user.contains(NON_REACTOR_USER_RULES.trim()));
        assert!(user.contains(EXAMPLE_ACTION_CALL_SHELL.trim()));
        assert!(user.contains(EXAMPLE_ACTION_CALL_FILE_READ.trim()));
        assert!(user.contains(EXAMPLE_DIRECT_RESPONSE.trim()));
        assert!(user.contains(EXAMPLE_CLARIFICATION.trim()));
    }
}
