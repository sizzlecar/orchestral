use std::sync::OnceLock;

const TEMPLATE: &str = include_str!("system_prompts.template.txt");
const PLANNER_START: &str = "[[planner]]";
const PLANNER_END: &str = "[[/planner]]";
const INTERPRETER_START: &str = "[[interpreter]]";
const INTERPRETER_END: &str = "[[/interpreter]]";

struct PromptTemplates {
    planner: &'static str,
    interpreter: &'static str,
}

static PROMPT_TEMPLATES: OnceLock<PromptTemplates> = OnceLock::new();

fn templates() -> &'static PromptTemplates {
    PROMPT_TEMPLATES.get_or_init(|| {
        let planner = extract_section(TEMPLATE, PLANNER_START, PLANNER_END)
            .expect("missing [[planner]] section in system prompts template");
        let interpreter = extract_section(TEMPLATE, INTERPRETER_START, INTERPRETER_END)
            .expect("missing [[interpreter]] section in system prompts template");
        PromptTemplates {
            planner,
            interpreter,
        }
    })
}

fn extract_section<'a>(text: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let start_idx = text.find(start)?;
    let after_start = &text[start_idx + start.len()..];
    let end_idx = after_start.find(end)?;
    Some(after_start[..end_idx].trim())
}

fn render_template(template: &str, bindings: &[(&str, &str)]) -> String {
    let mut rendered = template.to_string();
    for (key, value) in bindings {
        let placeholder = format!("{{{{{}}}}}", key);
        rendered = rendered.replace(&placeholder, value);
    }
    rendered
}

pub fn render_planner_prompt(
    base_prompt: &str,
    execution_environment: &str,
    action_catalog: &str,
) -> String {
    render_template(
        templates().planner,
        &[
            ("BASE_PROMPT", base_prompt.trim()),
            ("EXECUTION_ENVIRONMENT", execution_environment.trim()),
            ("ACTION_CATALOG", action_catalog.trim()),
        ],
    )
}

pub fn default_interpreter_prompt() -> &'static str {
    templates().interpreter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_prompt_template_renders_dynamic_fields() {
        let rendered = render_planner_prompt("base", "env", "- name: shell");
        assert!(rendered.contains("base"));
        assert!(rendered.contains("env"));
        assert!(rendered.contains("- name: shell"));
        assert!(!rendered.contains("{{BASE_PROMPT}}"));
    }

    #[test]
    fn interpreter_prompt_template_is_available() {
        let prompt = default_interpreter_prompt();
        assert!(prompt.contains("response synthesizer"));
    }
}
