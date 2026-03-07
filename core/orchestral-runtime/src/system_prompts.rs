use std::sync::OnceLock;

const TEMPLATE: &str = include_str!("system_prompts.template.txt");
const PLANNER_START: &str = "[[planner]]";
const PLANNER_END: &str = "[[/planner]]";

static PLANNER_TEMPLATE: OnceLock<&'static str> = OnceLock::new();

fn planner_template() -> &'static str {
    PLANNER_TEMPLATE.get_or_init(|| {
        extract_section(TEMPLATE, PLANNER_START, PLANNER_END)
            .expect("missing [[planner]] section in system prompts template")
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
    skill_knowledge: &str,
    conditional_rules: &str,
) -> String {
    render_template(
        planner_template(),
        &[
            ("BASE_PROMPT", base_prompt.trim()),
            ("EXECUTION_ENVIRONMENT", execution_environment.trim()),
            ("SKILL_KNOWLEDGE", skill_knowledge.trim()),
            ("CONDITIONAL_RULES", conditional_rules.trim()),
            ("ACTION_CATALOG", action_catalog.trim()),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_prompt_template_renders_dynamic_fields() {
        let rendered = render_planner_prompt(
            "base",
            "env",
            "- name: shell",
            "skill block",
            "- only for shell",
        );
        assert!(rendered.contains("base"));
        assert!(rendered.contains("env"));
        assert!(rendered.contains("skill block"));
        assert!(rendered.contains("- only for shell"));
        assert!(rendered.contains("- name: shell"));
        assert!(!rendered.contains("{{BASE_PROMPT}}"));
    }
}
