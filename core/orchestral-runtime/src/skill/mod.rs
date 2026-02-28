pub mod discovery;

use orchestral_core::planner::SkillInstruction;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SkillEntry {
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub source_path: PathBuf,
    pub scripts_dir: Option<PathBuf>,
    /// Skill-local virtual-env python binary, auto-detected from `<skill_dir>/.venv/bin/python3`.
    pub venv_python: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SkillCatalog {
    entries: Vec<SkillEntry>,
    max_active: usize,
}

impl SkillCatalog {
    pub fn new(entries: Vec<SkillEntry>, max_active: usize) -> Self {
        Self {
            entries,
            max_active,
        }
    }

    pub fn entries(&self) -> &[SkillEntry] {
        &self.entries
    }

    pub fn match_intent(&self, intent: &str) -> Vec<&SkillEntry> {
        if self.max_active == 0 {
            return Vec::new();
        }

        let intent_tokens = tokenize(intent);
        let mut scored = self
            .entries
            .iter()
            .map(|entry| {
                let mut haystack = String::new();
                haystack.push_str(&entry.name.to_ascii_lowercase());
                haystack.push(' ');
                haystack.push_str(&entry.description.to_ascii_lowercase());

                let score = intent_tokens
                    .iter()
                    .filter(|token| token.len() > 2 && haystack.contains(token.as_str()))
                    .count();
                (score, entry)
            })
            .filter(|(score, _)| *score > 0)
            .collect::<Vec<_>>();

        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.name.cmp(&b.1.name)));
        scored
            .into_iter()
            .take(self.max_active)
            .map(|(_, entry)| entry)
            .collect()
    }

    pub fn build_instructions(&self, intent: &str) -> Vec<SkillInstruction> {
        self.match_intent(intent)
            .into_iter()
            .map(|entry| SkillInstruction {
                skill_name: entry.name.clone(),
                instructions: entry.description.clone(),
                skill_path: Some(entry.source_path.to_string_lossy().to_string()),
                scripts_dir: entry
                    .scripts_dir
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
                venv_python: entry
                    .venv_python
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
            })
            .collect()
    }
}

fn is_skill_explicitly_requested(intent: &str, skill_name: &str) -> bool {
    if intent.trim().is_empty() || skill_name.trim().is_empty() {
        return false;
    }
    intent
        .to_ascii_lowercase()
        .contains(&skill_name.to_ascii_lowercase())
}

fn summarize_skill_instructions(input: &str, max_lines: usize, max_chars: usize) -> String {
    let text = input.trim();
    if text.is_empty() {
        return String::new();
    }
    let mut summary_lines = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(max_lines)
        .collect::<Vec<_>>()
        .join("\n");

    let char_count = summary_lines.chars().count();
    if char_count > max_chars {
        summary_lines = summary_lines.chars().take(max_chars).collect::<String>();
        summary_lines.push_str("... [skill summary truncated]");
    }
    summary_lines
}

fn tokenize(input: &str) -> Vec<String> {
    input
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_skill(name: &str, description: &str) -> SkillEntry {
        SkillEntry {
            name: name.to_string(),
            description: description.to_string(),
            instructions: format!("instructions for {name}"),
            source_path: PathBuf::from(format!("/tmp/{name}/SKILL.md")),
            scripts_dir: None,
            venv_python: None,
        }
    }

    #[test]
    fn test_skill_catalog_match_intent() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("xlsx", "xlsx creation and formula recalc"),
                make_skill("git", "git commit and branch operations"),
            ],
            3,
        );

        let matched = catalog.match_intent("please recalc xlsx formulas");
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].name, "xlsx");
    }

    #[test]
    fn test_skill_catalog_max_active() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("a", "demo skill one"),
                make_skill("b", "demo skill two"),
                make_skill("c", "demo skill three"),
            ],
            2,
        );

        let matched = catalog.match_intent("demo skill");
        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn test_skill_catalog_no_match() {
        let catalog = SkillCatalog::new(vec![make_skill("xlsx", "spreadsheet skill")], 3);

        let matched = catalog.match_intent("deploy kubernetes");
        assert!(matched.is_empty());
    }

    #[test]
    fn test_summarize_skill_instructions_limits_lines() {
        let input = (1..=12)
            .map(|i| format!("line-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let summary = summarize_skill_instructions(&input, 10, 10_000);
        let lines = summary.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 10);
        assert_eq!(lines[0], "line-1");
        assert_eq!(lines[9], "line-10");
    }

    #[test]
    fn test_build_instructions_returns_description_and_path_only() {
        let long = "x".repeat(3_000);
        let catalog = SkillCatalog::new(
            vec![SkillEntry {
                name: "xlsx".to_string(),
                description: "spreadsheet skill for formulas and formatting".to_string(),
                instructions: long,
                source_path: PathBuf::from("/tmp/xlsx/SKILL.md"),
                scripts_dir: Some(PathBuf::from("/tmp/xlsx/scripts")),
                venv_python: Some(PathBuf::from("/tmp/xlsx/.venv/bin/python3")),
            }],
            3,
        );

        let instructions = catalog.build_instructions("please help with spreadsheet data");
        assert_eq!(instructions.len(), 1);
        let first = &instructions[0];
        assert_eq!(first.skill_name, "xlsx");
        assert_eq!(
            first.instructions,
            "spreadsheet skill for formulas and formatting"
        );
        assert_eq!(first.skill_path.as_deref(), Some("/tmp/xlsx/SKILL.md"));
        assert_eq!(first.scripts_dir.as_deref(), Some("/tmp/xlsx/scripts"));
        assert_eq!(
            first.venv_python.as_deref(),
            Some("/tmp/xlsx/.venv/bin/python3")
        );
    }
}
