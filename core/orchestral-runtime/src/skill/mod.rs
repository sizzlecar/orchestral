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
                instructions: entry.instructions.clone(),
                scripts_dir: entry
                    .scripts_dir
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
            })
            .collect()
    }
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
}
