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

    pub fn active_entries(&self) -> Vec<&SkillEntry> {
        if self.max_active == 0 {
            return Vec::new();
        }

        self.entries.iter().take(self.max_active).collect()
    }

    pub fn build_instructions(&self, _intent: &str) -> Vec<SkillInstruction> {
        self.active_entries()
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
    fn test_skill_catalog_active_entries_returns_all_up_to_limit() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("xlsx", "xlsx creation and formula recalc"),
                make_skill("git", "git commit and branch operations"),
            ],
            3,
        );

        let active = catalog.active_entries();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].name, "xlsx");
        assert_eq!(active[1].name, "git");
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

        let active = catalog.active_entries();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].name, "a");
        assert_eq!(active[1].name, "b");
    }

    #[test]
    fn test_skill_catalog_zero_max_active_disables_loading() {
        let catalog = SkillCatalog::new(vec![make_skill("xlsx", "spreadsheet skill")], 3);
        let disabled = SkillCatalog::new(vec![make_skill("xlsx", "spreadsheet skill")], 0);

        assert_eq!(catalog.active_entries().len(), 1);
        assert!(disabled.active_entries().is_empty());
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
