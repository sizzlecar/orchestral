use std::fs;
use std::path::PathBuf;

use orchestral_core::project_instructions::{ProjectInstructionSource, ProjectInstructionsConfig};
use orchestral_project_instructions_fs::FileProjectInstructionSource;

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "coding-eval-instructions-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join(".git")).unwrap();
        Self(root)
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn overlapping_workspaces_load_each_source_once() {
    let root = Workspace::new();
    let child = root.0.join("component");
    fs::create_dir(&child).unwrap();
    fs::write(root.0.join("AGENTS.md"), "root conventions").unwrap();
    fs::write(child.join("CLAUDE.md"), "component conventions").unwrap();
    let root_name = root.0.to_string_lossy().into_owned();
    let child_name = child.to_string_lossy().into_owned();
    let source = FileProjectInstructionSource::new(Default::default()).unwrap();
    for selection in [
        vec![child_name.clone(), root_name.clone(), child_name.clone()],
        vec![root_name, child_name],
    ] {
        let documents = source.snapshot(&selection).unwrap();
        assert_eq!(
            documents
                .iter()
                .map(|d| d.content.as_str())
                .collect::<Vec<_>>(),
            ["root conventions", "component conventions"]
        );
    }
}

#[test]
fn aggregate_instruction_bytes_are_bounded() {
    let root = Workspace::new();
    let child = root.0.join("component");
    fs::create_dir(&child).unwrap();
    fs::write(root.0.join("AGENTS.md"), "根规则").unwrap();
    fs::write(child.join("AGENTS.md"), "nested rule").unwrap();
    let selection = [child.to_string_lossy().into_owned()];
    let exact = "根规则".len() + "nested rule".len();
    for (budget, valid) in [(exact as u64, true), (exact as u64 - 1, false)] {
        let source = FileProjectInstructionSource::new(ProjectInstructionsConfig {
            max_bytes: budget,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            source.snapshot(&selection).is_ok(),
            valid,
            "budget {budget}"
        );
    }
}

#[test]
fn zero_instruction_budget_is_invalid() {
    assert!(
        FileProjectInstructionSource::new(ProjectInstructionsConfig {
            max_bytes: 0,
            ..Default::default()
        })
        .is_err()
    );
}
