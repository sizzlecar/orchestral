use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use orchestral_core::project_instructions::{ProjectInstructionSource, ProjectInstructionsConfig};
use orchestral_project_instructions_fs::FileProjectInstructionSource;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "orchestral-instructions-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn gitfile_boundary_excludes_parent_rules_and_empty_override_falls_back() {
    let fixture = Fixture::new();
    fs::write(fixture.0.join("AGENTS.md"), "outside project").unwrap();
    let root = fixture.0.join("worktree");
    let child = root.join("service");
    fs::create_dir_all(&child).unwrap();
    fs::write(
        root.join(".git"),
        "gitdir: /some/shared/git/worktrees/service",
    )
    .unwrap();
    fs::write(root.join("AGENTS.override.md"), " \n").unwrap();
    fs::write(root.join("AGENTS.md"), "project").unwrap();
    fs::write(child.join("CLAUDE.md"), "service").unwrap();
    let source = FileProjectInstructionSource::new(Default::default()).unwrap();
    let documents = source
        .snapshot(&[child.to_str().unwrap().to_owned()])
        .unwrap();
    assert_eq!(
        documents
            .iter()
            .map(|document| document.content.as_str())
            .collect::<Vec<_>>(),
        ["project", "service"]
    );
}

#[test]
fn non_git_workspace_does_not_inherit_unrelated_parent_instructions() {
    let fixture = Fixture::new();
    let child = fixture.0.join("scratch");
    fs::create_dir(&child).unwrap();
    fs::write(fixture.0.join("AGENTS.md"), "outside").unwrap();
    fs::write(child.join("CLAUDE.md"), "local").unwrap();
    let source = FileProjectInstructionSource::new(Default::default()).unwrap();
    let documents = source
        .snapshot(&[child.to_str().unwrap().to_owned()])
        .unwrap();
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].content, "local");
}

#[test]
fn fallback_names_cannot_escape_the_instruction_directory() {
    for filename in [
        "../secret",
        "/absolute",
        "nested/file",
        "nested\\file",
        "C:private",
        "..",
    ] {
        let config = ProjectInstructionsConfig {
            fallback_filenames: vec![filename.to_owned()],
            ..Default::default()
        };
        assert!(
            FileProjectInstructionSource::new(config).is_err(),
            "{filename}"
        );
    }
}

#[test]
#[cfg(unix)]
fn instruction_symlinks_cannot_read_outside_the_selected_directory() {
    let fixture = Fixture::new();
    let child = fixture.0.join("project");
    fs::create_dir(&child).unwrap();
    fs::write(fixture.0.join("private.md"), "outside").unwrap();
    std::os::unix::fs::symlink("../private.md", child.join("AGENTS.md")).unwrap();
    let source = FileProjectInstructionSource::new(Default::default()).unwrap();
    assert!(source
        .snapshot(&[child.to_str().unwrap().to_owned()])
        .is_err());
}

#[test]
#[cfg(unix)]
fn same_directory_instruction_aliases_are_supported() {
    let fixture = Fixture::new();
    fs::write(fixture.0.join("CLAUDE.md"), "shared conventions").unwrap();
    std::os::unix::fs::symlink("CLAUDE.md", fixture.0.join("AGENTS.md")).unwrap();
    let source = FileProjectInstructionSource::new(Default::default()).unwrap();
    let documents = source
        .snapshot(&[fixture.0.to_str().unwrap().to_owned()])
        .unwrap();
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].content, "shared conventions");
    assert!(documents[0].source.ends_with("/AGENTS.md"));
}
