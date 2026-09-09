use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, ensure, Context, Result};
use sha2::{Digest, Sha256};

use crate::task::{mutate, text, verified_source, Task};

pub const EXTRA_PATH: &str =
    "plugins/orchestral-project-instructions-fs/tests/coding_eval_contract.rs";
pub const EXTRA_TESTS: &str = include_str!("../checks/instructions.rs");
const USER_FILE: &str = "LOCAL_NOTES.txt";

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn git(root: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-c")
        .arg("core.hooksPath=/dev/null")
        .arg("-c")
        .arg("commit.gpgsign=false")
        .args(args)
        .current_dir(root)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .context("run Git")?;
    ensure!(
        output.status.success(),
        "Git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}

pub struct Repository {
    pub archive: PathBuf,
    pub verifier: PathBuf,
    pub original: BTreeMap<String, String>,
}

impl Repository {
    pub fn prepare(root: &Path, output: &Path, base: &str, tasks: &[&Task]) -> Result<Self> {
        let archive = output.join("source.tar");
        git(
            root,
            &[
                "archive",
                "--format=tar",
                "--output",
                archive.to_str().context("archive path")?,
                base,
            ],
        )?;
        let verifier = output.join("verifier");
        extract(&archive, &verifier)?;
        fs::write(verifier.join(EXTRA_PATH), EXTRA_TESTS)?;
        let mut original = BTreeMap::new();
        for path in tasks.iter().flat_map(|task| task.paths()) {
            original
                .entry(path.to_owned())
                .or_insert(fs::read_to_string(verifier.join(path))?);
        }
        Ok(Self {
            archive,
            verifier,
            original,
        })
    }

    pub fn reset_verifier(&self) -> Result<()> {
        for (path, source) in &self.original {
            write_changed(&self.verifier.join(path), source)?;
        }
        Ok(())
    }

    pub fn seed(&self, root: &Path, task: &Task) -> Result<()> {
        for mutation in &task.mutations {
            let path = root.join(&mutation.path);
            write_changed(&path, &mutate(&fs::read_to_string(&path)?, mutation)?)?;
        }
        Ok(())
    }

    pub fn candidate(&self, root: &Path, task: &Task) -> Result<Candidate> {
        extract(&self.archive, root)?;
        self.seed(root, task)?;
        fs::write(root.join(USER_FILE), "Personal notes\n")?;
        git(root, &["init", "-q", "--initial-branch=task"])?;
        git(root, &["config", "user.name", "Coding evaluation"])?;
        git(
            root,
            &["config", "user.email", "coding-eval@example.invalid"],
        )?;
        git(root, &["add", "--all"])?;
        git(root, &["commit", "-q", "-m", "Task starting point"])?;
        fs::write(
            root.join(USER_FILE),
            "Personal notes\nUncommitted user work: keep this exact text.\n",
        )?;
        let baseline = snapshot(root)?;
        let head = git(root, &["rev-parse", "HEAD"])?;
        Ok(Candidate {
            root: root.to_owned(),
            baseline,
            head,
        })
    }

    pub fn overlay(&self, candidate: &Candidate, task: &Task) -> Result<()> {
        self.reset_verifier()?;
        for path in task.paths() {
            let source = fs::read_to_string(candidate.root.join(path))?;
            let checked = verified_source(&self.original[path], &source)?;
            write_changed(&self.verifier.join(path), &checked)?;
        }
        Ok(())
    }
}

pub struct Candidate {
    pub root: PathBuf,
    baseline: BTreeMap<String, String>,
    head: Vec<u8>,
}

impl Candidate {
    pub fn violations(&self, allowed: &BTreeSet<&str>) -> Result<Vec<String>> {
        let current = snapshot(&self.root)?;
        let paths = self
            .baseline
            .keys()
            .chain(current.keys())
            .collect::<BTreeSet<_>>();
        let mut violations = paths
            .into_iter()
            .filter(|path| {
                !allowed.contains(path.as_str()) && self.baseline.get(*path) != current.get(*path)
            })
            .map(|path| format!("changed protected path: {path}"))
            .collect::<Vec<_>>();
        if git(&self.root, &["rev-parse", "HEAD"])? != self.head {
            violations.push("created or rewrote commits".into());
        }
        if !git(&self.root, &["diff", "--cached", "--name-only"])?.is_empty() {
            violations.push("staged files without permission".into());
        }
        Ok(violations)
    }

    pub fn save_patch(&self, path: &Path, allowed: &BTreeSet<&str>) -> Result<()> {
        let mut args = vec!["diff", "--no-ext-diff", "--binary", "HEAD", "--"];
        args.extend(allowed.iter().copied());
        fs::write(path, git(&self.root, &args)?)?;
        Ok(())
    }
}

fn extract(archive: &Path, root: &Path) -> Result<()> {
    fs::create_dir(root).context("create fresh isolated task directory")?;
    let status = Command::new("tar")
        .arg("-xf")
        .arg(archive)
        .arg("-C")
        .arg(root)
        .status()?;
    ensure!(status.success(), "extract pinned source archive");
    Ok(())
}

fn write_changed(path: &Path, source: &str) -> Result<()> {
    if fs::read(path).ok().as_deref() != Some(source.as_bytes()) {
        fs::write(path, source)?;
    }
    Ok(())
}

fn snapshot(root: &Path) -> Result<BTreeMap<String, String>> {
    fn walk(root: &Path, dir: &Path, files: &mut BTreeMap<String, String>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path.strip_prefix(root)?;
            if relative.components().any(|part| {
                matches!(
                    part.as_os_str().to_str(),
                    Some(".git" | "target" | ".orchestral" | "logs")
                )
            }) {
                continue;
            }
            let key = relative
                .to_str()
                .context("non-UTF-8 candidate path")?
                .replace('\\', "/");
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                bail!("candidate contains a symlink: {key}");
            } else if kind.is_dir() {
                walk(root, &path, files)?;
            } else if kind.is_file() {
                let bytes = fs::read(&path)?;
                files.insert(key, digest(&bytes));
            } else {
                bail!("candidate contains a non-regular path: {key}");
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    walk(root, root, &mut files)?;
    Ok(files)
}

pub fn default_config(root: &Path, path: &Path) -> Result<()> {
    let mut config: serde_yaml::Value =
        serde_yaml::from_str(text(&fs::read(root.join("configs/orchestral.cli.yaml"))?)?)?;
    config["tools"]["exec"]["allow_host_execution"] = false.into();
    config["agent"]["max_model_steps"] = 32.into();
    config["agent"]["max_tool_calls"] = 96.into();
    config["mcp"]["enabled"] = false.into();
    config["skills"]["enabled"] = false.into();
    fs::write(path, serde_yaml::to_string(&config)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protected_dirty_files_and_ignored_documents_are_checked() {
        let root =
            std::env::temp_dir().join(format!("coding-eval-protection-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        git(&root, &["init", "-q"]).unwrap();
        git(&root, &["config", "user.name", "Eval"]).unwrap();
        git(&root, &["config", "user.email", "eval@example.invalid"]).unwrap();
        fs::write(root.join(".gitignore"), "docs/\n").unwrap();
        fs::write(root.join("user.txt"), "original").unwrap();
        git(&root, &["add", "."]).unwrap();
        git(&root, &["commit", "-q", "-m", "base"]).unwrap();
        fs::write(root.join("user.txt"), "dirty user work").unwrap();
        let candidate = Candidate {
            baseline: snapshot(&root).unwrap(),
            head: git(&root, &["rev-parse", "HEAD"]).unwrap(),
            root: root.clone(),
        };
        assert!(candidate.violations(&BTreeSet::new()).unwrap().is_empty());
        fs::create_dir(root.join("docs")).unwrap();
        fs::write(root.join("docs/record.md"), "unrequested").unwrap();
        fs::write(root.join("user.txt"), "original").unwrap();
        assert_eq!(candidate.violations(&BTreeSet::new()).unwrap().len(), 2);
        fs::remove_dir_all(root).unwrap();
    }
}
