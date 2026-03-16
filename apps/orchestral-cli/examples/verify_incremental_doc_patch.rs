use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct BaselineEntry {
    modified_nanos: u128,
    content: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BaselineSnapshot {
    files: BTreeMap<String, BaselineEntry>,
    changed_files: Vec<String>,
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        bail!("expected subcommand: prepare | verify");
    };
    let Some(baseline_path) = args.next() else {
        bail!("expected baseline path");
    };
    let Some(docs_root) = args.next() else {
        bail!("expected docs root");
    };

    let baseline_path = PathBuf::from(baseline_path);
    let docs_root = PathBuf::from(docs_root);
    let changed_files = args.collect::<Vec<_>>();

    match command.as_str() {
        "prepare" => prepare(&baseline_path, &docs_root, &changed_files),
        "verify" => verify(&baseline_path, &docs_root, &changed_files),
        other => bail!("unsupported subcommand '{}'", other),
    }
}

fn prepare(baseline_path: &Path, docs_root: &Path, changed_files: &[String]) -> Result<()> {
    let mut files = BTreeMap::new();
    for path in collect_markdown_files(docs_root)? {
        let relative = path
            .strip_prefix(docs_root)
            .with_context(|| format!("strip prefix '{}' failed", docs_root.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        files.insert(
            relative,
            BaselineEntry {
                modified_nanos: modified_nanos(&path)?,
                content: fs::read_to_string(&path)
                    .with_context(|| format!("read '{}' failed", path.display()))?,
            },
        );
    }

    if let Some(parent) = baseline_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create '{}' failed", parent.display()))?;
    }
    fs::write(
        baseline_path,
        serde_json::to_vec_pretty(&BaselineSnapshot {
            files,
            changed_files: changed_files.to_vec(),
        })?,
    )
    .with_context(|| format!("write '{}' failed", baseline_path.display()))?;

    for changed in changed_files {
        strip_leading_title(&docs_root.join(changed))?;
    }

    Ok(())
}

fn verify(baseline_path: &Path, docs_root: &Path, changed_files: &[String]) -> Result<()> {
    let baseline: BaselineSnapshot = serde_json::from_slice(
        &fs::read(baseline_path)
            .with_context(|| format!("read '{}' failed", baseline_path.display()))?,
    )
    .with_context(|| format!("parse '{}' failed", baseline_path.display()))?;

    let changed_set = if changed_files.is_empty() {
        baseline
            .changed_files
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        changed_files.iter().cloned().collect::<BTreeSet<_>>()
    };

    for (relative, entry) in &baseline.files {
        let path = docs_root.join(relative);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read '{}' failed", path.display()))?;
        let current_modified = modified_nanos(&path)?;

        if !content.starts_with("# ") {
            bail!(
                "expected '{}' to have a top-level title after rerun",
                relative
            );
        }
        if content.contains("TODO") {
            bail!(
                "expected '{}' to have no TODO placeholders after rerun",
                relative
            );
        }
        if content != entry.content {
            bail!(
                "expected '{}' content to match baseline after rerun",
                relative
            );
        }

        let changed = changed_set.contains(relative);
        if changed && current_modified <= entry.modified_nanos {
            bail!(
                "expected changed file '{}' to be rewritten after baseline",
                relative
            );
        }
        if !changed && current_modified != entry.modified_nanos {
            bail!(
                "expected untouched file '{}' to keep baseline mtime",
                relative
            );
        }
    }

    println!("incremental document patch verification passed");
    Ok(())
}

fn collect_markdown_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_markdown_files_recursive(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_markdown_files_recursive(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("read '{}' failed", root.display()))? {
        let entry = entry.with_context(|| format!("read entry in '{}' failed", root.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_markdown_files_recursive(&path, out)?;
        } else if path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|ext| matches!(ext, "md" | "markdown" | "mdx" | "txt"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn modified_nanos(path: &Path) -> Result<u128> {
    Ok(fs::metadata(path)
        .with_context(|| format!("metadata '{}' failed", path.display()))?
        .modified()
        .with_context(|| format!("modified time '{}' failed", path.display()))?
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("system time before epoch '{}'", path.display()))?
        .as_nanos())
}

fn strip_leading_title(path: &Path) -> Result<()> {
    let content =
        fs::read_to_string(path).with_context(|| format!("read '{}' failed", path.display()))?;
    let Some(first) = content.lines().next() else {
        bail!("expected '{}' to be non-empty", path.display());
    };
    let remaining = if first.starts_with("# ") {
        if let Some(first_newline) = content.find('\n') {
            let mut rest = &content[first_newline + 1..];
            if rest.starts_with('\n') {
                rest = &rest[1..];
            }
            rest.to_string()
        } else {
            String::new()
        }
    } else {
        content
    };
    fs::write(path, remaining.as_bytes())
        .with_context(|| format!("write '{}' failed", path.display()))?;
    Ok(())
}
