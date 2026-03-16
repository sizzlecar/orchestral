use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

const ROUND1_AUDIT_TARGETS: &[&str] = &[
    "runbooks/release-rollout.md",
    "runbooks/incident-triage.md",
    "runbooks/tenant-cutover.md",
    "runbooks/rollback-governor.md",
    "runbooks/sandbox-drill.md",
    "runbooks/pager-handoff.md",
    "customer-guides/production-deployment.md",
    "customer-guides/observability.md",
    "customer-guides/tenant-logging.md",
    "customer-guides/release-faq.md",
    "customer-guides/legacy-logging-guide.md",
];

const ROUND1_ALIAS_TARGETS: &[&str] = &[
    "customer-guides/production-deployment.md",
    "customer-guides/tenant-logging.md",
    "customer-guides/legacy-logging-guide.md",
];

const ROUND1_CHANGED_FOR_SUMMARY: &[&str] = &[
    "runbooks/release-rollout.md",
    "runbooks/incident-triage.md",
    "runbooks/tenant-cutover.md",
    "runbooks/rollback-governor.md",
    "runbooks/sandbox-drill.md",
    "runbooks/pager-handoff.md",
    "customer-guides/production-deployment.md",
    "customer-guides/observability.md",
    "customer-guides/tenant-logging.md",
    "customer-guides/release-faq.md",
    "customer-guides/legacy-logging-guide.md",
];

const ROUND1_UNCHANGED: &[(&str, &str)] = &[
    (
        "runbooks/access-review.md",
        include_str!("../../../fixtures/scenarios/compliance_rollout_batch_update/runbooks/access-review.md"),
    ),
    (
        "runbooks/retention-export.md",
        include_str!("../../../fixtures/scenarios/compliance_rollout_batch_update/runbooks/retention-export.md"),
    ),
    (
        "customer-guides/audit-mode.md",
        include_str!("../../../fixtures/scenarios/compliance_rollout_batch_update/customer-guides/audit-mode.md"),
    ),
];

const ROUND2_CHANGED: &[&str] = &[
    "runbooks/sandbox-drill.md",
    "customer-guides/tenant-logging.md",
    "customer-guides/release-faq.md",
    "customer-guides/legacy-logging-guide.md",
    "reports/compliance-rollout-summary.md",
];

const ROUND2_SUMMARY_DOCS: &[&str] = &[
    "runbooks/sandbox-drill.md",
    "customer-guides/tenant-logging.md",
    "customer-guides/release-faq.md",
    "customer-guides/legacy-logging-guide.md",
];

#[derive(Debug, Serialize, Deserialize)]
struct BaselineEntry {
    modified_nanos: u128,
    content: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BaselineSnapshot {
    files: BTreeMap<String, BaselineEntry>,
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        bail!("expected subcommand: prepare | verify");
    };
    let Some(baseline_path) = args.next() else {
        bail!("expected baseline path");
    };
    let Some(root) = args.next() else {
        bail!("expected scenario root");
    };

    let baseline_path = PathBuf::from(baseline_path);
    let root = PathBuf::from(root);

    match command.as_str() {
        "prepare" => prepare(&baseline_path, &root),
        "verify" => verify(&baseline_path, &root),
        other => bail!("unsupported subcommand '{}'", other),
    }
}

fn prepare(baseline_path: &Path, root: &Path) -> Result<()> {
    verify_round1(root)?;
    let snapshot = BaselineSnapshot {
        files: snapshot_files(root)?,
    };
    if let Some(parent) = baseline_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create '{}' failed", parent.display()))?;
    }
    fs::write(baseline_path, serde_json::to_vec_pretty(&snapshot)?)
        .with_context(|| format!("write '{}' failed", baseline_path.display()))?;
    println!("round-1 compliance rollout verification passed");
    Ok(())
}

fn verify(baseline_path: &Path, root: &Path) -> Result<()> {
    let baseline: BaselineSnapshot = serde_json::from_slice(
        &fs::read(baseline_path)
            .with_context(|| format!("read '{}' failed", baseline_path.display()))?,
    )
    .with_context(|| format!("parse '{}' failed", baseline_path.display()))?;
    verify_round2(root, &baseline)?;
    println!("round-2 compliance rollout verification passed");
    Ok(())
}

fn verify_round1(root: &Path) -> Result<()> {
    for relative in collect_document_paths() {
        let path = root.join(relative);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read '{}' failed", path.display()))?;
        assert_has_h1(relative, &content)?;
    }

    for relative in ROUND1_ALIAS_TARGETS {
        let content = read_relative(root, relative)?;
        if !content.contains("audit-review@company.example") {
            bail!(
                "expected '{}' to use the audit-review alias after round 1",
                relative
            );
        }
        if content
            .replace("audit-review@company.example", "")
            .contains("compliance@company.example")
        {
            bail!(
                "expected '{}' to replace the old compliance alias after round 1",
                relative
            );
        }
    }

    for relative in ROUND1_AUDIT_TARGETS {
        let content = read_relative(root, relative)?;
        if !content.contains("## Audit Readiness") {
            bail!(
                "expected '{}' to contain an Audit Readiness section after round 1",
                relative
            );
        }
    }

    for (relative, expected) in ROUND1_UNCHANGED {
        let content = read_relative(root, relative)?;
        if content != *expected {
            bail!("expected '{}' to remain unchanged after round 1", relative);
        }
    }

    verify_summary_round1(&read_relative(
        root,
        "reports/compliance-rollout-summary.md",
    )?)?;
    Ok(())
}

fn verify_round2(root: &Path, baseline: &BaselineSnapshot) -> Result<()> {
    let changed_set = ROUND2_CHANGED.iter().copied().collect::<BTreeSet<_>>();

    for (relative, entry) in &baseline.files {
        let path = root.join(relative);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read '{}' failed", path.display()))?;
        let modified = modified_nanos(&path)?;

        if changed_set.contains(relative.as_str()) {
            if content == entry.content {
                bail!("expected '{}' content to change in round 2", relative);
            }
            if modified <= entry.modified_nanos {
                bail!("expected '{}' mtime to advance in round 2", relative);
            }
        } else {
            if content != entry.content {
                bail!(
                    "expected '{}' content to remain unchanged in round 2",
                    relative
                );
            }
            if modified != entry.modified_nanos {
                bail!(
                    "expected '{}' mtime to remain unchanged in round 2",
                    relative
                );
            }
        }
    }

    let sandbox = read_relative(root, "runbooks/sandbox-drill.md")?;
    if sandbox.contains("## Audit Readiness") {
        bail!("expected sandbox-drill.md to remove Audit Readiness in round 2");
    }

    let tenant_logging = read_relative(root, "customer-guides/tenant-logging.md")?;
    if !tenant_logging.contains("/var/log/tenant.log") {
        bail!("expected tenant-logging.md to use /var/log/tenant.log in round 2");
    }
    if tenant_logging.contains("/tmp/tenant-debug.log") {
        bail!("expected tenant-logging.md to drop the old debug log path in round 2");
    }

    let release_faq = read_relative(root, "customer-guides/release-faq.md")?;
    if release_faq.contains("## Audit Readiness") {
        bail!("expected release-faq.md to remove Audit Readiness in round 2");
    }

    let legacy_logging = read_relative(root, "customer-guides/legacy-logging-guide.md")?;
    if !legacy_logging.contains("legacy-compliance@company.example") {
        bail!("expected legacy-logging-guide.md to use the legacy-compliance alias in round 2");
    }
    if legacy_logging
        .replace("legacy-compliance@company.example", "")
        .contains("audit-review@company.example")
    {
        bail!("expected legacy-logging-guide.md to replace the audit-review alias");
    }

    verify_summary_round2(&read_relative(
        root,
        "reports/compliance-rollout-summary.md",
    )?)?;
    Ok(())
}

fn verify_summary_round1(summary: &str) -> Result<()> {
    verify_summary_common(summary)?;
    if !summary.contains("## Runbooks") {
        bail!("expected round-1 summary to contain a Runbooks section");
    }
    if !summary.contains("## Customer Guides") {
        bail!("expected round-1 summary to contain a Customer Guides section");
    }
    for relative in ROUND1_CHANGED_FOR_SUMMARY {
        if !summary.contains(relative) {
            bail!(
                "expected round-1 summary to mention changed file '{}'",
                relative
            );
        }
    }
    for (relative, _) in ROUND1_UNCHANGED {
        if summary.contains(relative) {
            bail!(
                "expected round-1 summary to omit unchanged file '{}'",
                relative
            );
        }
    }
    Ok(())
}

fn verify_summary_round2(summary: &str) -> Result<()> {
    verify_summary_common(summary)?;
    for relative in ROUND2_SUMMARY_DOCS {
        if !summary.contains(relative) {
            bail!(
                "expected round-2 summary to mention changed file '{}'",
                relative
            );
        }
    }

    for relative in ROUND1_CHANGED_FOR_SUMMARY {
        if !ROUND2_SUMMARY_DOCS.contains(relative) && summary.contains(relative) {
            bail!(
                "expected round-2 summary to omit round-1-only file '{}'",
                relative
            );
        }
    }

    for (relative, _) in ROUND1_UNCHANGED {
        if summary.contains(relative) {
            bail!(
                "expected round-2 summary to omit unchanged file '{}'",
                relative
            );
        }
    }
    Ok(())
}

fn verify_summary_common(summary: &str) -> Result<()> {
    if summary.trim().is_empty() {
        bail!("expected summary to be non-empty");
    }
    if summary.contains("Pending run.") {
        bail!("expected summary placeholder to be replaced");
    }
    Ok(())
}

fn snapshot_files(root: &Path) -> Result<BTreeMap<String, BaselineEntry>> {
    let mut files = BTreeMap::new();
    for relative in collect_target_paths() {
        let path = root.join(relative);
        files.insert(
            relative.to_string(),
            BaselineEntry {
                modified_nanos: modified_nanos(&path)?,
                content: fs::read_to_string(&path)
                    .with_context(|| format!("read '{}' failed", path.display()))?,
            },
        );
    }
    Ok(files)
}

fn collect_target_paths() -> Vec<&'static str> {
    let mut files = collect_document_paths();
    files.push("reports/compliance-rollout-summary.md");
    files
}

fn collect_document_paths() -> Vec<&'static str> {
    vec![
        "runbooks/release-rollout.md",
        "runbooks/incident-triage.md",
        "runbooks/tenant-cutover.md",
        "runbooks/rollback-governor.md",
        "runbooks/sandbox-drill.md",
        "runbooks/pager-handoff.md",
        "runbooks/access-review.md",
        "runbooks/retention-export.md",
        "customer-guides/production-deployment.md",
        "customer-guides/observability.md",
        "customer-guides/tenant-logging.md",
        "customer-guides/release-faq.md",
        "customer-guides/legacy-logging-guide.md",
        "customer-guides/audit-mode.md",
    ]
}

fn read_relative(root: &Path, relative: &str) -> Result<String> {
    let path = root.join(relative);
    fs::read_to_string(&path).with_context(|| format!("read '{}' failed", path.display()))
}

fn assert_has_h1(relative: &str, content: &str) -> Result<()> {
    let Some(first_nonempty) = content.lines().find(|line| !line.trim().is_empty()) else {
        bail!("expected '{}' to be non-empty", relative);
    };
    if !first_nonempty.trim_start().starts_with("# ") {
        bail!("expected '{}' to start with a top-level H1 title", relative);
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
