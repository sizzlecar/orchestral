use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use toml::Spanned;

#[derive(Deserialize)]
struct Manifest {
    workspace: Workspace,
}

#[derive(Deserialize)]
struct Workspace {
    package: Package,
    #[serde(default)]
    dependencies: BTreeMap<String, Dependency>,
}

#[derive(Deserialize)]
struct Package {
    version: Spanned<String>,
}

enum Dependency {
    Version,
    Detail {
        path: Option<PathBuf>,
        version: Option<Spanned<String>>,
    },
}

impl<'de> Deserialize<'de> for Dependency {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Dependency;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a dependency version string or table")
            }

            fn visit_str<E: serde::de::Error>(
                self,
                _: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(Dependency::Version)
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut path = None;
                let mut version = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "path" => path = Some(map.next_value()?),
                        "version" => version = Some(map.next_value()?),
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Dependency::Detail { path, version })
            }
        }
        // Deserialize directly: an untagged enum's intermediate value loses
        // TOML source spans needed to preserve manifest comments and formatting.
        deserializer.deserialize_any(Visitor)
    }
}

fn stable_version(value: &str) -> Result<[u64; 3]> {
    super::validate_version(value)?;
    let parts = value
        .split('.')
        .map(str::parse)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("semantic version component exceeds u64")?;
    Ok([parts[0], parts[1], parts[2]])
}

fn promote_notes(changelog: &str, target: &str) -> Result<String> {
    let heading = |line: &str, version: &str| {
        let prefix = format!("## [{version}]");
        line == prefix
            || line
                .strip_prefix(&prefix)
                .is_some_and(|rest| rest.starts_with(" - "))
    };
    ensure!(
        changelog
            .lines()
            .filter(|line| heading(line, "Unreleased"))
            .count()
            == 1,
        "CHANGELOG.md must have exactly one Unreleased section"
    );
    ensure!(
        !changelog.lines().any(|line| heading(line, target)),
        "CHANGELOG.md already contains version {target}"
    );
    super::release_notes(changelog, "Unreleased")?;
    let promoted = changelog.replacen("## [Unreleased]", &format!("## [{target}]"), 1);
    super::release_notes(&promoted, target)?;
    Ok(promoted)
}

/// Prepare exactly the versioned workspace packages. Cargo owns lockfile refresh;
/// any unrelated lockfile change fails and restores all three original files.
pub(super) fn prepare(root: &Path, current: &str, target: &str) -> Result<()> {
    ensure!(
        stable_version(target)? > stable_version(current)?,
        "target version must be greater than {current}"
    );
    let paths = [
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        root.join("CHANGELOG.md"),
    ];
    let original = paths
        .iter()
        .map(fs::read_to_string)
        .collect::<std::io::Result<Vec<_>>>()?;
    let changelog = promote_notes(&original[2], target)?;
    let metadata = super::cargo_metadata(root)?;
    let members = metadata["workspace_members"]
        .as_array()
        .context("workspace members")?;
    let mut inherited_directories = BTreeSet::new();
    let mut inherited_packages = BTreeSet::new();
    for package in metadata["packages"]
        .as_array()
        .context("workspace packages")?
    {
        if !members.contains(&package["id"]) {
            continue;
        }
        let manifest_path = Path::new(
            package["manifest_path"]
                .as_str()
                .context("package manifest")?,
        );
        let manifest: toml::Value = fs::read_to_string(manifest_path)?.parse()?;
        if manifest
            .get("package")
            .and_then(|p| p.get("version"))
            .and_then(|v| v.get("workspace"))
            .and_then(toml::Value::as_bool)
            == Some(true)
        {
            inherited_directories.insert(
                manifest_path
                    .parent()
                    .context("package directory")?
                    .canonicalize()?,
            );
            ensure!(
                package["version"] == current,
                "inherited package version differs from workspace"
            );
            inherited_packages.insert(package["name"].as_str().context("package name")?.to_owned());
        }
    }
    let manifest: Manifest = toml::from_str(&original[0])?;
    ensure!(
        manifest.workspace.package.version.get_ref() == current,
        "workspace version changed during preparation"
    );
    let mut replacements = vec![manifest.workspace.package.version.span()];
    for dependency in manifest.workspace.dependencies.values() {
        match dependency {
            Dependency::Detail {
                path: Some(path),
                version: Some(version),
            } if inherited_directories.contains(&root.join(path).canonicalize()?) => {
                ensure!(
                    version.get_ref() == current,
                    "internal dependency version must match workspace version {current}"
                );
                replacements.push(version.span());
            }
            _ => {}
        }
    }
    replacements.sort_by_key(|range| std::cmp::Reverse(range.start));
    let mut updated_manifest = original[0].clone();
    for range in replacements {
        updated_manifest.replace_range(range, &format!("\"{target}\""));
    }
    let expected_lock = expected_lock(&original[1], &inherited_packages, current, target)?;

    let result = (|| -> Result<()> {
        fs::write(&paths[0], updated_manifest)?;
        fs::write(&paths[2], changelog)?;
        let update = Command::new("cargo")
            .args(["update", "--offline", "--workspace"])
            .current_dir(root)
            .output()?;
        ensure!(
            update.status.success(),
            "refresh internal lock versions: {}",
            String::from_utf8_lossy(&update.stderr)
        );
        let lock: toml::Value = fs::read_to_string(&paths[1])?.parse()?;
        ensure!(
            lock == expected_lock,
            "Cargo changed lock entries beyond the inherited workspace package versions"
        );
        ensure!(
            super::inventory(root)?.version == target,
            "prepared inventory version differs from target"
        );
        Ok(())
    })();
    if let Err(error) = result {
        let mut failures = Vec::new();
        for (path, text) in paths.iter().zip(&original) {
            if let Err(restore) = fs::write(path, text) {
                failures.push(format!("{}: {restore}", path.display()));
            }
        }
        ensure!(
            failures.is_empty(),
            "{error:#}; restoring original files failed: {}",
            failures.join("; ")
        );
        return Err(error);
    }
    Ok(())
}

fn expected_lock(
    text: &str,
    inherited: &BTreeSet<String>,
    current: &str,
    target: &str,
) -> Result<toml::Value> {
    let mut lock: toml::Value = text.parse()?;
    let mut found = BTreeSet::new();
    for package in lock
        .get_mut("package")
        .and_then(toml::Value::as_array_mut)
        .context("lock packages")?
    {
        let name = package["name"]
            .as_str()
            .context("locked package name")?
            .to_owned();
        if package.get("source").is_none() && inherited.contains(&name) {
            ensure!(
                package["version"].as_str() == Some(current),
                "unexpected locked version for {name}"
            );
            ensure!(
                found.insert(name.clone()),
                "duplicate local locked package {name}"
            );
            package["version"] = target.into();
        }
        if let Some(dependencies) = package
            .get_mut("dependencies")
            .and_then(toml::Value::as_array_mut)
        {
            for dependency in dependencies {
                if let Some(value) = dependency.as_str() {
                    if let Some(name) = value.strip_suffix(&format!(" {current}")) {
                        if inherited.contains(name) {
                            *dependency = format!("{name} {target}").into();
                        }
                    }
                }
            }
        }
    }
    ensure!(
        &found == inherited,
        "lockfile omits inherited workspace packages"
    );
    Ok(lock)
}

#[cfg(test)]
mod tests;
