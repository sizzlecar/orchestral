mod formula;
mod registry;

use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser)]
#[command(about = "Prepare and verify Orchestral distribution artifacts")]
struct Args {
    #[arg(long, default_value = ".")]
    workspace: PathBuf,
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Validate Cargo publication metadata and extract reviewed release notes.
    Prepare {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        tag: Option<String>,
    },
    /// Copy the tested web distribution inside the CLI's published package.
    StageWeb,
    /// Verify archives and generate a Homebrew formula from their actual hashes.
    Formula {
        #[arg(long)]
        assets: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Compare newly packaged crates against the already verified release bytes.
    VerifyCrates {
        #[arg(long)]
        artifacts: PathBuf,
    },
    /// Publish only missing versions; existing versions must match reviewed bytes.
    PublishCrates {
        #[arg(long)]
        artifacts: PathBuf,
    },
    /// Execute an installed CLI's version/help paths without a model call.
    Installed {
        #[arg(long)]
        binary: PathBuf,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct Inventory {
    version: String,
    source_commit: String,
    target_directory: PathBuf,
    packages: Vec<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let root = args.workspace.canonicalize().context("workspace path")?;
    let inventory = inventory(&root)?;
    match args.command {
        Action::Prepare { output, tag } => {
            if let Some(tag) = tag {
                ensure!(
                    tag == format!("v{}", inventory.version),
                    "tag does not match Cargo version"
                );
            }
            let notes = release_notes(
                &fs::read_to_string(root.join("CHANGELOG.md"))?,
                &inventory.version,
            )?;
            fs::create_dir_all(&output)?;
            fs::write(output.join("release-notes.md"), notes)?;
            fs::write(
                output.join("version.txt"),
                format!("{}\n", inventory.version),
            )?;
            fs::write(
                output.join("release.json"),
                serde_json::to_vec_pretty(&inventory)?,
            )?;
            println!("{}", inventory.version);
        }
        Action::StageWeb => {
            let source = root.join("web/orchestral-web/dist");
            ensure!(
                source.join("index.html").is_file(),
                "build the PWA before packaging"
            );
            let destination = root.join("apps/orchestral-cli/web-dist");
            if destination.exists() {
                fs::remove_dir_all(&destination)?;
            }
            copy_assets(&source, &destination)?;
        }
        Action::Formula { assets, output } => {
            let rendered = formula::render(&inventory.version, &assets)?;
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(output, rendered)?;
        }
        Action::VerifyCrates { artifacts } => {
            verify_inventory(&inventory, &artifacts)?;
            verify_crates(&inventory, &artifacts, &inventory.packages)?;
        }
        Action::PublishCrates { artifacts } => registry::publish(&root, &inventory, &artifacts)?,
        Action::Installed { binary } => installed(&binary, &inventory.version)?,
    }
    Ok(())
}

fn inventory(root: &Path) -> Result<Inventory> {
    let manifest: toml::Value = fs::read_to_string(root.join("Cargo.toml"))?.parse()?;
    let version = manifest["workspace"]["package"]["version"]
        .as_str()
        .context("workspace version")?
        .to_owned();
    validate_version(&version)?;
    let metadata = Command::new("cargo")
        .args([
            "metadata",
            "--locked",
            "--offline",
            "--no-deps",
            "--format-version",
            "1",
        ])
        .current_dir(root)
        .output()?;
    ensure!(
        metadata.status.success(),
        "Cargo metadata failed: {}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout)?;
    let members = metadata["workspace_members"]
        .as_array()
        .context("workspace members")?;
    let mut packages = Vec::new();
    for package in metadata["packages"]
        .as_array()
        .context("workspace packages")?
    {
        if !members.contains(&package["id"])
            || package["publish"].as_array().is_some_and(Vec::is_empty)
        {
            continue;
        }
        let name = package["name"].as_str().context("package name")?;
        ensure!(
            package["version"] == version,
            "{name}: package and release version differ"
        );
        for dependency in package["dependencies"].as_array().context("dependencies")? {
            if dependency["path"].is_string() && dependency["kind"] != "dev" {
                ensure!(
                    dependency["req"] != "*",
                    "{name}: production path dependency {} needs a registry version",
                    dependency["name"]
                );
            }
        }
        packages.push(name.to_owned());
    }
    packages.sort();
    ensure!(!packages.is_empty(), "no publishable workspace packages");
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()?;
    ensure!(commit.status.success(), "resolve release source commit");
    Ok(Inventory {
        version,
        source_commit: String::from_utf8(commit.stdout)?.trim().to_owned(),
        target_directory: metadata["target_directory"]
            .as_str()
            .context("target directory")?
            .into(),
        packages,
    })
}

fn validate_version(version: &str) -> Result<()> {
    let parts: Vec<_> = version.split('.').collect();
    ensure!(
        parts.len() == 3
            && parts.iter().all(|part| {
                !part.is_empty()
                    && part.bytes().all(|byte| byte.is_ascii_digit())
                    && (part.len() == 1 || !part.starts_with('0'))
            }),
        "release version must be MAJOR.MINOR.PATCH"
    );
    Ok(())
}

fn release_notes(changelog: &str, version: &str) -> Result<String> {
    let heading = format!("## [{version}]");
    let mut found = false;
    let mut notes = Vec::new();
    for line in changelog.lines() {
        if !found {
            if line == heading
                || line
                    .strip_prefix(&heading)
                    .is_some_and(|rest| rest.starts_with(" - "))
            {
                found = true;
            }
        } else if line.starts_with("## [") {
            break;
        } else {
            notes.push(line);
        }
    }
    let notes = notes.join("\n").trim().to_owned();
    ensure!(
        found && !notes.is_empty(),
        "nonempty release notes for {version} are missing"
    );
    Ok(format!("{notes}\n"))
}

fn copy_assets(source: &Path, destination: &Path) -> Result<()> {
    ensure!(
        !fs::symlink_metadata(source)?.file_type().is_symlink(),
        "web assets cannot contain symlinks"
    );
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let output = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_assets(&entry.path(), &output)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), output)?;
        } else {
            bail!(
                "web assets must be regular files or directories: {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn checksum(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn verify_crates(inventory: &Inventory, artifacts: &Path, packages: &[String]) -> Result<()> {
    for name in packages {
        let file = format!("{name}-{}.crate", inventory.version);
        ensure!(
            checksum(&inventory.target_directory.join("package").join(&file))?
                == checksum(&artifacts.join(&file))?,
            "packaged crate differs from verified release artifact: {file}"
        );
    }
    Ok(())
}

fn verify_inventory(current: &Inventory, artifacts: &Path) -> Result<()> {
    let reviewed: Inventory = serde_json::from_slice(&fs::read(artifacts.join("release.json"))?)?;
    ensure!(
        reviewed.version == current.version
            && reviewed.source_commit == current.source_commit
            && reviewed.packages == current.packages,
        "release artifact version, source commit, or package set differs from this checkout"
    );
    Ok(())
}

fn cargo(root: &Path, args: &[String]) -> Result<()> {
    let status = Command::new("cargo")
        .args(args)
        .current_dir(root)
        .status()?;
    ensure!(status.success(), "Cargo command failed with {status}");
    Ok(())
}

fn installed(binary: &Path, version: &str) -> Result<()> {
    let output = Command::new(binary).arg("--version").output()?;
    ensure!(
        output.status.success()
            && String::from_utf8_lossy(&output.stdout).trim() == format!("orchestral {version}"),
        "installed executable version mismatch"
    );
    for args in [vec!["--help"], vec!["serve", "--help"]] {
        let output = Command::new(binary).args(args).output()?;
        ensure!(
            output.status.success() && !output.stdout.is_empty(),
            "installed executable help failed"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_notes_use_the_exact_nonempty_section() {
        let changelog = "# Changes\n## [1.20.0] - today\nwrong\n## [1.2.0] - yesterday\n\nReal notes.\n## [1.1.0]\nold\n";
        assert_eq!(release_notes(changelog, "1.2.0").unwrap(), "Real notes.\n");
        assert!(release_notes("## [1.2.0]\n\n## [1.1.0]\nold", "1.2.0").is_err());
        assert!(release_notes(changelog, "1.2.1").is_err());
    }

    #[test]
    fn version_rejects_tag_path_and_prerelease_ambiguity() {
        for value in [
            "",
            "v1.2.3",
            "1.2",
            "01.2.3",
            "1.2.3-rc.1",
            "../1.2.3",
            "1.2.3\n",
        ] {
            assert!(validate_version(value).is_err(), "{value}");
        }
        validate_version("0.3.0").unwrap();
    }

    #[test]
    fn staged_assets_are_independent_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        fs::create_dir_all(source.join("assets")).unwrap();
        fs::write(source.join("index.html"), b"<html>bundle</html>").unwrap();
        fs::write(source.join("assets/module.wasm"), [0, 97, 115, 109, 0, 255]).unwrap();
        let destination = dir.path().join("package/web-dist");
        copy_assets(&source, &destination).unwrap();
        fs::remove_dir_all(source).unwrap();
        assert_eq!(
            fs::read(destination.join("assets/module.wasm")).unwrap(),
            [0, 97, 115, 109, 0, 255]
        );
    }

    #[test]
    fn publication_binds_reviewed_source_and_package_set_not_build_directory() {
        let dir = tempfile::tempdir().unwrap();
        let mut current = Inventory {
            version: "0.3.0".into(),
            source_commit: "reviewed-source".into(),
            packages: vec!["orchestral-cli".into()],
            target_directory: dir.path().join("first-run"),
        };
        fs::write(
            dir.path().join("release.json"),
            serde_json::to_vec(&current).unwrap(),
        )
        .unwrap();
        current.target_directory = dir.path().join("another-run");
        verify_inventory(&current, dir.path()).unwrap();
        current.packages.push("unreviewed-package".into());
        assert!(verify_inventory(&current, dir.path()).is_err());
        current.packages.pop();
        current.source_commit = "another-source".into();
        assert!(verify_inventory(&current, dir.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn asset_staging_rejects_external_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("source")).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", dir.path().join("source/link")).unwrap();
        assert!(copy_assets(&dir.path().join("source"), &dir.path().join("dest")).is_err());
    }
}
