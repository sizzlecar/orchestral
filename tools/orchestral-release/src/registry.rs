use crate::{cargo, checksum, verify_crates, verify_inventory, Inventory};
use anyhow::{bail, ensure, Context, Result};
use std::path::Path;
use std::time::Duration;

fn index_path(name: &str) -> String {
    match name.len() {
        1 => format!("1/{name}"),
        2 => format!("2/{name}"),
        3 => format!("3/{}/{name}", &name[..1]),
        _ => format!("{}/{}/{name}", &name[..2], &name[2..4]),
    }
}

fn existing_checksum(index: &str, version: &str) -> Result<Option<String>> {
    for line in index.lines().filter(|line| !line.trim().is_empty()) {
        let entry: serde_json::Value = serde_json::from_str(line)?;
        if entry["vers"] == version {
            ensure!(entry["yanked"] == false, "version {version} is yanked");
            return Ok(Some(
                entry["cksum"]
                    .as_str()
                    .context("registry checksum")?
                    .to_owned(),
            ));
        }
    }
    Ok(None)
}

pub(super) fn publish(root: &Path, inventory: &Inventory, artifacts: &Path) -> Result<()> {
    verify_inventory(inventory, artifacts)?;
    let client = reqwest::blocking::Client::builder()
        .user_agent("orchestral-release (https://github.com/sizzlecar/orchestral)")
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut missing = Vec::new();
    for package in &inventory.packages {
        let archive = artifacts.join(format!("{package}-{}.crate", inventory.version));
        let expected = checksum(&archive)?;
        let response = client
            .get(format!("https://index.crates.io/{}", index_path(package)))
            .send()?;
        let existing = match response.status() {
            reqwest::StatusCode::NOT_FOUND => None,
            status if status.is_success() => {
                existing_checksum(&response.text()?, &inventory.version)?
            }
            status => {
                bail!("registry lookup for {package} failed with {status}; not treated as missing")
            }
        };
        match existing {
            Some(actual) => ensure!(
                actual == expected,
                "published {package} differs from the reviewed crate"
            ),
            None => missing.push(package.clone()),
        }
    }
    if missing.is_empty() {
        println!("All reviewed crate versions are already published with matching checksums.");
        return Ok(());
    }
    let mut arguments = vec![
        "publish".into(),
        "--registry".into(),
        "crates-io".into(),
        "--locked".into(),
        "--allow-dirty".into(),
        "--no-verify".into(),
    ];
    for package in &missing {
        arguments.extend(["-p".into(), package.clone()]);
    }
    let mut dry_run = arguments.clone();
    dry_run.push("--dry-run".into());
    cargo(root, &dry_run)?;
    verify_crates(inventory, artifacts, &missing)?;
    // Cargo orders the selected workspace packages by their dependencies and
    // waits for index availability. Tokens stay in Cargo's normal environment.
    cargo(root, &arguments)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_path_handles_all_crate_name_lengths() {
        assert_eq!(index_path("a"), "1/a");
        assert_eq!(index_path("ab"), "2/ab");
        assert_eq!(index_path("abc"), "3/a/abc");
        assert_eq!(index_path("orchestral-cli"), "or/ch/orchestral-cli");
    }

    #[test]
    fn existing_versions_require_exact_version_and_non_yanked_bytes() {
        let index = "{\"vers\":\"0.2.0\",\"yanked\":false,\"cksum\":\"old\"}\n{\"vers\":\"0.3.0\",\"yanked\":false,\"cksum\":\"new\"}\n";
        assert_eq!(
            existing_checksum(index, "0.3.0").unwrap().as_deref(),
            Some("new")
        );
        assert_eq!(existing_checksum(index, "0.4.0").unwrap(), None);
        assert!(existing_checksum(
            "{\"vers\":\"0.3.0\",\"yanked\":true,\"cksum\":\"x\"}",
            "0.3.0"
        )
        .is_err());
        assert!(existing_checksum("not an index response", "0.3.0").is_err());
    }
}
