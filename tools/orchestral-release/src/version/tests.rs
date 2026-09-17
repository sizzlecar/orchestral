use super::*;

fn command(root: &Path, program: &str, args: &[&str]) {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{program}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::write(
        root.join("Cargo.toml"),
        r#"# Keep this comment and dependency formatting.
[workspace]
resolver = "2"
members = ["sdk", "app", "testkit"]
[workspace.package]
version = "0.3.1"
edition = "2021"
[workspace.dependencies]
sdk = { path = "sdk", version = "0.3.1" } # keep this too
testkit = { path = "testkit", version = "0.1.0" }
unused-registry = "1.2.3"
unused-registry-detail = { version = "4.5.6", features = [] }
"#,
    )
    .unwrap();
    for (name, manifest) in [
        ("sdk", "[package]\nname = \"sdk\"\nversion.workspace = true\nedition.workspace = true\n"),
        ("app", "[package]\nname = \"app\"\nversion.workspace = true\nedition.workspace = true\n[dependencies]\nsdk.workspace = true\n"),
        ("testkit", "[package]\nname = \"testkit\"\nversion = \"0.1.0\"\nedition = \"2021\"\npublish = false\n"),
    ] {
        fs::create_dir_all(root.join(name).join("src")).unwrap();
        fs::write(root.join(name).join("Cargo.toml"), manifest).unwrap();
        fs::write(root.join(name).join("src/lib.rs"), "").unwrap();
    }
    fs::write(root.join("CHANGELOG.md"), "# Changelog\n\n## [Unreleased]\n\n- Reviewed behavior change.\n\n## [0.3.1]\n\nEarlier release.\n").unwrap();
    command(root, "git", &["init", "--quiet"]);
    command(
        root,
        "git",
        &[
            "-c",
            "user.name=Release fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    );
    command(root, "cargo", &["generate-lockfile", "--offline"]);
    directory
}

fn snapshot(root: &Path) -> Vec<String> {
    ["Cargo.toml", "Cargo.lock", "CHANGELOG.md"]
        .into_iter()
        .map(|file| fs::read_to_string(root.join(file)).unwrap())
        .collect()
}

#[test]
fn prepares_real_workspace_without_changing_independent_packages_or_notes() {
    let fixture = fixture();
    let root = fixture.path();
    let original = snapshot(root);
    prepare(root, "0.3.1", "0.4.0").unwrap();
    let updated = snapshot(root);
    assert_eq!(updated[0], original[0].replace("\"0.3.1\"", "\"0.4.0\""));
    assert_eq!(
        updated[2],
        original[2].replace("## [Unreleased]", "## [0.4.0]")
    );
    assert!(updated[1].contains("name = \"testkit\"\nversion = \"0.1.0\""));
    let inventory = super::super::inventory(root).unwrap();
    assert_eq!(inventory.version, "0.4.0");
    assert_eq!(inventory.packages, ["app", "sdk"]);
    command(
        root,
        "cargo",
        &["metadata", "--locked", "--offline", "--format-version", "1"],
    );
}

#[test]
fn same_version_downgrades_and_non_stable_versions_are_rejected_before_changes() {
    let fixture = fixture();
    let original = snapshot(fixture.path());
    for target in [
        "0.3.1",
        "0.3.0",
        "0.4.0-rc.1",
        "0.4.0+build",
        "v0.4.0",
        "00.4.0",
        "0.4",
        "18446744073709551616.0.0",
    ] {
        assert!(
            prepare(fixture.path(), "0.3.1", target).is_err(),
            "accepted {target}"
        );
        assert_eq!(snapshot(fixture.path()), original);
    }
    assert!(stable_version("0.10.0").unwrap() > stable_version("0.9.9").unwrap());
}

#[test]
fn empty_missing_duplicate_unreleased_and_existing_target_notes_are_rejected() {
    for changelog in [
        "## [Unreleased]\n \n## [0.3.1]\nold\n",
        "## [0.3.1]\nold\n",
        "## [Unreleased]\nnotes\n## [Unreleased]\nother\n",
        "## [Unreleased]\nnotes\n## [0.4.0]\nexists\n",
        "## [Unreleased]\nnotes\n## [0.4.0] - 2026-09-18\nexists\n",
    ] {
        assert!(promote_notes(changelog, "0.4.0").is_err());
    }
}

#[test]
fn lock_expectation_keeps_registry_versions_checksums_and_independent_packages() {
    let lock = r#"version = 4
[[package]]
name = "sdk"
version = "0.3.1"
[[package]]
name = "sdk"
version = "0.3.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "unchanged"
[[package]]
name = "testkit"
version = "0.1.0"
dependencies = ["sdk 0.3.1", "sdk 0.3.1 (registry+https://github.com/rust-lang/crates.io-index)"]
"#;
    let expected =
        expected_lock(lock, &BTreeSet::from(["sdk".to_owned()]), "0.3.1", "0.4.0").unwrap();
    let packages = expected["package"].as_array().unwrap();
    assert_eq!(packages[0]["version"].as_str(), Some("0.4.0"));
    assert_eq!(packages[1]["version"].as_str(), Some("0.3.1"));
    assert_eq!(packages[1]["checksum"].as_str(), Some("unchanged"));
    assert_eq!(packages[2]["version"].as_str(), Some("0.1.0"));
    assert_eq!(packages[2]["dependencies"][0].as_str(), Some("sdk 0.4.0"));
    assert_eq!(
        packages[2]["dependencies"][1].as_str(),
        Some("sdk 0.3.1 (registry+https://github.com/rust-lang/crates.io-index)")
    );
}

#[test]
fn unexpected_lock_changes_restore_all_original_files() {
    let fixture = fixture();
    let root = fixture.path();
    // Cargo removes an unused entry when updating. Even this unrelated cleanup
    // is outside the release command's scope and must leave all inputs intact.
    let mut lock = fs::read_to_string(root.join("Cargo.lock")).unwrap();
    lock.push_str("\n[[package]]\nname = \"unused-independent-package\"\nversion = \"9.9.9\"\n");
    fs::write(root.join("Cargo.lock"), lock).unwrap();
    let original = snapshot(root);
    let error = prepare(root, "0.3.1", "0.4.0").unwrap_err();
    assert!(
        error.to_string().contains("beyond the inherited workspace"),
        "{error:#}"
    );
    assert_eq!(snapshot(root), original);
}
