//! Exact, read-only Host files used by installed developer tools.

use std::path::PathBuf;

/// Capture existing non-secret developer-tool cache files for an executor's
/// `runtime_readable_files`. This never creates a cache or grants writes to it
/// or its parent directory. The returned paths belong in the executor's frozen
/// planning contract, not in model-selected workspace authority.
pub fn host_toolchain_readable_files() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        macos::existing_xcrun_cache().into_iter().collect()
    }
    #[cfg(not(target_os = "macos"))]
    {
        Vec::new()
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};

    pub(super) fn existing_xcrun_cache() -> Option<PathBuf> {
        // xcrun uses Darwin's per-user directory even when TMPDIR points to a
        // Run's private temporary storage. Missing read access to an existing
        // cache can force compiler SDK lookups to attempt rebuilding it there.
        let mut buffer = vec![0_u8; libc::PATH_MAX as usize + 1];
        // SAFETY: buffer is writable for the supplied length; confstr writes
        // a NUL-terminated path when its reported size fits that buffer.
        let length = unsafe {
            libc::confstr(
                libc::_CS_DARWIN_USER_TEMP_DIR,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if length <= 1 || length > buffer.len() {
            return None;
        }
        let directory = std::ffi::CStr::from_bytes_with_nul(&buffer[..length]).ok()?;
        existing_cache_in(Path::new(std::ffi::OsStr::from_bytes(directory.to_bytes())))
    }

    fn existing_cache_in(directory: &Path) -> Option<PathBuf> {
        if !directory.is_absolute() {
            return None;
        }
        let path = std::fs::canonicalize(directory).ok()?.join("xcrun_db");
        let metadata = std::fs::symlink_metadata(&path).ok()?;
        // Do not turn a cache symlink or a file writable by other users into
        // access to another Host resource. No parent or temporary cache-file
        // pattern is exposed, including when the real cache is absent.
        // SAFETY: geteuid has no preconditions and does not mutate state.
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o022 != 0
        {
            return None;
        }
        Some(path)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        #[test]
        fn cache_discovery_returns_only_an_existing_private_regular_file() {
            let directory = tempfile::tempdir().unwrap();
            assert_eq!(existing_cache_in(directory.path()), None);
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
            let cache = directory.path().join("xcrun_db");
            std::fs::write(&cache, b"opaque developer-tool cache").unwrap();
            std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::write(directory.path().join("unrelated"), b"private").unwrap();
            assert_eq!(
                existing_cache_in(directory.path()),
                Some(cache.canonicalize().unwrap())
            );
            std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o620)).unwrap();
            assert_eq!(existing_cache_in(directory.path()), None);
        }

        #[test]
        fn cache_discovery_rejects_symlinks_and_directories() {
            let directory = tempfile::tempdir().unwrap();
            let outside = tempfile::NamedTempFile::new().unwrap();
            let cache = directory.path().join("xcrun_db");
            std::os::unix::fs::symlink(outside.path(), &cache).unwrap();
            assert_eq!(existing_cache_in(directory.path()), None);
            std::fs::remove_file(&cache).unwrap();
            std::fs::create_dir(&cache).unwrap();
            assert_eq!(existing_cache_in(directory.path()), None);
        }

        #[test]
        fn exact_cache_grant_keeps_siblings_and_cache_writes_denied() {
            const CHILD_CACHE: &str = "ORCHESTRAL_TEST_TOOLCHAIN_CACHE";
            if let Some(path) = std::env::var_os(CHILD_CACHE) {
                let cache = PathBuf::from(path);
                let contents = std::fs::read(&cache).unwrap();
                assert_eq!(contents, b"opaque cache");
                for error in [
                    std::fs::read(cache.with_file_name("private-sibling")).unwrap_err(),
                    std::fs::write(&cache, b"changed").unwrap_err(),
                    std::fs::write(cache.with_file_name("xcrun_db-new"), b"changed").unwrap_err(),
                ] {
                    assert!(matches!(error.kind(), std::io::ErrorKind::PermissionDenied));
                }
                std::fs::write("cache-copy", contents).unwrap();
                return;
            }

            let workspace = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let cache = outside.path().join("xcrun_db");
            std::fs::write(&cache, b"opaque cache").unwrap();
            std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::write(outside.path().join("private-sibling"), b"private").unwrap();
            let executable = std::env::current_exe().unwrap();
            let sandbox = crate::tools::shell_sandbox::sandbox_command(
                executable.to_string_lossy().into_owned(),
                vec![
                    "--exact".to_owned(),
                    concat!(
                        "tools::toolchain::macos::tests::",
                        "exact_cache_grant_keeps_siblings_and_cache_writes_denied"
                    )
                    .to_owned(),
                    "--nocapture".to_owned(),
                ],
                workspace.path(),
                &crate::tools::shell_sandbox::ShellSandboxPolicy {
                    readable_roots: vec![workspace.path().to_owned()],
                    readable_files: existing_cache_in(outside.path()).into_iter().collect(),
                    writable_roots: vec![workspace.path().to_owned()],
                    launcher_programs: vec![executable],
                    ..Default::default()
                },
            )
            .unwrap();
            let output = std::process::Command::new(sandbox.program)
                .args(sandbox.args)
                .env_clear()
                .envs(sandbox.env)
                .env(CHILD_CACHE, &cache)
                .current_dir(workspace.path())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "exact-file sandbox probe failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(std::fs::read(&cache).unwrap(), b"opaque cache");
            assert_eq!(
                std::fs::read(workspace.path().join("cache-copy")).unwrap(),
                std::fs::read(&cache).unwrap()
            );
            assert!(!cache.with_file_name("xcrun_db-new").exists());
        }
    }
}
