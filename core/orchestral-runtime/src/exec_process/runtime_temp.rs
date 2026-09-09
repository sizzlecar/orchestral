//! Host-owned temporary storage, separate from workspace authority.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::{Digest, RunId};

use super::ExecProcessError;

pub(super) struct RuntimeTempRoot {
    path: PathBuf,
    _owner: Option<tempfile::TempDir>,
}

impl RuntimeTempRoot {
    pub(super) fn temporary() -> Result<Self, ExecProcessError> {
        #[cfg(unix)]
        let parent = PathBuf::from("/tmp");
        #[cfg(not(unix))]
        let parent = std::env::temp_dir();
        let owner = tempfile::Builder::new()
            .prefix("orch-")
            .tempdir_in(parent)
            .map_err(io_error)?;
        let path = std::fs::canonicalize(owner.path()).map_err(io_error)?;
        Ok(Self {
            path,
            _owner: Some(owner),
        })
    }

    pub(super) fn open(path: &Path) -> Result<Self, ExecProcessError> {
        if !path.is_absolute() {
            return Err(ExecProcessError::Invalid(
                "Host runtime temp root must be absolute".into(),
            ));
        }
        match private_directory(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error(error)),
        }
        let metadata = std::fs::symlink_metadata(path).map_err(io_error)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ExecProcessError::Invalid(
                "Host runtime temp root must be a real directory".into(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // SAFETY: geteuid has no preconditions and does not mutate state.
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
                return Err(ExecProcessError::Invalid(
                    "Host runtime temp root must be private and owned by the current user".into(),
                ));
            }
        }
        Ok(Self {
            path: std::fs::canonicalize(path).map_err(io_error)?,
            _owner: None,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn run_path(&self, run_id: &RunId) -> PathBuf {
        // Run identities are opaque; never interpolate them as path components.
        // 128 bits retain an opaque identity while leaving room for Unix
        // socket names and nested temporary directories used by child tools.
        self.path
            .join(&Digest::sha256(run_id.as_str()).as_str()[..32])
    }

    pub(super) fn create_run(
        self: &Arc<Self>,
        run_id: &RunId,
    ) -> Result<Arc<RuntimeTempDirectory>, ExecProcessError> {
        let path = self.run_path(run_id);
        // Never adopt another Host's live directory or leftovers from a crash.
        private_directory(&path).map_err(io_error)?;
        Ok(Arc::new(RuntimeTempDirectory {
            path,
            _root: self.clone(),
        }))
    }
}

pub(super) struct RuntimeTempDirectory {
    path: PathBuf,
    _root: Arc<RuntimeTempRoot>,
}

impl RuntimeTempDirectory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for RuntimeTempDirectory {
    fn drop(&mut self) {
        let path = self.path.clone();
        let root = self._root.clone();
        let cleanup = move || {
            if let Err(error) = std::fs::remove_dir_all(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %path.display(), %error, "remove Run temporary directory");
                }
            }
            drop(root);
        };
        // A Run may have produced many temporary files. Reclamation must not
        // block the Agent's async execution path.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(cleanup);
        } else {
            cleanup();
        }
    }
}

fn private_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

fn io_error(error: std::io::Error) -> ExecProcessError {
    ExecProcessError::Io(format!("runtime temporary directory: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn run_temp_paths_leave_room_for_child_tools_unix_sockets() {
        let root = Arc::new(RuntimeTempRoot::temporary().unwrap());
        let run = root.create_run(&RunId::new("socket-run")).unwrap();
        let nested = tempfile::tempdir_in(run.path()).unwrap();
        let _listener = std::os::unix::net::UnixListener::bind(nested.path().join("control.sock"))
            .expect("child tools must be able to create Unix sockets under the Run temp directory");
    }

    #[test]
    fn stable_roots_preserve_policy_identity_and_never_adopt_existing_run_files() {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("host");
        let first = Arc::new(RuntimeTempRoot::open(&path).unwrap());
        let restarted = Arc::new(RuntimeTempRoot::open(&path).unwrap());
        let run = RunId::new("../../opaque/run");
        let planned = first.run_path(&run);
        assert_eq!(planned, restarted.run_path(&run));
        assert_eq!(planned.parent(), Some(first.path()));
        assert!(!planned.exists(), "planning must not create Run storage");
        let directory = first.create_run(&run).unwrap();
        std::fs::write(directory.path().join("evidence"), "retain").unwrap();
        assert!(restarted.create_run(&run).is_err());
        assert_eq!(
            std::fs::read_to_string(planned.join("evidence")).unwrap(),
            "retain"
        );
        drop(directory);
        assert!(!planned.exists());
        drop(first);
        drop(restarted);
        assert!(path.is_dir(), "the stable Host root outlives Run files");
    }

    #[cfg(unix)]
    #[test]
    fn host_roots_reject_symlinks_and_shared_permissions_without_mutating_them() {
        use std::os::unix::fs::PermissionsExt;
        let parent = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let link = parent.path().join("link");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        assert!(RuntimeTempRoot::open(&link).is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
        let shared = parent.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(RuntimeTempRoot::open(&shared).is_err());
        assert_eq!(
            std::fs::metadata(&shared).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
