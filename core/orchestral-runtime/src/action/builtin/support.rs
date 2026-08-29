use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;
use std::path::{Component, Path, PathBuf};

use tokio::io::AsyncReadExt;

/// Canonicalize Host-owned filesystem roots once before target checks.
/// Model-visible paths never participate in this normalization.
pub(super) fn canonical_roots(roots: &BTreeSet<String>) -> Result<Vec<PathBuf>, String> {
    roots
        .iter()
        .map(|root| {
            std::fs::canonicalize(PathBuf::from(root))
                .map_err(|error| format!("canonicalize policy root '{root}' failed: {error}"))
        })
        .collect()
}

/// Host-bound workspace identity shared by built-in inspection Tools.
///
/// Model paths are always workspace-relative. Existing targets are resolved
/// through the filesystem and then checked against both the composition-time
/// workspace and the invocation's effective readable roots. This makes path
/// traversal and symlink escape checks identical for read and search.
#[derive(Debug, Clone)]
pub(super) struct GuardedWorkspace {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AuthorizedWorkspacePath {
    canonical: PathBuf,
    relative: PathBuf,
}

impl AuthorizedWorkspacePath {
    pub(super) fn canonical(&self) -> &Path {
        &self.canonical
    }

    pub(super) fn display(&self) -> String {
        display_relative_path(&self.relative)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WorkspacePathError {
    Rejected { code: &'static str, message: String },
    Failed { code: &'static str, message: String },
}

impl GuardedWorkspace {
    pub(super) fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = std::fs::canonicalize(root)?;
        if !root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "guarded workspace must be a directory",
            ));
        }
        Ok(Self { root })
    }

    pub(super) fn resolve_existing(
        &self,
        raw_path: &str,
        readable_roots: &[PathBuf],
    ) -> Result<AuthorizedWorkspacePath, WorkspacePathError> {
        let relative = normalize_workspace_relative(raw_path)?;
        let unresolved = self.root.join(&relative);
        let canonical =
            std::fs::canonicalize(&unresolved).map_err(|error| WorkspacePathError::Failed {
                code: "workspace_path_unavailable",
                message: format!("resolve '{}': {error}", display_relative_path(&relative)),
            })?;
        if !canonical.starts_with(&self.root)
            || !readable_roots
                .iter()
                .any(|root| canonical.starts_with(root))
        {
            return Err(WorkspacePathError::Rejected {
                code: "workspace_path_escape",
                message: format!(
                    "resolved path '{}' is outside Host-approved readable workspace roots",
                    display_relative_path(&relative)
                ),
            });
        }
        Ok(AuthorizedWorkspacePath {
            canonical,
            relative,
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn display_path(&self, canonical: &Path) -> String {
        canonical
            .strip_prefix(&self.root)
            .map(display_relative_path)
            .unwrap_or_else(|_| canonical.to_string_lossy().replace('\\', "/"))
    }
}

fn normalize_workspace_relative(raw_path: &str) -> Result<PathBuf, WorkspacePathError> {
    let raw_path = raw_path.trim();
    if raw_path.is_empty() {
        return Err(WorkspacePathError::Rejected {
            code: "workspace_path_missing",
            message: "workspace path must be a non-empty relative path".to_owned(),
        });
    }
    let path = Path::new(raw_path);
    if path.is_absolute() {
        return Err(WorkspacePathError::Rejected {
            code: "workspace_path_absolute",
            message: "workspace path must be relative to the composed workspace".to_owned(),
        });
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspacePathError::Rejected {
                    code: "workspace_path_escape",
                    message: "workspace path contains traversal or a platform root".to_owned(),
                })
            }
        }
    }
    Ok(normalized)
}

pub(super) fn display_relative_path(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    if path.is_empty() {
        ".".to_owned()
    } else {
        path
    }
}

/// Build the exact process environment authorized by the Host.
///
/// The ambient environment is never inherited wholesale. Sandbox-owned values
/// are appended after the allowlisted Host values.
pub(super) fn build_allowlisted_env(
    allowlist: &HashSet<String>,
    sandbox_environment: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut environment = HashMap::new();
    for name in allowlist {
        if let Ok(value) = std::env::var(name) {
            environment.insert(name.clone(), value);
        }
    }
    environment.extend(sandbox_environment.clone());
    environment
}

pub(super) async fn read_stream_limited<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buffer = [0_u8; 8192];
    let mut kept = Vec::new();
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(kept.len());
        let copy = remaining.min(read);
        kept.extend_from_slice(&buffer[..copy]);
        truncated |= copy < read;
    }
    Ok((kept, truncated))
}

pub(super) fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> (String, bool, usize) {
    let total = bytes.len();
    let kept = bytes.get(..max_bytes.min(total)).unwrap_or(bytes);
    (
        String::from_utf8_lossy(kept).into_owned(),
        total > max_bytes,
        total,
    )
}

pub(super) fn stderr_preview(stderr: &str, max_chars: usize) -> String {
    let compact = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return "<empty>".to_owned();
    }
    if compact.chars().count() <= max_chars {
        return compact;
    }
    format!("{}...", compact.chars().take(max_chars).collect::<String>())
}
