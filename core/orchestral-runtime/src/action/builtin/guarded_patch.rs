//! Host-guarded workspace patch Tool.
//!
//! The model supplies patch intent only. Workspace identity, filesystem roots,
//! approval, effect journaling, and cancellation remain Host-owned.

use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};
use async_trait::async_trait;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions, Permissions};
use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::tool_protocol::{
    EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency,
    ToolOutcome, ToolRestriction,
};
use serde_json::{json, Map, Value};

use super::patch_parser::{
    apply_update_hunks, parse_patch, ParsedPatch, PatchOperation, PatchPath, MAX_PATCH_BYTES,
};
use super::support::canonical_roots;

const MAX_RESULTING_FILE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct GuardedApplyPatchExecutor {
    workspace: PathBuf,
    workspace_dir: Arc<Dir>,
}

impl GuardedApplyPatchExecutor {
    pub fn new(workspace: impl AsRef<Path>) -> io::Result<Self> {
        let workspace = std::fs::canonicalize(workspace)?;
        if !workspace.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "apply_patch workspace must be a directory",
            ));
        }
        let workspace_dir = Dir::open_ambient_dir(&workspace, ambient_authority())?;
        Ok(Self {
            workspace,
            workspace_dir: Arc::new(workspace_dir),
        })
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedApplyPatchExecutor {
    fn approval_summary(
        &self,
        invocation: &orchestral_core::tool_protocol::ToolInvocation,
    ) -> String {
        let Some(patch) = invocation.arguments.get("patch").and_then(Value::as_str) else {
            return "Apply an invalid workspace patch request".to_owned();
        };
        let Ok(parsed) = parse_patch(patch) else {
            return "Apply a workspace patch that failed preflight parsing".to_owned();
        };
        let mut changes = parsed
            .operations
            .iter()
            .take(8)
            .map(|operation| format!("{} {}", operation.label(), operation.path().display()))
            .collect::<Vec<_>>();
        if parsed.operations.len() > changes.len() {
            changes.push(format!(
                "and {} more",
                parsed.operations.len() - changes.len()
            ));
        }
        format!("Apply workspace patch: {}", changes.join(", "))
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let Some(patch) = execution
            .invocation
            .arguments
            .get("patch")
            .and_then(Value::as_str)
        else {
            return rejected("patch_missing", "apply_patch requires a patch string");
        };
        let parsed = match parse_patch(patch) {
            Ok(parsed) => parsed,
            Err(error) => return rejected("patch_invalid", error.to_string()),
        };
        let roots = match EffectiveRoots::from_execution(&execution) {
            Ok(roots) => roots,
            Err(error) => return error.into_outcome(0),
        };
        let prepared = match prepare_patch(&self.workspace_dir, &self.workspace, &roots, parsed) {
            Ok(prepared) => prepared,
            Err(error) => return error.into_outcome(0),
        };
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        if let Err(error) = recheck_preconditions(&prepared) {
            return error.into_outcome(0);
        }

        let mut output = Vec::with_capacity(prepared.len());
        for (index, change) in prepared.iter().enumerate() {
            if execution.cancellation.is_cancelled() {
                return ToolOutcome::Cancelled;
            }
            if let Err(error) = commit_change(change) {
                return error.into_outcome(index);
            }
            output.push(change.output());
        }
        ToolOutcome::Completed {
            output: json!({
                "changed_files": output.len(),
                "changes": output,
            })
            .into(),
        }
    }
}

pub fn guarded_apply_patch_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/apply_patch/v1"),
        model_schema: ModelToolSchema {
            name: "apply_patch".to_owned(),
            description: concat!(
                "Apply a structured patch to UTF-8 text files in the Host-approved workspace. ",
                "The patch must use `*** Begin Patch` / `*** End Patch` and one or more ",
                "exact directives: `*** Add File: path`, `*** Update File: path` with `@@` ",
                "hunks, or `*** Delete File: path`; each path directive requires the colon. ",
                "Paths must be normalized workspace-relative paths."
            )
            .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["patch"],
                "properties": {
                    "patch": {
                        "type": "string",
                        "maxLength": MAX_PATCH_BYTES
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["changed_files", "changes"],
            "properties": {
                "changed_files": { "type": "integer" },
                "changes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["operation", "path", "bytes"],
                        "properties": {
                            "operation": { "type": "string" },
                            "path": { "type": "string" },
                            "bytes": { "type": "integer" },
                            "before_digest": { "type": "string" },
                            "after_digest": { "type": "string" }
                        },
                        "additionalProperties": false
                    }
                }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::FilesystemRead, EffectScope::FilesystemWrite]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

#[derive(Debug)]
struct EffectiveRoots {
    readable: Vec<PathBuf>,
    writable: Vec<PathBuf>,
}

impl EffectiveRoots {
    fn from_execution(execution: &GuardedToolExecution) -> Result<Self, MutationError> {
        let bounds = execution.effective_policy.bounds();
        let readable = canonical_roots(&bounds.filesystem.readable_roots)
            .map_err(|message| MutationError::rejected("patch_workspace_denied", message))?;
        let writable = canonical_roots(&bounds.filesystem.writable_roots)
            .map_err(|message| MutationError::rejected("patch_workspace_denied", message))?;
        if readable.is_empty() || writable.is_empty() {
            return Err(MutationError::rejected(
                "patch_workspace_denied",
                "apply_patch requires Host-approved readable and writable roots",
            ));
        }
        Ok(Self { readable, writable })
    }

    fn authorize(&self, target: &Path) -> Result<(), MutationError> {
        if !self.readable.iter().any(|root| target.starts_with(root))
            || !self.writable.iter().any(|root| target.starts_with(root))
        {
            return Err(MutationError::rejected(
                "patch_path_escape",
                "patch target is outside Host-approved filesystem roots",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CapabilityTarget {
    parent: Dir,
    name: String,
    display: String,
}

#[derive(Debug)]
enum PreparedChange {
    Add {
        path: PatchPath,
        target: CapabilityTarget,
        after: Vec<u8>,
    },
    Update {
        path: PatchPath,
        target: CapabilityTarget,
        before: Vec<u8>,
        after: Vec<u8>,
        permissions: Permissions,
    },
    Delete {
        path: PatchPath,
        target: CapabilityTarget,
        before: Vec<u8>,
    },
}

impl PreparedChange {
    fn target(&self) -> &CapabilityTarget {
        match self {
            Self::Add { target, .. }
            | Self::Update { target, .. }
            | Self::Delete { target, .. } => target,
        }
    }

    fn output(&self) -> Value {
        let (operation, path, before, after) = match self {
            Self::Add { path, after, .. } => ("add", path, None, Some(after.as_slice())),
            Self::Update {
                path,
                before,
                after,
                ..
            } => (
                "update",
                path,
                Some(before.as_slice()),
                Some(after.as_slice()),
            ),
            Self::Delete { path, before, .. } => ("delete", path, Some(before.as_slice()), None),
        };
        let mut value = Map::from_iter([
            ("operation".to_owned(), Value::String(operation.to_owned())),
            ("path".to_owned(), Value::String(path.display().to_owned())),
            (
                "bytes".to_owned(),
                Value::from(after.or(before).map_or(0, <[u8]>::len) as u64),
            ),
        ]);
        if let Some(before) = before {
            value.insert(
                "before_digest".to_owned(),
                Value::String(Digest::sha256(before).to_string()),
            );
        }
        if let Some(after) = after {
            value.insert(
                "after_digest".to_owned(),
                Value::String(Digest::sha256(after).to_string()),
            );
        }
        Value::Object(value)
    }
}

fn prepare_patch(
    workspace_dir: &Dir,
    workspace: &Path,
    roots: &EffectiveRoots,
    patch: ParsedPatch,
) -> Result<Vec<PreparedChange>, MutationError> {
    let mut prepared = Vec::with_capacity(patch.operations.len());
    for operation in patch.operations {
        let path = operation.path();
        let target = resolve_target(workspace_dir, workspace, roots, path)?;
        let change = match operation {
            PatchOperation::Add { path, content } => {
                require_absent_file(&target)?;
                if content.len() > MAX_RESULTING_FILE_BYTES {
                    return Err(file_too_large(path.display(), content.len()));
                }
                PreparedChange::Add {
                    path,
                    target,
                    after: content.into_bytes(),
                }
            }
            PatchOperation::Update { path, hunks } => {
                let (before, permissions) = read_regular_text(&target)?;
                let original = std::str::from_utf8(&before).map_err(|error| {
                    MutationError::rejected(
                        "patch_not_utf8",
                        format!("'{}' is not UTF-8 text: {error}", path.display()),
                    )
                })?;
                let after = apply_update_hunks(original, &hunks)
                    .map_err(|error| {
                        MutationError::rejected(
                            "patch_conflict",
                            format!("'{}': {error}", path.display()),
                        )
                    })?
                    .into_bytes();
                if after.len() > MAX_RESULTING_FILE_BYTES {
                    return Err(file_too_large(path.display(), after.len()));
                }
                PreparedChange::Update {
                    path,
                    target,
                    before,
                    after,
                    permissions,
                }
            }
            PatchOperation::Delete { path } => {
                let (before, _) = read_regular_text(&target)?;
                std::str::from_utf8(&before).map_err(|error| {
                    MutationError::rejected(
                        "patch_not_utf8",
                        format!("'{}' is not UTF-8 text: {error}", path.display()),
                    )
                })?;
                PreparedChange::Delete {
                    path,
                    target,
                    before,
                }
            }
        };
        prepared.push(change);
    }
    Ok(prepared)
}

fn recheck_preconditions(prepared: &[PreparedChange]) -> Result<(), MutationError> {
    for change in prepared {
        let target = change.target();
        match change {
            PreparedChange::Add { .. } => require_absent_file(target)?,
            PreparedChange::Update { before, .. } | PreparedChange::Delete { before, .. } => {
                let (current, _) = read_regular_text(target)?;
                if &current != before {
                    return Err(MutationError::rejected(
                        "patch_conflict",
                        format!("'{}' changed after patch preparation", target.display),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn resolve_target(
    workspace_dir: &Dir,
    workspace: &Path,
    roots: &EffectiveRoots,
    path: &PatchPath,
) -> Result<CapabilityTarget, MutationError> {
    let relative = path.relative();
    let name = relative
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            MutationError::rejected("patch_path_invalid", "patch target has no UTF-8 file name")
        })?
        .to_owned();
    let parent_relative = relative.parent().ok_or_else(|| {
        MutationError::rejected("patch_path_invalid", "patch target has no parent directory")
    })?;
    roots.authorize(&workspace.join(parent_relative))?;
    let parent = open_parent_dir_no_symlinks(workspace_dir, parent_relative, path.display())?;
    match parent.symlink_metadata(&name) {
        Ok(metadata) if metadata.is_symlink() => {
            return Err(MutationError::rejected(
                "patch_path_escape",
                format!("patch target '{}' is a symbolic link", path.display()),
            ))
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(MutationError::failed(
                "patch_metadata_failed",
                format!("inspect '{}': {error}", path.display()),
            ))
        }
    }
    Ok(CapabilityTarget {
        parent,
        name,
        display: path.display().to_owned(),
    })
}

fn open_parent_dir_no_symlinks(
    workspace_dir: &Dir,
    parent_relative: &Path,
    display: &str,
) -> Result<Dir, MutationError> {
    let mut parent = workspace_dir.try_clone().map_err(|error| {
        MutationError::failed(
            "patch_workspace_failed",
            format!("clone workspace directory capability: {error}"),
        )
    })?;
    for component in parent_relative.components() {
        let component = component.as_os_str();
        match parent.symlink_metadata(component) {
            Ok(metadata) if metadata.is_symlink() => {
                return Err(MutationError::rejected(
                    "patch_path_escape",
                    format!("patch path '{display}' traverses a symbolic link"),
                ))
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(MutationError::rejected(
                    "patch_parent_invalid",
                    format!("parent component for '{display}' is not a directory"),
                ))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(MutationError::rejected(
                    "patch_parent_missing",
                    format!("parent directory for '{display}' does not exist"),
                ))
            }
            Err(error) => {
                return Err(MutationError::failed(
                    "patch_metadata_failed",
                    format!("inspect parent for '{display}': {error}"),
                ))
            }
        }
        parent = parent.open_dir(component).map_err(|error| {
            MutationError::rejected(
                "patch_path_escape",
                format!("open parent directory for '{display}' safely: {error}"),
            )
        })?;
    }
    Ok(parent)
}

fn require_absent_file(target: &CapabilityTarget) -> Result<(), MutationError> {
    match target.parent.symlink_metadata(&target.name) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(MutationError::rejected(
            "patch_conflict",
            format!("Add File target '{}' already exists", target.display),
        )),
        Err(error) => Err(MutationError::failed(
            "patch_metadata_failed",
            format!("inspect '{}': {error}", target.display),
        )),
    }
}

fn read_regular_text(target: &CapabilityTarget) -> Result<(Vec<u8>, Permissions), MutationError> {
    let metadata = target
        .parent
        .symlink_metadata(&target.name)
        .map_err(|error| {
            MutationError::rejected(
                "patch_conflict",
                format!("target '{}' is unavailable: {error}", target.display),
            )
        })?;
    if metadata.is_symlink() {
        return Err(MutationError::rejected(
            "patch_path_escape",
            format!("patch target '{}' is a symbolic link", target.display),
        ));
    }
    if !metadata.is_file() {
        return Err(MutationError::rejected(
            "patch_target_invalid",
            format!("target '{}' must be a regular file", target.display),
        ));
    }
    let mut file = target.parent.open(&target.name).map_err(|error| {
        MutationError::failed(
            "patch_read_failed",
            format!("read '{}': {error}", target.display),
        )
    })?;
    let opened_metadata = file.metadata().map_err(|error| {
        MutationError::failed(
            "patch_metadata_failed",
            format!("inspect opened '{}': {error}", target.display),
        )
    })?;
    if !opened_metadata.is_file() {
        return Err(MutationError::rejected(
            "patch_target_invalid",
            format!("target '{}' must be a regular file", target.display),
        ));
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(opened_metadata.len())
            .unwrap_or(MAX_RESULTING_FILE_BYTES + 1)
            .min(MAX_RESULTING_FILE_BYTES + 1),
    );
    Read::by_ref(&mut file)
        .take((MAX_RESULTING_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            MutationError::failed(
                "patch_read_failed",
                format!("read '{}': {error}", target.display),
            )
        })?;
    if bytes.len() > MAX_RESULTING_FILE_BYTES {
        return Err(file_too_large(&target.display, bytes.len()));
    }
    Ok((bytes, opened_metadata.permissions()))
}

fn commit_change(change: &PreparedChange) -> Result<(), MutationError> {
    match change {
        PreparedChange::Add { target, after, .. } => atomic_write(target, after, None, true),
        PreparedChange::Update {
            target,
            after,
            permissions,
            ..
        } => atomic_write(target, after, Some(permissions.clone()), false),
        PreparedChange::Delete { target, .. } => {
            target.parent.remove_file(&target.name).map_err(|error| {
                MutationError::failed(
                    "patch_delete_failed",
                    format!("delete '{}': {error}", target.display),
                )
            })?;
            sync_parent(target).map_err(|error| {
                MutationError::unknown(format!(
                    "deleted '{}' but directory durability is unknown: {}",
                    target.display, error.message
                ))
            })?;
            Ok(())
        }
    }
}

fn atomic_write(
    target: &CapabilityTarget,
    content: &[u8],
    permissions: Option<Permissions>,
    create_only: bool,
) -> Result<(), MutationError> {
    let temp = format!(
        ".orchestral-patch-{}-{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4()
    );
    let mut guard = TempFileGuard::new(
        target
            .parent
            .try_clone()
            .map_err(|error| mutation_io("patch_stage_failed", &target.display, error))?,
        temp.clone(),
    );
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = target
        .parent
        .open_with(&temp, &options)
        .map_err(|error| mutation_io("patch_stage_failed", &target.display, error))?;
    file.write_all(content)
        .map_err(|error| mutation_io("patch_stage_failed", &target.display, error))?;
    file.flush()
        .map_err(|error| mutation_io("patch_stage_failed", &target.display, error))?;
    file.sync_all()
        .map_err(|error| mutation_io("patch_stage_failed", &target.display, error))?;
    if let Some(permissions) = permissions {
        file.set_permissions(permissions)
            .map_err(|error| mutation_io("patch_stage_failed", &target.display, error))?;
    }
    drop(file);
    if create_only {
        target
            .parent
            .hard_link(&temp, &target.parent, &target.name)
            .map_err(|error| mutation_io("patch_add_failed", &target.display, error))?;
        if let Err(error) = target.parent.remove_file(&temp) {
            return Err(MutationError::unknown(format!(
                "created '{}' but cleanup of its staged link failed: {error}",
                target.display
            )));
        }
        guard.disarm();
    } else {
        target
            .parent
            .rename(&temp, &target.parent, &target.name)
            .map_err(|error| mutation_io("patch_update_failed", &target.display, error))?;
        guard.disarm();
    }
    sync_parent(target).map_err(|error| {
        MutationError::unknown(format!(
            "changed '{}' but directory durability is unknown: {}",
            target.display, error.message
        ))
    })?;
    Ok(())
}

fn sync_parent(target: &CapabilityTarget) -> Result<(), MutationError> {
    #[cfg(unix)]
    {
        target
            .parent
            .try_clone()
            .map(Dir::into_std_file)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| mutation_io("patch_sync_failed", &target.display, error))?;
    }
    Ok(())
}

struct TempFileGuard {
    parent: Dir,
    name: Option<String>,
}

impl TempFileGuard {
    fn new(parent: Dir, name: String) -> Self {
        Self {
            parent,
            name: Some(name),
        }
    }

    fn disarm(&mut self) {
        self.name = None;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            let _ = self.parent.remove_file(name);
        }
    }
}

fn file_too_large(path: &str, bytes: usize) -> MutationError {
    MutationError::rejected(
        "patch_file_too_large",
        format!(
            "'{path}' would contain {bytes} bytes, exceeding the {MAX_RESULTING_FILE_BYTES}-byte limit"
        ),
    )
}

fn mutation_io(code: &'static str, display: &str, error: io::Error) -> MutationError {
    MutationError::failed(code, format!("'{display}': {error}"))
}

#[derive(Debug)]
struct MutationError {
    code: &'static str,
    message: String,
    kind: MutationErrorKind,
}

#[derive(Debug, Clone, Copy)]
enum MutationErrorKind {
    Rejected,
    Failed,
    Unknown,
}

impl MutationError {
    fn rejected(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: MutationErrorKind::Rejected,
        }
    }

    fn failed(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: MutationErrorKind::Failed,
        }
    }

    fn unknown(message: impl Into<String>) -> Self {
        Self {
            code: "patch_effect_unknown",
            message: message.into(),
            kind: MutationErrorKind::Unknown,
        }
    }

    fn into_outcome(self, prior_commits: usize) -> ToolOutcome {
        if prior_commits > 0 {
            return ToolOutcome::UnknownEffect {
                message: format!(
                    "apply_patch committed {prior_commits} earlier file change(s) before '{}': {}",
                    self.code, self.message
                ),
            };
        }
        match self.kind {
            MutationErrorKind::Rejected => rejected(self.code, self.message),
            MutationErrorKind::Failed => ToolOutcome::Failed {
                code: self.code.to_owned(),
                message: self.message,
                retryable: false,
            },
            MutationErrorKind::Unknown => ToolOutcome::UnknownEffect {
                message: self.message,
            },
        }
    }
}

fn rejected(code: impl Into<String>, message: impl Into<String>) -> ToolOutcome {
    ToolOutcome::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::PathBuf;

    use cap_std::{ambient_authority, fs::Dir};

    use super::{commit_change, parse_patch, prepare_patch, EffectiveRoots};

    #[test]
    fn captured_parent_capability_prevents_symlink_swap_escape() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-patch-race-workspace-{}",
            uuid::Uuid::new_v4()
        ));
        let outside = std::env::temp_dir().join(format!(
            "orchestral-patch-race-outside-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let workspace_dir = Dir::open_ambient_dir(&root, ambient_authority()).unwrap();
        let roots = EffectiveRoots {
            readable: vec![PathBuf::from(&root)],
            writable: vec![PathBuf::from(&root)],
        };
        let patch = parse_patch(
            "*** Begin Patch\n*** Add File: nested/created.txt\n+inside capability\n*** End Patch",
        )
        .unwrap();
        let prepared = prepare_patch(&workspace_dir, &root, &roots, patch).unwrap();

        std::fs::rename(root.join("nested"), root.join("detached")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("nested")).unwrap();
        commit_change(&prepared[0]).unwrap();

        assert_eq!(
            std::fs::read_to_string(root.join("detached/created.txt")).unwrap(),
            "inside capability\n"
        );
        assert!(!outside.join("created.txt").exists());

        std::fs::remove_file(root.join("nested")).unwrap();
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }
}
