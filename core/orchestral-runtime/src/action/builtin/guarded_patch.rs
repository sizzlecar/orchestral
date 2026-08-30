//! Host-guarded workspace patch Tool.
//!
//! The model supplies patch intent only. Workspace identity, filesystem roots,
//! approval, effect journaling, and cancellation remain Host-owned.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};
use async_trait::async_trait;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions, Permissions};
use orchestral_core::agent_protocol::wire::{
    Digest, ToolActivityEvidence, ToolDiffLine, ToolDiffLineKind, ToolFileActivityKind,
};
use orchestral_core::tool_protocol::{
    CapabilityRequest, CapabilitySelector, EffectScope, ModelToolSchema, ToolConcurrency,
    ToolDescriptor, ToolId, ToolIdempotency, ToolInvocation, ToolOperationPlan, ToolOperationRisk,
    ToolOutcome, ToolRestriction,
};
use serde_json::{json, Map, Value};

use super::patch_parser::{
    apply_update_hunks, parse_patch, parse_path, ParsedPatch, PatchOperation, PatchPath,
    MAX_PATCH_BYTES,
};
use super::support::canonical_roots;

const MAX_RESULTING_FILE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
struct MutationWorkspace {
    root: PathBuf,
    selector: String,
    dir: Arc<Dir>,
}

#[derive(Debug, Clone)]
struct MutationWorkspaceSet {
    primary: String,
    by_selector: BTreeMap<String, MutationWorkspace>,
}

impl MutationWorkspaceSet {
    fn new<I, P>(primary: impl AsRef<Path>, additional: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        let primary = Self::open(primary)?;
        let primary_selector = primary.selector.clone();
        let mut by_selector = BTreeMap::from([(primary_selector.clone(), primary)]);
        for root in additional {
            let workspace = Self::open(root)?;
            by_selector
                .entry(workspace.selector.clone())
                .or_insert(workspace);
        }
        Ok(Self {
            primary: primary_selector,
            by_selector,
        })
    }

    fn open(root: impl AsRef<Path>) -> io::Result<MutationWorkspace> {
        let root = std::fs::canonicalize(root)?;
        if !root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mutation workspace must be a directory",
            ));
        }
        let selector = root.to_string_lossy().into_owned();
        let dir = Arc::new(Dir::open_ambient_dir(&root, ambient_authority())?);
        Ok(MutationWorkspace {
            root,
            selector,
            dir,
        })
    }

    fn select(&self, invocation: &ToolInvocation) -> Result<&MutationWorkspace, ToolOutcome> {
        let selector = invocation
            .arguments
            .get("workspace")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|selector| !selector.is_empty())
            .unwrap_or(&self.primary);
        self.by_selector.get(selector).ok_or_else(|| {
            rejected(
                "workspace_unknown",
                format!(
                    "workspace must be one of the exact Host-provided roots: {}",
                    self.by_selector
                        .keys()
                        .map(|root| format!("'{root}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        })
    }

    fn primary(&self) -> &MutationWorkspace {
        self.by_selector
            .get(&self.primary)
            .expect("primary workspace is inserted during construction")
    }
}

#[derive(Debug, Clone)]
pub struct GuardedFileWriteExecutor {
    workspaces: MutationWorkspaceSet,
}

impl GuardedFileWriteExecutor {
    pub fn new(workspace: impl AsRef<Path>) -> io::Result<Self> {
        Self::new_with_roots(workspace, std::iter::empty::<PathBuf>())
    }

    pub fn new_with_roots<I, P>(primary: impl AsRef<Path>, additional: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        Ok(Self {
            workspaces: MutationWorkspaceSet::new(primary, additional)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileWriteMode {
    Create,
    Replace,
}

impl FileWriteMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "create" => Some(Self::Create),
            "replace" => Some(Self::Replace),
            _ => None,
        }
    }
}

struct FileWriteRequest<'a> {
    path: PatchPath,
    content: &'a str,
    mode: FileWriteMode,
    expected_digest: Option<&'a str>,
}

impl<'a> FileWriteRequest<'a> {
    fn parse(invocation: &'a ToolInvocation) -> Result<Self, ToolOutcome> {
        let arguments = &invocation.arguments;
        let path = arguments
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| rejected("file_write_path_missing", "file_write requires a path"))?;
        let path = parse_path(path)
            .map_err(|error| rejected("file_write_path_invalid", error.to_string()))?;
        let content = arguments
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                rejected(
                    "file_write_content_missing",
                    "file_write requires UTF-8 content",
                )
            })?;
        if content.len() > MAX_RESULTING_FILE_BYTES {
            return Err(file_too_large(path.display(), content.len()).into_outcome(0));
        }
        if content.contains('\0') {
            return Err(rejected(
                "file_write_content_invalid",
                "file_write content contains a NUL byte",
            ));
        }
        let mode = arguments
            .get("mode")
            .and_then(Value::as_str)
            .and_then(FileWriteMode::parse)
            .ok_or_else(|| {
                rejected(
                    "file_write_mode_invalid",
                    "file_write mode must be 'create' or 'replace'",
                )
            })?;
        let expected_digest = arguments.get("expected_digest").and_then(Value::as_str);
        match (mode, expected_digest) {
            (FileWriteMode::Create, Some(_)) => {
                return Err(rejected(
                    "file_write_precondition_invalid",
                    "create mode does not accept expected_digest",
                ))
            }
            (FileWriteMode::Replace, Some(digest)) if Digest::new(digest).is_sha256() => {}
            (FileWriteMode::Replace, _) => {
                return Err(rejected(
                    "file_write_precondition_missing",
                    "replace mode requires the complete-file content_digest from file_read",
                ))
            }
            (FileWriteMode::Create, None) => {}
        }
        Ok(Self {
            path,
            content,
            mode,
            expected_digest,
        })
    }

    fn operation_label(&self) -> &'static str {
        match self.mode {
            FileWriteMode::Create => "Create",
            FileWriteMode::Replace => "Replace",
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedFileWriteExecutor {
    fn planning_contract(&self) -> Value {
        json!({ "contract": "orchestral.file-write-planner/v1" })
    }

    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        _outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        let Ok(request) = FileWriteRequest::parse(invocation) else {
            return Vec::new();
        };
        let Ok(workspace) = self.workspaces.select(invocation) else {
            return Vec::new();
        };
        let (diff, diff_omitted) = if request.mode == FileWriteMode::Create {
            added_content_preview(request.content)
        } else {
            (Vec::new(), 0)
        };
        vec![ToolActivityEvidence::File {
            operation: match request.mode {
                FileWriteMode::Create => ToolFileActivityKind::Create,
                FileWriteMode::Replace => ToolFileActivityKind::Update,
            },
            path: workspace
                .root
                .join(request.path.relative())
                .to_string_lossy()
                .into_owned(),
            diff,
            diff_omitted,
        }]
    }

    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        _descriptor: &ToolDescriptor,
        _effective_policy: &orchestral_core::tool_protocol::EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        let request = FileWriteRequest::parse(invocation)?;
        let workspace = self.workspaces.select(invocation)?;
        let target = workspace
            .root
            .join(request.path.relative())
            .to_string_lossy()
            .into_owned();
        let mut required_capabilities = CapabilityRequest::default();
        if request.mode == FileWriteMode::Replace {
            required_capabilities.insert_resource(
                EffectScope::FilesystemRead,
                CapabilitySelector::Exact(target.clone()),
            );
        }
        required_capabilities.insert_resource(
            EffectScope::FilesystemWrite,
            CapabilitySelector::Exact(target),
        );
        Ok(ToolOperationPlan {
            required_capabilities,
            risk: ToolOperationRisk::Routine,
            summary: format!(
                "{} file '{}' in workspace '{}'",
                request.operation_label(),
                request.path.display(),
                workspace.selector,
            ),
        })
    }

    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        FileWriteRequest::parse(invocation)
            .and_then(|request| {
                let workspace = self.workspaces.select(invocation)?;
                Ok(format!(
                    "{} file '{}' in workspace '{}'",
                    request.operation_label(),
                    request.path.display(),
                    workspace.selector,
                ))
            })
            .unwrap_or_else(|_| "Apply an invalid file_write request".to_owned())
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let request = match FileWriteRequest::parse(&execution.invocation) {
            Ok(request) => request,
            Err(outcome) => return outcome,
        };
        let workspace = match self.workspaces.select(&execution.invocation) {
            Ok(workspace) => workspace,
            Err(outcome) => return outcome,
        };
        let roots = match EffectiveRoots::from_execution(&execution) {
            Ok(roots) => roots,
            Err(error) => return error.into_outcome(0),
        };
        let target = match resolve_target(
            workspace.dir.as_ref(),
            &workspace.root,
            &roots,
            &request.path,
        ) {
            Ok(target) => target,
            Err(error) => return error.into_outcome(0),
        };
        let (operation, before, permissions, create_only) = match request.mode {
            FileWriteMode::Create => {
                if let Err(error) = require_absent_file(&target) {
                    return error.into_outcome(0);
                }
                ("add", None, None, true)
            }
            FileWriteMode::Replace => {
                let (before, permissions) = match read_regular_text(&target) {
                    Ok(value) => value,
                    Err(error) => return error.into_outcome(0),
                };
                let before_digest = Digest::sha256(&before);
                if request.expected_digest != Some(before_digest.as_str()) {
                    return rejected(
                        "file_write_conflict",
                        format!(
                            "'{}' changed since file_read; inspect it again before replacing",
                            request.path.display()
                        ),
                    );
                }
                ("update", Some(before), Some(permissions), false)
            }
        };
        let after = request.content.as_bytes();
        if before.as_deref() == Some(after) {
            return ToolOutcome::Completed {
                output: json!({
                    "workspace": workspace.selector,
                    "changed_files": 0,
                    "changes": []
                })
                .into(),
            };
        }
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        if let Err(error) = atomic_write(&target, after, permissions, create_only) {
            return error.into_outcome(0);
        }
        ToolOutcome::Completed {
            output: json!({
                "workspace": workspace.selector,
                "changed_files": 1,
                "changes": [change_output(
                    operation,
                    &request.path,
                    before.as_deref(),
                    Some(after),
                )],
            })
            .into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuardedApplyPatchExecutor {
    workspaces: MutationWorkspaceSet,
}

impl GuardedApplyPatchExecutor {
    pub fn new(workspace: impl AsRef<Path>) -> io::Result<Self> {
        Self::new_with_roots(workspace, std::iter::empty::<PathBuf>())
    }

    pub fn new_with_roots<I, P>(primary: impl AsRef<Path>, additional: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        Ok(Self {
            workspaces: MutationWorkspaceSet::new(primary, additional)?,
        })
    }

    pub fn workspace(&self) -> &Path {
        &self.workspaces.primary().root
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedApplyPatchExecutor {
    fn planning_contract(&self) -> Value {
        json!({ "contract": "orchestral.apply-patch-planner/v2" })
    }

    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        _outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        let mut evidence = patch_activity_evidence(invocation);
        let Ok(workspace) = self.workspaces.select(invocation) else {
            return evidence;
        };
        for item in &mut evidence {
            if let ToolActivityEvidence::File { path, .. } = item {
                *path = workspace.root.join(&*path).to_string_lossy().into_owned();
            }
        }
        evidence
    }

    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        _descriptor: &ToolDescriptor,
        _effective_policy: &orchestral_core::tool_protocol::EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        let patch = invocation
            .arguments
            .get("patch")
            .and_then(Value::as_str)
            .ok_or_else(|| rejected("patch_missing", "apply_patch requires a patch string"))?;
        let parsed =
            parse_patch(patch).map_err(|error| rejected("patch_invalid", error.to_string()))?;
        let workspace = self.workspaces.select(invocation)?;
        let mut required_capabilities = CapabilityRequest::default();
        let mut risk = ToolOperationRisk::Routine;
        for operation in &parsed.operations {
            let target = workspace
                .root
                .join(operation.path().relative())
                .to_string_lossy()
                .into_owned();
            if matches!(
                operation,
                PatchOperation::Update { .. } | PatchOperation::Delete { .. }
            ) {
                required_capabilities.insert_resource(
                    EffectScope::FilesystemRead,
                    CapabilitySelector::Exact(target.clone()),
                );
            }
            required_capabilities.insert_resource(
                EffectScope::FilesystemWrite,
                CapabilitySelector::Exact(target),
            );
            if matches!(operation, PatchOperation::Delete { .. }) {
                risk = ToolOperationRisk::Destructive;
            }
        }
        Ok(ToolOperationPlan {
            required_capabilities,
            risk,
            summary: format!(
                "{} in workspace '{}'",
                parsed_patch_summary(&parsed),
                workspace.selector
            ),
        })
    }

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
        let Ok(workspace) = self.workspaces.select(invocation) else {
            return "Apply a patch to an unknown workspace".to_owned();
        };
        format!(
            "{} in workspace '{}'",
            parsed_patch_summary(&parsed),
            workspace.selector
        )
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
        let workspace = match self.workspaces.select(&execution.invocation) {
            Ok(workspace) => workspace,
            Err(outcome) => return outcome,
        };
        let roots = match EffectiveRoots::from_execution(&execution) {
            Ok(roots) => roots,
            Err(error) => return error.into_outcome(0),
        };
        let prepared = match prepare_patch(workspace.dir.as_ref(), &workspace.root, &roots, parsed)
        {
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
                "workspace": workspace.selector,
                "changed_files": output.len(),
                "changes": output,
            })
            .into(),
        }
    }
}

fn patch_activity_evidence(invocation: &ToolInvocation) -> Vec<ToolActivityEvidence> {
    const MAX_FILES: usize = 15;
    let Some(patch) = invocation.arguments.get("patch").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Ok(parsed) = parse_patch(patch) else {
        return Vec::new();
    };
    let previews = patch_diff_previews(patch, parsed.operations.len());
    let mut evidence = parsed
        .operations
        .iter()
        .take(MAX_FILES)
        .enumerate()
        .map(|(index, operation)| {
            let preview = previews.get(index).cloned().unwrap_or_default();
            ToolActivityEvidence::File {
                operation: match operation {
                    PatchOperation::Add { .. } => ToolFileActivityKind::Create,
                    PatchOperation::Update { .. } => ToolFileActivityKind::Update,
                    PatchOperation::Delete { .. } => ToolFileActivityKind::Delete,
                },
                path: operation.path().display().to_owned(),
                diff: preview.lines,
                diff_omitted: preview.omitted,
            }
        })
        .collect::<Vec<_>>();
    if parsed.operations.len() > MAX_FILES {
        evidence.push(ToolActivityEvidence::Omitted {
            count: u32::try_from(parsed.operations.len() - MAX_FILES).unwrap_or(u32::MAX),
        });
    }
    evidence
}

#[derive(Debug, Clone, Default)]
struct DiffPreview {
    lines: Vec<ToolDiffLine>,
    omitted: u32,
}

fn patch_diff_previews(patch: &str, operation_count: usize) -> Vec<DiffPreview> {
    const MAX_LINES_PER_FILE: usize = 16;
    #[derive(Clone, Copy)]
    enum Mode {
        Add,
        Update,
        Delete,
    }

    let mut previews = vec![DiffPreview::default(); operation_count];
    let mut current = None::<(usize, Mode)>;
    let mut next_index = 0usize;
    for line in patch.lines() {
        let mode = if line.starts_with("*** Add File: ") {
            Some(Mode::Add)
        } else if line.starts_with("*** Update File: ") {
            Some(Mode::Update)
        } else if line.starts_with("*** Delete File: ") {
            Some(Mode::Delete)
        } else {
            None
        };
        if let Some(mode) = mode {
            current = (next_index < operation_count).then_some((next_index, mode));
            next_index = next_index.saturating_add(1);
            continue;
        }
        let Some((index, mode)) = current else {
            continue;
        };
        let change = match mode {
            Mode::Add => line
                .strip_prefix('+')
                .map(|text| (ToolDiffLineKind::Addition, text)),
            Mode::Update if line.starts_with("@@") => None,
            Mode::Update => line
                .strip_prefix('+')
                .map(|text| (ToolDiffLineKind::Addition, text))
                .or_else(|| {
                    line.strip_prefix('-')
                        .map(|text| (ToolDiffLineKind::Deletion, text))
                })
                .or_else(|| {
                    line.strip_prefix(' ')
                        .map(|text| (ToolDiffLineKind::Context, text))
                }),
            Mode::Delete => None,
        };
        if let Some((kind, text)) = change {
            let preview = &mut previews[index];
            if preview.lines.len() < MAX_LINES_PER_FILE {
                preview.lines.push(ToolDiffLine {
                    kind,
                    text: bounded_diff_text(text),
                });
            } else {
                preview.omitted = preview.omitted.saturating_add(1);
            }
        }
    }
    previews
}

fn added_content_preview(content: &str) -> (Vec<ToolDiffLine>, u32) {
    const MAX_LINES: usize = 16;
    let mut lines = Vec::new();
    let mut omitted = 0u32;
    for line in content.lines() {
        if lines.len() < MAX_LINES {
            lines.push(ToolDiffLine {
                kind: ToolDiffLineKind::Addition,
                text: bounded_diff_text(line),
            });
        } else {
            omitted = omitted.saturating_add(1);
        }
    }
    (lines, omitted)
}

fn bounded_diff_text(value: &str) -> String {
    const MAX_CHARS: usize = 240;
    let mut chars = value.chars();
    let mut text = chars
        .by_ref()
        .take(MAX_CHARS)
        .map(|character| {
            if character == '\t' {
                ' '
            } else if character.is_control() {
                '�'
            } else {
                character
            }
        })
        .collect::<String>();
    if chars.next().is_some() {
        text.push('…');
    }
    text
}

fn parsed_patch_summary(parsed: &ParsedPatch) -> String {
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
                "Paths must be normalized paths relative to the selected workspace. When multiple ",
                "workspaces are provided, select one with its exact canonical workspace root."
            )
            .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["patch"],
                "properties": {
                    "patch": {
                        "type": "string",
                        "maxLength": MAX_PATCH_BYTES
                    },
                    "workspace": {
                        "type": "string",
                        "description": "Optional exact Host-provided canonical workspace root. Omit to use the primary workspace."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: file_change_output_schema(),
        effect_scopes: BTreeSet::from([EffectScope::FilesystemRead, EffectScope::FilesystemWrite]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

pub fn guarded_file_write_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/file_write/v1"),
        model_schema: ModelToolSchema {
            name: "file_write".to_owned(),
            description: concat!(
                "Create a new UTF-8 text file or intentionally replace a complete existing file ",
                "inside a Host-approved workspace. Use mode='create' only when the path must ",
                "not exist. Use mode='replace' only after reading the complete file from offset 1 ",
                "through eof, and pass that file_read content_digest as expected_digest. Use ",
                "apply_patch instead for targeted edits to existing files. Parent directories ",
                "must already exist. When multiple workspaces are provided, select one with its ",
                "exact canonical workspace root."
            )
            .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["path", "content", "mode"],
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Normalized path relative to the selected workspace."
                    },
                    "workspace": {
                        "type": "string",
                        "description": "Optional exact Host-provided canonical workspace root. Omit to use the primary workspace."
                    },
                    "content": {
                        "type": "string",
                        "maxLength": MAX_RESULTING_FILE_BYTES
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["create", "replace"]
                    },
                    "expected_digest": {
                        "type": "string",
                        "description": "Required only for replace: complete-file content_digest returned by file_read."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: file_change_output_schema(),
        effect_scopes: BTreeSet::from([EffectScope::FilesystemRead, EffectScope::FilesystemWrite]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

fn file_change_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["workspace", "changed_files", "changes"],
        "properties": {
            "workspace": { "type": "string" },
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
    })
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
        change_output(operation, path, before, after)
    }
}

fn change_output(
    operation: &str,
    path: &PatchPath,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
) -> Value {
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
            .open(".")
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
    use orchestral_core::agent_protocol::wire::{
        RunId, ToolActivityEvidence, ToolDiffLine, ToolDiffLineKind, ToolFileActivityKind,
    };
    use orchestral_core::tool_protocol::{ToolCallId, ToolId, ToolInvocation};
    use serde_json::json;

    use super::{
        commit_change, parse_patch, patch_activity_evidence, prepare_patch, EffectiveRoots,
    };

    #[test]
    fn patch_adapter_emits_structured_file_and_diff_evidence() {
        let invocation = ToolInvocation {
            run_id: RunId::new("run-evidence"),
            call_id: ToolCallId::new("call-evidence"),
            tool_id: ToolId::new("orchestral/apply_patch/v1"),
            arguments: json!({
                "patch": concat!(
                    "*** Begin Patch\n",
                    "*** Update File: src/lib.rs\n",
                    "@@\n",
                    "-old\n",
                    "+new\n",
                    "*** Add File: tests/new.rs\n",
                    "+test\n",
                    "*** End Patch"
                )
            }),
        };
        assert_eq!(
            patch_activity_evidence(&invocation),
            vec![
                ToolActivityEvidence::File {
                    operation: ToolFileActivityKind::Update,
                    path: "src/lib.rs".to_owned(),
                    diff: vec![
                        ToolDiffLine {
                            kind: ToolDiffLineKind::Deletion,
                            text: "old".to_owned(),
                        },
                        ToolDiffLine {
                            kind: ToolDiffLineKind::Addition,
                            text: "new".to_owned(),
                        },
                    ],
                    diff_omitted: 0,
                },
                ToolActivityEvidence::File {
                    operation: ToolFileActivityKind::Create,
                    path: "tests/new.rs".to_owned(),
                    diff: vec![ToolDiffLine {
                        kind: ToolDiffLineKind::Addition,
                        text: "test".to_owned(),
                    }],
                    diff_omitted: 0,
                },
            ]
        );
    }

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
