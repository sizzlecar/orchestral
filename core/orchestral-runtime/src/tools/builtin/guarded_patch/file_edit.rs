//! Exact local text edits using the existing guarded file transaction.

use super::*;
use tokio_util::sync::CancellationToken;

/// Replaces one unique text occurrence in a Host-approved existing file.
#[derive(Debug, Clone)]
pub struct GuardedFileEditExecutor {
    workspaces: MutationWorkspaceSet,
}

impl GuardedFileEditExecutor {
    /// Opens the primary workspace capability.
    pub fn new(workspace: impl AsRef<Path>) -> io::Result<Self> {
        Self::new_with_roots(workspace, std::iter::empty::<PathBuf>())
    }

    /// Opens only the workspace roots supplied by the Host.
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

struct FileEditRequest<'a> {
    path: PatchPath,
    old_text: &'a str,
    new_text: &'a str,
}

impl<'a> FileEditRequest<'a> {
    fn parse(invocation: &'a ToolInvocation) -> Result<Self, ToolOutcome> {
        let invalid = || {
            rejected("file_edit_invalid", "file_edit requires path, non-empty old_text and new_text strings, with only an optional exact workspace root")
        };
        let arguments = invocation.arguments.as_object().ok_or_else(invalid)?;
        if arguments
            .keys()
            .any(|key| !matches!(key.as_str(), "path" | "old_text" | "new_text" | "workspace"))
        {
            return Err(invalid());
        }
        if let Some(workspace) = arguments.get("workspace") {
            let workspace = workspace.as_str().ok_or_else(invalid)?;
            if workspace.is_empty() || workspace.trim() != workspace {
                return Err(invalid());
            }
        }
        let path = arguments
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        if path.contains('\0') {
            return Err(invalid());
        }
        let path = parse_path(path)
            .map_err(|error| rejected("file_edit_path_invalid", error.to_string()))?;
        let old_text = arguments
            .get("old_text")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        let new_text = arguments
            .get("new_text")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        if old_text.is_empty() || old_text.contains('\0') || new_text.contains('\0') {
            return Err(invalid());
        }
        for text in [old_text, new_text] {
            if text.len() > MAX_RESULTING_FILE_BYTES {
                return Err(file_too_large(path.display(), text.len()).into_outcome(0));
            }
        }
        Ok(Self {
            path,
            old_text,
            new_text,
        })
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedFileEditExecutor {
    fn planning_contract(&self) -> Value {
        json!({ "contract": "orchestral.file-edit-planner/v1" })
    }

    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        _outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        let Ok(request) = FileEditRequest::parse(invocation) else {
            return Vec::new();
        };
        let Ok(workspace) = self.workspaces.select(invocation) else {
            return Vec::new();
        };
        let (mut before, before_omitted) = added_content_preview(request.old_text);
        for line in &mut before {
            line.kind = ToolDiffLineKind::Deletion;
        }
        let (after, after_omitted) = added_content_preview(request.new_text);
        before.extend(after);
        vec![ToolActivityEvidence::File {
            operation: ToolFileActivityKind::Update,
            path: workspace
                .root
                .join(request.path.relative())
                .to_string_lossy()
                .into_owned(),
            diff: before,
            diff_omitted: before_omitted.saturating_add(after_omitted),
        }]
    }

    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        _descriptor: &ToolDescriptor,
        _effective_policy: &orchestral_core::tool_protocol::EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        let request = FileEditRequest::parse(invocation)?;
        let workspace = self.workspaces.select(invocation)?;
        let target = workspace
            .root
            .join(request.path.relative())
            .to_string_lossy()
            .into_owned();
        let mut required_capabilities = CapabilityRequest::default();
        for effect in [EffectScope::FilesystemRead, EffectScope::FilesystemWrite] {
            required_capabilities
                .insert_resource(effect, CapabilitySelector::Exact(target.clone()));
        }
        Ok(ToolOperationPlan {
            required_capabilities,
            risk: ToolOperationRisk::Routine,
            session_approval_scope: None,
            summary: format!(
                "Edit one text occurrence in '{}' in workspace '{}'",
                request.path.display(),
                workspace.selector
            ),
        })
    }

    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        FileEditRequest::parse(invocation)
            .and_then(|request| {
                let workspace = self.workspaces.select(invocation)?;
                Ok(format!(
                    "Edit one text occurrence in '{}' in workspace '{}'",
                    request.path.display(),
                    workspace.selector
                ))
            })
            .unwrap_or_else(|_| "Apply an invalid file_edit request".to_owned())
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let request = match FileEditRequest::parse(&execution.invocation) {
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
        match prepare_edit(workspace, &roots, request) {
            Ok(prepared) => finish_edit(prepared, &workspace.selector, &execution.cancellation),
            Err(error) => error.into_outcome(0),
        }
    }
}

fn prepare_edit(
    workspace: &MutationWorkspace,
    roots: &EffectiveRoots,
    request: FileEditRequest<'_>,
) -> Result<PreparedChange, MutationError> {
    let target = resolve_target(
        workspace.dir.as_ref(),
        &workspace.root,
        roots,
        &request.path,
    )?;
    let (before, permissions) = read_regular_text(&target)?;
    let original = std::str::from_utf8(&before).map_err(|_| {
        MutationError::rejected(
            "file_edit_not_utf8",
            "file_edit requires an existing UTF-8 text file",
        )
    })?;
    if original.contains('\0') {
        return Err(MutationError::rejected(
            "file_edit_not_text",
            "file_edit does not edit files containing NUL bytes",
        ));
    }
    let start = original.find(request.old_text).ok_or_else(|| {
        MutationError::rejected(
            "file_edit_no_match",
            no_match_message(original, request.old_text),
        )
    })?;
    // Advance one Unicode scalar, not the whole match: overlapping occurrences
    // are ambiguous too (for example, replacing "aa" in "aaa").
    let next = start
        + request
            .old_text
            .chars()
            .next()
            .expect("validated non-empty old_text")
            .len_utf8();
    if original[next..].contains(request.old_text) {
        return Err(MutationError::rejected(
            "file_edit_ambiguous",
            "old_text occurs more than once; include more unchanged context",
        ));
    }
    let end = start + request.old_text.len();
    let after_len = before.len() - request.old_text.len() + request.new_text.len();
    if after_len > MAX_RESULTING_FILE_BYTES {
        return Err(file_too_large(request.path.display(), after_len));
    }
    let mut after = Vec::with_capacity(after_len);
    after.extend_from_slice(&before[..start]);
    after.extend_from_slice(request.new_text.as_bytes());
    after.extend_from_slice(&before[end..]);
    Ok(PreparedChange::Update {
        path: request.path,
        target,
        before,
        after,
        permissions,
    })
}

/// Diagnostic observations only: these candidates never reach PreparedChange.
/// The caller has already authorized both reading and writing this exact file.
fn no_match_message(original: &str, requested: &str) -> String {
    const MAX_LINES: usize = 8;
    const MAX_ANCHORS: usize = 3;
    const MAX_CANDIDATES: usize = 3;
    let mut message = String::from(
        "old_text does not occur exactly. No files changed. Copy exact source text from file_read or text_search; do not infer whitespace or escaping.",
    );
    let requested_lines: Vec<_> = requested.split_inclusive('\n').take(MAX_LINES).collect();
    let anchors: Vec<_> = requested_lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let body = diagnostic_line_body(line).trim_start_matches([' ', '\t']);
            (!body.is_empty()).then_some((index, body))
        })
        .take(MAX_ANCHORS)
        .collect();
    if anchors.is_empty() {
        return message;
    }
    let mut candidates = Vec::new();
    let mut omitted = false;
    'scan: for (line_index, line) in original.split_inclusive('\n').enumerate() {
        let body = diagnostic_line_body(line).trim_start_matches([' ', '\t']);
        for &(anchor_index, anchor) in &anchors {
            if body != anchor {
                continue;
            }
            let Some(start) = line_index.checked_sub(anchor_index) else {
                continue;
            };
            if candidates.contains(&start) {
                continue;
            }
            if candidates.len() == MAX_CANDIDATES {
                omitted = true;
                break 'scan;
            }
            candidates.push(start);
        }
    }
    if candidates.is_empty() {
        return message;
    }
    message.push_str(
        "\nCandidate locations below use unchanged line text, ignoring only leading ASCII spaces/tabs and the line terminator for locating an anchor. They are not complete matches or replacement instructions. Source previews preserve literal backslashes and quotes; tabs, CR, LF and ESC appear as ⟦TAB⟧, ⟦CR⟧, ⟦LF⟧ and ⟦ESC⟧, and other controls as ⟦U+XXXX⟧. These previews are not complete replacement text. Byte columns are 1-indexed UTF-8 bytes.",
    );
    for start in candidates {
        let actual_lines: Vec<_> = original
            .split_inclusive('\n')
            .skip(start)
            .take(requested_lines.len())
            .collect();
        let difference = requested_lines
            .iter()
            .enumerate()
            .find_map(|(index, &sent)| {
                let actual = actual_lines.get(index).copied().unwrap_or("");
                let common = sent
                    .bytes()
                    .zip(actual.bytes())
                    .take_while(|(a, b)| a == b)
                    .count();
                // A request may end partway through a source line, including just
                // before its terminator. Extra source bytes there are not a mismatch.
                (common != sent.len() || (sent.ends_with('\n') && common != actual.len()))
                    .then_some((index, common, sent, actual))
            });
        message.push_str(&format!(
            "\nCandidate starting at source line {}:",
            start + 1
        ));
        if let Some((index, column, sent, actual)) = difference {
            let byte = |text: &str| match text.as_bytes().get(column) {
                Some(value) => format!("0x{value:02x}"),
                None => "end of span".to_owned(),
            };
            let indentation = |text: &str| {
                let prefix = text.bytes().take_while(|b| matches!(*b, b' ' | b'\t'));
                let (mut spaces, mut tabs) = (0, 0);
                for value in prefix {
                    spaces += usize::from(value == b' ');
                    tabs += usize::from(value == b'\t');
                }
                format!("{spaces} spaces, {tabs} tabs")
            };
            message.push_str(&format!(
                " first difference at old_text line {}, source line {}, byte column {}: supplied {}, actual {}. Leading whitespace: supplied {}; actual {}.\nActual source line: {}",
                index + 1, start + index + 1, column + 1, byte(sent), byte(actual),
                indentation(sent), indentation(actual), diagnostic_preview(actual),
            ));
        } else {
            message.push_str(" no difference found within the inspected lines; read more context.");
        }
    }
    if omitted {
        message.push_str("\nAdditional candidate locations omitted; no candidate was selected.");
    }
    if requested.split_inclusive('\n').nth(MAX_LINES).is_some() {
        message.push_str("\nFurther old_text lines were not inspected for diagnostics.");
    }
    message
}

fn diagnostic_line_body(line: &str) -> &str {
    line.strip_suffix('\n')
        .map(|body| body.strip_suffix('\r').unwrap_or(body))
        .unwrap_or(line)
}

fn diagnostic_preview(line: &str) -> String {
    const MAX_CHARS: usize = 160;
    let mut characters = line.chars();
    let mut preview = String::new();
    for character in characters.by_ref().take(MAX_CHARS) {
        match character {
            '\t' => preview.push_str("⟦TAB⟧"),
            '\r' => preview.push_str("⟦CR⟧"),
            '\n' => preview.push_str("⟦LF⟧"),
            '\u{1b}' => preview.push_str("⟦ESC⟧"),
            control if control.is_control() => {
                preview.push_str(&format!("⟦U+{:04X}⟧", u32::from(control)));
            }
            literal => preview.push(literal),
        }
    }
    if characters.next().is_some() {
        preview.push_str(" [truncated]");
    }
    preview
}

fn finish_edit(
    prepared: PreparedChange,
    workspace: &str,
    cancellation: &CancellationToken,
) -> ToolOutcome {
    if cancellation.is_cancelled() {
        return ToolOutcome::Cancelled;
    }
    if let Err(error) = recheck_preconditions(std::slice::from_ref(&prepared)) {
        return error.into_outcome(0);
    }
    let unchanged =
        matches!(&prepared, PreparedChange::Update { before, after, .. } if before == after);
    if cancellation.is_cancelled() {
        return ToolOutcome::Cancelled;
    }
    if !unchanged {
        if let Err(error) = commit_change(&prepared) {
            return error.into_outcome(0);
        }
    }
    ToolOutcome::Completed {
        output: json!({
            "workspace": workspace,
            "changed_files": usize::from(!unchanged),
            "changes": if unchanged { Vec::new() } else { vec![prepared.output()] },
        })
        .into(),
    }
}

/// Describes a single exact edit; Host policy supplies all filesystem authority.
pub fn guarded_file_edit_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/file_edit/v1"),
        model_schema: ModelToolSchema {
            name: "file_edit".to_owned(),
            description: concat!(
                "Replace exactly one occurrence of old_text in an existing UTF-8 file. ",
                "Use the smallest unique text region needed for the edit; add unchanged context ",
                "only to disambiguate. Copy old_text exactly, including indentation and line endings. ",
                "No patch markers. Empty new_text deletes ",
                "the matched text. Paths are normalized and relative to the selected Host workspace."
            ).to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["path", "old_text", "new_text"],
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "old_text": { "type": "string", "minLength": 1, "maxLength": MAX_RESULTING_FILE_BYTES },
                    "new_text": { "type": "string", "maxLength": MAX_RESULTING_FILE_BYTES },
                    "workspace": {
                        "type": "string", "minLength": 1,
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::agent_protocol::wire::RunId;
    use orchestral_core::tool_protocol::ToolCallId;

    struct PreparedFixture {
        root: PathBuf,
        workspaces: MutationWorkspaceSet,
    }

    impl PreparedFixture {
        fn new() -> Self {
            let root = std::env::temp_dir()
                .join(format!("orchestral-edit-prepare-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            let root = std::fs::canonicalize(root).unwrap();
            std::fs::write(root.join("source.rs"), "before").unwrap();
            let workspaces =
                MutationWorkspaceSet::new(&root, std::iter::empty::<PathBuf>()).unwrap();
            Self { root, workspaces }
        }

        fn prepare(&self) -> PreparedChange {
            let invocation = ToolInvocation {
                run_id: RunId::new("prepare-run"),
                call_id: ToolCallId::new("prepare-call"),
                tool_id: ToolId::new("orchestral/file_edit/v1"),
                arguments: json!({ "path": "source.rs", "old_text": "before", "new_text": "after" }),
            };
            let roots = EffectiveRoots {
                readable: vec![self.root.clone()],
                writable: vec![self.root.clone()],
            };
            prepare_edit(
                self.workspaces.primary(),
                &roots,
                FileEditRequest::parse(&invocation).unwrap(),
            )
            .unwrap()
        }
    }

    impl Drop for PreparedFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn file_edit_changed_after_preparation_is_not_overwritten() {
        let fixture = PreparedFixture::new();
        let prepared = fixture.prepare();
        std::fs::write(fixture.root.join("source.rs"), "external change").unwrap();
        let outcome = finish_edit(
            prepared,
            &fixture.workspaces.primary().selector,
            &CancellationToken::new(),
        );
        assert!(
            matches!(outcome, ToolOutcome::Rejected { ref code, .. } if code == "patch_conflict")
        );
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("source.rs")).unwrap(),
            "external change"
        );
        assert_eq!(std::fs::read_dir(&fixture.root).unwrap().count(), 1);
    }

    #[test]
    fn file_edit_cancelled_after_preparation_does_not_commit() {
        let fixture = PreparedFixture::new();
        let prepared = fixture.prepare();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let outcome = finish_edit(
            prepared,
            &fixture.workspaces.primary().selector,
            &cancellation,
        );
        assert!(matches!(outcome, ToolOutcome::Cancelled));
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("source.rs")).unwrap(),
            "before"
        );
        assert_eq!(std::fs::read_dir(&fixture.root).unwrap().count(), 1);
    }
}
