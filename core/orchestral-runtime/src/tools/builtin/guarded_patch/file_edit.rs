//! Exact local text edits using the existing guarded file transaction.

use super::*;
use tokio_util::sync::CancellationToken;

/// Atomically replaces unique text occurrences in a Host-approved existing file.
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
    edits: Vec<TextEdit<'a>>,
}

struct TextEdit<'a> {
    old_text: &'a str,
    new_text: &'a str,
}

impl<'a> FileEditRequest<'a> {
    fn parse(invocation: &'a ToolInvocation) -> Result<Self, ToolOutcome> {
        let invalid = || {
            rejected("file_edit_invalid", "file_edit requires path and either old_text/new_text or a non-empty edits array of old_text/new_text objects, with only an optional exact workspace root. Do not mix the two forms; old_text must be non-empty.")
        };
        let arguments = invocation.arguments.as_object().ok_or_else(invalid)?;
        if arguments.keys().any(|key| {
            !matches!(
                key.as_str(),
                "path" | "old_text" | "new_text" | "edits" | "workspace"
            )
        }) {
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
        let parse_edit = |fields: &'a Map<String, Value>| {
            let old_text = fields
                .get("old_text")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?;
            let new_text = fields
                .get("new_text")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?;
            if old_text.is_empty() || old_text.contains('\0') || new_text.contains('\0') {
                return Err(invalid());
            }
            Ok(TextEdit { old_text, new_text })
        };
        let edits = if let Some(edits) = arguments.get("edits") {
            if arguments.contains_key("old_text") || arguments.contains_key("new_text") {
                return Err(invalid());
            }
            let edits = edits
                .as_array()
                .filter(|edits| !edits.is_empty())
                .ok_or_else(invalid)?;
            edits
                .iter()
                .map(|edit| {
                    let fields = edit.as_object().ok_or_else(invalid)?;
                    if fields
                        .keys()
                        .any(|key| !matches!(key.as_str(), "old_text" | "new_text"))
                    {
                        return Err(invalid());
                    }
                    parse_edit(fields)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![parse_edit(arguments)?]
        };
        // Disjoint matches cannot consume more than one input file, and the
        // replacements themselves cannot exceed the resulting-file capacity.
        // Check the totals before searching or allocating the new file.
        let (mut old_bytes, mut new_bytes) = (0usize, 0usize);
        for edit in &edits {
            old_bytes = old_bytes.saturating_add(edit.old_text.len());
            new_bytes = new_bytes.saturating_add(edit.new_text.len());
            for bytes in [old_bytes, new_bytes] {
                if bytes > MAX_RESULTING_FILE_BYTES {
                    return Err(file_too_large(path.display(), bytes).into_outcome(0));
                }
            }
        }
        Ok(Self { path, edits })
    }

    fn summary(&self, workspace: &str) -> String {
        let action = if self.edits.len() == 1 {
            "Edit one text occurrence".to_owned()
        } else {
            format!("Atomically edit {} text occurrences", self.edits.len())
        };
        format!(
            "{action} in '{}' in workspace '{workspace}'",
            self.path.display()
        )
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedFileEditExecutor {
    fn model_output_contract(&self) -> Value {
        super::super::model_output::contract("mutation")
    }

    fn project_model_output(&self, invocation: &ToolInvocation, output: &Value) -> Value {
        super::super::model_output::mutation(invocation, output)
    }

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
        let (diff, diff_omitted) = edit_activity_preview(&request.edits);
        vec![ToolActivityEvidence::File {
            operation: ToolFileActivityKind::Update,
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
            summary: request.summary(&workspace.selector),
        })
    }

    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        FileEditRequest::parse(invocation)
            .and_then(|request| {
                let workspace = self.workspaces.select(invocation)?;
                Ok(request.summary(&workspace.selector))
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
    let mut spans = Vec::with_capacity(request.edits.len());
    for (index, edit) in request.edits.iter().enumerate() {
        let start = unique_match(original, edit.old_text).map_err(|mut error| {
            // Keep diagnostics for the legacy single-edit form unchanged.
            if request.edits.len() > 1 {
                error.message = format!("edits[{index}]: {}", error.message);
            }
            error
        })?;
        spans.push((start, start + edit.old_text.len(), edit.new_text, index));
    }
    spans.sort_unstable_by_key(|span| span.0);
    for pair in spans.windows(2) {
        if pair[1].0 < pair[0].1 {
            return Err(MutationError::rejected(
                "file_edit_overlap",
                format!("edits[{}] and edits[{}] overlap in the original file; combine them into one replacement. No files changed.", pair[0].3, pair[1].3),
            ));
        }
    }
    let removed: usize = spans.iter().map(|span| span.1 - span.0).sum();
    let inserted: usize = spans.iter().map(|span| span.2.len()).sum();
    let after_len = before.len() - removed + inserted;
    if after_len > MAX_RESULTING_FILE_BYTES {
        return Err(file_too_large(request.path.display(), after_len));
    }
    let mut after = Vec::with_capacity(after_len);
    let mut cursor = 0;
    for (start, end, replacement, _) in spans {
        after.extend_from_slice(&before[cursor..start]);
        after.extend_from_slice(replacement.as_bytes());
        cursor = end;
    }
    after.extend_from_slice(&before[cursor..]);
    Ok(PreparedChange::Update {
        path: request.path,
        target,
        before,
        after,
        permissions,
    })
}

fn unique_match(original: &str, old_text: &str) -> Result<usize, MutationError> {
    let start = original.find(old_text).ok_or_else(|| {
        MutationError::rejected("file_edit_no_match", no_match_message(original, old_text))
    })?;
    // Advance one Unicode scalar, not the whole match: overlapping occurrences
    // are ambiguous too (for example, replacing "aa" in "aaa").
    let next = start
        + old_text
            .chars()
            .next()
            .expect("validated non-empty old_text")
            .len_utf8();
    if original[next..].contains(old_text) {
        return Err(MutationError::rejected(
            "file_edit_ambiguous",
            "old_text occurs more than once; include more unchanged context",
        ));
    }
    Ok(start)
}

fn edit_activity_preview(edits: &[TextEdit<'_>]) -> (Vec<ToolDiffLine>, u32) {
    const MAX_LINES: usize = 32;
    let mut diff = Vec::new();
    let mut omitted = 0u32;
    for edit in edits {
        let (mut before, before_omitted) = added_content_preview(edit.old_text);
        for line in &mut before {
            line.kind = ToolDiffLineKind::Deletion;
        }
        let (after, after_omitted) = added_content_preview(edit.new_text);
        omitted = omitted
            .saturating_add(before_omitted)
            .saturating_add(after_omitted);
        for line in before.into_iter().chain(after) {
            if diff.len() < MAX_LINES {
                diff.push(line);
            } else {
                omitted = omitted.saturating_add(1);
            }
        }
    }
    (diff, omitted)
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

/// Describes atomic exact edits; Host policy supplies all filesystem authority.
pub fn guarded_file_edit_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/file_edit/v1"),
        model_schema: ModelToolSchema {
            name: "file_edit".to_owned(),
            description: concat!(
                "Atomically replace exact text in one UTF-8 file. For multiple known changes, ",
                "send one edits array of {old_text,new_text} objects. Each old_text must occur ",
                "exactly once in the ORIGINAL file; matches must not overlap. All matches are ",
                "checked before writing. Copy whitespace and line endings exactly, with only ",
                "enough unchanged context to disambiguate. Empty new_text deletes a match. ",
                "A single change may use top-level old_text/new_text instead of edits; never ",
                "mix the forms. No patch markers. Use normalized workspace-relative paths."
            )
            .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["path"],
                "oneOf": [
                    {"required": ["old_text", "new_text"], "not": {"required": ["edits"]}},
                    {"required": ["edits"], "not": {"anyOf": [
                        {"required": ["old_text"]}, {"required": ["new_text"]}
                    ]}}
                ],
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "old_text": { "type": "string", "minLength": 1, "maxLength": MAX_RESULTING_FILE_BYTES },
                    "new_text": { "type": "string", "maxLength": MAX_RESULTING_FILE_BYTES },
                    "edits": {
                        "type": "array", "minItems": 1,
                        "items": {
                            "type": "object", "required": ["old_text", "new_text"],
                            "properties": {
                                "old_text": {"type": "string", "minLength": 1, "maxLength": MAX_RESULTING_FILE_BYTES},
                                "new_text": {"type": "string", "maxLength": MAX_RESULTING_FILE_BYTES}
                            },
                            "additionalProperties": false
                        }
                    },
                    "workspace": {
                        "type": "string", "minLength": 1,
                        "description": "Exact canonical Host workspace root; omit for primary."
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

        fn prepare(&self, batch: bool) -> PreparedChange {
            let invocation = ToolInvocation {
                run_id: RunId::new("prepare-run"),
                call_id: ToolCallId::new("prepare-call"),
                tool_id: ToolId::new("orchestral/file_edit/v1"),
                arguments: if batch {
                    json!({"path":"source.rs", "edits":[
                        {"old_text":"be", "new_text":"af"},
                        {"old_text":"fore", "new_text":"ter"}
                    ]})
                } else {
                    json!({ "path": "source.rs", "old_text": "before", "new_text": "after" })
                },
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
        for batch in [false, true] {
            let fixture = PreparedFixture::new();
            let prepared = fixture.prepare(batch);
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
    }

    #[test]
    fn file_edit_cancelled_after_preparation_does_not_commit() {
        for batch in [false, true] {
            let fixture = PreparedFixture::new();
            let prepared = fixture.prepare(batch);
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

    #[test]
    fn batch_activity_preview_keeps_one_bounded_file_diff() {
        let edits = [
            TextEdit {
                old_text: "left",
                new_text: "first",
            },
            TextEdit {
                old_text: "right",
                new_text: "second",
            },
        ];
        let (diff, omitted) = edit_activity_preview(&edits);
        assert_eq!(omitted, 0);
        assert_eq!(
            diff.iter()
                .map(|line| (line.kind, line.text.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (ToolDiffLineKind::Deletion, "left"),
                (ToolDiffLineKind::Addition, "first"),
                (ToolDiffLineKind::Deletion, "right"),
                (ToolDiffLineKind::Addition, "second"),
            ]
        );
        let old = "old\n".repeat(30);
        let new = "new\n".repeat(30);
        let edits = [
            TextEdit {
                old_text: &old,
                new_text: &new,
            },
            TextEdit {
                old_text: "last",
                new_text: "changed",
            },
        ];
        let (diff, omitted) = edit_activity_preview(&edits);
        assert_eq!(diff.len() + omitted as usize, 62);
        assert!(diff.len() <= 32);
    }
}
