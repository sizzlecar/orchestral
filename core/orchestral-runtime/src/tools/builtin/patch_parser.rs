use std::collections::BTreeSet;
use std::fmt;
use std::path::{Component, Path, PathBuf};

pub(super) const MAX_PATCH_BYTES: usize = 1024 * 1024;
pub(super) const MAX_PATCH_FILES: usize = 64;

const BEGIN_PATCH: &str = "*** Begin Patch";
const END_PATCH: &str = "*** End Patch";
const ADD_FILE: &str = "*** Add File: ";
const UPDATE_FILE: &str = "*** Update File: ";
const DELETE_FILE: &str = "*** Delete File: ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedPatch {
    pub operations: Vec<PatchOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PatchOperation {
    Add {
        path: PatchPath,
        content: String,
    },
    Update {
        path: PatchPath,
        hunks: Vec<UpdateHunk>,
    },
    Delete {
        path: PatchPath,
    },
}

impl PatchOperation {
    pub fn path(&self) -> &PatchPath {
        match self {
            Self::Add { path, .. } | Self::Update { path, .. } | Self::Delete { path } => path,
        }
    }

    pub const fn label(&self) -> &'static str {
        match self {
            Self::Add { .. } => "add",
            Self::Update { .. } => "update",
            Self::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PatchPath {
    display: String,
    relative: PathBuf,
}

impl PatchPath {
    pub fn display(&self) -> &str {
        &self.display
    }

    pub fn relative(&self) -> &Path {
        &self.relative
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UpdateHunk {
    before: Vec<String>,
    after: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PatchParseError {
    message: String,
}

impl PatchParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PatchParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for PatchParseError {}

pub(super) fn parse_patch(input: &str) -> Result<ParsedPatch, PatchParseError> {
    if input.len() > MAX_PATCH_BYTES {
        return Err(PatchParseError::new(format!(
            "patch exceeds the {MAX_PATCH_BYTES}-byte limit"
        )));
    }
    if input.contains('\0') {
        return Err(PatchParseError::new("patch contains a NUL byte"));
    }
    if input.contains('\r') && !input.contains("\r\n") {
        return Err(PatchParseError::new(
            "patch contains an unsupported bare carriage return",
        ));
    }
    let normalized = input.replace("\r\n", "\n");
    let mut lines = normalized.split('\n').collect::<Vec<_>>();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    if lines.first() != Some(&BEGIN_PATCH) || lines.last() != Some(&END_PATCH) {
        return Err(PatchParseError::new(
            "patch must start with '*** Begin Patch' and end with '*** End Patch'",
        ));
    }

    let mut operations = Vec::new();
    let mut paths = BTreeSet::new();
    let mut cursor = 1;
    let end = lines.len() - 1;
    while cursor < end {
        if operations.len() == MAX_PATCH_FILES {
            return Err(PatchParseError::new(format!(
                "patch exceeds the {MAX_PATCH_FILES}-file limit"
            )));
        }
        let header = lines[cursor];
        cursor += 1;
        let operation = if let Some(raw_path) = header.strip_prefix(ADD_FILE) {
            let path = parse_path(raw_path)?;
            let mut content = String::new();
            while cursor < end && !is_file_header(lines[cursor]) {
                let Some(line) = lines[cursor].strip_prefix('+') else {
                    return Err(PatchParseError::new(format!(
                        "Add File '{}' requires every content line to start with '+'",
                        path.display()
                    )));
                };
                content.push_str(line);
                content.push('\n');
                cursor += 1;
            }
            PatchOperation::Add { path, content }
        } else if let Some(raw_path) = header.strip_prefix(UPDATE_FILE) {
            let path = parse_path(raw_path)?;
            let mut hunks = Vec::new();
            while cursor < end && !is_file_header(lines[cursor]) {
                if !lines[cursor].starts_with("@@") {
                    return Err(PatchParseError::new(format!(
                        "Update File '{}' requires an '@@' hunk header",
                        path.display()
                    )));
                }
                cursor += 1;
                let mut before = Vec::new();
                let mut after = Vec::new();
                let mut changed = false;
                while cursor < end
                    && !is_file_header(lines[cursor])
                    && !lines[cursor].starts_with("@@")
                {
                    let line = lines[cursor];
                    cursor += 1;
                    if let Some(line) = line.strip_prefix(' ') {
                        before.push(line.to_owned());
                        after.push(line.to_owned());
                    } else if let Some(line) = line.strip_prefix('-') {
                        before.push(line.to_owned());
                        changed = true;
                    } else if let Some(line) = line.strip_prefix('+') {
                        after.push(line.to_owned());
                        changed = true;
                    } else if line.is_empty() {
                        before.push(String::new());
                        after.push(String::new());
                    } else {
                        return Err(PatchParseError::new(format!(
                            "Update File '{}' contains a hunk line without ' ', '+', or '-'",
                            path.display()
                        )));
                    }
                }
                if !changed {
                    return Err(PatchParseError::new(format!(
                        "Update File '{}' contains a hunk with no change",
                        path.display()
                    )));
                }
                hunks.push(UpdateHunk { before, after });
            }
            if hunks.is_empty() {
                return Err(PatchParseError::new(format!(
                    "Update File '{}' requires at least one hunk",
                    path.display()
                )));
            }
            PatchOperation::Update { path, hunks }
        } else if let Some(raw_path) = header.strip_prefix(DELETE_FILE) {
            let path = parse_path(raw_path)?;
            if cursor < end && !is_file_header(lines[cursor]) {
                return Err(PatchParseError::new(format!(
                    "Delete File '{}' does not accept body lines",
                    path.display()
                )));
            }
            PatchOperation::Delete { path }
        } else {
            return Err(PatchParseError::new(format!(
                "unsupported patch directive: {header}"
            )));
        };
        if !paths.insert(operation.path().clone()) {
            return Err(PatchParseError::new(format!(
                "patch targets '{}' more than once",
                operation.path().display()
            )));
        }
        operations.push(operation);
    }
    if operations.is_empty() {
        return Err(PatchParseError::new("patch contains no file operation"));
    }
    Ok(ParsedPatch { operations })
}

pub(super) fn apply_update_hunks(
    original: &str,
    hunks: &[UpdateHunk],
) -> Result<String, PatchParseError> {
    let trailing_newline = original.ends_with('\n');
    let mut lines = split_lines(original);
    for (index, hunk) in hunks.iter().enumerate() {
        let positions = matching_positions(&lines, &hunk.before);
        let position = match positions.as_slice() {
            [position] => *position,
            [] => {
                return Err(PatchParseError::new(format!(
                    "update hunk {} did not match the current file",
                    index + 1
                )))
            }
            _ => {
                return Err(PatchParseError::new(format!(
                    "update hunk {} matched more than one location",
                    index + 1
                )))
            }
        };
        lines.splice(position..position + hunk.before.len(), hunk.after.clone());
    }
    if lines.is_empty() {
        return Ok(String::new());
    }
    let mut updated = lines.join("\n");
    if trailing_newline {
        updated.push('\n');
    }
    if updated == original {
        return Err(PatchParseError::new(
            "patch does not change the current file",
        ));
    }
    Ok(updated)
}

pub(super) fn parse_path(raw: &str) -> Result<PatchPath, PatchParseError> {
    if raw.is_empty() || raw.trim() != raw || raw.contains('\\') {
        return Err(PatchParseError::new(
            "patch paths must be non-empty normalized workspace-relative paths",
        ));
    }
    let relative = PathBuf::from(raw);
    let components = relative
        .components()
        .map(|component| match component {
            Component::Normal(component) => component.to_str().map(str::to_owned),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    let Some(components) = components else {
        return Err(PatchParseError::new(format!(
            "patch path '{raw}' is not a normalized workspace-relative path"
        )));
    };
    let normalized = components.join("/");
    if normalized.is_empty() || normalized != raw {
        return Err(PatchParseError::new(format!(
            "patch path '{raw}' is not a normalized workspace-relative path"
        )));
    }
    Ok(PatchPath {
        display: normalized.clone(),
        relative: PathBuf::from(normalized),
    })
}

fn is_file_header(line: &str) -> bool {
    line.starts_with(ADD_FILE) || line.starts_with(UPDATE_FILE) || line.starts_with(DELETE_FILE)
}

fn split_lines(content: &str) -> Vec<String> {
    if content.is_empty() {
        return Vec::new();
    }
    let mut lines = content.split('\n').map(str::to_owned).collect::<Vec<_>>();
    if content.ends_with('\n') {
        lines.pop();
    }
    lines
}

fn matching_positions(lines: &[String], needle: &[String]) -> Vec<usize> {
    if needle.is_empty() {
        return if lines.is_empty() {
            vec![0]
        } else {
            Vec::new()
        };
    }
    if needle.len() > lines.len() {
        return Vec::new();
    }
    (0..=lines.len() - needle.len())
        .filter(|start| lines[*start..*start + needle.len()] == *needle)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{apply_update_hunks, parse_patch, PatchOperation};

    #[test]
    fn parses_add_update_and_delete() {
        let patch = "*** Begin Patch\n*** Add File: src/new.rs\n+fn new() {}\n*** Update File: src/lib.rs\n@@ function\n-old\n+new\n context\n*** Delete File: old.txt\n*** End Patch";
        let parsed = parse_patch(patch).unwrap();
        assert_eq!(parsed.operations.len(), 3);
        assert!(matches!(
            &parsed.operations[0],
            PatchOperation::Add { path, content }
                if path.display() == "src/new.rs" && content == "fn new() {}\n"
        ));
        assert!(matches!(
            &parsed.operations[1],
            PatchOperation::Update { path, hunks }
                if path.display() == "src/lib.rs" && hunks.len() == 1
        ));
        assert!(matches!(
            &parsed.operations[2],
            PatchOperation::Delete { path } if path.display() == "old.txt"
        ));
    }

    #[test]
    fn applies_multiple_unique_hunks_and_preserves_final_newline() {
        let parsed = parse_patch(
            "*** Begin Patch\n*** Update File: src/lib.rs\n@@\n alpha\n-beta\n+BETA\n@@\n gamma\n+delta\n*** End Patch",
        )
        .unwrap();
        let PatchOperation::Update { hunks, .. } = &parsed.operations[0] else {
            panic!("expected update")
        };
        assert_eq!(
            apply_update_hunks("alpha\nbeta\ngamma\n", hunks).unwrap(),
            "alpha\nBETA\ngamma\ndelta\n"
        );
    }

    #[test]
    fn rejects_ambiguous_or_missing_context() {
        for original in ["same\nsame\n", "other\n"] {
            let parsed = parse_patch(
                "*** Begin Patch\n*** Update File: file.txt\n@@\n-same\n+changed\n*** End Patch",
            )
            .unwrap();
            let PatchOperation::Update { hunks, .. } = &parsed.operations[0] else {
                panic!("expected update")
            };
            assert!(apply_update_hunks(original, hunks).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_and_escaping_paths() {
        let duplicate = "*** Begin Patch\n*** Add File: same.txt\n+one\n*** Delete File: same.txt\n*** End Patch";
        assert!(parse_patch(duplicate).is_err());
        let alias_duplicate = "*** Begin Patch\n*** Add File: dir/file.txt\n+one\n*** Delete File: dir//file.txt\n*** End Patch";
        assert!(parse_patch(alias_duplicate).is_err());
        for path in [
            "../outside",
            "/tmp/outside",
            "./inside",
            "dir\\file",
            "dir//file",
            "dir/file/",
        ] {
            let patch = format!("*** Begin Patch\n*** Add File: {path}\n+content\n*** End Patch");
            assert!(parse_patch(&patch).is_err(), "accepted {path}");
        }
    }
}
