//! Host-guarded workspace inspection Tools.
//!
//! Both Tools bind model paths to one composition-time workspace, apply the
//! invocation's effective readable roots, respect ignore files, never follow
//! symlinks, and report incomplete searches explicitly.
//! Traversal and content matching use ripgrep's Rust libraries directly, so
//! the Host does not need an `rg` executable and no shell process is spawned.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use globset::{GlobBuilder, GlobMatcher};
use grep_matcher::Matcher;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{
    BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkFinish, SinkMatch,
};
use ignore::{DirEntry, WalkBuilder};
use orchestral_core::tool_protocol::{
    EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency,
    ToolOutcome, ToolRestriction,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};

use super::support::{canonical_roots, GuardedWorkspace, WorkspacePathError};

const DEFAULT_FILE_SEARCH_LIMIT: usize = 100;
const MAX_FILE_SEARCH_LIMIT: usize = 500;
const DEFAULT_TEXT_SEARCH_LIMIT: usize = 50;
const MAX_TEXT_SEARCH_LIMIT: usize = 200;
const MAX_CONTEXT_LINES: usize = 3;
const MAX_PATTERN_BYTES: usize = 2 * 1024;
const MAX_SCANNED_ENTRIES: usize = 100_000;
const MAX_SCANNED_FILES: usize = 20_000;
const MAX_TEXT_SCAN_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SEARCH_LINE_BYTES: usize = 4 * 1024 * 1024;
const MAX_PREVIEW_CHARS: usize = 2_000;
const MAX_WARNING_COUNT: usize = 8;
const MAX_SEARCH_DEADLINE_MS: u64 = 15_000;
const SEARCH_OUTPUT_RESERVE_BYTES: usize = 4 * 1024;

const NOISE_DIRECTORIES: &[&str] = &[
    ".git",
    ".orchestral",
    "target",
    "node_modules",
    "__pycache__",
];

#[derive(Debug, Clone)]
pub struct GuardedFileSearchExecutor {
    workspace: GuardedWorkspace,
}

impl GuardedFileSearchExecutor {
    pub fn new(workspace: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            workspace: GuardedWorkspace::new(workspace)?,
        })
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedFileSearchExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let roots = match effective_readable_roots(&execution) {
            Ok(roots) => roots,
            Err(outcome) => return outcome,
        };
        let Some(pattern) = string_argument(&execution, "pattern") else {
            return rejected(
                "file_search_pattern_missing",
                "file_search pattern must be a non-empty glob",
            );
        };
        if pattern.len() > MAX_PATTERN_BYTES {
            return rejected(
                "file_search_pattern_too_large",
                format!("file_search pattern exceeds {MAX_PATTERN_BYTES} bytes"),
            );
        }
        let case_sensitive = bool_argument(&execution, "case_sensitive", true);
        let matcher = match compile_glob(pattern, case_sensitive) {
            Ok(matcher) => matcher,
            Err(message) => return rejected("file_search_pattern_invalid", message),
        };
        let path = execution
            .invocation
            .arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or(".");
        let target = match self.workspace.resolve_existing(path, &roots) {
            Ok(target) => target,
            Err(error) => return workspace_path_outcome(error),
        };
        if !target.canonical().is_dir() {
            return rejected(
                "file_search_root_not_directory",
                "file_search path must resolve to a directory",
            );
        }
        let limit = usize_argument(
            &execution,
            "limit",
            DEFAULT_FILE_SEARCH_LIMIT,
            MAX_FILE_SEARCH_LIMIT,
        );
        let include_directories = bool_argument(&execution, "include_directories", false);
        let limits = match SearchLimits::from_execution(&execution) {
            Ok(limits) => limits,
            Err(outcome) => return outcome,
        };
        let cancellation = execution.cancellation.clone();
        let scan_cancellation = cancellation.clone();
        let workspace = self.workspace.clone();
        let root = target.canonical().to_path_buf();
        let root_display = target.display();
        let task = tokio::task::spawn_blocking(move || {
            scan_file_paths(
                &workspace,
                &root,
                &matcher,
                include_directories,
                limit,
                limits,
                &scan_cancellation,
            )
        });
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return ToolOutcome::Cancelled,
            result = task => result,
        };
        match result {
            Ok(BlockingSearch::Completed(page)) => ToolOutcome::Completed {
                output: search_page_output(root_display, page).into(),
            },
            Ok(BlockingSearch::Cancelled) => ToolOutcome::Cancelled,
            Err(error) => failed("file_search_worker_failed", error.to_string(), true),
        }
    }
}

pub fn guarded_file_search_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/file_search/v1"),
        model_schema: ModelToolSchema {
            name: "file_search".to_owned(),
            description: "Find workspace-relative file paths with a glob. Respects .gitignore, includes hidden source files, skips dependency/build noise, never follows symlinks, and reports partial results explicitly."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["pattern"],
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob matched against both workspace-relative path and basename, for example `**/*.rs` or `Cargo.toml`."
                    },
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative directory to search. Defaults to `.`."
                    },
                    "case_sensitive": {
                        "type": "boolean",
                        "description": "Whether glob matching is case-sensitive. Defaults to true."
                    },
                    "include_directories": {
                        "type": "boolean",
                        "description": "Also return matching directories. Defaults to false."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_FILE_SEARCH_LIMIT,
                        "description": "Maximum returned paths. Defaults to 100."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: search_output_schema(json!({
            "type": "array",
            "items": { "type": "string" }
        })),
        effect_scopes: BTreeSet::from([EffectScope::FilesystemRead]),
        restriction,
        idempotency: ToolIdempotency::Pure,
        concurrency: ToolConcurrency::ParallelSafe,
    }
}

#[derive(Debug, Clone)]
pub struct GuardedTextSearchExecutor {
    workspace: GuardedWorkspace,
}

impl GuardedTextSearchExecutor {
    pub fn new(workspace: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            workspace: GuardedWorkspace::new(workspace)?,
        })
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedTextSearchExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let roots = match effective_readable_roots(&execution) {
            Ok(roots) => roots,
            Err(outcome) => return outcome,
        };
        let Some(pattern) = string_argument(&execution, "pattern") else {
            return rejected(
                "text_search_pattern_missing",
                "text_search pattern must be a non-empty literal or regular expression",
            );
        };
        if pattern.len() > MAX_PATTERN_BYTES {
            return rejected(
                "text_search_pattern_too_large",
                format!("text_search pattern exceeds {MAX_PATTERN_BYTES} bytes"),
            );
        }
        let literal = bool_argument(&execution, "literal", false);
        let case_sensitive = bool_argument(&execution, "case_sensitive", true);
        let matcher = match compile_text_matcher(pattern, literal, case_sensitive) {
            Ok(matcher) => matcher,
            Err(message) => return rejected("text_search_pattern_invalid", message),
        };
        let include = execution
            .invocation
            .arguments
            .get("include")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let include_matcher = match include.map(|pattern| compile_glob(pattern, true)) {
            Some(Ok(matcher)) => Some(matcher),
            Some(Err(message)) => return rejected("text_search_include_invalid", message),
            None => None,
        };
        let path = execution
            .invocation
            .arguments
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or(".");
        let target = match self.workspace.resolve_existing(path, &roots) {
            Ok(target) => target,
            Err(error) => return workspace_path_outcome(error),
        };
        let limit = usize_argument(
            &execution,
            "limit",
            DEFAULT_TEXT_SEARCH_LIMIT,
            MAX_TEXT_SEARCH_LIMIT,
        );
        let context = usize_argument(&execution, "context", 0, MAX_CONTEXT_LINES);
        let limits = match SearchLimits::from_execution(&execution) {
            Ok(limits) => limits,
            Err(outcome) => return outcome,
        };
        let cancellation = execution.cancellation.clone();
        let scan_cancellation = cancellation.clone();
        let workspace = self.workspace.clone();
        let root = target.canonical().to_path_buf();
        let root_display = target.display();
        let spec = TextScanSpec {
            matcher,
            include_matcher,
            context,
            limit,
            limits,
        };
        let task = tokio::task::spawn_blocking(move || {
            scan_text(&workspace, &root, &spec, &scan_cancellation)
        });
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return ToolOutcome::Cancelled,
            result = task => result,
        };
        match result {
            Ok(BlockingSearch::Completed(page)) => ToolOutcome::Completed {
                output: search_page_output(root_display, page).into(),
            },
            Ok(BlockingSearch::Cancelled) => ToolOutcome::Cancelled,
            Err(error) => failed("text_search_worker_failed", error.to_string(), true),
        }
    }
}

pub fn guarded_text_search_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/text_search/v1"),
        model_schema: ModelToolSchema {
            name: "text_search".to_owned(),
            description: "Search UTF-8 workspace files with ripgrep's streaming Rust matcher using a regular expression or literal. Results are resource-bounded, gitignore-aware, and explicitly complete or partial."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["pattern"],
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Ripgrep-compatible Rust regular expression, or exact text when literal is true."
                    },
                    "literal": {
                        "type": "boolean",
                        "description": "Escape pattern as literal text. Defaults to false."
                    },
                    "case_sensitive": {
                        "type": "boolean",
                        "description": "Whether matching is case-sensitive. Defaults to true."
                    },
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative file or directory. Defaults to `.`."
                    },
                    "include": {
                        "type": "string",
                        "description": "Optional file glob, for example `**/*.rs`."
                    },
                    "context": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": MAX_CONTEXT_LINES,
                        "description": "Context lines before and after each match. Defaults to 0."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_TEXT_SEARCH_LIMIT,
                        "description": "Maximum matching lines. Defaults to 50."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: search_output_schema(json!({
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "path", "line_number", "column", "match_start_byte",
                    "match_end_byte", "preview", "preview_truncated",
                    "context_before", "context_after"
                ],
                "properties": {
                    "path": { "type": "string" },
                    "line_number": { "type": "integer" },
                    "column": { "type": "integer" },
                    "match_start_byte": { "type": "integer" },
                    "match_end_byte": { "type": "integer" },
                    "preview": { "type": "string" },
                    "preview_truncated": { "type": "boolean" },
                    "context_before": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "context_after": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "additionalProperties": false
            }
        })),
        effect_scopes: BTreeSet::from([EffectScope::FilesystemRead]),
        restriction,
        idempotency: ToolIdempotency::Pure,
        concurrency: ToolConcurrency::ParallelSafe,
    }
}

#[derive(Debug, Clone, Copy)]
struct SearchLimits {
    output_bytes: usize,
    deadline: Duration,
}

impl SearchLimits {
    fn from_execution(execution: &GuardedToolExecution) -> Result<Self, ToolOutcome> {
        let bounds = execution.effective_policy.bounds();
        let configured_output =
            usize::try_from(bounds.max_output_bytes.unwrap_or(512 * 1024)).unwrap_or(usize::MAX);
        if configured_output < SEARCH_OUTPUT_RESERVE_BYTES.saturating_add(512) {
            return Err(failed(
                "search_output_limit_too_small",
                "effective output policy leaves fewer than 512 bytes for search matches",
                false,
            ));
        }
        let output_bytes = configured_output.saturating_sub(SEARCH_OUTPUT_RESERVE_BYTES);
        let deadline_ms = bounds
            .max_timeout_ms
            .unwrap_or(MAX_SEARCH_DEADLINE_MS)
            .clamp(1, MAX_SEARCH_DEADLINE_MS);
        Ok(Self {
            output_bytes,
            deadline: Duration::from_millis(deadline_ms),
        })
    }
}

#[derive(Debug)]
struct TextScanSpec {
    matcher: RegexMatcher,
    include_matcher: Option<GlobMatcher>,
    context: usize,
    limit: usize,
    limits: SearchLimits,
}

#[derive(Debug)]
enum BlockingSearch<T> {
    Completed(T),
    Cancelled,
}

#[derive(Debug, Default)]
struct SearchStats {
    scanned_entries: usize,
    considered_files: usize,
    searched_files: usize,
    scanned_bytes: u64,
    skipped_binary_files: usize,
    skipped_unreadable_files: usize,
}

#[derive(Debug)]
struct SearchPage<T> {
    matches: Vec<T>,
    reasons: BTreeSet<&'static str>,
    warnings: Vec<String>,
    stats: SearchStats,
}

fn scan_file_paths(
    workspace: &GuardedWorkspace,
    root: &Path,
    matcher: &GlobMatcher,
    include_directories: bool,
    limit: usize,
    limits: SearchLimits,
    cancellation: &CancellationToken,
) -> BlockingSearch<SearchPage<String>> {
    let started = Instant::now();
    let mut page = SearchPage {
        matches: Vec::new(),
        reasons: BTreeSet::new(),
        warnings: Vec::new(),
        stats: SearchStats::default(),
    };
    let builder = walk_builder(workspace, root);
    for entry in builder.build() {
        if cancellation.is_cancelled() {
            return BlockingSearch::Cancelled;
        }
        if started.elapsed() >= limits.deadline {
            page.reasons.insert("timeout");
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                page.reasons.insert("scan_error");
                push_warning(&mut page.warnings, error.to_string());
                continue;
            }
        };
        if !entry.path().starts_with(root) {
            continue;
        }
        page.stats.scanned_entries = page.stats.scanned_entries.saturating_add(1);
        if page.stats.scanned_entries > MAX_SCANNED_ENTRIES {
            page.reasons.insert("scan_limit");
            break;
        }
        if entry.depth() == 0 || entry.file_type().is_some_and(|kind| kind.is_symlink()) {
            continue;
        }
        let is_dir = entry.file_type().is_some_and(|kind| kind.is_dir());
        let is_file = entry.file_type().is_some_and(|kind| kind.is_file());
        if !(is_file || include_directories && is_dir) {
            continue;
        }
        let display = workspace.display_path(entry.path());
        let basename = entry.file_name().to_string_lossy();
        if matcher.is_match(&display) || matcher.is_match(basename.as_ref()) {
            page.matches.push(display);
            if page.matches.len() > limit {
                page.reasons.insert("result_limit");
                break;
            }
        }
    }
    page.matches.sort();
    page.matches.dedup();
    if page.matches.len() > limit {
        page.matches.truncate(limit);
        page.reasons.insert("result_limit");
    }
    enforce_output_budget(&mut page, limits.output_bytes);
    BlockingSearch::Completed(page)
}

fn scan_text(
    workspace: &GuardedWorkspace,
    root: &Path,
    spec: &TextScanSpec,
    cancellation: &CancellationToken,
) -> BlockingSearch<SearchPage<Value>> {
    let started = Instant::now();
    let mut page = SearchPage {
        matches: Vec::new(),
        reasons: BTreeSet::new(),
        warnings: Vec::new(),
        stats: SearchStats::default(),
    };
    let mut files = Vec::new();
    if root.is_file() {
        let display = workspace.display_path(root);
        let basename = root
            .file_name()
            .map(|name| name.to_string_lossy())
            .unwrap_or_default();
        page.stats.considered_files = 1;
        if spec
            .include_matcher
            .as_ref()
            .is_none_or(|matcher| matcher.is_match(&display) || matcher.is_match(basename.as_ref()))
        {
            files.push((display, root.to_path_buf()));
        }
    } else {
        let builder = walk_builder(workspace, root);
        for entry in builder.build() {
            if cancellation.is_cancelled() {
                return BlockingSearch::Cancelled;
            }
            if started.elapsed() >= spec.limits.deadline {
                page.reasons.insert("timeout");
                break;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    page.reasons.insert("scan_error");
                    push_warning(&mut page.warnings, error.to_string());
                    continue;
                }
            };
            if !entry.path().starts_with(root) {
                continue;
            }
            page.stats.scanned_entries = page.stats.scanned_entries.saturating_add(1);
            if page.stats.scanned_entries > MAX_SCANNED_ENTRIES {
                page.reasons.insert("scan_limit");
                break;
            }
            if entry.depth() == 0
                || entry.file_type().is_some_and(|kind| kind.is_symlink())
                || !entry.file_type().is_some_and(|kind| kind.is_file())
            {
                continue;
            }
            page.stats.considered_files = page.stats.considered_files.saturating_add(1);
            if page.stats.considered_files > MAX_SCANNED_FILES {
                page.reasons.insert("file_limit");
                break;
            }
            let display = workspace.display_path(entry.path());
            let basename = entry.file_name().to_string_lossy();
            if spec.include_matcher.as_ref().is_some_and(|matcher| {
                !matcher.is_match(&display) && !matcher.is_match(basename.as_ref())
            }) {
                continue;
            }
            files.push((display, entry.into_path()));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));

    for (display, path) in files {
        if cancellation.is_cancelled() {
            return BlockingSearch::Cancelled;
        }
        if started.elapsed() >= spec.limits.deadline {
            page.reasons.insert("timeout");
            break;
        }
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) => {
                page.stats.skipped_unreadable_files =
                    page.stats.skipped_unreadable_files.saturating_add(1);
                page.reasons.insert("read_error");
                push_warning(&mut page.warnings, format!("{display}: {error}"));
                continue;
            }
        };
        let remaining_bytes = MAX_TEXT_SCAN_BYTES.saturating_sub(page.stats.scanned_bytes);
        if remaining_bytes == 0 {
            page.reasons.insert("byte_limit");
            break;
        }
        let deadline = started + spec.limits.deadline;
        let mut reader = BoundedSearchReader::new(file, cancellation, deadline, remaining_bytes);
        let remaining_matches = spec
            .limit
            .saturating_add(1)
            .saturating_sub(page.matches.len())
            .max(1);
        let mut sink = TextMatchSink::new(&spec.matcher, remaining_matches);
        let mut searcher = SearcherBuilder::new()
            .line_number(true)
            .before_context(spec.context)
            .after_context(spec.context)
            .binary_detection(BinaryDetection::quit(b'\0'))
            .heap_limit(Some(MAX_SEARCH_LINE_BYTES))
            .build();
        let search_result = searcher.search_reader(&spec.matcher, &mut reader, &mut sink);
        page.stats.scanned_bytes = page.stats.scanned_bytes.saturating_add(reader.bytes_read);
        match reader.stop {
            Some(ReaderStop::Cancelled) => return BlockingSearch::Cancelled,
            Some(ReaderStop::Timeout) => {
                page.reasons.insert("timeout");
                break;
            }
            Some(ReaderStop::ByteLimit) => {
                page.reasons.insert("byte_limit");
                break;
            }
            None => {}
        }
        if sink.binary || sink.invalid_utf8 {
            page.stats.skipped_binary_files = page.stats.skipped_binary_files.saturating_add(1);
            continue;
        }
        if let Err(error) = search_result {
            page.stats.skipped_unreadable_files =
                page.stats.skipped_unreadable_files.saturating_add(1);
            page.reasons.insert("search_error");
            push_warning(&mut page.warnings, format!("{display}: {error}"));
            continue;
        }
        page.stats.searched_files = page.stats.searched_files.saturating_add(1);
        page.matches
            .extend(sink.into_matches(&display, spec.context));
        if page.matches.len() > spec.limit {
            page.reasons.insert("result_limit");
            break;
        }
    }
    if page.matches.len() > spec.limit {
        page.matches.truncate(spec.limit);
    }
    enforce_output_budget(&mut page, spec.limits.output_bytes);
    BlockingSearch::Completed(page)
}

#[derive(Debug)]
struct RawTextMatch {
    line_number: usize,
    match_start_byte: usize,
    match_end_byte: usize,
    line: String,
}

struct TextMatchSink<'a> {
    matcher: &'a RegexMatcher,
    max_matches: usize,
    matches: Vec<RawTextMatch>,
    lines: BTreeMap<usize, String>,
    binary: bool,
    invalid_utf8: bool,
}

impl<'a> TextMatchSink<'a> {
    fn new(matcher: &'a RegexMatcher, max_matches: usize) -> Self {
        Self {
            matcher,
            max_matches,
            matches: Vec::new(),
            lines: BTreeMap::new(),
            binary: false,
            invalid_utf8: false,
        }
    }

    fn into_matches(self, path: &str, context: usize) -> Vec<Value> {
        let Self { matches, lines, .. } = self;
        matches
            .into_iter()
            .map(|found| {
                let context_before = (found.line_number.saturating_sub(context)..found.line_number)
                    .filter_map(|line| lines.get(&line))
                    .map(|line| truncate_chars(line, MAX_PREVIEW_CHARS))
                    .collect::<Vec<_>>();
                let context_after = (found.line_number.saturating_add(1)
                    ..=found.line_number.saturating_add(context))
                    .filter_map(|line| lines.get(&line))
                    .map(|line| truncate_chars(line, MAX_PREVIEW_CHARS))
                    .collect::<Vec<_>>();
                let (preview, preview_truncated) =
                    preview_match(&found.line, found.match_start_byte, found.match_end_byte);
                json!({
                    "path": path,
                    "line_number": found.line_number,
                    "column": found.line[..found.match_start_byte].chars().count() + 1,
                    "match_start_byte": found.match_start_byte,
                    "match_end_byte": found.match_end_byte,
                    "preview": preview,
                    "preview_truncated": preview_truncated,
                    "context_before": context_before,
                    "context_after": context_after,
                })
            })
            .collect()
    }

    fn remember_line(&mut self, line_number: Option<u64>, bytes: &[u8]) -> io::Result<()> {
        let Some(line_number) = line_number.and_then(|line| usize::try_from(line).ok()) else {
            return Err(io::Error::other(
                "ripgrep searcher did not provide a representable line number",
            ));
        };
        let bytes = strip_line_terminator(bytes);
        let Ok(line) = std::str::from_utf8(bytes) else {
            self.invalid_utf8 = true;
            return Ok(());
        };
        self.lines.insert(line_number, line.to_owned());
        Ok(())
    }
}

impl Sink for TextMatchSink<'_> {
    type Error = io::Error;

    fn matched(
        &mut self,
        _searcher: &Searcher,
        matched: &SinkMatch<'_>,
    ) -> Result<bool, Self::Error> {
        self.remember_line(matched.line_number(), matched.bytes())?;
        if self.invalid_utf8 {
            return Ok(false);
        }
        let line_number = matched
            .line_number()
            .and_then(|line| usize::try_from(line).ok())
            .ok_or_else(|| io::Error::other("matching line number is unavailable"))?;
        let line = self
            .lines
            .get(&line_number)
            .expect("matching line was inserted before lookup");
        let found = self
            .matcher
            .find(line.as_bytes())
            .map_err(|error| io::Error::other(error.to_string()))?
            .ok_or_else(|| io::Error::other("searcher reported an unconfirmed match"))?;
        self.matches.push(RawTextMatch {
            line_number,
            match_start_byte: found.start(),
            match_end_byte: found.end(),
            line: line.clone(),
        });
        Ok(self.matches.len() < self.max_matches)
    }

    fn context(
        &mut self,
        _searcher: &Searcher,
        context: &SinkContext<'_>,
    ) -> Result<bool, Self::Error> {
        self.remember_line(context.line_number(), context.bytes())?;
        Ok(!self.invalid_utf8)
    }

    fn binary_data(
        &mut self,
        _searcher: &Searcher,
        _binary_byte_offset: u64,
    ) -> Result<bool, Self::Error> {
        self.binary = true;
        Ok(false)
    }

    fn finish(&mut self, _searcher: &Searcher, _finish: &SinkFinish) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderStop {
    Cancelled,
    Timeout,
    ByteLimit,
}

struct BoundedSearchReader<'a> {
    inner: File,
    cancellation: &'a CancellationToken,
    deadline: Instant,
    remaining_bytes: u64,
    bytes_read: u64,
    stop: Option<ReaderStop>,
}

impl<'a> BoundedSearchReader<'a> {
    fn new(
        inner: File,
        cancellation: &'a CancellationToken,
        deadline: Instant,
        remaining_bytes: u64,
    ) -> Self {
        Self {
            inner,
            cancellation,
            deadline,
            remaining_bytes,
            bytes_read: 0,
            stop: None,
        }
    }

    fn interrupt(&mut self, stop: ReaderStop) -> io::Result<usize> {
        self.stop = Some(stop);
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "workspace search interrupted by its resource policy",
        ))
    }
}

impl Read for BoundedSearchReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if let Some(stop) = self.stop {
            return self.interrupt(stop);
        }
        if self.cancellation.is_cancelled() {
            return self.interrupt(ReaderStop::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return self.interrupt(ReaderStop::Timeout);
        }
        if self.remaining_bytes == 0 {
            let mut probe = [0_u8; 1];
            let read = self.inner.read(&mut probe)?;
            if read == 0 {
                return Ok(0);
            }
            self.bytes_read = self.bytes_read.saturating_add(read as u64);
            return self.interrupt(ReaderStop::ByteLimit);
        }
        let allowed = buffer
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(read as u64);
        self.bytes_read = self.bytes_read.saturating_add(read as u64);
        if read > 0 && self.remaining_bytes == 0 {
            let mut probe = [0_u8; 1];
            let overflow = self.inner.read(&mut probe)?;
            if overflow > 0 {
                self.bytes_read = self.bytes_read.saturating_add(overflow as u64);
                self.stop = Some(ReaderStop::ByteLimit);
            }
        }
        Ok(read)
    }
}

fn strip_line_terminator(mut bytes: &[u8]) -> &[u8] {
    if bytes.ends_with(b"\n") {
        bytes = &bytes[..bytes.len().saturating_sub(1)];
    }
    if bytes.ends_with(b"\r") {
        bytes = &bytes[..bytes.len().saturating_sub(1)];
    }
    bytes
}

fn walk_builder(workspace: &GuardedWorkspace, root: &Path) -> WalkBuilder {
    let mut builder = WalkBuilder::new(workspace.root());
    builder
        .hidden(false)
        .parents(false)
        .ignore(true)
        .git_ignore(true)
        .git_global(false)
        .git_exclude(true)
        .require_git(false)
        .follow_links(false)
        .sort_by_file_path(|left, right| left.cmp(right))
        .filter_entry({
            let root = root.to_path_buf();
            move |entry| keep_walk_entry(entry, &root)
        });
    builder
}

fn keep_walk_entry(entry: &DirEntry, root: &Path) -> bool {
    if entry.depth() == 0 || entry.path() == root {
        return true;
    }
    if !entry.path().starts_with(root) && !root.starts_with(entry.path()) {
        return false;
    }
    if !entry.file_type().is_some_and(|kind| kind.is_dir()) {
        return true;
    }
    !NOISE_DIRECTORIES
        .iter()
        .any(|name| entry.file_name() == *name)
}

fn compile_glob(pattern: &str, case_sensitive: bool) -> Result<GlobMatcher, String> {
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .case_insensitive(!case_sensitive)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|error| error.to_string())
}

fn compile_text_matcher(
    pattern: &str,
    literal: bool,
    case_sensitive: bool,
) -> Result<RegexMatcher, String> {
    RegexMatcherBuilder::new()
        .case_insensitive(!case_sensitive)
        .fixed_strings(literal)
        .line_terminator(Some(b'\n'))
        .build(pattern)
        .map_err(|error| error.to_string())
}

fn preview_match(line: &str, match_start: usize, match_end: usize) -> (String, bool) {
    let total_chars = line.chars().count();
    if total_chars <= MAX_PREVIEW_CHARS {
        return (line.to_owned(), false);
    }
    let match_start_chars = line[..match_start].chars().count();
    let match_chars = line[match_start..match_end].chars().count().max(1);
    let start = match_start_chars.saturating_sub(MAX_PREVIEW_CHARS.saturating_sub(match_chars) / 3);
    let mut body = line
        .chars()
        .skip(start)
        .take(MAX_PREVIEW_CHARS)
        .collect::<String>();
    if start > 0 {
        body.insert(0, '…');
    }
    if start.saturating_add(MAX_PREVIEW_CHARS) < total_chars {
        body.push('…');
    }
    (body, true)
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_owned()
    } else {
        format!("{}…", value.chars().take(max_chars).collect::<String>())
    }
}

fn enforce_output_budget<T: serde::Serialize>(page: &mut SearchPage<T>, max_bytes: usize) {
    loop {
        let estimate = serde_json::to_vec(&json!({
            "matches": page.matches,
            "partial_reasons": page.reasons,
            "warnings": page.warnings,
        }))
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
        if estimate <= max_bytes || page.matches.is_empty() {
            break;
        }
        page.matches.pop();
        page.reasons.insert("output_limit");
    }
}

fn search_page_output<T: serde::Serialize>(root: String, page: SearchPage<T>) -> Value {
    let complete = page.reasons.is_empty();
    let count = page.matches.len();
    json!({
        "root": root,
        "matches": page.matches,
        "count": count,
        "completeness": if complete { "complete" } else { "partial" },
        "partial_reasons": page.reasons,
        "refinement": if complete {
            ""
        } else {
            "Results are incomplete; narrow path/include/pattern and search again before concluding that no other match exists."
        },
        "warnings": page.warnings,
        "stats": {
            "scanned_entries": page.stats.scanned_entries,
            "considered_files": page.stats.considered_files,
            "searched_files": page.stats.searched_files,
            "scanned_bytes": page.stats.scanned_bytes,
            "skipped_binary_files": page.stats.skipped_binary_files,
            "skipped_unreadable_files": page.stats.skipped_unreadable_files,
        }
    })
}

fn search_output_schema(matches: Value) -> Value {
    json!({
        "type": "object",
        "required": [
            "root", "matches", "count", "completeness", "partial_reasons",
            "refinement", "warnings", "stats"
        ],
        "properties": {
            "root": { "type": "string" },
            "matches": matches,
            "count": { "type": "integer" },
            "completeness": { "type": "string", "enum": ["complete", "partial"] },
            "partial_reasons": {
                "type": "array",
                "items": { "type": "string" }
            },
            "refinement": { "type": "string" },
            "warnings": {
                "type": "array",
                "items": { "type": "string" }
            },
            "stats": {
                "type": "object",
                "required": [
                    "scanned_entries", "considered_files", "searched_files", "scanned_bytes",
                    "skipped_binary_files", "skipped_unreadable_files"
                ],
                "properties": {
                    "scanned_entries": { "type": "integer" },
                    "considered_files": { "type": "integer" },
                    "searched_files": { "type": "integer" },
                    "scanned_bytes": { "type": "integer" },
                    "skipped_binary_files": { "type": "integer" },
                    "skipped_unreadable_files": { "type": "integer" }
                },
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    })
}

fn effective_readable_roots(execution: &GuardedToolExecution) -> Result<Vec<PathBuf>, ToolOutcome> {
    match canonical_roots(
        &execution
            .effective_policy
            .bounds()
            .filesystem
            .readable_roots,
    ) {
        Ok(roots) if !roots.is_empty() => Ok(roots),
        Ok(_) => Err(rejected(
            "filesystem_root_denied",
            "effective policy contains no readable filesystem root",
        )),
        Err(message) => Err(rejected("filesystem_root_invalid", message)),
    }
}

fn string_argument<'a>(execution: &'a GuardedToolExecution, name: &str) -> Option<&'a str> {
    execution
        .invocation
        .arguments
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bool_argument(execution: &GuardedToolExecution, name: &str, default: bool) -> bool {
    execution
        .invocation
        .arguments
        .get(name)
        .and_then(Value::as_bool)
        .unwrap_or(default)
}

fn usize_argument(
    execution: &GuardedToolExecution,
    name: &str,
    default: usize,
    maximum: usize,
) -> usize {
    execution
        .invocation
        .arguments
        .get(name)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default)
        .min(maximum)
}

fn push_warning(warnings: &mut Vec<String>, warning: String) {
    if warnings.len() < MAX_WARNING_COUNT {
        warnings.push(truncate_chars(&warning, 512));
    }
}

fn workspace_path_outcome(error: WorkspacePathError) -> ToolOutcome {
    match error {
        WorkspacePathError::Rejected { code, message } => rejected(code, message),
        WorkspacePathError::Failed { code, message } => failed(code, message, false),
    }
}

fn rejected(code: &'static str, message: impl Into<String>) -> ToolOutcome {
    ToolOutcome::Rejected {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn failed(code: &'static str, message: impl Into<String>, retryable: bool) -> ToolOutcome {
    ToolOutcome::Failed {
        code: code.to_owned(),
        message: message.into(),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_file(contents: &[u8]) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("orchestral-search-reader-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn bounded_reader_distinguishes_exact_eof_from_byte_overflow() {
        let path = fixture_file(b"abc");
        let cancellation = CancellationToken::new();

        let mut exact = BoundedSearchReader::new(
            File::open(&path).unwrap(),
            &cancellation,
            Instant::now() + Duration::from_secs(1),
            3,
        );
        let mut bytes = Vec::new();
        exact.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"abc");
        assert_eq!(exact.stop, None);

        let mut limited = BoundedSearchReader::new(
            File::open(&path).unwrap(),
            &cancellation,
            Instant::now() + Duration::from_secs(1),
            2,
        );
        let mut bytes = [0_u8; 2];
        assert_eq!(limited.read(&mut bytes).unwrap(), 2);
        assert_eq!(&bytes, b"ab");
        assert_eq!(limited.stop, Some(ReaderStop::ByteLimit));
        assert_eq!(limited.bytes_read, 3);
        assert_eq!(
            limited.read(&mut [0_u8; 1]).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn bounded_reader_observes_cancel_and_deadline_before_reading() {
        let path = fixture_file(b"abc");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = BoundedSearchReader::new(
            File::open(&path).unwrap(),
            &cancellation,
            Instant::now() + Duration::from_secs(1),
            3,
        );
        assert_eq!(
            cancelled.read(&mut [0_u8; 1]).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(cancelled.stop, Some(ReaderStop::Cancelled));
        assert_eq!(cancelled.bytes_read, 0);

        let cancellation = CancellationToken::new();
        let mut expired =
            BoundedSearchReader::new(File::open(&path).unwrap(), &cancellation, Instant::now(), 3);
        assert_eq!(
            expired.read(&mut [0_u8; 1]).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(expired.stop, Some(ReaderStop::Timeout));
        assert_eq!(expired.bytes_read, 0);

        std::fs::remove_file(path).unwrap();
    }
}
