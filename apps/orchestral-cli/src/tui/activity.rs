use std::collections::BTreeMap;

use orchestral_core::agent_protocol::wire::{
    ToolActivityEvidence, ToolActivityState, ToolDiffLineKind, ToolFileActivityKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivityStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActivityProjection {
    pub id: String,
    pub summary: String,
    pub status: ActivityStatus,
    pub details: Vec<ActivityDetail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivityDetailStyle {
    Primary,
    Context,
    Addition,
    Deletion,
    Error,
    Muted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActivityDetail {
    pub text: String,
    pub depth: u8,
    pub style: ActivityDetailStyle,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ActivityFamily {
    Read,
    Command,
    Edit,
    Skill,
    Mcp,
    Other(String),
}

impl ActivityFamily {
    fn for_activity(tool_name: &str, evidence: &[ToolActivityEvidence]) -> Self {
        if evidence.iter().any(|item| {
            matches!(
                item,
                ToolActivityEvidence::File {
                    operation: ToolFileActivityKind::Create
                        | ToolFileActivityKind::Update
                        | ToolFileActivityKind::Delete,
                    ..
                }
            )
        }) {
            return Self::Edit;
        }
        if evidence.iter().any(|item| {
            matches!(
                item,
                ToolActivityEvidence::File {
                    operation: ToolFileActivityKind::Read,
                    ..
                }
            )
        }) {
            return Self::Read;
        }
        if evidence
            .iter()
            .any(|item| matches!(item, ToolActivityEvidence::Command { .. }))
        {
            return Self::Command;
        }
        match tool_name {
            "file_read" | "file_search" | "text_search" | "artifact_read" => Self::Read,
            "exec_command" | "write_stdin" => Self::Command,
            "apply_patch" | "file_write" => Self::Edit,
            "skill_read" => Self::Skill,
            name if name.starts_with("mcp__") => Self::Mcp,
            name => Self::Other(name.to_owned()),
        }
    }

    fn id(&self) -> String {
        match self {
            Self::Read => "read".to_owned(),
            Self::Command => "command".to_owned(),
            Self::Edit => "edit".to_owned(),
            Self::Skill => "skill".to_owned(),
            Self::Mcp => "mcp".to_owned(),
            Self::Other(name) => format!("other:{name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivityCall {
    family: ActivityFamily,
    tool_name: String,
    state: ToolActivityState,
    evidence: Vec<ToolActivityEvidence>,
    order: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ActivityReducer {
    generation: u64,
    next_order: u64,
    calls: BTreeMap<String, ActivityCall>,
}

impl ActivityReducer {
    pub(crate) fn begin_run(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.next_order = 0;
        self.calls.clear();
    }

    pub(crate) fn observe(
        &mut self,
        activity_id: String,
        tool_name: String,
        state: ToolActivityState,
        evidence: Vec<ToolActivityEvidence>,
    ) -> ActivityProjection {
        let family = ActivityFamily::for_activity(&tool_name, &evidence);
        match self.calls.get_mut(&activity_id) {
            Some(call) if call.family == family => {
                if call.state == ToolActivityState::Running {
                    call.state = state;
                }
                if !evidence.is_empty() {
                    call.evidence = evidence;
                }
            }
            Some(_) => {}
            None => {
                self.next_order = self.next_order.saturating_add(1);
                self.calls.insert(
                    activity_id,
                    ActivityCall {
                        family: family.clone(),
                        tool_name,
                        state,
                        evidence,
                        order: self.next_order,
                    },
                );
            }
        }
        self.project(&family)
    }

    pub(crate) fn settle(&mut self, terminal: ToolActivityState) -> Vec<ActivityProjection> {
        let mut families = Vec::new();
        for call in self.calls.values_mut() {
            if call.state == ToolActivityState::Running {
                call.state = terminal;
                if !families.contains(&call.family) {
                    families.push(call.family.clone());
                }
            }
        }
        families
            .into_iter()
            .map(|family| self.project(&family))
            .collect()
    }

    fn project(&self, family: &ActivityFamily) -> ActivityProjection {
        let calls = self
            .calls
            .values()
            .filter(|call| &call.family == family)
            .collect::<Vec<_>>();
        let running = count_state(&calls, ToolActivityState::Running);
        let failed = count_state(&calls, ToolActivityState::Failed);
        let cancelled = count_state(&calls, ToolActivityState::Cancelled);
        let status = if running > 0 {
            ActivityStatus::Running
        } else if failed > 0 {
            ActivityStatus::Failed
        } else if cancelled > 0 {
            ActivityStatus::Cancelled
        } else {
            ActivityStatus::Succeeded
        };
        let count = primary_count(family, &calls);
        let mut summary = family_summary(family, status, count);
        if failed > 0 {
            summary.push_str(&format!(" · {failed} failed"));
        }
        if cancelled > 0 {
            summary.push_str(&format!(" · {cancelled} cancelled"));
        }
        ActivityProjection {
            id: format!("{}:{}", self.generation, family.id()),
            summary,
            status,
            details: projected_details(&calls),
        }
    }
}

fn count_state(calls: &[&ActivityCall], state: ToolActivityState) -> usize {
    calls.iter().filter(|call| call.state == state).count()
}

fn primary_count(family: &ActivityFamily, calls: &[&ActivityCall]) -> usize {
    if family == &ActivityFamily::Command {
        let commands = calls
            .iter()
            .filter(|call| call.tool_name == "exec_command")
            .count();
        if commands > 0 {
            return commands;
        }
    }
    if family == &ActivityFamily::Edit {
        let files = calls
            .iter()
            .flat_map(|call| call.evidence.iter())
            .filter_map(|item| match item {
                ToolActivityEvidence::File {
                    operation:
                        ToolFileActivityKind::Create
                        | ToolFileActivityKind::Update
                        | ToolFileActivityKind::Delete,
                    path,
                    ..
                } => Some(path),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        if !files.is_empty() {
            return files.len();
        }
    }
    calls.len()
}

fn projected_details(calls: &[&ActivityCall]) -> Vec<ActivityDetail> {
    const MAX_VISIBLE: usize = 20;
    const HEAD_VISIBLE: usize = 8;
    const TAIL_VISIBLE: usize = MAX_VISIBLE - HEAD_VISIBLE - 1;
    let mut ordered = calls.to_vec();
    ordered.sort_by_key(|call| call.order);
    let mut details = Vec::new();
    for call in ordered {
        for item in &call.evidence {
            details.extend(project_evidence(item, call.state));
        }
    }
    if details.len() <= MAX_VISIBLE {
        return details;
    }
    let omitted = details.len() - MAX_VISIBLE;
    let mut bounded = details
        .iter()
        .take(HEAD_VISIBLE)
        .cloned()
        .collect::<Vec<_>>();
    bounded.push(ActivityDetail {
        text: format!("… {omitted} more lines"),
        depth: 0,
        style: ActivityDetailStyle::Muted,
    });
    bounded.extend(details.iter().skip(details.len() - TAIL_VISIBLE).cloned());
    bounded
}

fn project_evidence(
    evidence: &ToolActivityEvidence,
    state: ToolActivityState,
) -> Vec<ActivityDetail> {
    let terminal_suffix = match state {
        ToolActivityState::Failed => " (failed)",
        ToolActivityState::Cancelled => " (cancelled)",
        ToolActivityState::Running | ToolActivityState::Succeeded => "",
    };
    match evidence {
        ToolActivityEvidence::Command { command } => vec![ActivityDetail {
            text: format!("{command}{terminal_suffix}"),
            depth: 0,
            style: ActivityDetailStyle::Primary,
        }],
        ToolActivityEvidence::File {
            operation,
            path,
            diff,
            diff_omitted,
        } => {
            let operation = match operation {
                ToolFileActivityKind::Read => "Read",
                ToolFileActivityKind::Create => "Add",
                ToolFileActivityKind::Update => "Update",
                ToolFileActivityKind::Delete => "Delete",
            };
            let mut details = vec![ActivityDetail {
                text: format!("{operation} {path}{terminal_suffix}"),
                depth: 0,
                style: ActivityDetailStyle::Primary,
            }];
            details.extend(diff.iter().map(|line| ActivityDetail {
                text: match line.kind {
                    ToolDiffLineKind::Context => format!("  {}", line.text),
                    ToolDiffLineKind::Addition => format!("+ {}", line.text),
                    ToolDiffLineKind::Deletion => format!("- {}", line.text),
                },
                depth: 1,
                style: match line.kind {
                    ToolDiffLineKind::Context => ActivityDetailStyle::Context,
                    ToolDiffLineKind::Addition => ActivityDetailStyle::Addition,
                    ToolDiffLineKind::Deletion => ActivityDetailStyle::Deletion,
                },
            }));
            if *diff_omitted > 0 {
                details.push(ActivityDetail {
                    text: format!("… {diff_omitted} diff lines omitted"),
                    depth: 1,
                    style: ActivityDetailStyle::Muted,
                });
            }
            details
        }
        ToolActivityEvidence::Note { text } => vec![ActivityDetail {
            text: format!("{text}{terminal_suffix}"),
            depth: 0,
            style: ActivityDetailStyle::Primary,
        }],
        ToolActivityEvidence::Error { code, message } => vec![ActivityDetail {
            text: format!("Error [{code}] {}", compact_error_message(message)),
            depth: 0,
            style: ActivityDetailStyle::Error,
        }],
        ToolActivityEvidence::Omitted { count } => vec![ActivityDetail {
            text: format!("… {count} more operations"),
            depth: 0,
            style: ActivityDetailStyle::Muted,
        }],
        _ => Vec::new(),
    }
}

fn compact_error_message(message: &str) -> String {
    const MAX_CHARS: usize = 800;
    let compact = message.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = compact.chars();
    let mut bounded = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}

fn family_summary(family: &ActivityFamily, status: ActivityStatus, count: usize) -> String {
    let running = status == ActivityStatus::Running;
    match family {
        ActivityFamily::Read => counted(if running { "Reading" } else { "Read" }, count, "file"),
        ActivityFamily::Command => {
            counted(if running { "Running" } else { "Ran" }, count, "command")
        }
        ActivityFamily::Edit => counted(if running { "Editing" } else { "Edited" }, count, "file"),
        ActivityFamily::Skill => {
            counted(if running { "Loading" } else { "Loaded" }, count, "skill")
        }
        ActivityFamily::Mcp => counted(
            if running { "Calling" } else { "Called" },
            count,
            "MCP tool",
        ),
        ActivityFamily::Other(name) => {
            let display = name.replace('_', " ");
            counted(if running { "Using" } else { "Used" }, count, &display)
        }
    }
}

fn counted(verb: &str, count: usize, noun: &str) -> String {
    let suffix = if count == 1 { "" } else { "s" };
    format!("{verb} {count} {noun}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe(
        reducer: &mut ActivityReducer,
        id: impl Into<String>,
        tool: &str,
        state: ToolActivityState,
    ) -> ActivityProjection {
        reducer.observe(id.into(), tool.to_owned(), state, Vec::new())
    }

    #[test]
    fn repeated_reads_reduce_to_one_counted_activity() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        let mut projection = None;
        for index in 0..16 {
            let id = format!("read-{index}");
            observe(&mut reducer, &id, "file_read", ToolActivityState::Running);
            projection = Some(observe(
                &mut reducer,
                &id,
                "file_read",
                ToolActivityState::Succeeded,
            ));
        }
        assert_eq!(
            projection,
            Some(ActivityProjection {
                id: "1:read".to_owned(),
                summary: "Read 16 files".to_owned(),
                status: ActivityStatus::Succeeded,
                details: Vec::new(),
            })
        );
    }

    #[test]
    fn activity_projection_keeps_a_bounded_first_and_latest_preview() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        let mut projection = None;
        for index in 0..24 {
            let id = format!("command-{index}");
            reducer.observe(
                id.clone(),
                "exec_command".to_owned(),
                ToolActivityState::Running,
                vec![ToolActivityEvidence::Command {
                    command: format!("command {index}"),
                }],
            );
            projection = Some(reducer.observe(
                id,
                "exec_command".to_owned(),
                ToolActivityState::Succeeded,
                Vec::new(),
            ));
        }
        let projection = projection.expect("command projection");
        assert!(projection.summary.starts_with("Ran 24 commands"));
        let details = projection
            .details
            .iter()
            .map(|detail| detail.text.as_str())
            .collect::<Vec<_>>();
        assert!(details.contains(&"command 0"));
        assert!(details.contains(&"… 4 more lines"));
        assert!(details.contains(&"command 23"));
        assert!(!details.contains(&"command 10"));
    }

    #[test]
    fn command_polls_do_not_inflate_command_count() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        observe(
            &mut reducer,
            "exec-1",
            "exec_command",
            ToolActivityState::Succeeded,
        );
        let mut projection = None;
        for index in 0..8 {
            projection = Some(observe(
                &mut reducer,
                format!("poll-{index}"),
                "write_stdin",
                ToolActivityState::Succeeded,
            ));
        }
        assert_eq!(projection.expect("projection").summary, "Ran 1 command");
    }

    #[test]
    fn failures_remain_visible_and_terminal_transitions_are_monotonic() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        observe(
            &mut reducer,
            "read-1",
            "file_read",
            ToolActivityState::Failed,
        );
        let projection = observe(
            &mut reducer,
            "read-1",
            "file_read",
            ToolActivityState::Succeeded,
        );
        assert_eq!(projection.status, ActivityStatus::Failed);
        assert_eq!(projection.summary, "Read 1 file · 1 failed");
    }

    #[test]
    fn settling_a_run_removes_dangling_running_activity() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        observe(
            &mut reducer,
            "patch-1",
            "apply_patch",
            ToolActivityState::Running,
        );
        assert_eq!(
            reducer.settle(ToolActivityState::Cancelled),
            vec![ActivityProjection {
                id: "1:edit".to_owned(),
                summary: "Edited 1 file · 1 cancelled".to_owned(),
                status: ActivityStatus::Cancelled,
                details: Vec::new(),
            }]
        );
    }

    #[test]
    fn patch_preview_lines_do_not_inflate_the_edited_file_count() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        let projection = reducer.observe(
            "patch-1".to_owned(),
            "apply_patch".to_owned(),
            ToolActivityState::Succeeded,
            vec![ToolActivityEvidence::File {
                operation: ToolFileActivityKind::Update,
                path: "src/lib.rs".to_owned(),
                diff: vec![
                    orchestral_core::agent_protocol::wire::ToolDiffLine {
                        kind: ToolDiffLineKind::Deletion,
                        text: "old".to_owned(),
                    },
                    orchestral_core::agent_protocol::wire::ToolDiffLine {
                        kind: ToolDiffLineKind::Addition,
                        text: "new".to_owned(),
                    },
                ],
                diff_omitted: 0,
            }],
        );
        assert!(projection.summary.starts_with("Edited 1 file"));
        assert!(projection
            .details
            .iter()
            .any(|detail| detail.text == "+ new"));
    }

    #[test]
    fn patch_failure_keeps_the_parser_error_readable() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        let projection = reducer.observe(
            "patch-1".to_owned(),
            "apply_patch".to_owned(),
            ToolActivityState::Failed,
            vec![
                ToolActivityEvidence::File {
                    operation: ToolFileActivityKind::Create,
                    path: "src/new.rs".to_owned(),
                    diff: Vec::new(),
                    diff_omitted: 0,
                },
                ToolActivityEvidence::Error {
                    code: "patch_invalid".to_owned(),
                    message: "Add File lines must start with '+'".to_owned(),
                },
            ],
        );
        let details = projection
            .details
            .iter()
            .map(|detail| detail.text.as_str())
            .collect::<Vec<_>>();
        assert!(details.contains(&"Add src/new.rs (failed)"));
        assert!(details.contains(&"Error [patch_invalid] Add File lines must start with '+'"));
        assert!(
            !details.contains(&"Error [patch_invalid] Add File lines must start with '+' (failed)")
        );
    }

    #[test]
    fn structured_tool_errors_are_compact_and_bounded() {
        assert_eq!(
            compact_error_message("reason: schema omitted\n\nhow_to_get: search first"),
            "reason: schema omitted how_to_get: search first"
        );
        let bounded = compact_error_message(&"远程错误".repeat(500));
        assert!(bounded.chars().count() <= 801);
        assert!(bounded.ends_with('…'));
    }
}
