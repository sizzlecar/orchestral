use std::collections::BTreeMap;

use orchestral_core::agent_protocol::wire::ToolActivityState;

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
    fn for_tool(tool_name: &str) -> Self {
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
    details: Vec<String>,
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
        details: Vec<String>,
    ) -> ActivityProjection {
        let family = ActivityFamily::for_tool(&tool_name);
        match self.calls.get_mut(&activity_id) {
            Some(call) if call.family == family => {
                if call.state == ToolActivityState::Running {
                    call.state = state;
                }
                if !details.is_empty() {
                    call.details = details;
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
                        details,
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
        for detail in projected_details(&calls) {
            summary.push_str("\n  └ ");
            summary.push_str(&detail);
        }
        ActivityProjection {
            id: format!("{}:{}", self.generation, family.id()),
            summary,
            status,
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
            .flat_map(|call| call.details.iter())
            .filter(|detail| is_file_operation_detail(detail))
            .collect::<std::collections::BTreeSet<_>>();
        if !files.is_empty() {
            return files.len();
        }
    }
    calls.len()
}

fn projected_details(calls: &[&ActivityCall]) -> Vec<String> {
    const MAX_VISIBLE: usize = 4;
    let mut ordered = calls.to_vec();
    ordered.sort_by_key(|call| call.order);
    let mut details = Vec::new();
    for call in ordered {
        for detail in &call.details {
            let annotate_terminal_state = is_file_operation_detail(detail)
                || (!detail.starts_with('+')
                    && !detail.starts_with('-')
                    && !detail.starts_with("Error ["));
            let detail = if call.state == ToolActivityState::Failed && annotate_terminal_state {
                format!("{detail} (failed)")
            } else if call.state == ToolActivityState::Cancelled && annotate_terminal_state {
                format!("{detail} (cancelled)")
            } else {
                detail.clone()
            };
            if !details.contains(&detail) {
                details.push(detail);
            }
        }
    }
    if details.len() <= MAX_VISIBLE {
        return details;
    }
    let omitted = details.len() - MAX_VISIBLE;
    vec![
        details[0].clone(),
        details[1].clone(),
        format!("… {omitted} more"),
        details[details.len() - 2].clone(),
        details[details.len() - 1].clone(),
    ]
}

fn is_file_operation_detail(detail: &str) -> bool {
    ["Add ", "Update ", "Delete "]
        .iter()
        .any(|prefix| detail.starts_with(prefix))
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
            })
        );
    }

    #[test]
    fn activity_projection_keeps_a_bounded_first_and_latest_preview() {
        let mut reducer = ActivityReducer::default();
        reducer.begin_run();
        let mut projection = None;
        for index in 0..7 {
            let id = format!("command-{index}");
            reducer.observe(
                id.clone(),
                "exec_command".to_owned(),
                ToolActivityState::Running,
                vec![format!("command {index}")],
            );
            projection = Some(reducer.observe(
                id,
                "exec_command".to_owned(),
                ToolActivityState::Succeeded,
                Vec::new(),
            ));
        }
        let projection = projection.expect("command projection");
        assert!(projection.summary.starts_with("Ran 7 commands"));
        assert!(projection.summary.contains("command 0"));
        assert!(projection.summary.contains("… 3 more"));
        assert!(projection.summary.contains("command 6"));
        assert!(!projection.summary.contains("command 3"));
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
            vec![
                "Update src/lib.rs".to_owned(),
                "-old".to_owned(),
                "+new".to_owned(),
            ],
        );
        assert!(projection.summary.starts_with("Edited 1 file"));
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
                "Add src/new.rs".to_owned(),
                "Error [patch_invalid] Add File lines must start with '+'".to_owned(),
            ],
        );
        assert!(projection.summary.contains("Add src/new.rs (failed)"));
        assert!(projection
            .summary
            .contains("Error [patch_invalid] Add File lines must start with '+'"));
        assert!(!projection
            .summary
            .contains("Error [patch_invalid] Add File lines must start with '+' (failed)"));
    }
}
