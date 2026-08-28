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
            "file_read" | "artifact_read" => Self::Read,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ActivityReducer {
    generation: u64,
    calls: BTreeMap<String, ActivityCall>,
}

impl ActivityReducer {
    pub(crate) fn begin_run(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.calls.clear();
    }

    pub(crate) fn observe(
        &mut self,
        activity_id: String,
        tool_name: String,
        state: ToolActivityState,
    ) -> ActivityProjection {
        let family = ActivityFamily::for_tool(&tool_name);
        match self.calls.get_mut(&activity_id) {
            Some(call) if call.family == family => {
                if call.state == ToolActivityState::Running {
                    call.state = state;
                }
            }
            Some(_) => {}
            None => {
                self.calls.insert(
                    activity_id,
                    ActivityCall {
                        family: family.clone(),
                        tool_name,
                        state,
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
    calls.len()
}

fn family_summary(family: &ActivityFamily, status: ActivityStatus, count: usize) -> String {
    let running = status == ActivityStatus::Running;
    match family {
        ActivityFamily::Read => counted(if running { "Reading" } else { "Read" }, count, "file"),
        ActivityFamily::Command => {
            counted(if running { "Running" } else { "Ran" }, count, "command")
        }
        ActivityFamily::Edit => {
            counted(if running { "Applying" } else { "Applied" }, count, "patch")
        }
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
        reducer.observe(id.into(), tool.to_owned(), state)
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
                summary: "Applied 1 patch · 1 cancelled".to_owned(),
                status: ActivityStatus::Cancelled,
            }]
        );
    }
}
