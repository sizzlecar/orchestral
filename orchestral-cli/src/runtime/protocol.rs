#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransientSlot {
    Status,
    Footer,
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActivityKind {
    Ran,
    Edited,
    Explored,
}

#[derive(Debug, Clone)]
pub enum RuntimeMsg {
    PlanningStart,
    PlanningEnd,
    ExecutionStart {
        total: usize,
    },
    ExecutionProgress {
        step: usize,
    },
    ExecutionEnd,
    ActivityStart {
        kind: ActivityKind,
        step_id: String,
        action: String,
        input_summary: Option<String>,
    },
    ActivityItem {
        step_id: String,
        action: String,
        line: String,
    },
    ActivityEnd {
        step_id: String,
        action: String,
        failed: bool,
    },
    OutputPersist(String),
    AssistantDelta {
        chunk: String,
        done: bool,
    },
    OutputTransient {
        slot: TransientSlot,
        text: String,
    },
    ApprovalRequested {
        reason: String,
        command: Option<String>,
    },
    Error(String),
}
