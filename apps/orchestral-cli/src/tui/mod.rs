//! Minimal terminal projection over Agent Protocol state.
//!
//! The UI owns presentation state only. Agent lifecycle and durable truth stay
//! behind `AgentClient`; the C4 adapter translates between the two boundaries.

mod render;
mod state;

#[allow(unused_imports)] // Used by the C4 terminal loop; C3 validates it with TestBackend.
pub(crate) use render::render;
#[allow(unused_imports)] // Used by the C4 Agent event adapter and input loop.
pub(crate) use state::{
    update, ApprovalChoice, PendingOverlay, ToolActivityStatus, TranscriptEntry, TranscriptRole,
    UiEffect, UiMsg, UiPhase, UiState,
};
