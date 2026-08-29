//! Minimal terminal projection over Agent Protocol state.
//!
//! The UI owns presentation state only. Agent lifecycle and durable truth stay
//! behind `AgentClient`; the C4 adapter translates between the two boundaries.

mod activity;
mod app;
mod render;
mod state;
mod terminal;

pub(crate) use app::run_tui;
pub(crate) use render::render;
#[cfg(test)]
pub(crate) use state::TranscriptEntry;
pub(crate) use state::{update, ApprovalChoice, UiEffect, UiMsg, UiPhase, UiState};
