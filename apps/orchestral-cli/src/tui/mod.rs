//! Minimal terminal projection over Agent Protocol state.
//!
//! The UI owns presentation state only. Agent lifecycle and durable truth stay
//! behind `AgentClient`; the C4 adapter translates between the two boundaries.

mod activity;
mod app;
mod editor;
mod files;
mod history;
mod insights;
mod interaction;
mod menu;
#[cfg(test)]
mod performance;
#[cfg(test)]
mod regression;
mod render;
mod services;
mod skills;
mod state;
mod terminal;
mod text;
mod viewport;

pub(crate) use app::{run_tui, TuiResume};
pub(crate) use history::history_entries;
pub(crate) use render::{render_cached, RenderCache};
#[cfg(test)]
pub(crate) use state::TranscriptEntry;
pub(crate) use state::{update, ApprovalChoice, UiEffect, UiMsg, UiPhase, UiState};
