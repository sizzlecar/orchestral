//! # Orchestral Actions
//!
//! Official Action collection for Orchestral (optional).
//!
//! This crate provides:
//! - Built-in action implementations
//! - Hot-reloadable action registry
//!
//! Configuration types are provided by `orchestral-config`.

mod builtin;
mod document;

mod factory;
mod loader;
mod mcp;
mod providers;
mod shell_sandbox;
mod spreadsheet;
mod structured;
pub(crate) mod test_hooks;

// Re-export core action traits
pub use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

pub use orchestral_core::config::{ActionInterfaceSpec, ActionSpec, ActionsConfig};

pub use builtin::*;
pub use document::*;

pub use factory::{ActionBuildError, ActionFactory, DefaultActionFactory};
pub use loader::{ActionConfigError, ActionRegistryManager};
pub use mcp::build_mcp_action;
pub use structured::*;
