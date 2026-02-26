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
mod external;
mod factory;
mod loader;
mod mcp;
mod providers;
mod shell_sandbox;

// Re-export core action traits
pub use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

pub use orchestral_core::config::{ActionInterfaceSpec, ActionSpec, ActionsConfig};

pub use builtin::*;
pub use external::*;
pub use factory::{ActionBuildError, ActionFactory, DefaultActionFactory};
pub use loader::{ActionConfigError, ActionRegistryManager, ActionWatcher};
pub use mcp::build_mcp_action;
