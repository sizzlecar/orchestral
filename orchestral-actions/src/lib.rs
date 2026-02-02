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
mod factory;
mod loader;

// Re-export core action traits
pub use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

// Re-export config types from orchestral-config
pub use orchestral_config::{ActionInterfaceSpec, ActionSpec, ActionsConfig};

pub use builtin::*;
pub use factory::{ActionBuildError, ActionFactory, DefaultActionFactory};
pub use loader::{ActionConfigError, ActionRegistryManager, ActionWatcher};
