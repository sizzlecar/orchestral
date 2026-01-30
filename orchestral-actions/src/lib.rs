//! # Orchestral Actions
//!
//! Official Action collection for Orchestral (optional).
//!
//! This crate provides:
//! - Built-in action implementations
//! - YAML config loading
//! - Hot-reloadable action registry

mod builtin;
mod config;
mod factory;
mod loader;

// Re-export core action traits
pub use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

pub use builtin::*;
pub use config::{ActionSpec, ActionsConfig};
pub use factory::{ActionBuildError, ActionFactory, DefaultActionFactory};
pub use loader::{ActionConfigError, ActionRegistryManager, ActionWatcher};
