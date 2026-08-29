//! # Orchestral
//!
//! Facade crate that re-exports [`orchestral_core`] and [`orchestral_runtime`]
//! for convenient single-dependency access.
//!
//! ```rust,ignore
//! use orchestral::prelude::*;
//! ```

pub use orchestral_core as core;
pub use orchestral_runtime as runtime;

pub use orchestral_core::prelude::*;

// Agent Protocol control-plane SDK re-exports.
pub use orchestral_runtime::api::AgentApi;
pub use orchestral_runtime::{
    AgentClient, AgentController, AgentRunHandle, AgentSdkError, AgentTurn,
};
