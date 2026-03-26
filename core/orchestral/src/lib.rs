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

// SDK re-exports
pub use orchestral_runtime::sdk::{
    Orchestral, OrchestralApp, OrchestralBuilder, RunResult, SdkError,
};
