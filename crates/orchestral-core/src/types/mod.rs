//! Core type definitions for Orchestral
//!
//! This module contains the fundamental types used throughout the system:
//! - Intent: User's goal description
//! - Plan: LLM-generated execution plan
//! - Step: Individual execution unit with dependencies
//! - Task: Stateful execution context

mod intent;
mod plan;
mod step;
mod task;

pub use intent::{Intent, IntentContext};
pub use plan::Plan;
pub use step::{Step, StepId, StepIoBinding, StepKind};
pub use task::{Task, TaskId, TaskState, WaitUserReason};
