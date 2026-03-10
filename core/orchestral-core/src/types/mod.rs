//! Core type definitions for Orchestral
//!
//! This module contains the fundamental types used throughout the system:
//! - Intent: User's goal description
//! - Plan: LLM-generated execution plan
//! - Step: Individual execution unit with dependencies
//! - Task: Stateful execution context

mod continuation;
mod intent;
mod patch_candidates;
mod plan;
mod skeleton;
mod stage;
mod stage_plan;
mod step;
mod task;
mod verify;

pub use continuation::{ContinuationState, ContinuationStatus};
pub use intent::{Intent, IntentContext};
pub use patch_candidates::PatchCandidatesEnvelope;
pub use plan::Plan;
pub use skeleton::{SkeletonChoice, SkeletonKind};
pub use stage::{ArtifactFamily, DerivationPolicy, StageKind};
pub use stage_plan::{StageChoice, StagePlan};
pub use step::{Step, StepId, StepIoBinding, StepKind};
pub use task::{ReactorTaskState, Task, TaskId, TaskState, WaitUserReason};
pub use verify::{VerifyDecision, VerifyStatus};
