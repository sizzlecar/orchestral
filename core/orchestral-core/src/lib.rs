//! # Orchestral Core
//!
//! Agent, model, tool, skill, MCP, workflow, and extension contracts.
//!
//! This crate provides:
//! - Agent Protocol v1 contracts and deterministic reducer
//! - Model and Tool provider boundaries
//! - Plan normalization and guarded DAG execution
//! - Stable SPI contracts for runtime component and hook extensions
//! - Unified configuration management

pub mod agent_connector;
pub mod agent_protocol;
pub mod agent_session;
pub mod config;
pub mod executor;
pub mod io;
pub mod mcp_protocol;
pub mod model_protocol;
pub mod normalizer;
pub mod skill_protocol;
pub mod spi;
pub mod tool_effect;
pub mod tool_protocol;
pub mod types;
pub mod workflow_state;

/// Prelude for convenient imports
pub mod prelude {
    pub use crate::executor::{
        DagNode, ExecutionDag, ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult,
        Executor, ExecutorContext, NodeState, StepExecutionPort, StepExecutionRequest, StepOutcome,
    };
    pub use crate::io::{
        ArtifactPublishError, ArtifactPublishRequest, ArtifactPublisher, ArtifactResolveError,
        ArtifactResolver, BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobStream,
        BlobWriteRequest, ResolvedArtifact,
    };
    pub use crate::normalizer::{
        FixError, NormalizeError, NormalizedPlan, PlanFixer, PlanNormalizer, PlanValidator,
        ValidationError,
    };
    pub use crate::types::{Plan, Step, StepId, StepIoBinding, StepKind, WorkflowId};
    pub use crate::workflow_state::{Scope, WorkingSet};
}

// Re-export key types at crate root
pub use executor::{
    ExecutionDag, ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor,
    StepOutcome,
};
pub use io::{
    ArtifactPublishError, ArtifactPublishRequest, ArtifactPublisher, ArtifactResolveError,
    ArtifactResolver, BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobStream,
    BlobWriteRequest, ResolvedArtifact,
};
pub use normalizer::{NormalizedPlan, PlanNormalizer};
pub use types::{Plan, Step, StepId, StepIoBinding, StepKind, WorkflowId};
pub use workflow_state::{Scope, WorkingSet};
