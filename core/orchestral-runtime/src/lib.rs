//! # Orchestral Runtime
//!
//! Agent Protocol control plane, Generic Agent, guarded tools, and context.

pub mod agent_control;
pub mod agent_sdk;
pub mod api;
pub mod approval_bridge;
pub mod generic_agent;
pub mod generic_agent_checkpoint;
mod in_memory_blob;
pub mod pty_process;
pub mod session_context;
pub mod skill;
pub mod tool_runtime;
pub mod tools;
pub mod workflow_strategy;

pub use agent_control::{AgentControlError, AgentControlEvent, AgentController};
pub use agent_sdk::{AgentClient, AgentRunHandle, AgentSdkError, AgentTurn};
pub use approval_bridge::{AgentApprovalBridge, ApprovalBridgeError, InMemoryHostApprovalBroker};
pub use generic_agent::{GenericAgentConfig, InternalGenericAgentProvider};
pub use generic_agent_checkpoint::{
    replay_generic_agent_checkpoint, AppendGenericCheckpointOutcome, CommandCheckpoint,
    CreateGenericRunOutcome, GenericAgentCheckpointProjection, GenericAgentCheckpointStore,
    GenericAgentRunRegistration, GenericCheckpointDraft, GenericCheckpointError,
    GenericCheckpointEvent, GenericCheckpointEventId, GenericCheckpointPhase,
    GenericCheckpointRecord, GenericLoopBoundary, GenericModelContextTrace,
    GenericModelObservation, GenericObservedToolCall, InMemoryGenericAgentCheckpointStore,
    StoredGenericAgentRun,
};
pub use in_memory_blob::InMemoryBlobStore;
pub use orchestral_core::spi::{
    ComponentRegistry, HookDispatchError, HookDispatchMode, HookError, HookExecutionPolicy,
    HookFailurePolicy, HookRegistry, RuntimeBuildRequest, RuntimeComponentFactory, RuntimeHook,
    RuntimeHookContext, RuntimeHookEventEnvelope, SpiError, SpiMeta,
};
pub use pty_process::{
    PtyProcessError, PtyProcessId, PtyProcessManager, PtyReadResult, PtySpawnSpec,
};
pub use session_context::{
    AgentSessionCompactor, AgentSessionContextEngine, AgentSessionSummarizer,
    DeterministicExtractiveSessionSummarizer, JsonSizeTokenMeter, ModelTokenMeter,
    SessionCompactionGroup, SessionCompactionInput, SessionCompactionPolicy, SessionContextError,
    SessionContextProjection, SessionContextRequest, SessionSummarizerDescriptor,
};
pub use skill::{
    ActivatedSkillSet, SkillActivationOutcome, SkillActivationPolicy, SkillActivationRequest,
    SkillConflict, SkillHostProfile, SkillRoot, SkillRuntime, SkillRuntimeError,
};
pub use tool_runtime::{
    AgentToolRuntime, GuardedToolExecution, GuardedToolExecutor, GuardedToolResult,
    GuardedToolRuntime, ToolArtifactError, ToolArtifactStore, ToolOutcomeRecoveryError,
    ToolRuntimeError,
};
pub use tools::{
    GuardedMcpServerConfig, McpServerConnectionManager, McpServerHealth, McpToolsAdapterError,
    McpToolsAdapterRegistry, StdioMcpTransportFactory,
};
pub use workflow_strategy::{
    workflow_plan_digest, workflow_step_call_id, RunBoundGuardedToolPort, WorkflowExecutionError,
    WorkflowExecutionRequest, WorkflowExecutionSnapshot, WorkflowExecutionStrategy,
};

// Re-export core types for convenience
pub use orchestral_core::prelude::*;
