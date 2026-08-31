//! # Orchestral Runtime
//!
//! Agent Protocol control plane, Generic Agent, guarded tools, and context.

pub mod agent_control;
pub mod agent_directory;
pub mod agent_sdk;
pub mod api;
pub mod approval_bridge;
pub mod exec_process;
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
pub use agent_directory::{AgentDirectory, AgentDirectoryError};
pub use agent_sdk::{AgentClient, AgentRunHandle, AgentSdkError, AgentTurn};
pub use approval_bridge::{AgentApprovalBridge, ApprovalBridgeError, InMemoryHostApprovalBroker};
pub use exec_process::{
    ExecPollResult, ExecProcessError, ExecSessionEvent, ExecSessionId, ExecSessionSnapshot,
    ExecSessionStatus, ExecSpawnSpec, ProcessSupervisor,
};
pub use generic_agent::{
    ContinuationPolicy, GenericAgentConfig, InternalGenericAgentProvider, ModelCostPolicy,
};
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
pub use orchestral_core::model_protocol::{ModelTokenMeter, ModelTokenMeterDescriptor};
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
    DeterministicExtractiveSessionSummarizer, JsonSizeTokenMeter, SessionCompactionGroup,
    SessionCompactionInput, SessionCompactionPolicy, SessionContextError, SessionContextProjection,
    SessionContextRequest, SessionSummarizerDescriptor,
};
pub use skill::{
    LoadedSkillSet, SkillConflict, SkillLoadOutcome, SkillRoot, SkillRuntime, SkillRuntimeError,
};
pub use tool_runtime::{
    tool_permission_decision_digest, AgentToolRuntime, DescriptorPermissionPolicy,
    GuardedToolExecution, GuardedToolExecutor, GuardedToolResult, GuardedToolRuntime,
    ToolArtifactError, ToolArtifactStore, ToolOutcomeRecoveryError, ToolPermissionDecision,
    ToolPermissionPolicy, ToolRuntimeError, WorkspacePermissionPolicy,
};
pub use tools::{
    GuardedMcpServerConfig, McpServerConnectionManager, McpServerHealth, McpToolsAdapterError,
    McpToolsAdapterRegistry, StdioMcpSandboxPolicy, StdioMcpTransportFactory,
    MCP_STDIO_SANDBOX_PROFILE,
};
pub use workflow_strategy::{
    workflow_plan_digest, workflow_step_call_id, RunBoundGuardedToolPort, WorkflowExecutionError,
    WorkflowExecutionRequest, WorkflowExecutionSnapshot, WorkflowExecutionStrategy,
};

// Re-export core types for convenience
pub use orchestral_core::prelude::*;
