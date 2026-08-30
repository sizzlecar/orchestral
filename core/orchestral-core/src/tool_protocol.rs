//! Provider-neutral Tool Protocol security contracts.
//!
//! The model-facing schema and Host authority are deliberately different
//! types. Model output can name a tool and provide arguments, but it cannot
//! carry an approval decision, sandbox mode, network grant, environment grant,
//! credential grant, or any other authority fact.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as ShaDigest, Sha256};

use crate::agent_protocol::wire::{ArtifactRefWithDigest, Digest, RunId};

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.trim().is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(/// Stable Host identity for one registered tool.
    ToolId);
string_id!(/// Stable identity for one normalized tool invocation.
    ToolCallId);
string_id!(/// Unpredictable, single-use approval capability nonce.
    ApprovalNonce);

/// The only tool metadata exposed to a model.
///
/// Authority metadata is intentionally absent. Arbitrary properties in
/// `input_schema` are ordinary tool arguments and never have policy meaning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolSchema {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl ModelToolSchema {
    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        if self.name.trim().is_empty()
            || self.description.trim().is_empty()
            || !self.input_schema.is_object()
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidDescriptor,
                "model tool schema requires a name, description, and object input schema",
            ));
        }
        Ok(())
    }

    pub fn validate_arguments(&self, arguments: &Value) -> Result<(), ToolProtocolError> {
        self.validate()?;
        validate_json_value(arguments, &self.input_schema, "$").map_err(|message| {
            ToolProtocolError::new(ToolProtocolErrorCode::SchemaViolation, message)
        })
    }
}

/// Coarse effect classes guarded by the Host reference monitor.
///
/// Target-level restrictions (for example hosts, environment variable names,
/// and credential references) remain in [`ToolPolicyBounds`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EffectScope {
    Process,
    Network,
    FilesystemRead,
    FilesystemWrite,
    ArtifactRead,
    EnvironmentRead,
    SecretRead,
    ExternalSideEffect,
    /// Execute a process outside the default OS sandbox. This is an explicit
    /// Host capability, not a model-controlled sandbox switch. Operations
    /// requesting it must remain bound to an exact approval capability.
    HostExecution,
}

/// Host-normalized selector for a resource required by one operation.
///
/// `Unrestricted` is deliberately explicit. An adapter that cannot enforce an
/// exact resource boundary must request this wider selector before policy is
/// evaluated; it may never silently widen an `Exact` request at execution
/// time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "scope", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum CapabilitySelector {
    /// No narrower resource identity exists for this effect.
    Unscoped,
    /// One exact Host-normalized resource identity.
    Exact(String),
    /// One canonical hierarchy root, such as a filesystem subtree.
    Subtree(String),
    /// Open-world access for the named effect.
    Unrestricted,
}

/// One typed resource requirement. The effect is part of the identity so a
/// read lease can never be reused as a write lease for the same path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityResource {
    pub effect: EffectScope,
    pub selector: CapabilitySelector,
}

/// Complete authority requested by one inspected operation.
///
/// This is intent, not permission. It is produced before policy evaluation and
/// remains unchanged whether the eventual decision is automatic, interactive,
/// or denied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequest {
    #[serde(default)]
    pub effects: BTreeSet<EffectScope>,
    #[serde(default)]
    pub resources: BTreeSet<CapabilityResource>,
}

impl CapabilityRequest {
    pub fn from_effects(effects: BTreeSet<EffectScope>) -> Self {
        Self {
            effects,
            resources: BTreeSet::new(),
        }
    }

    pub fn insert_resource(&mut self, effect: EffectScope, selector: CapabilitySelector) {
        self.effects.insert(effect);
        self.resources
            .insert(CapabilityResource { effect, selector });
    }

    pub fn requires(&self, effect: EffectScope) -> bool {
        self.effects.contains(&effect)
    }

    pub fn resources_for(&self, effect: EffectScope) -> impl Iterator<Item = &CapabilitySelector> {
        self.resources
            .iter()
            .filter(move |resource| resource.effect == effect)
            .map(|resource| &resource.selector)
    }

    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        for resource in &self.resources {
            if !self.effects.contains(&resource.effect) {
                return Err(ToolProtocolError::new(
                    ToolProtocolErrorCode::InvalidInvocation,
                    "capability resource must belong to a requested effect",
                ));
            }
            match &resource.selector {
                CapabilitySelector::Exact(value) | CapabilitySelector::Subtree(value)
                    if value.trim().is_empty()
                        || value.chars().any(char::is_control)
                        || value != value.trim() =>
                {
                    return Err(ToolProtocolError::new(
                        ToolProtocolErrorCode::InvalidInvocation,
                        "capability resource identities must be normalized non-empty strings",
                    ));
                }
                CapabilitySelector::Unrestricted
                    if !(matches!(
                        resource.effect,
                        EffectScope::Network | EffectScope::ExternalSideEffect
                    ) || self.effects.contains(&EffectScope::HostExecution)
                        && matches!(
                            resource.effect,
                            EffectScope::FilesystemRead | EffectScope::FilesystemWrite
                        )) =>
                {
                    return Err(ToolProtocolError::new(
                        ToolProtocolErrorCode::InvalidInvocation,
                        "unrestricted capability selectors are limited to open-world effects",
                    ));
                }
                _ => {}
            }
        }
        if self.effects.contains(&EffectScope::Network)
            && self.resources_for(EffectScope::Network).next().is_none()
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "network operations must declare exact or unrestricted network authority",
            ));
        }
        if self.effects.contains(&EffectScope::HostExecution) {
            let ambient_effects = [
                EffectScope::Process,
                EffectScope::Network,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
                EffectScope::ExternalSideEffect,
            ];
            if ambient_effects
                .into_iter()
                .any(|effect| !self.effects.contains(&effect))
            {
                return Err(ToolProtocolError::new(
                    ToolProtocolErrorCode::InvalidInvocation,
                    "Host execution must declare process, unrestricted filesystem/network, and external-side-effect authority",
                ));
            }
            for effect in [
                EffectScope::Network,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
                EffectScope::ExternalSideEffect,
            ] {
                if !self
                    .resources_for(effect)
                    .any(|selector| matches!(selector, CapabilitySelector::Unrestricted))
                {
                    return Err(ToolProtocolError::new(
                        ToolProtocolErrorCode::InvalidInvocation,
                        "Host execution ambient effects must use unrestricted selectors",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest, ToolProtocolError> {
        self.validate()?;
        canonical_json_digest(self)
    }
}

/// Host-normalized risk carried by one concrete Tool operation.
///
/// This is deliberately smaller than a policy decision. A planner describes
/// what an invocation will do; the Host permission policy decides whether that
/// operation may run, needs review, or must be denied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolOperationRisk {
    #[default]
    Routine,
    /// Interactive or otherwise ambiguous operation that should be reviewed
    /// by policies which auto-authorize only routine sandboxed work.
    Elevated,
    Destructive,
}

/// Invocation-specific effect plan produced before any Tool side effect.
///
/// [`ToolDescriptor::effect_scopes`] is only the immutable maximum envelope a
/// Tool registration can ever request. This type records the narrower effects
/// and targets of one concrete invocation. Approval and durable effect records
/// bind to its digest so a capability for one planned operation cannot be
/// reused after the operation is reclassified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOperationPlan {
    pub required_capabilities: CapabilityRequest,
    #[serde(default)]
    pub risk: ToolOperationRisk,
    /// Stable Tool-defined review class that a Host may remember for the
    /// lifetime of one interactive session. This never grants authority by
    /// itself: every invocation still receives a fresh capability bound to
    /// its exact arguments and operation.
    #[serde(default)]
    pub session_approval_scope: Option<Digest>,
    pub summary: String,
}

impl ToolOperationPlan {
    pub fn validate_shape(&self) -> Result<(), ToolProtocolError> {
        self.required_capabilities.validate()?;
        if self.summary.trim().is_empty() || self.summary.chars().any(char::is_control) {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "Tool operation plan requires a safe summary and normalized targets",
            ));
        }
        if self
            .session_approval_scope
            .as_ref()
            .is_some_and(|scope| !scope.is_sha256())
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "session approval scope must be a SHA-256 digest",
            ));
        }
        Ok(())
    }

    pub fn validate_envelope(
        &self,
        effect_envelope: &BTreeSet<EffectScope>,
    ) -> Result<(), ToolProtocolError> {
        self.validate_shape()?;
        if !self
            .required_capabilities
            .effects
            .is_subset(effect_envelope)
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::PolicyDenied,
                "planned Tool effects exceed the registered effect envelope",
            ));
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest, ToolProtocolError> {
        self.validate_shape()?;
        #[derive(Serialize)]
        struct AuthorityView<'a> {
            required_capabilities: &'a CapabilityRequest,
            risk: ToolOperationRisk,
        }
        canonical_json_digest(&AuthorityView {
            required_capabilities: &self.required_capabilities,
            risk: self.risk,
        })
    }
}

/// Whether an invocation may proceed without a single-use Host capability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ApprovalPolicy {
    NotRequired,
    Required,
    #[default]
    Deny,
}

impl ApprovalPolicy {
    fn restrict(self, other: Self) -> Self {
        use ApprovalPolicy::{Deny, NotRequired, Required};
        match (self, other) {
            (Deny, _) | (_, Deny) => Deny,
            (Required, _) | (_, Required) => Required,
            (NotRequired, NotRequired) => NotRequired,
        }
    }

    fn is_at_least_as_restrictive_as(self, ceiling: Self) -> bool {
        fn rank(value: ApprovalPolicy) -> u8 {
            match value {
                ApprovalPolicy::NotRequired => 0,
                ApprovalPolicy::Required => 1,
                ApprovalPolicy::Deny => 2,
            }
        }
        rank(self) >= rank(ceiling)
    }
}

/// Host-only sandbox restriction. An empty profile set with `required=true`
/// is a deliberate fail-closed policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPolicy {
    pub required: bool,
    #[serde(default)]
    pub allowed_profiles: BTreeSet<String>,
}

/// Host policy for the generic interactive command surface.
///
/// The model supplies command text, but cannot select the launcher or decide
/// whether descendants may execute. Descendants inherit the same effect
/// sandbox; they are not enumerated as transport identities.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractiveCommandPolicy {
    pub enabled: bool,
    #[serde(default)]
    pub command_shells: BTreeSet<String>,
    pub allow_child_processes: bool,
}

/// Process-tree authority used only to establish Host-configured transports
/// such as MCP stdio connections. It is independent from model-facing command
/// execution, and every descendant remains inside the transport sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportLaunchPolicy {
    #[serde(default)]
    pub allowed_programs: BTreeSet<String>,
    #[serde(default)]
    pub allow_child_processes: bool,
}

/// Process authority keeps generic commands and infrastructure transports in
/// separate lanes so credentials or executable identities cannot bleed across.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessPolicy {
    pub interactive: InteractiveCommandPolicy,
    pub transport: TransportLaunchPolicy,
}

/// Host-normalized filesystem boundary. Adapters must resolve symlinks and
/// canonicalize a target before comparing it with these roots.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemPolicy {
    #[serde(default)]
    pub readable_roots: BTreeSet<String>,
    #[serde(default)]
    pub writable_roots: BTreeSet<String>,
}

/// Host-only network restriction. Targets are exact, Host-normalized names;
/// wildcard and URL interpretation belongs to the reference monitor adapter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkPolicy {
    #[serde(default)]
    pub allowed_targets: BTreeSet<String>,
    /// Hard ceiling for open-world network access. This does not authorize an
    /// invocation by itself: the operation must still request unrestricted
    /// network and the permission policy will normally require approval.
    #[serde(default)]
    pub allow_unrestricted: bool,
}

/// Host-only environment restriction. `inherit_host_environment` is narrowed
/// with logical AND and therefore cannot be enabled by a Run or tool.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentPolicy {
    #[serde(default)]
    pub allowed_variables: BTreeSet<String>,
    pub inherit_host_environment: bool,
}

/// Common shape used by each independent policy source.
///
/// Empty allow-sets deny the corresponding authority. `None` for a numeric
/// maximum means no additional cap at that layer; the effective value is the
/// minimum finite cap supplied by any layer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolPolicyBounds {
    #[serde(default)]
    pub allowed_effects: BTreeSet<EffectScope>,
    pub approval: ApprovalPolicy,
    pub sandbox: SandboxPolicy,
    pub process: ProcessPolicy,
    pub filesystem: FilesystemPolicy,
    pub network: NetworkPolicy,
    pub environment: EnvironmentPolicy,
    #[serde(default)]
    pub allowed_credentials: BTreeSet<String>,
    #[serde(default)]
    pub max_timeout_ms: Option<u64>,
    /// Maximum serialized result kept inline in model context. Larger results
    /// require a Host Artifact store and are replaced by a digest-bound
    /// reference; the Artifact store applies its own hard byte ceiling.
    #[serde(default)]
    pub max_output_bytes: Option<u64>,
}

impl ToolPolicyBounds {
    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        if self.max_timeout_ms == Some(0) || self.max_output_bytes == Some(0) {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidPolicy,
                "tool policy limits must be positive",
            ));
        }
        validate_non_empty_set("sandbox profile", &self.sandbox.allowed_profiles)?;
        validate_non_empty_set(
            "interactive command shell",
            &self.process.interactive.command_shells,
        )?;
        validate_non_empty_set(
            "transport program",
            &self.process.transport.allowed_programs,
        )?;
        validate_non_empty_set("readable root", &self.filesystem.readable_roots)?;
        validate_non_empty_set("writable root", &self.filesystem.writable_roots)?;
        validate_non_empty_set("network target", &self.network.allowed_targets)?;
        validate_non_empty_set("environment variable", &self.environment.allowed_variables)?;
        validate_non_empty_set("credential reference", &self.allowed_credentials)?;
        Ok(())
    }

    fn restrictive_intersection(&self, other: &Self) -> Self {
        Self {
            allowed_effects: set_intersection(&self.allowed_effects, &other.allowed_effects),
            approval: self.approval.restrict(other.approval),
            sandbox: SandboxPolicy {
                required: self.sandbox.required || other.sandbox.required,
                allowed_profiles: set_intersection(
                    &self.sandbox.allowed_profiles,
                    &other.sandbox.allowed_profiles,
                ),
            },
            process: ProcessPolicy {
                interactive: InteractiveCommandPolicy {
                    enabled: self.process.interactive.enabled && other.process.interactive.enabled,
                    command_shells: set_intersection(
                        &self.process.interactive.command_shells,
                        &other.process.interactive.command_shells,
                    ),
                    allow_child_processes: self.process.interactive.allow_child_processes
                        && other.process.interactive.allow_child_processes,
                },
                transport: TransportLaunchPolicy {
                    allowed_programs: set_intersection(
                        &self.process.transport.allowed_programs,
                        &other.process.transport.allowed_programs,
                    ),
                    allow_child_processes: self.process.transport.allow_child_processes
                        && other.process.transport.allow_child_processes,
                },
            },
            filesystem: FilesystemPolicy {
                readable_roots: set_intersection(
                    &self.filesystem.readable_roots,
                    &other.filesystem.readable_roots,
                ),
                writable_roots: set_intersection(
                    &self.filesystem.writable_roots,
                    &other.filesystem.writable_roots,
                ),
            },
            network: NetworkPolicy {
                allowed_targets: set_intersection(
                    &self.network.allowed_targets,
                    &other.network.allowed_targets,
                ),
                allow_unrestricted: self.network.allow_unrestricted
                    && other.network.allow_unrestricted,
            },
            environment: EnvironmentPolicy {
                allowed_variables: set_intersection(
                    &self.environment.allowed_variables,
                    &other.environment.allowed_variables,
                ),
                inherit_host_environment: self.environment.inherit_host_environment
                    && other.environment.inherit_host_environment,
            },
            allowed_credentials: set_intersection(
                &self.allowed_credentials,
                &other.allowed_credentials,
            ),
            max_timeout_ms: minimum_cap(self.max_timeout_ms, other.max_timeout_ms),
            max_output_bytes: minimum_cap(self.max_output_bytes, other.max_output_bytes),
        }
    }

    /// Returns true when every authority dimension is within `ceiling`.
    pub fn is_no_more_permissive_than(&self, ceiling: &Self) -> bool {
        self.allowed_effects.is_subset(&ceiling.allowed_effects)
            && self
                .sandbox
                .allowed_profiles
                .is_subset(&ceiling.sandbox.allowed_profiles)
            && (!ceiling.sandbox.required || self.sandbox.required)
            && (!self.process.interactive.enabled || ceiling.process.interactive.enabled)
            && self
                .process
                .interactive
                .command_shells
                .is_subset(&ceiling.process.interactive.command_shells)
            && (!self.process.interactive.allow_child_processes
                || ceiling.process.interactive.allow_child_processes)
            && self
                .process
                .transport
                .allowed_programs
                .is_subset(&ceiling.process.transport.allowed_programs)
            && (!self.process.transport.allow_child_processes
                || ceiling.process.transport.allow_child_processes)
            && self
                .filesystem
                .readable_roots
                .is_subset(&ceiling.filesystem.readable_roots)
            && self
                .filesystem
                .writable_roots
                .is_subset(&ceiling.filesystem.writable_roots)
            && self
                .network
                .allowed_targets
                .is_subset(&ceiling.network.allowed_targets)
            && (!self.network.allow_unrestricted || ceiling.network.allow_unrestricted)
            && self
                .environment
                .allowed_variables
                .is_subset(&ceiling.environment.allowed_variables)
            && (!self.environment.inherit_host_environment
                || ceiling.environment.inherit_host_environment)
            && self
                .allowed_credentials
                .is_subset(&ceiling.allowed_credentials)
            && cap_is_within(self.max_timeout_ms, ceiling.max_timeout_ms)
            && cap_is_within(self.max_output_bytes, ceiling.max_output_bytes)
            && self
                .approval
                .is_at_least_as_restrictive_as(ceiling.approval)
    }
}

fn validate_non_empty_set(label: &str, values: &BTreeSet<String>) -> Result<(), ToolProtocolError> {
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(ToolProtocolError::new(
            ToolProtocolErrorCode::InvalidPolicy,
            format!("{label} must not be empty"),
        ));
    }
    Ok(())
}

fn set_intersection<T: Clone + Ord>(left: &BTreeSet<T>, right: &BTreeSet<T>) -> BTreeSet<T> {
    left.intersection(right).cloned().collect()
}

fn minimum_cap(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn cap_is_within(value: Option<u64>, ceiling: Option<u64>) -> bool {
    match (value, ceiling) {
        (_, None) => true,
        (Some(value), Some(ceiling)) => value <= ceiling,
        (None, Some(_)) => false,
    }
}

/// Immutable Host authority ceiling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostToolPolicy {
    pub bounds: ToolPolicyBounds,
}

/// Authority granted to one Agent Run by the Host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunToolGrant {
    pub bounds: ToolPolicyBounds,
}

/// Immutable restriction owned by one registered tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRestriction {
    pub bounds: ToolPolicyBounds,
}

/// Host registration record. Only `model_schema` may be projected into a
/// model request; `restriction` and effect declarations remain Host-only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDescriptor {
    pub tool_id: ToolId,
    pub model_schema: ModelToolSchema,
    /// Host-side output contract. It is not exposed to the model as authority
    /// and is validated before a Completed outcome is committed.
    pub output_schema: Value,
    /// Maximum effect envelope for every invocation of this Tool. Runtime
    /// planning must derive a narrower [`ToolOperationPlan`] per invocation.
    #[serde(default)]
    pub effect_scopes: BTreeSet<EffectScope>,
    pub restriction: ToolRestriction,
    pub idempotency: ToolIdempotency,
    pub concurrency: ToolConcurrency,
}

/// Recovery semantics for a Tool invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolIdempotency {
    Pure,
    Idempotent,
    IdempotentWithKey,
    NonIdempotent,
}

/// Minimum serialization boundary required by the Tool implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolConcurrency {
    ParallelSafe,
    PerRunSerial,
    GlobalSerial,
}

impl ToolDescriptor {
    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        if self.tool_id.is_empty() {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidDescriptor,
                "tool_id must not be empty",
            ));
        }
        self.model_schema.validate()?;
        if !self.output_schema.is_object() {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidDescriptor,
                "tool output_schema must be an object",
            ));
        }
        self.restriction.bounds.validate()?;
        if !self
            .effect_scopes
            .is_subset(&self.restriction.bounds.allowed_effects)
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidDescriptor,
                "declared effects must be within the tool restriction",
            ));
        }
        Ok(())
    }

    pub fn model_schema(&self) -> &ModelToolSchema {
        &self.model_schema
    }

    pub fn validate_output(&self, output: &Value) -> Result<(), ToolProtocolError> {
        validate_json_value(output, &self.output_schema, "$").map_err(|message| {
            ToolProtocolError::new(ToolProtocolErrorCode::SchemaViolation, message)
        })
    }

    pub fn digest(&self) -> Result<Digest, ToolProtocolError> {
        self.validate()?;
        canonical_json_digest(self)
    }
}

/// Result of the only supported policy composition operation.
///
/// Its fields are private so production code cannot deserialize or construct
/// an allegedly effective policy without applying all three Host-owned inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveToolPolicy {
    bounds: ToolPolicyBounds,
}

impl EffectiveToolPolicy {
    pub fn resolve(
        host_ceiling: &HostToolPolicy,
        run_grant: &RunToolGrant,
        tool_restriction: &ToolRestriction,
    ) -> Result<Self, ToolProtocolError> {
        host_ceiling.bounds.validate()?;
        run_grant.bounds.validate()?;
        tool_restriction.bounds.validate()?;

        let bounds = host_ceiling
            .bounds
            .restrictive_intersection(&run_grant.bounds)
            .restrictive_intersection(&tool_restriction.bounds);

        debug_assert!(bounds.is_no_more_permissive_than(&host_ceiling.bounds));
        debug_assert!(bounds.is_no_more_permissive_than(&run_grant.bounds));
        debug_assert!(bounds.is_no_more_permissive_than(&tool_restriction.bounds));
        Ok(Self { bounds })
    }

    pub fn bounds(&self) -> &ToolPolicyBounds {
        &self.bounds
    }

    pub fn digest(&self) -> Result<Digest, ToolProtocolError> {
        canonical_json_digest(&self.bounds)
    }

    /// Checks the coarse effect boundary. Target-specific checks remain a
    /// mandatory responsibility of the reference monitor adapter.
    pub fn authorizes_scopes(&self, requested: &BTreeSet<EffectScope>) -> bool {
        self.bounds.approval != ApprovalPolicy::Deny
            && requested.is_subset(&self.bounds.allowed_effects)
            && (!self.bounds.sandbox.required || !self.bounds.sandbox.allowed_profiles.is_empty())
    }

    /// Checks both the effect envelope and the normalized resource ceiling.
    /// Policy review can authorize a request within this ceiling, but can
    /// never widen it.
    pub fn authorizes_request(&self, requested: &CapabilityRequest) -> bool {
        if requested.validate().is_err() || !self.authorizes_scopes(&requested.effects) {
            return false;
        }
        requested.resources.iter().all(|resource| {
            let exact_or_subtree = |allowed: &BTreeSet<String>| match &resource.selector {
                CapabilitySelector::Exact(value) | CapabilitySelector::Subtree(value) => {
                    allowed.iter().any(|root| {
                        let value = std::path::Path::new(value);
                        let root = std::path::Path::new(root);
                        value == root || value.starts_with(root)
                    })
                }
                CapabilitySelector::Unscoped => true,
                CapabilitySelector::Unrestricted => false,
            };
            match resource.effect {
                EffectScope::FilesystemRead => match &resource.selector {
                    CapabilitySelector::Unrestricted => {
                        requested.effects.contains(&EffectScope::HostExecution)
                            && self
                                .bounds
                                .allowed_effects
                                .contains(&EffectScope::HostExecution)
                    }
                    _ => exact_or_subtree(&self.bounds.filesystem.readable_roots),
                },
                EffectScope::FilesystemWrite => match &resource.selector {
                    CapabilitySelector::Unrestricted => {
                        requested.effects.contains(&EffectScope::HostExecution)
                            && self
                                .bounds
                                .allowed_effects
                                .contains(&EffectScope::HostExecution)
                    }
                    _ => exact_or_subtree(&self.bounds.filesystem.writable_roots),
                },
                EffectScope::Network => match &resource.selector {
                    CapabilitySelector::Exact(target) => {
                        self.bounds.network.allowed_targets.contains(target)
                            || self.bounds.network.allow_unrestricted
                    }
                    CapabilitySelector::Unrestricted => self.bounds.network.allow_unrestricted,
                    _ => false,
                },
                EffectScope::EnvironmentRead => match &resource.selector {
                    CapabilitySelector::Exact(name) => {
                        self.bounds.environment.allowed_variables.contains(name)
                    }
                    CapabilitySelector::Unscoped => true,
                    _ => false,
                },
                EffectScope::SecretRead => match &resource.selector {
                    CapabilitySelector::Exact(reference) => {
                        self.bounds.allowed_credentials.contains(reference)
                    }
                    CapabilitySelector::Unscoped => true,
                    _ => false,
                },
                EffectScope::Process => match &resource.selector {
                    CapabilitySelector::Exact(program) => {
                        self.bounds
                            .process
                            .interactive
                            .command_shells
                            .contains(program)
                            || self
                                .bounds
                                .process
                                .transport
                                .allowed_programs
                                .contains(program)
                    }
                    CapabilitySelector::Unscoped => true,
                    _ => false,
                },
                EffectScope::ArtifactRead
                | EffectScope::ExternalSideEffect
                | EffectScope::HostExecution => true,
            }
        })
    }

    pub fn requires_approval(&self) -> bool {
        self.bounds.approval == ApprovalPolicy::Required
    }
}

/// Host-normalized invocation. Arguments may express requested effects (for
/// example an escalated execution mode), but never carry a grant, approval,
/// lease, or other authority. Only the Host can issue those capabilities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocation {
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub tool_id: ToolId,
    pub arguments: Value,
}

impl ToolInvocation {
    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        if self.run_id.is_empty() || self.call_id.is_empty() || self.tool_id.is_empty() {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "tool invocation identities must not be empty",
            ));
        }
        if !self.arguments.is_object() {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "tool arguments must be a JSON object",
            ));
        }
        Ok(())
    }

    pub fn args_digest(&self) -> Result<Digest, ToolProtocolError> {
        canonical_json_digest(&self.arguments)
    }
}

/// Immutable reference returned when a Tool result is too large to keep in
/// model context. The referenced bytes are the canonical JSON serialization of
/// the Tool's schema-validated output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolArtifact {
    pub artifact: ArtifactRefWithDigest,
    pub media_type: String,
    pub byte_size: u64,
    pub summary: String,
}

impl ToolArtifact {
    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        self.artifact.validate_integrity().map_err(|error| {
            ToolProtocolError::new(ToolProtocolErrorCode::InvalidInvocation, error.to_string())
        })?;
        if self.media_type.trim().is_empty()
            || self.byte_size == 0
            || self.summary.trim().is_empty()
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "Tool artifact requires media_type, positive byte_size, and a summary",
            ));
        }
        Ok(())
    }
}

/// Durable Tool output. Executors produce `Inline`; the Host may replace it
/// with an immutable `Artifact` after validating the original output schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[non_exhaustive]
pub enum ToolOutput {
    Inline(Value),
    Artifact(ToolArtifact),
}

impl ToolOutput {
    pub fn inline(value: Value) -> Self {
        Self::Inline(value)
    }

    pub fn validate(&self) -> Result<(), ToolProtocolError> {
        match self {
            Self::Inline(_) => Ok(()),
            Self::Artifact(artifact) => artifact.validate(),
        }
    }
}

impl From<Value> for ToolOutput {
    fn from(value: Value) -> Self {
        Self::Inline(value)
    }
}

/// Durable semantic result of a tool invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ToolOutcome {
    Completed {
        output: ToolOutput,
    },
    Rejected {
        code: String,
        message: String,
    },
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
    Cancelled,
    UnknownEffect {
        message: String,
    },
}

impl ToolOutcome {
    pub fn validate_shape(&self) -> Result<(), ToolProtocolError> {
        match self {
            Self::Completed { output } => output.validate(),
            Self::Rejected { code, message } | Self::Failed { code, message, .. }
                if code.trim().is_empty() || message.trim().is_empty() =>
            {
                Err(ToolProtocolError::new(
                    ToolProtocolErrorCode::InvalidInvocation,
                    "Tool failure outcomes require a code and message",
                ))
            }
            Self::UnknownEffect { message } if message.trim().is_empty() => {
                Err(ToolProtocolError::new(
                    ToolProtocolErrorCode::InvalidInvocation,
                    "UnknownEffect requires a reason",
                ))
            }
            _ => Ok(()),
        }
    }

    pub fn digest(&self) -> Result<Digest, ToolProtocolError> {
        self.validate_shape()?;
        canonical_json_digest(self)
    }
}

/// Exact operation approved by the Host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalBinding {
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub tool_id: ToolId,
    pub args_digest: Digest,
    pub operation_digest: Digest,
    /// Binds the Host permission-policy contract and its normalized decision
    /// for this operation. A retry cannot turn a previously reviewed
    /// operation into an automatically authorized one.
    pub permission_digest: Digest,
    /// Exact authority request reviewed by policy and, when required, by the
    /// user. This prevents an approval for one resource from authorizing a
    /// different resource with the same coarse effect class.
    pub requested_capabilities: CapabilityRequest,
    /// Optional review class used only by the Host's in-session decision
    /// cache. The signed capability remains exact and single-use.
    #[serde(default)]
    pub session_approval_scope: Option<Digest>,
    pub policy_digest: Digest,
}

impl ApprovalBinding {
    pub fn for_operation(
        invocation: &ToolInvocation,
        operation: &ToolOperationPlan,
        effective_policy: &EffectiveToolPolicy,
        permission_digest: Digest,
    ) -> Result<Self, ToolProtocolError> {
        invocation.validate()?;
        operation.validate_shape()?;
        if !permission_digest.is_sha256() {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidInvocation,
                "permission decision requires a SHA-256 digest",
            ));
        }
        if !effective_policy.authorizes_request(&operation.required_capabilities) {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::PolicyDenied,
                "requested effects are outside the effective policy",
            ));
        }
        Ok(Self {
            run_id: invocation.run_id.clone(),
            call_id: invocation.call_id.clone(),
            tool_id: invocation.tool_id.clone(),
            args_digest: invocation.args_digest()?,
            operation_digest: operation.digest()?,
            permission_digest,
            requested_capabilities: operation.required_capabilities.clone(),
            session_approval_scope: operation.session_approval_scope.clone(),
            policy_digest: effective_policy.digest()?,
        })
    }

    pub fn digest(&self) -> Result<Digest, ToolProtocolError> {
        canonical_json_digest(self)
    }
}

/// Signed capability claims. Any mutation invalidates `authenticator`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalCapabilityClaims {
    pub binding: ApprovalBinding,
    pub expires_at_unix_ms: i64,
    pub nonce: ApprovalNonce,
}

/// Serializable, single-use Host approval capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalCapability {
    pub claims: ApprovalCapabilityClaims,
    pub authenticator: Digest,
}

impl ApprovalCapability {
    fn validate_shape(&self) -> Result<(), ToolProtocolError> {
        if self.claims.run_or_operation_is_empty()
            || self.claims.expires_at_unix_ms <= 0
            || self.claims.nonce.is_empty()
            || !self.claims.binding.args_digest.is_sha256()
            || !self.claims.binding.operation_digest.is_sha256()
            || !self.claims.binding.permission_digest.is_sha256()
            || self
                .claims
                .binding
                .session_approval_scope
                .as_ref()
                .is_some_and(|scope| !scope.is_sha256())
            || !self.claims.binding.policy_digest.is_sha256()
            || !self.authenticator.is_sha256()
        {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidCapability,
                "approval capability has an invalid shape",
            ));
        }
        Ok(())
    }
}

impl ApprovalCapabilityClaims {
    fn run_or_operation_is_empty(&self) -> bool {
        self.binding.run_id.is_empty()
            || self.binding.call_id.is_empty()
            || self.binding.tool_id.is_empty()
    }
}

/// Host-only approval issuer. The signing key is never serializable or exposed
/// through the Tool Protocol.
pub struct HostApprovalIssuer {
    signing_key: Vec<u8>,
}

impl HostApprovalIssuer {
    pub fn new(signing_key: impl AsRef<[u8]>) -> Result<Self, ToolProtocolError> {
        Ok(Self {
            signing_key: validated_signing_key(signing_key.as_ref())?,
        })
    }

    pub fn issue(
        &self,
        binding: ApprovalBinding,
        expires_at_unix_ms: i64,
    ) -> Result<ApprovalCapability, ToolProtocolError> {
        if expires_at_unix_ms <= 0 {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidCapability,
                "approval expiry must be a positive Unix timestamp",
            ));
        }
        let claims = ApprovalCapabilityClaims {
            binding,
            expires_at_unix_ms,
            nonce: ApprovalNonce::new(uuid::Uuid::new_v4().to_string()),
        };
        let authenticator = authenticate_claims(&self.signing_key, &claims)?;
        Ok(ApprovalCapability {
            claims,
            authenticator,
        })
    }
}

/// Atomic replay ledger. Durable hosts should implement this trait with the
/// same transaction boundary as operation authorization.
pub trait ApprovalCapabilityStore: Send + Sync {
    /// Returns `true` only for the first successful consumption of `nonce`.
    fn consume_once(&self, nonce: &ApprovalNonce) -> Result<bool, ToolProtocolError>;
}

/// Minimal process-local replay store for embedded hosts and tests.
#[derive(Default)]
pub struct InMemoryApprovalCapabilityStore {
    consumed: Mutex<BTreeSet<ApprovalNonce>>,
}

impl ApprovalCapabilityStore for InMemoryApprovalCapabilityStore {
    fn consume_once(&self, nonce: &ApprovalNonce) -> Result<bool, ToolProtocolError> {
        let mut consumed = self.consumed.lock().map_err(|_| {
            ToolProtocolError::new(
                ToolProtocolErrorCode::StoreFailure,
                "approval capability store lock is poisoned",
            )
        })?;
        Ok(consumed.insert(nonce.clone()))
    }
}

/// Non-serializable proof returned only after authentication, exact binding,
/// expiry, and atomic replay checks all succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedApprovalCapability {
    binding: ApprovalBinding,
    nonce: ApprovalNonce,
}

impl VerifiedApprovalCapability {
    pub fn binding(&self) -> &ApprovalBinding {
        &self.binding
    }

    pub fn nonce(&self) -> &ApprovalNonce {
        &self.nonce
    }
}

/// Host verifier coupled to a replay store.
pub struct HostApprovalVerifier<S> {
    signing_key: Vec<u8>,
    store: S,
}

impl<S: ApprovalCapabilityStore> HostApprovalVerifier<S> {
    pub fn new(signing_key: impl AsRef<[u8]>, store: S) -> Result<Self, ToolProtocolError> {
        Ok(Self {
            signing_key: validated_signing_key(signing_key.as_ref())?,
            store,
        })
    }

    pub fn verify_and_consume(
        &self,
        capability: &ApprovalCapability,
        expected: &ApprovalBinding,
        now_unix_ms: i64,
    ) -> Result<VerifiedApprovalCapability, ToolProtocolError> {
        capability.validate_shape()?;
        let expected_authenticator = authenticate_claims(&self.signing_key, &capability.claims)?;
        if !constant_time_eq(
            capability.authenticator.as_str().as_bytes(),
            expected_authenticator.as_str().as_bytes(),
        ) {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::InvalidCapability,
                "approval capability authentication failed",
            ));
        }
        if now_unix_ms >= capability.claims.expires_at_unix_ms {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::CapabilityExpired,
                "approval capability has expired",
            ));
        }
        if &capability.claims.binding != expected {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::CapabilityBindingMismatch,
                "approval capability does not match this operation",
            ));
        }
        if !self.store.consume_once(&capability.claims.nonce)? {
            return Err(ToolProtocolError::new(
                ToolProtocolErrorCode::CapabilityReplayed,
                "approval capability has already been consumed",
            ));
        }
        Ok(VerifiedApprovalCapability {
            binding: capability.claims.binding.clone(),
            nonce: capability.claims.nonce.clone(),
        })
    }

    pub fn store(&self) -> &S {
        &self.store
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolProtocolErrorCode {
    InvalidDescriptor,
    InvalidInvocation,
    InvalidPolicy,
    SchemaViolation,
    PolicyDenied,
    Serialization,
    InvalidCapability,
    CapabilityExpired,
    CapabilityBindingMismatch,
    CapabilityReplayed,
    StoreFailure,
}

fn validate_json_value(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let schema = schema
        .as_object()
        .ok_or_else(|| format!("schema at '{path}' must be an object"))?;
    if let Some(type_spec) = schema.get("type") {
        validate_json_type(value, type_spec, path)?;
    }
    if let Some(constant) = schema.get("const") {
        if value != constant {
            return Err(format!("{path} does not match const"));
        }
    }
    if let Some(variants) = schema.get("enum").and_then(Value::as_array) {
        if !variants.iter().any(|candidate| candidate == value) {
            return Err(format!("{path} is not an allowed enum value"));
        }
    }
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{path} must be an object"))?;
        for key in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(key) {
                return Err(format!("{path} is missing required field '{key}'"));
            }
        }
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{path} must be an object"))?;
        for (key, child_schema) in properties {
            if let Some(child) = object.get(key) {
                validate_json_value(child, child_schema, &format!("{path}.{key}"))?;
            }
        }
        if schema.get("additionalProperties").and_then(Value::as_bool) == Some(false) {
            for key in object.keys() {
                if !properties.contains_key(key) {
                    return Err(format!("{path} contains unknown field '{key}'"));
                }
            }
        }
    }
    if let Some(item_schema) = schema.get("items") {
        let items = value
            .as_array()
            .ok_or_else(|| format!("{path} must be an array"))?;
        for (index, item) in items.iter().enumerate() {
            validate_json_value(item, item_schema, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_json_type(value: &Value, type_spec: &Value, path: &str) -> Result<(), String> {
    let matches = |name: &str| match name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    };
    let valid = match type_spec {
        Value::String(name) => matches(name),
        Value::Array(names) => names.iter().filter_map(Value::as_str).any(matches),
        _ => return Err(format!("{path} schema type must be a string or array")),
    };
    valid
        .then_some(())
        .ok_or_else(|| format!("{path} has the wrong JSON type"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code:?}: {message}")]
#[serde(deny_unknown_fields)]
pub struct ToolProtocolError {
    pub code: ToolProtocolErrorCode,
    pub message: String,
}

impl ToolProtocolError {
    pub fn new(code: ToolProtocolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn canonical_json_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, ToolProtocolError> {
    let bytes = serde_jcs::to_vec(value).map_err(|error| {
        ToolProtocolError::new(ToolProtocolErrorCode::Serialization, error.to_string())
    })?;
    Ok(Digest::sha256(bytes))
}

fn validated_signing_key(key: &[u8]) -> Result<Vec<u8>, ToolProtocolError> {
    if key.len() < 32 {
        return Err(ToolProtocolError::new(
            ToolProtocolErrorCode::InvalidCapability,
            "approval signing key must contain at least 32 bytes",
        ));
    }
    Ok(key.to_vec())
}

fn authenticate_claims(
    signing_key: &[u8],
    claims: &ApprovalCapabilityClaims,
) -> Result<Digest, ToolProtocolError> {
    let canonical = serde_jcs::to_vec(claims).map_err(|error| {
        ToolProtocolError::new(ToolProtocolErrorCode::Serialization, error.to_string())
    })?;
    Ok(Digest::new(hex::encode(hmac_sha256(
        signing_key,
        &canonical,
    ))))
}

/// RFC 2104 HMAC-SHA-256, kept local so the authority contract does not add a
/// transport or provider dependency.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK_SIZE: usize = 64;
    let mut key_block = [0_u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36_u8; BLOCK_SIZE];
    let mut outer_pad = [0x5c_u8; BLOCK_SIZE];
    for index in 0..BLOCK_SIZE {
        inner_pad[index] ^= key_block[index];
        outer_pad[index] ^= key_block[index];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn strings(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn effects(values: &[EffectScope]) -> BTreeSet<EffectScope> {
        values.iter().copied().collect()
    }

    fn operation(values: &[EffectScope]) -> ToolOperationPlan {
        let mut required_capabilities = CapabilityRequest::from_effects(effects(values));
        if values.contains(&EffectScope::Network) {
            required_capabilities
                .insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
        }
        ToolOperationPlan {
            required_capabilities,
            risk: ToolOperationRisk::Routine,
            session_approval_scope: None,
            summary: "test operation".to_owned(),
        }
    }

    fn bounds(allowed_effects: &[EffectScope], approval: ApprovalPolicy) -> ToolPolicyBounds {
        ToolPolicyBounds {
            allowed_effects: effects(allowed_effects),
            approval,
            sandbox: SandboxPolicy {
                required: false,
                allowed_profiles: strings(&["strict", "networked"]),
            },
            process: ProcessPolicy {
                interactive: InteractiveCommandPolicy {
                    enabled: true,
                    command_shells: strings(&["sh", "zsh"]),
                    allow_child_processes: true,
                },
                transport: TransportLaunchPolicy {
                    allowed_programs: strings(&["git", "rg"]),
                    allow_child_processes: true,
                },
            },
            filesystem: FilesystemPolicy {
                readable_roots: strings(&["/workspace", "/workspace/docs"]),
                writable_roots: strings(&["/workspace"]),
            },
            network: NetworkPolicy {
                allowed_targets: strings(&["api.example", "docs.example"]),
                allow_unrestricted: true,
            },
            environment: EnvironmentPolicy {
                allowed_variables: strings(&["PATH", "LANG"]),
                inherit_host_environment: true,
            },
            allowed_credentials: strings(&["credential/read-only"]),
            max_timeout_ms: Some(1_000),
            max_output_bytes: Some(10_000),
        }
    }

    #[test]
    fn model_surface_has_no_authority_fields() {
        let schema = ModelToolSchema {
            name: "exec_command".to_owned(),
            description: "Run a command".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": { "command": { "type": "string" } }
            }),
        };
        let schema_json = serde_json::to_value(schema).unwrap();
        let keys: BTreeSet<_> = schema_json.as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys, strings(&["name", "description", "input_schema"]));

        let invocation = ToolInvocation {
            run_id: RunId::new("run-1"),
            call_id: ToolCallId::new("call-1"),
            tool_id: ToolId::new("builtin/exec_command"),
            arguments: json!({ "cmd": "pwd" }),
        };
        let invocation_json = serde_json::to_value(invocation).unwrap();
        let invocation_keys: BTreeSet<_> = invocation_json
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            invocation_keys,
            strings(&["run_id", "call_id", "tool_id", "arguments"])
        );
    }

    #[test]
    fn network_request_cannot_hide_or_silently_widen_its_resource_scope() {
        let missing = CapabilityRequest::from_effects(effects(&[EffectScope::Network]));
        assert_eq!(
            missing.validate().unwrap_err().code,
            ToolProtocolErrorCode::InvalidInvocation
        );

        let mut exact = CapabilityRequest::from_effects(effects(&[EffectScope::Network]));
        exact.insert_resource(
            EffectScope::Network,
            CapabilitySelector::Exact("api.example".to_owned()),
        );
        let mut unrestricted = CapabilityRequest::from_effects(effects(&[EffectScope::Network]));
        unrestricted.insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
        assert_ne!(exact.digest().unwrap(), unrestricted.digest().unwrap());

        let mut exact_only = bounds(&[EffectScope::Network], ApprovalPolicy::NotRequired);
        exact_only.network.allow_unrestricted = false;
        let policy = EffectiveToolPolicy::resolve(
            &HostToolPolicy {
                bounds: exact_only.clone(),
            },
            &RunToolGrant {
                bounds: exact_only.clone(),
            },
            &ToolRestriction { bounds: exact_only },
        )
        .unwrap();
        assert!(policy.authorizes_request(&exact));
        assert!(!policy.authorizes_request(&unrestricted));
    }

    #[test]
    fn unrestricted_filesystem_requires_explicit_host_execution_authority() {
        let mut hidden_widening = CapabilityRequest::from_effects(effects(&[
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
        ]));
        hidden_widening.insert_resource(
            EffectScope::FilesystemRead,
            CapabilitySelector::Unrestricted,
        );
        hidden_widening.insert_resource(
            EffectScope::FilesystemWrite,
            CapabilitySelector::Unrestricted,
        );
        assert_eq!(
            hidden_widening.validate().unwrap_err().code,
            ToolProtocolErrorCode::InvalidInvocation
        );

        let underdeclared_host_execution = CapabilityRequest::from_effects(effects(&[
            EffectScope::Process,
            EffectScope::HostExecution,
        ]));
        assert_eq!(
            underdeclared_host_execution.validate().unwrap_err().code,
            ToolProtocolErrorCode::InvalidInvocation
        );

        let mut host_execution = CapabilityRequest::from_effects(effects(&[
            EffectScope::Process,
            EffectScope::Network,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
            EffectScope::HostExecution,
        ]));
        host_execution.insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
        host_execution.insert_resource(
            EffectScope::FilesystemRead,
            CapabilitySelector::Unrestricted,
        );
        host_execution.insert_resource(
            EffectScope::FilesystemWrite,
            CapabilitySelector::Unrestricted,
        );
        host_execution.insert_resource(
            EffectScope::ExternalSideEffect,
            CapabilitySelector::Unrestricted,
        );
        host_execution.validate().unwrap();

        let common = bounds(
            &[
                EffectScope::Process,
                EffectScope::Network,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
                EffectScope::ExternalSideEffect,
                EffectScope::HostExecution,
            ],
            ApprovalPolicy::Required,
        );
        let policy = EffectiveToolPolicy::resolve(
            &HostToolPolicy {
                bounds: common.clone(),
            },
            &RunToolGrant {
                bounds: common.clone(),
            },
            &ToolRestriction { bounds: common },
        )
        .unwrap();
        assert!(policy.authorizes_request(&host_execution));

        let without_host_execution = bounds(
            &[
                EffectScope::Process,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
            ],
            ApprovalPolicy::Required,
        );
        let policy = EffectiveToolPolicy::resolve(
            &HostToolPolicy {
                bounds: without_host_execution.clone(),
            },
            &RunToolGrant {
                bounds: without_host_execution.clone(),
            },
            &ToolRestriction {
                bounds: without_host_execution,
            },
        )
        .unwrap();
        assert!(!policy.authorizes_request(&host_execution));
    }

    #[test]
    fn effective_policy_is_the_restrictive_intersection() {
        let host = HostToolPolicy {
            bounds: bounds(
                &[
                    EffectScope::Process,
                    EffectScope::Network,
                    EffectScope::FilesystemRead,
                ],
                ApprovalPolicy::NotRequired,
            ),
        };
        let mut run_bounds = bounds(
            &[EffectScope::Process, EffectScope::Network],
            ApprovalPolicy::Required,
        );
        run_bounds.sandbox.allowed_profiles = strings(&["strict"]);
        run_bounds.process.interactive.command_shells = strings(&["sh"]);
        run_bounds.process.interactive.allow_child_processes = false;
        run_bounds.process.transport.allowed_programs = strings(&["rg"]);
        run_bounds.filesystem.readable_roots = strings(&["/workspace/docs"]);
        run_bounds.filesystem.writable_roots.clear();
        run_bounds.network.allowed_targets = strings(&["api.example"]);
        run_bounds.environment.allowed_variables = strings(&["PATH"]);
        run_bounds.environment.inherit_host_environment = false;
        run_bounds.max_output_bytes = Some(5_000);
        let run = RunToolGrant { bounds: run_bounds };

        let mut restriction_bounds = bounds(
            &[EffectScope::Process, EffectScope::SecretRead],
            ApprovalPolicy::NotRequired,
        );
        restriction_bounds.sandbox.required = true;
        restriction_bounds.sandbox.allowed_profiles = strings(&["strict"]);
        restriction_bounds.process.interactive.command_shells = strings(&["sh"]);
        restriction_bounds.process.transport.allowed_programs = strings(&["git", "rg"]);
        restriction_bounds.filesystem.readable_roots = strings(&["/workspace/docs"]);
        restriction_bounds.network.allowed_targets = strings(&["api.example", "other.example"]);
        restriction_bounds.environment.allowed_variables = strings(&["PATH"]);
        restriction_bounds.max_timeout_ms = Some(800);
        restriction_bounds.max_output_bytes = None;
        let restriction = ToolRestriction {
            bounds: restriction_bounds,
        };

        let effective = EffectiveToolPolicy::resolve(&host, &run, &restriction).unwrap();
        let actual = effective.bounds();
        assert_eq!(actual.allowed_effects, effects(&[EffectScope::Process]));
        assert_eq!(actual.approval, ApprovalPolicy::Required);
        assert!(actual.sandbox.required);
        assert_eq!(actual.sandbox.allowed_profiles, strings(&["strict"]));
        assert_eq!(actual.process.interactive.command_shells, strings(&["sh"]));
        assert!(!actual.process.interactive.allow_child_processes);
        assert_eq!(actual.process.transport.allowed_programs, strings(&["rg"]));
        assert_eq!(
            actual.filesystem.readable_roots,
            strings(&["/workspace/docs"])
        );
        assert!(actual.filesystem.writable_roots.is_empty());
        assert_eq!(actual.network.allowed_targets, strings(&["api.example"]));
        assert_eq!(actual.environment.allowed_variables, strings(&["PATH"]));
        assert!(!actual.environment.inherit_host_environment);
        assert_eq!(actual.max_timeout_ms, Some(800));
        assert_eq!(actual.max_output_bytes, Some(5_000));
        assert!(actual.is_no_more_permissive_than(&host.bounds));
        assert!(actual.is_no_more_permissive_than(&run.bounds));
        assert!(actual.is_no_more_permissive_than(&restriction.bounds));
    }

    #[test]
    fn approval_capability_rejects_args_cross_run_and_replay() {
        let common = bounds(&[EffectScope::Process], ApprovalPolicy::Required);
        let effective = EffectiveToolPolicy::resolve(
            &HostToolPolicy {
                bounds: common.clone(),
            },
            &RunToolGrant {
                bounds: common.clone(),
            },
            &ToolRestriction { bounds: common },
        )
        .unwrap();
        let invocation = ToolInvocation {
            run_id: RunId::new("run-1"),
            call_id: ToolCallId::new("call-1"),
            tool_id: ToolId::new("builtin/shell"),
            arguments: json!({ "command": "pwd" }),
        };
        let binding = ApprovalBinding::for_operation(
            &invocation,
            &operation(&[EffectScope::Process]),
            &effective,
            Digest::sha256("permission"),
        )
        .unwrap();
        let capability = HostApprovalIssuer::new(SIGNING_KEY)
            .unwrap()
            .issue(binding.clone(), 10_000)
            .unwrap();
        let verifier =
            HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap();

        let mut changed_args = invocation.clone();
        changed_args.arguments = json!({ "command": "rm -rf ./target" });
        let changed_args_binding = ApprovalBinding::for_operation(
            &changed_args,
            &operation(&[EffectScope::Process]),
            &effective,
            Digest::sha256("permission"),
        )
        .unwrap();
        assert_eq!(
            verifier
                .verify_and_consume(&capability, &changed_args_binding, 1)
                .unwrap_err()
                .code,
            ToolProtocolErrorCode::CapabilityBindingMismatch
        );

        let mut changed_operation = operation(&[EffectScope::Process]);
        changed_operation.risk = ToolOperationRisk::Destructive;
        let changed_operation_binding = ApprovalBinding::for_operation(
            &invocation,
            &changed_operation,
            &effective,
            Digest::sha256("permission"),
        )
        .unwrap();
        assert_eq!(
            verifier
                .verify_and_consume(&capability, &changed_operation_binding, 1)
                .unwrap_err()
                .code,
            ToolProtocolErrorCode::CapabilityBindingMismatch
        );

        let mut cross_run = invocation.clone();
        cross_run.run_id = RunId::new("run-2");
        let cross_run_binding = ApprovalBinding::for_operation(
            &cross_run,
            &operation(&[EffectScope::Process]),
            &effective,
            Digest::sha256("permission"),
        )
        .unwrap();
        assert_eq!(
            verifier
                .verify_and_consume(&capability, &cross_run_binding, 1)
                .unwrap_err()
                .code,
            ToolProtocolErrorCode::CapabilityBindingMismatch
        );

        verifier
            .verify_and_consume(&capability, &binding, 1)
            .unwrap();
        assert_eq!(
            verifier
                .verify_and_consume(&capability, &binding, 1)
                .unwrap_err()
                .code,
            ToolProtocolErrorCode::CapabilityReplayed
        );
    }

    fn next_random(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }

    fn generated_strings(seed: &mut u64, universe: &[&str]) -> BTreeSet<String> {
        universe
            .iter()
            .enumerate()
            .filter(|(index, _)| next_random(seed).rotate_left(*index as u32) & 1 == 1)
            .map(|(_, value)| (*value).to_owned())
            .collect()
    }

    fn generated_bounds(seed: &mut u64) -> ToolPolicyBounds {
        const EFFECTS: [EffectScope; 9] = [
            EffectScope::Process,
            EffectScope::Network,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ArtifactRead,
            EffectScope::EnvironmentRead,
            EffectScope::SecretRead,
            EffectScope::ExternalSideEffect,
            EffectScope::HostExecution,
        ];
        let allowed_effects = EFFECTS
            .into_iter()
            .filter(|_| next_random(seed) & 1 == 1)
            .collect();
        let approval = match next_random(seed) % 3 {
            0 => ApprovalPolicy::NotRequired,
            1 => ApprovalPolicy::Required,
            _ => ApprovalPolicy::Deny,
        };
        let random_cap = |seed: &mut u64| {
            let value = next_random(seed);
            (!value.is_multiple_of(4)).then_some(1 + value % 100_000)
        };
        ToolPolicyBounds {
            allowed_effects,
            approval,
            sandbox: SandboxPolicy {
                required: next_random(seed) & 1 == 1,
                allowed_profiles: generated_strings(seed, &["strict", "networked", "isolated"]),
            },
            process: ProcessPolicy {
                interactive: InteractiveCommandPolicy {
                    enabled: next_random(seed) & 1 == 1,
                    command_shells: generated_strings(seed, &["sh", "bash", "zsh"]),
                    allow_child_processes: next_random(seed) & 1 == 1,
                },
                transport: TransportLaunchPolicy {
                    allowed_programs: generated_strings(seed, &["node", "python", "mcp"]),
                    allow_child_processes: next_random(seed) & 1 == 1,
                },
            },
            filesystem: FilesystemPolicy {
                readable_roots: generated_strings(
                    seed,
                    &["/workspace", "/workspace/docs", "/data"],
                ),
                writable_roots: generated_strings(seed, &["/workspace", "/workspace/out", "/data"]),
            },
            network: NetworkPolicy {
                allowed_targets: generated_strings(
                    seed,
                    &["api.example", "docs.example", "127.0.0.1"],
                ),
                allow_unrestricted: next_random(seed) & 1 == 1,
            },
            environment: EnvironmentPolicy {
                allowed_variables: generated_strings(seed, &["PATH", "LANG", "TOKEN"]),
                inherit_host_environment: next_random(seed) & 1 == 1,
            },
            allowed_credentials: generated_strings(
                seed,
                &["credential/read", "credential/write", "credential/admin"],
            ),
            max_timeout_ms: random_cap(seed),
            max_output_bytes: random_cap(seed),
        }
    }

    #[test]
    fn ten_thousand_generated_policy_compositions_never_exceed_any_authority_ceiling() {
        const CASES: usize = 10_000;

        let mut seed = 0x5EC0_71A1_5AFE_CE11_u64;
        let mut ceiling_violations = 0usize;
        let mut model_authority_influences = 0usize;
        for case in 0..CASES {
            let host = HostToolPolicy {
                bounds: generated_bounds(&mut seed),
            };
            let run = RunToolGrant {
                bounds: generated_bounds(&mut seed),
            };
            let restriction = ToolRestriction {
                bounds: generated_bounds(&mut seed),
            };
            let effective = EffectiveToolPolicy::resolve(&host, &run, &restriction).unwrap();
            ceiling_violations += usize::from(
                !effective.bounds().is_no_more_permissive_than(&host.bounds)
                    || !effective.bounds().is_no_more_permissive_than(&run.bounds)
                    || !effective
                        .bounds()
                        .is_no_more_permissive_than(&restriction.bounds),
            );

            let model_arguments = json!({
                "case": case,
                "approval": "not_required",
                "sandbox": {"required": false, "profile": "host"},
                "network": ["*"],
                "environment": ["TOKEN"],
                "credentials": ["credential/admin"]
            });
            let invocation = ToolInvocation {
                run_id: RunId::new(format!("policy-run-{case}")),
                call_id: ToolCallId::new(format!("policy-call-{case}")),
                tool_id: ToolId::new("generated/tool"),
                arguments: model_arguments,
            };
            invocation.validate().unwrap();
            let repeated = EffectiveToolPolicy::resolve(&host, &run, &restriction).unwrap();
            model_authority_influences += usize::from(repeated != effective);
        }
        assert_eq!(ceiling_violations, 0);
        assert_eq!(model_authority_influences, 0);
    }

    #[test]
    fn one_thousand_each_capability_mutation_expiry_and_replay_attempts_are_rejected() {
        const CASES: usize = 1_000;

        let common = bounds(&[EffectScope::Process], ApprovalPolicy::Required);
        let effective = EffectiveToolPolicy::resolve(
            &HostToolPolicy {
                bounds: common.clone(),
            },
            &RunToolGrant {
                bounds: common.clone(),
            },
            &ToolRestriction { bounds: common },
        )
        .unwrap();
        let issuer = HostApprovalIssuer::new(SIGNING_KEY).unwrap();
        let verifier =
            HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap();
        let mut changed_args_rejected = 0usize;
        let mut cross_run_rejected = 0usize;
        let mut cross_tool_rejected = 0usize;
        let mut expired_rejected = 0usize;
        let mut replay_rejected = 0usize;

        for case in 0..CASES {
            let invocation = ToolInvocation {
                run_id: RunId::new(format!("capability-run-{case}")),
                call_id: ToolCallId::new(format!("capability-call-{case}")),
                tool_id: ToolId::new("builtin/shell"),
                arguments: json!({"command": format!("echo {case}")}),
            };
            let binding = ApprovalBinding::for_operation(
                &invocation,
                &operation(&[EffectScope::Process]),
                &effective,
                Digest::sha256("permission"),
            )
            .unwrap();

            let mut changed_args = invocation.clone();
            changed_args.arguments = json!({"command": format!("mutated {case}")});
            let changed_args_binding = ApprovalBinding::for_operation(
                &changed_args,
                &operation(&[EffectScope::Process]),
                &effective,
                Digest::sha256("permission"),
            )
            .unwrap();
            let capability = issuer.issue(binding.clone(), 10_000).unwrap();
            changed_args_rejected += usize::from(
                verifier
                    .verify_and_consume(&capability, &changed_args_binding, 1)
                    .unwrap_err()
                    .code
                    == ToolProtocolErrorCode::CapabilityBindingMismatch,
            );

            let mut cross_run = invocation.clone();
            cross_run.run_id = RunId::new(format!("foreign-run-{case}"));
            let cross_run_binding = ApprovalBinding::for_operation(
                &cross_run,
                &operation(&[EffectScope::Process]),
                &effective,
                Digest::sha256("permission"),
            )
            .unwrap();
            let capability = issuer.issue(binding.clone(), 10_000).unwrap();
            cross_run_rejected += usize::from(
                verifier
                    .verify_and_consume(&capability, &cross_run_binding, 1)
                    .unwrap_err()
                    .code
                    == ToolProtocolErrorCode::CapabilityBindingMismatch,
            );

            let mut cross_tool = invocation.clone();
            cross_tool.tool_id = ToolId::new("builtin/other-shell");
            let cross_tool_binding = ApprovalBinding::for_operation(
                &cross_tool,
                &operation(&[EffectScope::Process]),
                &effective,
                Digest::sha256("permission"),
            )
            .unwrap();
            let capability = issuer.issue(binding.clone(), 10_000).unwrap();
            cross_tool_rejected += usize::from(
                verifier
                    .verify_and_consume(&capability, &cross_tool_binding, 1)
                    .unwrap_err()
                    .code
                    == ToolProtocolErrorCode::CapabilityBindingMismatch,
            );

            let expired = issuer.issue(binding.clone(), 100).unwrap();
            expired_rejected += usize::from(
                verifier
                    .verify_and_consume(&expired, &binding, 100)
                    .unwrap_err()
                    .code
                    == ToolProtocolErrorCode::CapabilityExpired,
            );

            let replayed = issuer.issue(binding.clone(), 10_000).unwrap();
            verifier.verify_and_consume(&replayed, &binding, 1).unwrap();
            replay_rejected += usize::from(
                verifier
                    .verify_and_consume(&replayed, &binding, 1)
                    .unwrap_err()
                    .code
                    == ToolProtocolErrorCode::CapabilityReplayed,
            );
        }

        assert_eq!(changed_args_rejected, CASES);
        assert_eq!(cross_run_rejected, CASES);
        assert_eq!(cross_tool_rejected, CASES);
        assert_eq!(expired_rejected, CASES);
        assert_eq!(replay_rejected, CASES);
    }
}
