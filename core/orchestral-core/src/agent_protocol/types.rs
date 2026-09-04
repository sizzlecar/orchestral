use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as ShaDigest, Sha256};

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
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
                self.0.is_empty()
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

string_id!(/// Orchestral conversation container identifier.
    AgentSessionId);
string_id!(/// One logical Agent execution within a session.
    RunId);
string_id!(/// Stable normalized Agent event identifier.
    AgentEventId);
string_id!(/// Idempotency identifier for a control command.
    CommandId);
string_id!(/// Correlation identifier for a blocking or non-blocking request.
    RequestId);
string_id!(/// Agent delivery identifier.
    DeliveryId);
string_id!(/// Explicitly non-final partial delivery identifier.
    PartialDeliveryId);
string_id!(/// Stable committed output identifier.
    OutputId);
string_id!(/// Public Agent provider identifier.
    AgentProviderId);
string_id!(/// Agent implementation/profile identifier within a provider.
    AgentId);
string_id!(/// Stable host-side provider binding; never a provider-native session id.
    ProviderBindingRef);
string_id!(/// One resource binding in a run specification.
    ResourceBindingId);
string_id!(/// Opaque resource identifier resolved by the host or adapter.
    ResourceId);
string_id!(/// Immutable resource snapshot/generation identifier.
    ResourceRevision);
string_id!(/// Approval grant reference issued by the host.
    ApprovalGrantRef);
string_id!(/// Host-side reconciliation record reference.
    ReconciliationProofRef);
string_id!(/// Immutable Host/adapter-resolved structured-output schema snapshot.
    SchemaRef);
string_id!(/// Artifact identifier independent of physical storage.
    ArtifactRef);
string_id!(/// Stable telemetry item identifier.
    TelemetryId);
string_id!(/// Stable identifier for one Tool activity across state transitions.
    ToolActivityId);

/// Hex-encoded SHA-256 digest used for immutable protocol bindings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct Digest(String);

impl Digest {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn sha256(bytes: impl AsRef<[u8]>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes.as_ref());
        Self(hex::encode(hasher.finalize()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_sha256(&self) -> bool {
        self.0.len() == 64 && self.0.bytes().all(|byte| byte.is_ascii_hexdigit())
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Version of the public Agent Protocol wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Returns true when `self` can be served by `supported`.
    pub const fn is_compatible_with(self, supported: Self) -> bool {
        self.major == supported.major && self.minor <= supported.minor
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

pub type Extensions = BTreeMap<String, Value>;

/// Schema-only representation of the runtime `namespace/name` extension-key
/// invariant. The serialized wire type remains [`Extensions`].
#[cfg(feature = "agent-protocol-schema")]
#[derive(schemars::JsonSchema)]
#[schemars(transparent, extend("pattern" = r"^[^/][^/]*/[\s\S]+$"))]
#[allow(dead_code)]
struct NamespacedExtensionKey(String);

fn canonical_json_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, AgentProtocolError> {
    let bytes = serde_jcs::to_vec(value).map_err(|error| {
        AgentProtocolError::new(AgentProtocolErrorCode::Serialization, error.to_string())
    })?;
    Ok(Digest::sha256(bytes))
}

/// Open, namespaced resource kind such as `workspace-view/v1`.
///
/// MCP is intentionally not a kind: an MCP adapter publishes ordinary tools
/// into a `tool-catalog` resource. Skill and tool domain types remain separate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct ResourceKind(String);

impl ResourceKind {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_namespaced(&self) -> bool {
        is_namespaced_key(&self.0)
    }
}

fn is_namespaced_key(value: &str) -> bool {
    value
        .split_once('/')
        .is_some_and(|(namespace, name)| !namespace.is_empty() && !name.is_empty())
}

fn validate_extensions(extensions: &Extensions) -> Result<(), AgentProtocolError> {
    if let Some(key) = extensions.keys().find(|key| !is_namespaced_key(key)) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            format!("extension key must be namespaced: {key}"),
        ));
    }
    Ok(())
}

/// Stable protocol error code suitable for RPC adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentProtocolErrorCode {
    InvalidSpec,
    InvalidDigest,
    UnsupportedProtocol,
    Unsupported,
    RunIdConflict,
    RunNotFound,
    CommandNotFound,
    RequestNotFound,
    RequestTypeMismatch,
    InvalidTransition,
    SequenceConflict,
    SequenceGap,
    DuplicateConflict,
    TerminalRun,
    ProviderUnavailable,
    Serialization,
    Internal,
}

/// Structured protocol failure. This is distinct from an Agent run failure.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[error("{code:?}: {message}")]
#[serde(deny_unknown_fields)]
pub struct AgentProtocolError {
    pub code: AgentProtocolErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

impl AgentProtocolError {
    pub fn new(code: AgentProtocolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            details: Value::Null,
        }
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

impl From<serde_json::Error> for AgentProtocolError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(AgentProtocolErrorCode::Serialization, error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRejectionCode {
    InvalidSpec,
    UnsupportedProtocol,
    UnsupportedCapability,
    UnsupportedResource,
    RunIdConflict,
    SessionConflict,
    ProviderUnavailable,
}

/// A start rejection does not create an Agent execution.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[error("{code:?}: {message}")]
#[serde(deny_unknown_fields)]
pub struct AgentRejection {
    pub code: AgentRejectionCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

impl AgentRejection {
    pub fn new(code: AgentRejectionCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            details: Value::Null,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }
}

/// Provider-neutral content block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Content {
    pub media_type: String,
    #[serde(default)]
    pub schema_id: Option<SchemaRef>,
    pub body: ContentBody,
}

impl Content {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            media_type: "text/plain".to_owned(),
            schema_id: None,
            body: ContentBody::Inline(Value::String(text.into())),
        }
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.media_type.trim().is_empty()
            || self.schema_id.as_ref().is_some_and(SchemaRef::is_empty)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "content media_type and optional schema reference must be valid",
            ));
        }
        if let ContentBody::Artifact(artifact) = &self.body {
            artifact.validate_integrity()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[non_exhaustive]
pub enum ContentBody {
    Inline(Value),
    Artifact(ArtifactRefWithDigest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ArtifactRefWithDigest {
    pub artifact_ref: ArtifactRef,
    pub digest: Digest,
}

impl ArtifactRefWithDigest {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.artifact_ref.is_empty() || !self.digest.is_sha256() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "artifact reference and SHA-256 digest must be valid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct NamedOutput {
    pub name: String,
    pub content: Content,
}

impl NamedOutput {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.name.trim().is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "named output name must not be empty",
            ));
        }
        self.content.validate_integrity()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunLimitKind {
    Deadline,
    ModelSteps,
    ToolCalls,
    InputTokens,
    OutputTokens,
    Cost,
}

/// Limits have protocol meaning only when a provider declares support.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RunLimits {
    #[serde(default)]
    pub deadline_unix_ms: Option<i64>,
    #[serde(default)]
    pub max_model_steps: Option<u64>,
    #[serde(default)]
    pub max_tool_calls: Option<u64>,
    #[serde(default)]
    pub max_input_tokens: Option<u64>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub max_cost: Option<MoneyAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MoneyAmount {
    /// ISO 4217-style uppercase currency code, for example `USD`.
    pub currency: String,
    pub microunits: u64,
}

impl RunLimits {
    pub fn requested_kinds(&self) -> BTreeSet<RunLimitKind> {
        let mut kinds = BTreeSet::new();
        if self.deadline_unix_ms.is_some() {
            kinds.insert(RunLimitKind::Deadline);
        }
        if self.max_model_steps.is_some() {
            kinds.insert(RunLimitKind::ModelSteps);
        }
        if self.max_tool_calls.is_some() {
            kinds.insert(RunLimitKind::ToolCalls);
        }
        if self.max_input_tokens.is_some() {
            kinds.insert(RunLimitKind::InputTokens);
        }
        if self.max_output_tokens.is_some() {
            kinds.insert(RunLimitKind::OutputTokens);
        }
        if self.max_cost.is_some() {
            kinds.insert(RunLimitKind::Cost);
        }
        kinds
    }
}

/// Immutable execution boundary for one logical Agent run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentRunSpec {
    pub protocol_version: ProtocolVersion,
    pub session_id: AgentSessionId,
    pub run_id: RunId,
    pub input: Vec<Content>,
    #[serde(default)]
    pub resources: Vec<ResourceBinding>,
    #[serde(default)]
    pub limits: RunLimits,
    #[serde(default)]
    pub output_schema: Option<SchemaRef>,
    #[serde(default)]
    #[cfg_attr(
        feature = "agent-protocol-schema",
        schemars(with = "BTreeMap::<NamespacedExtensionKey, Value>")
    )]
    pub extensions: Extensions,
}

/// Immutable resource visibility binding. It never conveys effect authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceBinding {
    pub binding_id: ResourceBindingId,
    pub resource: ResourceRef,
    pub requirement: BindingRequirement,
    pub mode: ResourceBindingMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: ResourceId,
    pub revision: ResourceRevision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BindingRequirement {
    Required,
    Optional,
}

/// Digest-bearing transport envelope. Keeping the digest outside the spec
/// avoids a self-referential canonical representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentRunEnvelope {
    pub spec: AgentRunSpec,
    pub spec_digest: Digest,
}

/// Host-selected immutable Provider contract for starting a run. Binding the
/// descriptor in the request prevents describe/start time-of-check drift.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentStartRequest {
    pub run: AgentRunEnvelope,
    pub provider_binding: ProviderBindingRef,
    pub expected_descriptor_digest: Digest,
}

impl AgentRunEnvelope {
    pub fn new(
        protocol_version: ProtocolVersion,
        session_id: AgentSessionId,
        run_id: RunId,
        input: Vec<Content>,
    ) -> Result<Self, AgentProtocolError> {
        Self::seal(AgentRunSpec {
            protocol_version,
            session_id,
            run_id,
            input,
            resources: Vec::new(),
            limits: RunLimits::default(),
            output_schema: None,
            extensions: Extensions::new(),
        })
    }

    pub fn seal(spec: AgentRunSpec) -> Result<Self, AgentProtocolError> {
        let spec_digest = Self::compute_digest(&spec)?;
        Ok(Self { spec, spec_digest })
    }

    pub fn compute_digest(spec: &AgentRunSpec) -> Result<Digest, AgentProtocolError> {
        canonical_json_digest(spec)
    }

    pub fn verify_digest(&self) -> Result<(), AgentProtocolError> {
        let computed = Self::compute_digest(&self.spec)?;
        if computed != self.spec_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "AgentRunSpec digest does not match its canonical payload",
            ));
        }
        Ok(())
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.spec.protocol_version.major == 0 {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::UnsupportedProtocol,
                "protocol major version must be non-zero",
            ));
        }
        if self.spec.session_id.is_empty() || self.spec.run_id.is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "required AgentRunSpec references must not be empty",
            ));
        }
        if self.spec.input.is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "AgentRunSpec input must not be empty",
            ));
        }
        for content in &self.spec.input {
            content.validate_integrity()?;
        }
        if self
            .spec
            .output_schema
            .as_ref()
            .is_some_and(SchemaRef::is_empty)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "output schema reference must not be empty",
            ));
        }
        validate_run_limits(&self.spec.limits)?;
        validate_extensions(&self.spec.extensions)?;
        let mut binding_ids = BTreeSet::new();
        for binding in &self.spec.resources {
            if binding.binding_id.is_empty()
                || binding.resource.kind.is_empty()
                || binding.resource.id.is_empty()
                || binding.resource.revision.is_empty()
                || !binding.resource.kind.is_namespaced()
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "resource binding identifiers, kind, and revision must not be empty",
                ));
            }
            if !binding_ids.insert(binding.binding_id.clone()) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "resource binding_id must be unique within a run",
                ));
            }
        }
        self.verify_digest()
    }
}

impl AgentStartRequest {
    /// Binds a sealed Run to the exact immutable Provider descriptor selected
    /// by the Host. Providers should validate this request before creating any
    /// native work.
    pub fn new(
        run: AgentRunEnvelope,
        provider_binding: ProviderBindingRef,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<Self, AgentProtocolError> {
        let request = Self {
            run,
            provider_binding,
            expected_descriptor_digest: descriptor.descriptor_digest.clone(),
        };
        request.validate_for_descriptor(descriptor)?;
        Ok(request)
    }

    pub fn validate_for_descriptor(
        &self,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<(), AgentProtocolError> {
        self.run.validate_integrity()?;
        descriptor.validate_integrity()?;
        if self.provider_binding.is_empty()
            || self.expected_descriptor_digest != descriptor.descriptor_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "start request does not match the selected Provider binding and descriptor",
            ));
        }
        Ok(())
    }
}

fn validate_run_limits(limits: &RunLimits) -> Result<(), AgentProtocolError> {
    if limits.deadline_unix_ms.is_some_and(|value| value <= 0)
        || limits.max_model_steps == Some(0)
        || limits.max_tool_calls == Some(0)
        || limits.max_input_tokens == Some(0)
        || limits.max_output_tokens == Some(0)
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            "run limits must be positive",
        ));
    }
    if let Some(cost) = &limits.max_cost {
        validate_money(cost)?;
    }
    Ok(())
}

fn validate_money(amount: &MoneyAmount) -> Result<(), AgentProtocolError> {
    if amount.currency.len() != 3
        || !amount
            .currency
            .bytes()
            .all(|byte| byte.is_ascii_uppercase())
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            "money currency must be a three-letter uppercase code",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PendingRequestKind {
    Input,
    Approval,
    ExternalAction,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ControlCapabilities {
    pub steer: bool,
    pub cancel: CancelSupport,
    /// Provider-native stream recovery. Host journal replay is always a
    /// separate Orchestral responsibility.
    pub recover: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CancelSupport {
    #[default]
    Unsupported,
    /// The provider accepts cancellation but cannot prove the final native state.
    BestEffort,
    /// The provider emits an authoritative cancellation or competing terminal.
    /// A disconnect/Unknown outcome does not satisfy this capability.
    Confirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResourceBindingMode {
    Snapshot,
    OnDemand,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceCapability {
    pub kind: ResourceKind,
    #[serde(default)]
    pub modes: BTreeSet<ResourceBindingMode>,
    #[serde(default)]
    pub max_bindings: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EffectMediation {
    #[default]
    None,
    HostMediated,
    ProviderManaged,
}

/// Capabilities are promises and must be checked by conformance tests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentCapabilities {
    pub session_reuse: bool,
    pub structured_output: bool,
    pub controls: ControlCapabilities,
    #[serde(default)]
    pub pending_request_kinds: BTreeSet<PendingRequestKind>,
    #[serde(default)]
    pub supported_limits: BTreeSet<RunLimitKind>,
    #[serde(default)]
    pub resources: Vec<ResourceCapability>,
    pub effect_mediation: EffectMediation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentDescriptor {
    pub provider_id: AgentProviderId,
    pub agent_id: AgentId,
    pub supported_protocol_versions: Vec<ProtocolVersion>,
    pub accepted_content_types: BTreeSet<String>,
    pub capabilities: AgentCapabilities,
    #[serde(default)]
    #[cfg_attr(
        feature = "agent-protocol-schema",
        schemars(with = "BTreeMap::<NamespacedExtensionKey, Value>")
    )]
    pub extensions: Extensions,
}

/// Immutable capability promise selected for a Run. A Provider configuration
/// change must produce a new descriptor digest (and normally a new binding).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentDescriptorEnvelope {
    pub descriptor: AgentDescriptor,
    pub descriptor_digest: Digest,
}

impl AgentDescriptorEnvelope {
    pub fn seal(descriptor: AgentDescriptor) -> Result<Self, AgentProtocolError> {
        descriptor.validate_integrity()?;
        let descriptor_digest = canonical_json_digest(&descriptor)?;
        Ok(Self {
            descriptor,
            descriptor_digest,
        })
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        self.descriptor.validate_integrity()?;
        if canonical_json_digest(&self.descriptor)? != self.descriptor_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "descriptor digest does not match its canonical payload",
            ));
        }
        Ok(())
    }
}

impl AgentDescriptor {
    pub fn supports_protocol(&self, requested: ProtocolVersion) -> bool {
        self.supported_protocol_versions
            .iter()
            .copied()
            .any(|supported| requested.is_compatible_with(supported))
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.provider_id.is_empty() || self.agent_id.is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "provider_id and agent_id must not be empty",
            ));
        }
        if self.supported_protocol_versions.is_empty()
            || self
                .supported_protocol_versions
                .iter()
                .any(|version| version.major == 0)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "descriptor must declare at least one non-zero protocol version",
            ));
        }
        if self.accepted_content_types.is_empty()
            || self.accepted_content_types.iter().any(String::is_empty)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "descriptor must declare non-empty accepted content types",
            ));
        }
        let mut resource_kinds = BTreeSet::new();
        for capability in &self.capabilities.resources {
            if !capability.kind.is_namespaced()
                || capability.modes.is_empty()
                || capability.max_bindings == Some(0)
                || !resource_kinds.insert(capability.kind.clone())
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "resource capabilities require a unique namespaced kind, at least one mode, and positive capacity",
                ));
            }
        }
        validate_extensions(&self.extensions)
    }

    /// Checks static compatibility between a sealed run and the provider's
    /// declared capabilities. This does not resolve resources, validate final
    /// output against a schema, or prove that runtime behavior is conformant.
    ///
    /// Optional resources that cannot be consumed are returned explicitly so
    /// the provider can emit `ResourceBindingSkipped` before `RunStarted`.
    pub fn check_run_compatibility(
        &self,
        run: &AgentRunEnvelope,
    ) -> Result<AgentCompatibility, AgentRejection> {
        self.validate_integrity().map_err(|error| {
            AgentRejection::new(
                AgentRejectionCode::ProviderUnavailable,
                format!("provider descriptor is invalid: {error}"),
            )
        })?;
        run.validate_integrity().map_err(|error| {
            AgentRejection::new(AgentRejectionCode::InvalidSpec, error.to_string())
                .with_details(error.details)
        })?;

        if !self.supports_protocol(run.spec.protocol_version) {
            return Err(AgentRejection::new(
                AgentRejectionCode::UnsupportedProtocol,
                format!(
                    "provider does not support Agent Protocol {}",
                    run.spec.protocol_version
                ),
            ));
        }

        let unsupported_content = run.spec.input.iter().find(|content| {
            !self
                .accepted_content_types
                .iter()
                .any(|accepted| media_type_matches(accepted, &content.media_type))
        });
        if let Some(content) = unsupported_content {
            return Err(AgentRejection::new(
                AgentRejectionCode::UnsupportedCapability,
                format!("unsupported input media type: {}", content.media_type),
            ));
        }

        if run.spec.output_schema.is_some() && !self.capabilities.structured_output {
            return Err(AgentRejection::new(
                AgentRejectionCode::UnsupportedCapability,
                "provider does not support structured output",
            ));
        }

        let unsupported_limits = run
            .spec
            .limits
            .requested_kinds()
            .difference(&self.capabilities.supported_limits)
            .cloned()
            .collect::<Vec<_>>();
        if !unsupported_limits.is_empty() {
            return Err(AgentRejection::new(
                AgentRejectionCode::UnsupportedCapability,
                format!("unsupported run limits: {unsupported_limits:?}"),
            ));
        }

        self.validate_resource_bindings(run)
    }

    fn validate_resource_bindings(
        &self,
        run: &AgentRunEnvelope,
    ) -> Result<AgentCompatibility, AgentRejection> {
        let capabilities = self
            .capabilities
            .resources
            .iter()
            .map(|capability| (capability.kind.clone(), capability))
            .collect::<BTreeMap<_, _>>();

        let mut admitted_per_kind = BTreeMap::<ResourceKind, u32>::new();
        for binding in run
            .spec
            .resources
            .iter()
            .filter(|binding| binding.requirement == BindingRequirement::Required)
        {
            let Some(capability) = capabilities.get(&binding.resource.kind) else {
                return Err(unsupported_resource(
                    binding,
                    ResourceBindingSkipCode::UnsupportedKind,
                    "resource kind is unsupported",
                ));
            };
            if !capability.modes.contains(&binding.mode) {
                return Err(unsupported_resource(
                    binding,
                    ResourceBindingSkipCode::UnsupportedMode,
                    "binding mode is unsupported",
                ));
            }
            let count = admitted_per_kind
                .entry(binding.resource.kind.clone())
                .or_default();
            *count += 1;
            if capability
                .max_bindings
                .is_some_and(|maximum| *count > maximum)
            {
                return Err(unsupported_resource(
                    binding,
                    ResourceBindingSkipCode::CapacityExceeded,
                    "required resource bindings exceed provider capacity",
                ));
            }
        }

        let mut skipped_optional_bindings = Vec::new();
        for binding in run
            .spec
            .resources
            .iter()
            .filter(|binding| binding.requirement == BindingRequirement::Optional)
        {
            let skip = match capabilities.get(&binding.resource.kind) {
                None => Some((
                    ResourceBindingSkipCode::UnsupportedKind,
                    "resource kind is unsupported",
                )),
                Some(capability) if !capability.modes.contains(&binding.mode) => Some((
                    ResourceBindingSkipCode::UnsupportedMode,
                    "binding mode is unsupported",
                )),
                Some(capability) => {
                    let count = admitted_per_kind
                        .entry(binding.resource.kind.clone())
                        .or_default();
                    if capability
                        .max_bindings
                        .is_some_and(|maximum| *count >= maximum)
                    {
                        Some((
                            ResourceBindingSkipCode::CapacityExceeded,
                            "resource binding capacity is exhausted",
                        ))
                    } else {
                        *count += 1;
                        None
                    }
                }
            };
            if let Some((code, reason)) = skip {
                skipped_optional_bindings.push(ResourceBindingSkip {
                    binding_id: binding.binding_id.clone(),
                    code,
                    reason: reason.to_owned(),
                });
            }
        }

        Ok(AgentCompatibility {
            skipped_optional_bindings,
        })
    }
}

fn media_type_matches(accepted: &str, actual: &str) -> bool {
    if accepted == actual || accepted == "*/*" {
        return true;
    }
    accepted
        .strip_suffix("/*")
        .is_some_and(|family| !family.is_empty() && actual.starts_with(&format!("{family}/")))
}

fn unsupported_resource(
    binding: &ResourceBinding,
    code: ResourceBindingSkipCode,
    reason: &str,
) -> AgentRejection {
    AgentRejection::new(
        AgentRejectionCode::UnsupportedResource,
        format!(
            "resource binding {} ({}) cannot be admitted: {reason}",
            binding.binding_id,
            binding.resource.kind.as_str()
        ),
    )
    .with_details(serde_json::json!({
        "binding_id": binding.binding_id.as_str(),
        "resource_kind": binding.resource.kind.as_str(),
        "binding_mode": binding.mode,
        "code": code,
        "reason": reason,
    }))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompatibility {
    /// Bindings that cannot be consumed according to the static descriptor.
    #[serde(default)]
    pub skipped_optional_bindings: Vec<ResourceBindingSkip>,
}

/// Final resource admission after the adapter has resolved every binding that
/// must be available before native work starts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentAdmission {
    #[serde(default)]
    pub skipped_optional_bindings: Vec<ResourceBindingSkip>,
}

impl AgentAdmission {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        let mut seen = BTreeSet::new();
        for skip in &self.skipped_optional_bindings {
            skip.validate_integrity()?;
            if !seen.insert(skip.binding_id.clone()) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "admission skip binding IDs must be unique",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_for_run(&self, run: &AgentRunEnvelope) -> Result<(), AgentProtocolError> {
        self.validate_integrity()?;
        let bindings = run
            .spec
            .resources
            .iter()
            .map(|binding| (binding.binding_id.clone(), binding))
            .collect::<BTreeMap<_, _>>();
        for skip in &self.skipped_optional_bindings {
            let Some(binding) = bindings.get(&skip.binding_id) else {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "admission skip references a binding absent from the run",
                ));
            };
            if binding.requirement != BindingRequirement::Optional {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "admission can skip only optional resource bindings",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_against(
        &self,
        run: &AgentRunEnvelope,
        compatibility: &AgentCompatibility,
    ) -> Result<(), AgentProtocolError> {
        self.validate_for_run(run)?;
        let actual = self
            .skipped_optional_bindings
            .iter()
            .map(|skip| (skip.binding_id.clone(), skip))
            .collect::<BTreeMap<_, _>>();
        let static_skips = compatibility
            .skipped_optional_bindings
            .iter()
            .map(|skip| (skip.binding_id.clone(), skip))
            .collect::<BTreeMap<_, _>>();

        for (binding_id, expected) in &static_skips {
            let Some(observed) = actual.get(binding_id) else {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "admission omitted a statically unsupported optional binding",
                ));
            };
            if observed.code != expected.code {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "admission changed the static resource skip code",
                ));
            }
        }
        if actual.iter().any(|(binding_id, skip)| {
            !static_skips.contains_key(binding_id)
                && skip.code != ResourceBindingSkipCode::ResolutionFailed
        }) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "additional admission skips must be pre-start resolution failures",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceBindingSkip {
    pub binding_id: ResourceBindingId,
    pub code: ResourceBindingSkipCode,
    pub reason: String,
}

impl ResourceBindingSkip {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.binding_id.is_empty() || self.reason.trim().is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "resource binding skip requires a binding_id and reason",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResourceBindingSkipCode {
    UnsupportedKind,
    UnsupportedMode,
    CapacityExceeded,
    ResolutionFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentCommandEnvelope {
    pub command_id: CommandId,
    pub run_id: RunId,
    #[serde(default)]
    pub request_id: Option<RequestId>,
    pub payload: AgentCommand,
    /// Digest-bound Host metadata. Concrete Provider wire details do not
    /// belong here; cross-cutting session/control semantics use namespaced
    /// extensions instead.
    #[serde(default, skip_serializing_if = "Extensions::is_empty")]
    #[cfg_attr(
        feature = "agent-protocol-schema",
        schemars(with = "BTreeMap::<NamespacedExtensionKey, Value>")
    )]
    pub extensions: Extensions,
    pub command_digest: Digest,
}

#[derive(Serialize)]
struct AgentCommandDigestView<'a> {
    command_id: &'a CommandId,
    run_id: &'a RunId,
    request_id: &'a Option<RequestId>,
    payload: &'a AgentCommand,
    #[serde(skip_serializing_if = "Extensions::is_empty")]
    extensions: &'a Extensions,
}

impl AgentCommandEnvelope {
    pub fn new(
        command_id: CommandId,
        run_id: RunId,
        request_id: Option<RequestId>,
        payload: AgentCommand,
    ) -> Result<Self, AgentProtocolError> {
        Self::new_with_extensions(command_id, run_id, request_id, payload, Extensions::new())
    }

    pub fn new_with_extensions(
        command_id: CommandId,
        run_id: RunId,
        request_id: Option<RequestId>,
        payload: AgentCommand,
        extensions: Extensions,
    ) -> Result<Self, AgentProtocolError> {
        let mut command = Self {
            command_id,
            run_id,
            request_id,
            payload,
            extensions,
            command_digest: Digest::sha256([]),
        };
        command.validate_shape()?;
        command.command_digest = command.computed_digest()?;
        Ok(command)
    }

    pub fn computed_digest(&self) -> Result<Digest, AgentProtocolError> {
        canonical_json_digest(&AgentCommandDigestView {
            command_id: &self.command_id,
            run_id: &self.run_id,
            request_id: &self.request_id,
            payload: &self.payload,
            extensions: &self.extensions,
        })
    }

    pub fn verify_digest(&self) -> Result<(), AgentProtocolError> {
        self.validate_shape()?;
        if self.computed_digest()? != self.command_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "command digest does not match its canonical payload",
            ));
        }
        Ok(())
    }

    pub fn validate_shape(&self) -> Result<(), AgentProtocolError> {
        if self.command_id.is_empty() || self.run_id.is_empty() {
            return Err(invalid_command("command_id and run_id must not be empty"));
        }
        validate_extensions(&self.extensions)?;
        match (&self.request_id, &self.payload) {
            (None, AgentCommand::Steer { content }) if !content.is_empty() => {
                for item in content {
                    item.validate_integrity()?;
                }
            }
            (None, AgentCommand::Cancel { reason }) if !reason.trim().is_empty() => {}
            (Some(request_id), AgentCommand::ResolveRequest { response })
                if !request_id.is_empty() =>
            {
                response.validate_shape()?;
            }
            (None, AgentCommand::Steer { .. }) => {
                return Err(invalid_command("Steer content must not be empty"));
            }
            (None, AgentCommand::Cancel { .. }) => {
                return Err(invalid_command("Cancel reason must not be empty"));
            }
            (None, AgentCommand::ResolveRequest { .. }) => {
                return Err(invalid_command("ResolveRequest requires request_id"));
            }
            (Some(_), AgentCommand::Steer { .. } | AgentCommand::Cancel { .. }) => {
                return Err(invalid_command("only ResolveRequest may carry request_id"));
            }
            (Some(_), AgentCommand::ResolveRequest { .. }) => {
                return Err(invalid_command("request_id must not be empty"));
            }
        }
        Ok(())
    }
}

fn invalid_command(message: impl Into<String>) -> AgentProtocolError {
    AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, message)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentCommand {
    Steer { content: Vec<Content> },
    ResolveRequest { response: RequestResolution },
    Cancel { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum RequestResolution {
    Input {
        content: Vec<Content>,
    },
    Approval {
        decision: ApprovalDecision,
        #[serde(default)]
        grant_ref: Option<ApprovalGrantRef>,
    },
    ExternalResult {
        result: Vec<Content>,
    },
}

impl RequestResolution {
    pub const fn kind(&self) -> PendingRequestKind {
        match self {
            Self::Input { .. } => PendingRequestKind::Input,
            Self::Approval { .. } => PendingRequestKind::Approval,
            Self::ExternalResult { .. } => PendingRequestKind::ExternalAction,
        }
    }

    pub fn digest(&self) -> Result<Digest, AgentProtocolError> {
        canonical_json_digest(self)
    }

    pub fn validate_shape(&self) -> Result<(), AgentProtocolError> {
        match self {
            Self::Input { content } if content.is_empty() => Err(invalid_command(
                "input resolution content must not be empty",
            )),
            Self::Input { content } | Self::ExternalResult { result: content } => {
                if content.is_empty() {
                    return Err(invalid_command(
                        "input and external result content must not be empty",
                    ));
                }
                for item in content {
                    item.validate_integrity()?;
                }
                Ok(())
            }
            Self::Approval {
                decision: ApprovalDecision::Allow,
                grant_ref: Some(grant_ref),
            } if !grant_ref.is_empty() => Ok(()),
            Self::Approval {
                decision: ApprovalDecision::Allow,
                ..
            } => Err(invalid_command(
                "allowed approval requires a non-empty Host grant reference",
            )),
            Self::Approval {
                decision: ApprovalDecision::Deny,
                grant_ref: None,
            } => Ok(()),
            Self::Approval {
                decision: ApprovalDecision::Deny,
                grant_ref: Some(_),
            } => Err(invalid_command("denied approval cannot carry a grant")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ApprovalDecision {
    Allow,
    Deny,
}

/// Provider-native response. It never contains Host journal sequence numbers
/// and cannot claim that a causal effect has been durably applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ProviderCommandDisposition {
    pub command_id: CommandId,
    pub run_id: RunId,
    pub outcome: ProviderCommandOutcome,
    pub duplicate: bool,
}

impl ProviderCommandDisposition {
    pub fn validate_for(&self, command: &AgentCommandEnvelope) -> Result<(), AgentProtocolError> {
        if self.command_id != command.command_id || self.run_id != command.run_id {
            return Err(invalid_command(
                "Provider command disposition does not match the command identity",
            ));
        }
        self.outcome.validate_shape()?;
        Ok(())
    }

    /// Converts the Provider-native command response into its stable durable
    /// observation. The Host still owns sequencing and authority metadata,
    /// while the Provider can reproduce this exact draft during recovery.
    pub fn to_event_draft(&self) -> Result<AgentEventDraft, AgentProtocolError> {
        if self.command_id.is_empty() || self.run_id.is_empty() {
            return Err(invalid_command(
                "Provider command disposition identities must not be empty",
            ));
        }
        self.outcome.validate_shape()?;
        Ok(AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "provider-command-disposition-{}-{}",
                self.run_id.as_str(),
                self.command_id.as_str()
            )),
            run_id: self.run_id.clone(),
            causation_id: Some(self.command_id.clone()),
            source_fingerprint: None,
            payload: AgentEvent::CommandDispositionRecorded {
                command_id: self.command_id.clone(),
                outcome: self.outcome.clone(),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ProviderCommandOutcome {
    Accepted,
    Rejected {
        code: AgentProtocolErrorCode,
        message: String,
    },
    Unsupported {
        feature: String,
    },
}

impl ProviderCommandOutcome {
    pub fn validate_shape(&self) -> Result<(), AgentProtocolError> {
        match self {
            Self::Accepted => Ok(()),
            Self::Rejected { message, .. } if !message.trim().is_empty() => Ok(()),
            Self::Unsupported { feature } if !feature.trim().is_empty() => Ok(()),
            Self::Rejected { .. } => Err(invalid_command(
                "rejected Provider disposition requires a message",
            )),
            Self::Unsupported { .. } => Err(invalid_command(
                "unsupported Provider disposition requires a feature name",
            )),
        }
    }
}

/// Host control API acknowledgement derived only from committed journal facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CommandAck {
    pub command_id: CommandId,
    pub run_id: RunId,
    pub duplicate: bool,
    pub state: CommandAckState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum CommandAckState {
    Accepted {
        recorded_seq: u64,
    },
    Applied {
        recorded_seq: u64,
        applied_seq: u64,
    },
    Rejected {
        recorded_seq: u64,
        code: AgentProtocolErrorCode,
        message: String,
    },
    Unsupported {
        recorded_seq: u64,
        feature: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PendingRequest {
    pub request_id: RequestId,
    pub blocking: bool,
    pub payload: PendingRequestPayload,
}

impl PendingRequest {
    pub const fn kind(&self) -> PendingRequestKind {
        self.payload.kind()
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.request_id.is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "pending request_id must not be empty",
            ));
        }
        self.payload.validate_integrity()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum PendingRequestPayload {
    Input {
        prompt: Vec<Content>,
        #[serde(default)]
        input_schema: Option<SchemaRef>,
    },
    Approval {
        operation_digest: Digest,
        requested_scope: Vec<String>,
        /// Opaque Host review class that may be remembered for this
        /// interactive session. It never replaces the exact operation digest
        /// or the single-use approval grant.
        #[serde(default)]
        session_approval_scope: Option<Digest>,
        reason: String,
    },
    ExternalAction {
        name: String,
        arguments: Value,
        #[serde(default)]
        result_schema: Option<SchemaRef>,
    },
}

impl PendingRequestPayload {
    pub const fn kind(&self) -> PendingRequestKind {
        match self {
            Self::Input { .. } => PendingRequestKind::Input,
            Self::Approval { .. } => PendingRequestKind::Approval,
            Self::ExternalAction { .. } => PendingRequestKind::ExternalAction,
        }
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        match self {
            Self::Input {
                prompt,
                input_schema,
            } => {
                if prompt.is_empty() || input_schema.as_ref().is_some_and(SchemaRef::is_empty) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidSpec,
                        "input request requires a prompt and valid optional schema",
                    ));
                }
                for content in prompt {
                    content.validate_integrity()?;
                }
            }
            Self::Approval {
                operation_digest,
                requested_scope,
                session_approval_scope,
                reason,
            } => {
                if !operation_digest.is_sha256()
                    || session_approval_scope
                        .as_ref()
                        .is_some_and(|scope| !scope.is_sha256())
                    || requested_scope.is_empty()
                    || requested_scope.iter().any(|scope| scope.trim().is_empty())
                    || reason.trim().is_empty()
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidSpec,
                        "approval request requires an operation digest, scope, and reason",
                    ));
                }
            }
            Self::ExternalAction {
                name,
                result_schema,
                ..
            } if name.trim().is_empty()
                || result_schema.as_ref().is_some_and(SchemaRef::is_empty) =>
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "external action requires a name and valid optional result schema",
                ));
            }
            Self::ExternalAction { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct UsageReport {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub tool_calls: Option<u64>,
    #[serde(default)]
    pub cost: Option<MoneyAmount>,
}

impl UsageReport {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if let Some(cost) = &self.cost {
            validate_money(cost)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub provider_id: AgentProviderId,
    pub agent_id: AgentId,
    /// Non-authoritative trace hints. Every ID must name a prior durable event
    /// in this Run; existence does not prove that the event caused the output.
    #[serde(default)]
    pub supporting_event_ids: Vec<AgentEventId>,
    #[serde(default)]
    #[cfg_attr(
        feature = "agent-protocol-schema",
        schemars(with = "BTreeMap::<NamespacedExtensionKey, Value>")
    )]
    pub extensions: Extensions,
}

impl Provenance {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.provider_id.is_empty()
            || self.agent_id.is_empty()
            || self.supporting_event_ids.iter().any(AgentEventId::is_empty)
            || self
                .supporting_event_ids
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.supporting_event_ids.len()
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "provenance requires provider/agent IDs and unique non-empty supporting event IDs",
            ));
        }
        validate_extensions(&self.extensions)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentDelivery {
    pub delivery_id: DeliveryId,
    pub run_id: RunId,
    pub spec_digest: Digest,
    pub final_response: Content,
    #[serde(default)]
    pub outputs: Vec<NamedOutput>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactRefWithDigest>,
    #[serde(default)]
    pub unresolved_issues: Vec<String>,
    #[serde(default)]
    pub usage: Option<UsageReport>,
    pub provenance: Provenance,
}

impl AgentDelivery {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.delivery_id.is_empty() || self.run_id.is_empty() || !self.spec_digest.is_sha256() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "delivery identity and spec digest must be valid",
            ));
        }
        self.final_response.validate_integrity()?;
        validate_named_outputs(&self.outputs)?;
        validate_artifacts(&self.artifacts)?;
        if self
            .unresolved_issues
            .iter()
            .any(|issue| issue.trim().is_empty())
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "delivery unresolved issues must not contain empty entries",
            ));
        }
        if let Some(usage) = &self.usage {
            usage.validate_integrity()?;
        }
        self.provenance.validate_integrity()
    }
}

/// Partial output attached to an incomplete run. It cannot be accepted as a
/// successful [`AgentDelivery`] because the types are intentionally distinct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PartialDelivery {
    pub partial_delivery_id: PartialDeliveryId,
    pub run_id: RunId,
    pub spec_digest: Digest,
    #[serde(default)]
    pub response: Option<Content>,
    #[serde(default)]
    pub outputs: Vec<NamedOutput>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactRefWithDigest>,
    #[serde(default)]
    pub unresolved_issues: Vec<String>,
    #[serde(default)]
    pub usage: Option<UsageReport>,
    pub provenance: Provenance,
}

impl PartialDelivery {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.partial_delivery_id.is_empty()
            || self.run_id.is_empty()
            || !self.spec_digest.is_sha256()
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "partial delivery identity and spec digest must be valid",
            ));
        }
        if let Some(response) = &self.response {
            response.validate_integrity()?;
        }
        validate_named_outputs(&self.outputs)?;
        validate_artifacts(&self.artifacts)?;
        if self
            .unresolved_issues
            .iter()
            .any(|issue| issue.trim().is_empty())
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "partial delivery unresolved issues must not contain empty entries",
            ));
        }
        if let Some(usage) = &self.usage {
            usage.validate_integrity()?;
        }
        self.provenance.validate_integrity()
    }
}

fn validate_named_outputs(outputs: &[NamedOutput]) -> Result<(), AgentProtocolError> {
    let mut names = BTreeSet::new();
    for output in outputs {
        output.validate_integrity()?;
        if !names.insert(output.name.as_str()) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "named output names must be unique",
            ));
        }
    }
    Ok(())
}

fn validate_artifacts(artifacts: &[ArtifactRefWithDigest]) -> Result<(), AgentProtocolError> {
    let mut references = BTreeSet::new();
    for artifact in artifacts {
        artifact.validate_integrity()?;
        if !references.insert(artifact.artifact_ref.as_str()) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "delivery artifact references must be unique",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

impl AgentFailure {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.code.trim().is_empty() || self.message.trim().is_empty() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Agent failure requires a code and message",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum IncompleteReason {
    LimitReached { limit: RunLimitKind },
    Interrupted { reason: String },
    ProviderEnded { reason: String },
}

impl IncompleteReason {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        match self {
            Self::LimitReached { .. } => Ok(()),
            Self::Interrupted { reason } | Self::ProviderEnded { reason }
                if !reason.trim().is_empty() =>
            {
                Ok(())
            }
            Self::Interrupted { .. } | Self::ProviderEnded { .. } => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "incomplete reason must not be empty",
            )),
        }
    }
}

/// Durable, normalized state event payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentEvent {
    RunAccepted {
        session_id: AgentSessionId,
        spec_digest: Digest,
    },
    ResourceBindingSkipped {
        skip: ResourceBindingSkip,
    },
    RunStarted,
    CommandReceived {
        command: AgentCommandEnvelope,
    },
    CommandDispositionRecorded {
        command_id: CommandId,
        outcome: ProviderCommandOutcome,
    },
    InputCommitted {
        content: Vec<Content>,
    },
    OutputCommitted {
        output_id: OutputId,
        content: Vec<Content>,
    },
    RequestOpened {
        request: PendingRequest,
    },
    RequestResolved {
        request_id: RequestId,
        resolution: RequestResolution,
        resolution_digest: Digest,
    },
    /// The Provider authoritatively reports that a request is no longer
    /// pending, but cannot attest which competing client supplied a response
    /// (or whether native lifecycle cleanup cleared it).
    RequestClosed {
        request_id: RequestId,
        reason: String,
    },
    StopRequested {
        reason: String,
    },
    DeliveryCommitted {
        delivery: AgentDelivery,
    },
    RunIncomplete {
        reason: IncompleteReason,
        #[serde(default)]
        partial_delivery: Option<PartialDelivery>,
    },
    RunFailed {
        failure: AgentFailure,
    },
    RunCancelled {
        reason: String,
    },
    ContinuityLost {
        last_confirmed_seq: u64,
        reason: String,
    },
    ContinuityRestored {
        proof: ReconciliationProof,
        reason: String,
    },
}

/// Provider-neutral evidence rule for one committed event during Run recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryReplayPolicy {
    /// The recovered Provider stream must replay the exact event identity and
    /// digest before the Host restores continuity.
    ProviderEvidenceRequired,
    /// The event is a completed Host-controlled fact already authenticated by
    /// the durable Host journal; its digest remains bound into recovery proof.
    HostJournalSufficient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ReconciliationProof {
    pub proof_ref: ReconciliationProofRef,
    pub last_confirmed_seq: u64,
    pub loss_event_digest: Digest,
    pub authoritative_snapshot_digest: Digest,
    pub evidence_digest: Digest,
}

#[derive(Serialize)]
struct ReconciliationProofDigestView<'a> {
    proof_ref: &'a ReconciliationProofRef,
    last_confirmed_seq: u64,
    loss_event_digest: &'a Digest,
    authoritative_snapshot_digest: &'a Digest,
}

impl ReconciliationProof {
    pub fn new(
        proof_ref: ReconciliationProofRef,
        last_confirmed_seq: u64,
        loss_event_digest: Digest,
        authoritative_snapshot_digest: Digest,
    ) -> Result<Self, AgentProtocolError> {
        let mut proof = Self {
            proof_ref,
            last_confirmed_seq,
            loss_event_digest,
            authoritative_snapshot_digest,
            evidence_digest: Digest::sha256([]),
        };
        proof.evidence_digest = proof.computed_evidence_digest()?;
        Ok(proof)
    }

    pub fn computed_evidence_digest(&self) -> Result<Digest, AgentProtocolError> {
        canonical_json_digest(&ReconciliationProofDigestView {
            proof_ref: &self.proof_ref,
            last_confirmed_seq: self.last_confirmed_seq,
            loss_event_digest: &self.loss_event_digest,
            authoritative_snapshot_digest: &self.authoritative_snapshot_digest,
        })
    }

    pub fn verify_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.proof_ref.is_empty()
            || !self.loss_event_digest.is_sha256()
            || !self.authoritative_snapshot_digest.is_sha256()
            || self.computed_evidence_digest()? != self.evidence_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "reconciliation proof is malformed or has an invalid evidence digest",
            ));
        }
        Ok(())
    }
}

impl AgentEvent {
    pub const fn is_host_event(&self) -> bool {
        matches!(
            self,
            Self::RunAccepted { .. }
                | Self::ResourceBindingSkipped { .. }
                | Self::CommandReceived { .. }
                | Self::ContinuityLost { .. }
                | Self::ContinuityRestored { .. }
        )
    }

    /// Whether recovery of the current Run state requires this Provider event
    /// to be replayed from authoritative Provider evidence.
    ///
    /// Command acknowledgements and completed control lifecycles are already
    /// authenticated by the Host journal. Requiring an external Provider to
    /// recreate their former notification stream after restart would conflate
    /// event history with current native state. An unresolved request remains
    /// strict because the recovered Provider must prove it can still service
    /// that request. All native execution and output facts require replay.
    pub fn recovery_replay_policy(
        &self,
        pending_request_ids: &BTreeSet<RequestId>,
    ) -> RecoveryReplayPolicy {
        match self {
            Self::CommandDispositionRecorded { .. } | Self::StopRequested { .. } => {
                RecoveryReplayPolicy::HostJournalSufficient
            }
            Self::RequestOpened { request } => {
                if pending_request_ids.contains(&request.request_id) {
                    RecoveryReplayPolicy::ProviderEvidenceRequired
                } else {
                    RecoveryReplayPolicy::HostJournalSufficient
                }
            }
            Self::RequestResolved { .. } | Self::RequestClosed { .. } => {
                RecoveryReplayPolicy::HostJournalSufficient
            }
            _ => RecoveryReplayPolicy::ProviderEvidenceRequired,
        }
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if let Self::CommandReceived { command } = self {
            command.verify_digest()?;
        }
        if let Self::CommandDispositionRecorded {
            command_id,
            outcome,
        } = self
        {
            if command_id.is_empty() {
                return Err(invalid_command(
                    "command disposition command_id must not be empty",
                ));
            }
            outcome.validate_shape()?;
        }
        if let Self::RequestResolved {
            resolution,
            resolution_digest,
            ..
        } = self
        {
            resolution.validate_shape()?;
            if resolution.digest()? != *resolution_digest {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "request resolution digest does not match resolution payload",
                ));
            }
        }
        if let Self::ContinuityRestored { proof, .. } = self {
            proof.verify_integrity()?;
        }

        match self {
            Self::RunAccepted {
                session_id,
                spec_digest,
            } if session_id.is_empty() || !spec_digest.is_sha256() => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "RunAccepted requires a session_id and SHA-256 spec digest",
                ));
            }
            Self::ResourceBindingSkipped { skip } => skip.validate_integrity()?,
            Self::InputCommitted { content } => validate_content_list(content, "committed input")?,
            Self::OutputCommitted { output_id, content } => {
                if output_id.is_empty() {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidSpec,
                        "committed output_id must not be empty",
                    ));
                }
                validate_content_list(content, "committed output")?;
            }
            Self::RequestOpened { request } => request.validate_integrity()?,
            Self::RequestResolved { request_id, .. } if request_id.is_empty() => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "resolved request_id must not be empty",
                ));
            }
            Self::RequestClosed { request_id, reason }
                if request_id.is_empty() || reason.trim().is_empty() =>
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "closed request requires a request_id and reason",
                ));
            }
            Self::StopRequested { reason } | Self::RunCancelled { reason }
                if reason.trim().is_empty() =>
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "stop and cancellation reasons must not be empty",
                ));
            }
            Self::DeliveryCommitted { delivery } => delivery.validate_integrity()?,
            Self::RunIncomplete {
                reason,
                partial_delivery,
            } => {
                reason.validate_integrity()?;
                if let Some(partial) = partial_delivery {
                    partial.validate_integrity()?;
                }
            }
            Self::RunFailed { failure } => failure.validate_integrity()?,
            Self::ContinuityLost { reason, .. } | Self::ContinuityRestored { reason, .. }
                if reason.trim().is_empty() =>
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "continuity transition reason must not be empty",
                ));
            }
            Self::RunAccepted { .. }
            | Self::RunStarted
            | Self::CommandReceived { .. }
            | Self::CommandDispositionRecorded { .. }
            | Self::RequestResolved { .. }
            | Self::RequestClosed { .. }
            | Self::StopRequested { .. }
            | Self::RunCancelled { .. }
            | Self::ContinuityLost { .. }
            | Self::ContinuityRestored { .. } => {}
        }
        Ok(())
    }
}

fn validate_content_list(content: &[Content], label: &str) -> Result<(), AgentProtocolError> {
    if content.is_empty() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            format!("{label} must not be empty"),
        ));
    }
    for item in content {
        item.validate_integrity()?;
    }
    Ok(())
}

/// Unsequenced durable fact submitted to the host journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentEventDraft {
    pub event_id: AgentEventId,
    pub run_id: RunId,
    #[serde(default)]
    pub causation_id: Option<CommandId>,
    #[serde(default)]
    pub source_fingerprint: Option<Digest>,
    pub payload: AgentEvent,
}

#[derive(Serialize)]
struct AgentEventDraftDigestView<'a> {
    event_id: &'a AgentEventId,
    run_id: &'a RunId,
    causation_id: &'a Option<CommandId>,
    source_fingerprint: &'a Option<Digest>,
    payload: &'a AgentEvent,
}

impl AgentEventDraft {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.event_id.is_empty()
            || self.run_id.is_empty()
            || self.causation_id.as_ref().is_some_and(CommandId::is_empty)
            || self
                .source_fingerprint
                .as_ref()
                .is_some_and(|digest| !digest.is_sha256())
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "event draft identity, causation, and source fingerprint must be valid",
            ));
        }
        self.payload.validate_integrity()
    }

    pub fn computed_digest(&self) -> Result<Digest, AgentProtocolError> {
        self.validate_integrity()?;
        canonical_json_digest(&AgentEventDraftDigestView {
            event_id: &self.event_id,
            run_id: &self.run_id,
            causation_id: &self.causation_id,
            source_fingerprint: &self.source_fingerprint,
            payload: &self.payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentEventEnvelope {
    pub event_id: AgentEventId,
    pub run_id: RunId,
    pub run_seq: u64,
    #[serde(default)]
    pub causation_id: Option<CommandId>,
    #[serde(default)]
    pub source_fingerprint: Option<Digest>,
    pub event_digest: Digest,
    pub payload: AgentEvent,
}

#[derive(Serialize)]
struct AgentEventDigestView<'a> {
    event_id: &'a AgentEventId,
    run_id: &'a RunId,
    run_seq: u64,
    causation_id: &'a Option<CommandId>,
    source_fingerprint: &'a Option<Digest>,
    payload: &'a AgentEvent,
}

impl AgentEventEnvelope {
    pub(crate) fn seal(draft: AgentEventDraft, run_seq: u64) -> Result<Self, AgentProtocolError> {
        draft.validate_integrity()?;
        let mut event = Self {
            event_id: draft.event_id,
            run_id: draft.run_id,
            run_seq,
            causation_id: draft.causation_id,
            source_fingerprint: draft.source_fingerprint,
            event_digest: Digest::sha256([]),
            payload: draft.payload,
        };
        event.event_digest = event.computed_digest()?;
        Ok(event)
    }

    pub fn computed_digest(&self) -> Result<Digest, AgentProtocolError> {
        self.validate_semantic_fields()?;
        canonical_json_digest(&AgentEventDigestView {
            event_id: &self.event_id,
            run_id: &self.run_id,
            run_seq: self.run_seq,
            causation_id: &self.causation_id,
            source_fingerprint: &self.source_fingerprint,
            payload: &self.payload,
        })
    }

    pub fn computed_draft_digest(&self) -> Result<Digest, AgentProtocolError> {
        self.validate_semantic_fields()?;
        canonical_json_digest(&AgentEventDraftDigestView {
            event_id: &self.event_id,
            run_id: &self.run_id,
            causation_id: &self.causation_id,
            source_fingerprint: &self.source_fingerprint,
            payload: &self.payload,
        })
    }

    pub fn verify_event_digest(&self) -> Result<(), AgentProtocolError> {
        if self.computed_digest()? != self.event_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "Agent event digest does not match its canonical semantic fields",
            ));
        }
        Ok(())
    }

    fn validate_semantic_fields(&self) -> Result<(), AgentProtocolError> {
        if self.event_id.is_empty()
            || self.run_id.is_empty()
            || self.run_seq == 0
            || self.causation_id.as_ref().is_some_and(CommandId::is_empty)
            || self
                .source_fingerprint
                .as_ref()
                .is_some_and(|digest| !digest.is_sha256())
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "event identity, sequence, causation, and source fingerprint must be valid",
            ));
        }
        self.payload.validate_integrity()
    }
}

/// Authenticated origin stored beside a normalized journal event. Authority is
/// Host journal metadata, not a field supplied by an Agent Provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "authority", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentEventAuthority {
    Provider,
    Host {
        #[serde(default)]
        reconciliation_proof_ref: Option<ReconciliationProofRef>,
    },
}

/// Durable input to replay. `run_seq`, the event digest, and authority must be
/// committed atomically by the Host journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentJournalRecord {
    pub authority: AgentEventAuthority,
    pub draft_digest: Digest,
    pub event: AgentEventEnvelope,
}

impl AgentJournalRecord {
    pub(crate) fn seal_provider(
        draft: AgentEventDraft,
        run_seq: u64,
    ) -> Result<Self, AgentProtocolError> {
        let draft_digest = draft.computed_digest()?;
        Ok(Self {
            authority: AgentEventAuthority::Provider,
            draft_digest,
            event: AgentEventEnvelope::seal(draft, run_seq)?,
        })
    }

    pub(crate) fn seal_host(
        draft: AgentEventDraft,
        run_seq: u64,
        reconciliation_proof_ref: Option<ReconciliationProofRef>,
    ) -> Result<Self, AgentProtocolError> {
        let draft_digest = draft.computed_digest()?;
        Ok(Self {
            authority: AgentEventAuthority::Host {
                reconciliation_proof_ref,
            },
            draft_digest,
            event: AgentEventEnvelope::seal(draft, run_seq)?,
        })
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        self.event.verify_event_digest()?;
        if self.event.computed_draft_digest()? != self.draft_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "journal record draft digest does not match its semantic fields",
            ));
        }
        match (&self.authority, &self.event.payload) {
            (
                AgentEventAuthority::Host {
                    reconciliation_proof_ref: Some(authority_ref),
                },
                AgentEvent::ContinuityRestored { proof, .. },
            ) if *authority_ref == proof.proof_ref => Ok(()),
            (AgentEventAuthority::Provider, AgentEvent::ContinuityRestored { .. }) => {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Provider authority cannot restore Host journal continuity",
                ))
            }
            (
                AgentEventAuthority::Host {
                    reconciliation_proof_ref: None,
                },
                AgentEvent::ContinuityRestored { .. },
            ) => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "ContinuityRestored requires authenticated Host proof metadata",
            )),
            (
                AgentEventAuthority::Host {
                    reconciliation_proof_ref: Some(_),
                },
                _,
            ) => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "reconciliation proof metadata is valid only for ContinuityRestored",
            )),
            (AgentEventAuthority::Host { .. }, payload) if !payload.is_host_event() => {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "event kind is not permitted with Host authority",
                ))
            }
            (AgentEventAuthority::Provider, payload) if payload.is_host_event() => {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "event kind is not permitted with Provider authority",
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Best-effort telemetry never drives [`AgentRunState`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentTelemetryEnvelope {
    pub telemetry_id: TelemetryId,
    pub run_id: RunId,
    #[serde(default)]
    pub provider_seq: Option<u64>,
    pub payload: AgentTelemetry,
}

impl AgentTelemetryEnvelope {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.telemetry_id.is_empty() || self.run_id.is_empty() || self.provider_seq == Some(0) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "telemetry identity and optional provider sequence must be valid",
            ));
        }
        self.payload.validate_integrity()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ToolActivityState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ToolFileActivityKind {
    Read,
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ToolDiffLineKind {
    Context,
    Addition,
    Deletion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ToolDiffLine {
    pub kind: ToolDiffLineKind,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ToolActivityErrorDetail {
    pub label: String,
    pub value: String,
}

impl ToolActivityErrorDetail {
    fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if !is_safe_tool_activity_text(&self.label, 64, false)
            || !is_safe_tool_activity_text(&self.value, 512, false)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Tool error detail must contain bounded presentation-safe text",
            ));
        }
        Ok(())
    }
}

impl ToolDiffLine {
    fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if !is_safe_tool_activity_text(&self.text, 512, true) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Tool diff evidence must contain bounded presentation-safe text",
            ));
        }
        Ok(())
    }
}

/// Bounded, Host-selected evidence about one Tool operation.
///
/// This is deliberately semantic rather than a copy of arbitrary Tool JSON:
/// adapters describe their own operations, while Agent clients only aggregate
/// and render the stable evidence vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ToolActivityEvidence {
    Command {
        command: String,
    },
    File {
        operation: ToolFileActivityKind,
        path: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        diff: Vec<ToolDiffLine>,
        #[serde(default, skip_serializing_if = "is_zero_u32")]
        diff_omitted: u32,
    },
    Note {
        text: String,
    },
    Error {
        code: String,
        message: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        details: Vec<ToolActivityErrorDetail>,
    },
    Omitted {
        count: u32,
    },
}

impl ToolActivityEvidence {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        let valid = match self {
            Self::Command { command } => is_safe_tool_activity_text(command, 512, false),
            Self::File { path, diff, .. } => {
                is_safe_tool_activity_text(path, 512, false)
                    && diff.len() <= 24
                    && diff.iter().all(|line| line.validate_integrity().is_ok())
            }
            Self::Note { text } => is_safe_tool_activity_text(text, 512, false),
            Self::Error {
                code,
                message,
                details,
            } => {
                is_safe_tool_activity_text(code, 128, false)
                    && is_safe_tool_activity_text(message, 512, false)
                    && details.len() <= 8
                    && details
                        .iter()
                        .all(|detail| detail.validate_integrity().is_ok())
            }
            Self::Omitted { count } => *count > 0,
        };
        if !valid {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Tool activity evidence must be bounded and presentation-safe",
            ));
        }
        Ok(())
    }
}

fn is_safe_tool_activity_text(value: &str, max_chars: usize, allow_empty: bool) -> bool {
    (allow_empty || !value.trim().is_empty())
        && value.chars().count() <= max_chars
        && !value.chars().any(char::is_control)
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentTelemetry {
    OutputDelta {
        output_id: OutputId,
        delta: Content,
    },
    ProgressReported {
        message: String,
        #[serde(default)]
        fraction: Option<f64>,
    },
    ToolActivity {
        activity_id: ToolActivityId,
        tool_name: String,
        state: ToolActivityState,
        /// Bounded semantic evidence selected by the Host. Raw Tool arguments
        /// and results must never be copied here wholesale.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        evidence: Vec<ToolActivityEvidence>,
    },
    Extension {
        #[cfg_attr(
            feature = "agent-protocol-schema",
            schemars(regex(pattern = r"^[^/][^/]*/[\s\S]+$"))
        )]
        namespace: String,
        value: Value,
    },
}

impl AgentTelemetry {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        match self {
            Self::OutputDelta { output_id, delta } => {
                if output_id.is_empty() {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidSpec,
                        "telemetry output_id must not be empty",
                    ));
                }
                delta.validate_integrity()
            }
            Self::ProgressReported { message, fraction } => {
                if message.trim().is_empty()
                    || fraction
                        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidSpec,
                        "progress telemetry requires a message and fraction within [0, 1]",
                    ));
                }
                Ok(())
            }
            Self::ToolActivity {
                activity_id,
                tool_name,
                evidence,
                ..
            } => {
                if activity_id.is_empty()
                    || tool_name.trim().is_empty()
                    || evidence.len() > 16
                    || evidence
                        .iter()
                        .any(|item| item.validate_integrity().is_err())
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidSpec,
                        "Tool activity telemetry requires an activity_id, tool_name, and at most sixteen valid evidence items",
                    ));
                }
                Ok(())
            }
            Self::Extension { namespace, .. } if !is_namespaced_key(namespace) => {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "telemetry extension namespace must be namespaced",
                ))
            }
            Self::Extension { .. } => Ok(()),
        }
    }
}

/// Provider observation before Host journal sequencing. The Provider supplies a
/// stable event ID; only the Host journal may assign normalized `run_seq`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "channel",
    content = "item",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[non_exhaustive]
pub enum AgentProviderStreamItem {
    Event(Box<AgentEventDraft>),
    Telemetry(AgentTelemetryEnvelope),
}

impl AgentProviderStreamItem {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        match self {
            Self::Event(event) => event.validate_integrity(),
            Self::Telemetry(telemetry) => telemetry.validate_integrity(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentTerminalState {
    Delivered { delivery_id: DeliveryId },
    Incomplete { reason: IncompleteReason },
    Cancelled { reason: String },
    Failed { failure: AgentFailure },
}

/// Reducer phase. Waiting and Unknown are projections, not competing phases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentRunPhase {
    Accepted,
    Running,
    Stopping,
    Terminal { terminal: AgentTerminalState },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "continuity", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentContinuityState {
    Confirmed {
        through_seq: u64,
    },
    Unknown {
        last_confirmed_seq: u64,
        loss_event_digest: Digest,
        reason: String,
    },
}

/// Public state derived from phase, continuity authority, and pending requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentRunState {
    Accepted,
    Running,
    Waiting {
        pending_request_ids: Vec<RequestId>,
    },
    Stopping,
    Unknown {
        last_confirmed_seq: u64,
        reason: String,
    },
    Terminal {
        terminal: AgentTerminalState,
    },
}

impl AgentRunState {
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal { .. })
    }

    pub const fn status(&self) -> AgentRunStatus {
        match self {
            Self::Accepted => AgentRunStatus::Accepted,
            Self::Running => AgentRunStatus::Running,
            Self::Waiting { .. } => AgentRunStatus::Waiting,
            Self::Stopping => AgentRunStatus::Stopping,
            Self::Unknown { .. } => AgentRunStatus::Unknown,
            Self::Terminal { terminal } => match terminal {
                AgentTerminalState::Delivered { .. } => AgentRunStatus::Delivered,
                AgentTerminalState::Incomplete { .. } => AgentRunStatus::Incomplete,
                AgentTerminalState::Cancelled { .. } => AgentRunStatus::Cancelled,
                AgentTerminalState::Failed { .. } => AgentRunStatus::Failed,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRunStatus {
    Accepted,
    Running,
    Waiting,
    Stopping,
    Unknown,
    Delivered,
    Incomplete,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentExecutionRef {
    pub provider_id: AgentProviderId,
    pub agent_id: AgentId,
    pub binding_ref: ProviderBindingRef,
    pub descriptor_digest: Digest,
    pub session_id: AgentSessionId,
    pub run_id: RunId,
    pub spec_digest: Digest,
}

impl AgentExecutionRef {
    /// Constructs the only public execution identity valid for a selected
    /// start request. Provider-native handles remain adapter-private.
    pub fn for_start(
        request: &AgentStartRequest,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<Self, AgentProtocolError> {
        request.validate_for_descriptor(descriptor)?;
        Ok(Self {
            provider_id: descriptor.descriptor.provider_id.clone(),
            agent_id: descriptor.descriptor.agent_id.clone(),
            binding_ref: request.provider_binding.clone(),
            descriptor_digest: descriptor.descriptor_digest.clone(),
            session_id: request.run.spec.session_id.clone(),
            run_id: request.run.spec.run_id.clone(),
            spec_digest: request.run.spec_digest.clone(),
        })
    }

    pub fn validate_for(
        &self,
        request: &AgentStartRequest,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<(), AgentProtocolError> {
        request.validate_for_descriptor(descriptor)?;
        self.validate_integrity()?;
        if self.provider_id != descriptor.descriptor.provider_id
            || self.agent_id != descriptor.descriptor.agent_id
            || self.binding_ref != request.provider_binding
            || self.descriptor_digest != request.expected_descriptor_digest
            || self.session_id != request.run.spec.session_id
            || self.run_id != request.run.spec.run_id
            || self.spec_digest != request.run.spec_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "execution does not match the selected run/provider descriptor contract",
            ));
        }
        Ok(())
    }

    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        if self.provider_id.is_empty()
            || self.agent_id.is_empty()
            || self.binding_ref.is_empty()
            || self.session_id.is_empty()
            || self.run_id.is_empty()
            || !self.descriptor_digest.is_sha256()
            || !self.spec_digest.is_sha256()
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "execution identity and immutable descriptor/spec digests must be valid",
            ));
        }
        Ok(())
    }
}

/// Stable, bounded Host read model for `inspect(run)`. Unbounded committed
/// output history is read separately through journal pagination bounded by
/// `last_run_seq`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "agent-protocol-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentRunView {
    pub execution: AgentExecutionRef,
    pub admission: AgentAdmission,
    pub state: AgentRunState,
    #[serde(default)]
    pub last_run_seq: Option<u64>,
    #[serde(default)]
    pub pending_requests: Vec<PendingRequest>,
    #[serde(default)]
    pub delivery: Option<AgentDelivery>,
    #[serde(default)]
    pub partial_delivery: Option<PartialDelivery>,
}

impl AgentRunView {
    pub fn validate_integrity(&self) -> Result<(), AgentProtocolError> {
        self.execution.validate_integrity()?;
        self.admission.validate_integrity()?;
        if self.last_run_seq == Some(0) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Run view cursor must be absent or a positive run_seq",
            ));
        }
        for request in &self.pending_requests {
            request.validate_integrity()?;
        }
        if self
            .pending_requests
            .windows(2)
            .any(|window| window[0].request_id >= window[1].request_id)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Run view pending requests must be unique and sorted by request_id",
            ));
        }

        if let Some(delivery) = &self.delivery {
            delivery.validate_integrity()?;
            validate_run_output_identity(
                &self.execution,
                &delivery.run_id,
                &delivery.spec_digest,
                &delivery.provenance,
            )?;
        }
        if let Some(partial) = &self.partial_delivery {
            partial.validate_integrity()?;
            validate_run_output_identity(
                &self.execution,
                &partial.run_id,
                &partial.spec_digest,
                &partial.provenance,
            )?;
        }
        if !self.state.is_terminal() && (self.delivery.is_some() || self.partial_delivery.is_some())
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "non-terminal Run view contains terminal delivery data",
            ));
        }
        if self.state.is_terminal() && self.last_run_seq.is_none() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "terminal Run view requires a durable journal cursor",
            ));
        }

        let blocking_ids = self
            .pending_requests
            .iter()
            .filter(|request| request.blocking)
            .map(|request| request.request_id.clone())
            .collect::<Vec<_>>();
        match &self.state {
            AgentRunState::Waiting {
                pending_request_ids,
            } if !pending_request_ids.is_empty() && *pending_request_ids == blocking_ids => {}
            AgentRunState::Waiting { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Waiting IDs must equal the blocking pending request subset",
                ));
            }
            AgentRunState::Running if blocking_ids.is_empty() => {}
            AgentRunState::Running => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Running with blocking pending requests must project as Waiting",
                ));
            }
            AgentRunState::Terminal { terminal } => {
                if !self.pending_requests.is_empty() {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidTransition,
                        "terminal Run views cannot expose actionable pending requests",
                    ));
                }
                match terminal {
                    AgentTerminalState::Delivered { delivery_id }
                        if self
                            .delivery
                            .as_ref()
                            .is_some_and(|delivery| delivery.delivery_id == *delivery_id)
                            && self.partial_delivery.is_none() => {}
                    AgentTerminalState::Incomplete { .. } if self.delivery.is_none() => {}
                    AgentTerminalState::Cancelled { .. } | AgentTerminalState::Failed { .. }
                        if self.delivery.is_none() && self.partial_delivery.is_none() => {}
                    _ => {
                        return Err(AgentProtocolError::new(
                            AgentProtocolErrorCode::InvalidTransition,
                            "terminal state and complete/partial delivery fields disagree",
                        ));
                    }
                }
            }
            AgentRunState::Accepted | AgentRunState::Stopping | AgentRunState::Unknown { .. }
                if self.delivery.is_none() && self.partial_delivery.is_none() => {}
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "non-terminal Run view contains terminal delivery data",
                ));
            }
        }
        Ok(())
    }

    pub fn projection_digest(&self) -> Result<Digest, AgentProtocolError> {
        self.validate_integrity()?;
        canonical_json_digest(self)
    }
}

fn validate_run_output_identity(
    execution: &AgentExecutionRef,
    run_id: &RunId,
    spec_digest: &Digest,
    provenance: &Provenance,
) -> Result<(), AgentProtocolError> {
    if run_id != &execution.run_id
        || spec_digest != &execution.spec_digest
        || provenance.provider_id != execution.provider_id
        || provenance.agent_id != execution.agent_id
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Run output is not bound to the inspected execution",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandRecord {
    pub command: AgentCommandEnvelope,
    pub received_seq: u64,
    #[serde(default)]
    pub disposition: Option<ProviderCommandOutcome>,
    #[serde(default)]
    pub disposition_seq: Option<u64>,
    #[serde(default)]
    pub applied_seq: Option<u64>,
}

impl CommandRecord {
    pub(crate) fn to_ack(&self, duplicate: bool) -> Result<CommandAck, AgentProtocolError> {
        let disposition_seq = self.disposition_seq.ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "command has no durable Provider disposition",
            )
        })?;
        let state = match (&self.disposition, self.applied_seq) {
            (Some(ProviderCommandOutcome::Accepted), Some(applied_seq)) => {
                CommandAckState::Applied {
                    recorded_seq: disposition_seq,
                    applied_seq,
                }
            }
            (Some(ProviderCommandOutcome::Accepted), None) => CommandAckState::Accepted {
                recorded_seq: disposition_seq,
            },
            (Some(ProviderCommandOutcome::Rejected { code, message }), None) => {
                CommandAckState::Rejected {
                    recorded_seq: disposition_seq,
                    code: code.clone(),
                    message: message.clone(),
                }
            }
            (Some(ProviderCommandOutcome::Unsupported { feature }), None) => {
                CommandAckState::Unsupported {
                    recorded_seq: disposition_seq,
                    feature: feature.clone(),
                }
            }
            (Some(ProviderCommandOutcome::Rejected { .. }), Some(_))
            | (Some(ProviderCommandOutcome::Unsupported { .. }), Some(_))
            | (None, _) => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "command ledger contains an impossible disposition/effect combination",
                ));
            }
        };
        Ok(CommandAck {
            command_id: self.command.command_id.clone(),
            run_id: self.command.run_id.clone(),
            duplicate,
            state,
        })
    }
}

/// Durable projection returned by `inspect` and reconstructed by replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentExecutionSnapshot {
    pub execution: AgentExecutionRef,
    pub admission: AgentAdmission,
    #[serde(default)]
    pub output_schema: Option<SchemaRef>,
    pub run_accepted: bool,
    pub phase: AgentRunPhase,
    pub continuity: AgentContinuityState,
    #[serde(default)]
    pub last_run_seq: Option<u64>,
    #[serde(default)]
    pub last_event_id: Option<AgentEventId>,
    #[serde(default)]
    pub last_event_digest: Option<Digest>,
    #[serde(default)]
    pub pending_requests: BTreeMap<RequestId, PendingRequest>,
    #[serde(default)]
    pub commands: BTreeMap<CommandId, CommandRecord>,
    #[serde(default)]
    pub committed_inputs: BTreeMap<CommandId, Vec<Content>>,
    #[serde(default)]
    pub resolved_requests: BTreeMap<RequestId, Digest>,
    #[serde(default)]
    pub closed_requests: BTreeSet<RequestId>,
    #[serde(default)]
    pub skipped_optional_bindings: BTreeMap<ResourceBindingId, ResourceBindingSkip>,
    #[serde(default)]
    pub resource_requirements: BTreeMap<ResourceBindingId, BindingRequirement>,
    #[serde(default)]
    pub committed_outputs: BTreeMap<OutputId, Vec<Content>>,
    #[serde(default)]
    pub delivery: Option<AgentDelivery>,
    #[serde(default)]
    pub partial_delivery: Option<PartialDelivery>,
}

impl AgentExecutionSnapshot {
    pub(crate) fn accepted(
        execution: AgentExecutionRef,
        run: &AgentRunEnvelope,
        admission: AgentAdmission,
    ) -> Self {
        let resource_requirements = run
            .spec
            .resources
            .iter()
            .map(|binding| (binding.binding_id.clone(), binding.requirement))
            .collect();
        Self {
            execution,
            admission,
            output_schema: run.spec.output_schema.clone(),
            run_accepted: false,
            phase: AgentRunPhase::Accepted,
            continuity: AgentContinuityState::Confirmed { through_seq: 0 },
            last_run_seq: None,
            last_event_id: None,
            last_event_digest: None,
            pending_requests: BTreeMap::new(),
            commands: BTreeMap::new(),
            committed_inputs: BTreeMap::new(),
            resolved_requests: BTreeMap::new(),
            closed_requests: BTreeSet::new(),
            skipped_optional_bindings: BTreeMap::new(),
            resource_requirements,
            committed_outputs: BTreeMap::new(),
            delivery: None,
            partial_delivery: None,
        }
    }

    pub(crate) fn state(&self) -> AgentRunState {
        if let AgentRunPhase::Terminal { terminal } = &self.phase {
            return AgentRunState::Terminal {
                terminal: terminal.clone(),
            };
        }
        if let AgentContinuityState::Unknown {
            last_confirmed_seq,
            reason,
            ..
        } = &self.continuity
        {
            return AgentRunState::Unknown {
                last_confirmed_seq: *last_confirmed_seq,
                reason: reason.clone(),
            };
        }
        match self.phase {
            AgentRunPhase::Accepted => AgentRunState::Accepted,
            AgentRunPhase::Running => {
                let pending_request_ids = self
                    .pending_requests
                    .values()
                    .filter(|request| request.blocking)
                    .map(|request| request.request_id.clone())
                    .collect::<Vec<_>>();
                if pending_request_ids.is_empty() {
                    AgentRunState::Running
                } else {
                    AgentRunState::Waiting {
                        pending_request_ids,
                    }
                }
            }
            AgentRunPhase::Stopping => AgentRunState::Stopping,
            AgentRunPhase::Terminal { .. } => unreachable!("terminal returned above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_run() -> AgentRunEnvelope {
        AgentRunEnvelope::new(
            super::super::AGENT_PROTOCOL_V1,
            AgentSessionId::new("session-1"),
            RunId::new("run-1"),
            vec![Content::text("hello")],
        )
        .expect("sample run should seal")
    }

    fn binding(
        id: &str,
        kind: &str,
        revision: &str,
        requirement: BindingRequirement,
        mode: ResourceBindingMode,
    ) -> ResourceBinding {
        ResourceBinding {
            binding_id: ResourceBindingId::new(id),
            resource: ResourceRef {
                kind: ResourceKind::new(kind),
                id: ResourceId::new(format!("resource-{id}")),
                revision: ResourceRevision::new(revision),
            },
            requirement,
            mode,
        }
    }

    fn descriptor() -> AgentDescriptor {
        AgentDescriptor {
            provider_id: AgentProviderId::new("provider-1"),
            agent_id: AgentId::new("agent-1"),
            supported_protocol_versions: vec![super::super::AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: false,
                structured_output: false,
                controls: ControlCapabilities::default(),
                pending_request_kinds: BTreeSet::new(),
                supported_limits: BTreeSet::new(),
                resources: vec![ResourceCapability {
                    kind: ResourceKind::new("tool-catalog/v1"),
                    modes: BTreeSet::from([ResourceBindingMode::Snapshot]),
                    max_bindings: Some(1),
                }],
                effect_mediation: EffectMediation::HostMediated,
            },
            extensions: Extensions::new(),
        }
    }

    #[test]
    fn spec_digest_detects_mutation() {
        let mut run = sample_run();
        assert!(run.verify_digest().is_ok());
        run.spec.input.push(Content::text("tampered"));
        assert_eq!(
            run.verify_digest()
                .expect_err("mutation must be rejected")
                .code,
            AgentProtocolErrorCode::InvalidDigest
        );
    }

    #[test]
    fn event_digest_detects_mutation() {
        let mut event = AgentEventEnvelope::seal(
            AgentEventDraft {
                event_id: AgentEventId::new("event-1"),
                run_id: RunId::new("run-1"),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
            1,
        )
        .expect("event should seal");
        assert!(event.verify_event_digest().is_ok());
        event.payload = AgentEvent::StopRequested {
            reason: "tampered".to_owned(),
        };
        assert_eq!(
            event
                .verify_event_digest()
                .expect_err("mutation must be rejected")
                .code,
            AgentProtocolErrorCode::InvalidDigest
        );
    }

    #[test]
    fn newer_minor_provider_serves_older_v1_client() {
        assert!(ProtocolVersion::new(1, 0).is_compatible_with(ProtocolVersion::new(1, 3)));
        assert!(!ProtocolVersion::new(1, 4).is_compatible_with(ProtocolVersion::new(1, 3)));
        assert!(!ProtocolVersion::new(2, 0).is_compatible_with(ProtocolVersion::new(1, 3)));
    }

    #[test]
    fn resource_revision_is_bound_by_spec_digest() {
        let mut run = sample_run();
        run.spec.resources.push(binding(
            "tools",
            "tool-catalog/v1",
            "revision-1",
            BindingRequirement::Required,
            ResourceBindingMode::Snapshot,
        ));
        let sealed = AgentRunEnvelope::seal(run.spec.clone()).expect("resource run seals");
        run.spec.resources[0].resource.revision = ResourceRevision::new("revision-2");
        let changed = AgentRunEnvelope::compute_digest(&run.spec).expect("digest computes");
        assert_ne!(sealed.spec_digest, changed);
    }

    #[test]
    fn descriptor_compatibility_reserves_capacity_for_required_resources() {
        let mut run = sample_run();
        run.spec.resources = vec![
            binding(
                "optional-tools",
                "tool-catalog/v1",
                "revision-1",
                BindingRequirement::Optional,
                ResourceBindingMode::Snapshot,
            ),
            binding(
                "required-tools",
                "tool-catalog/v1",
                "revision-2",
                BindingRequirement::Required,
                ResourceBindingMode::Snapshot,
            ),
            binding(
                "optional-skills",
                "skill-catalog/v1",
                "revision-1",
                BindingRequirement::Optional,
                ResourceBindingMode::Snapshot,
            ),
        ];
        let run = AgentRunEnvelope::seal(run.spec).expect("resource run seals");
        let compatibility = descriptor()
            .check_run_compatibility(&run)
            .expect("required binding is supported");
        assert_eq!(compatibility.skipped_optional_bindings.len(), 2);
        assert_eq!(
            compatibility.skipped_optional_bindings[0].code,
            ResourceBindingSkipCode::CapacityExceeded
        );
        assert_eq!(
            compatibility.skipped_optional_bindings[1].code,
            ResourceBindingSkipCode::UnsupportedKind
        );
    }

    #[test]
    fn required_unsupported_resource_is_a_structured_rejection() {
        let mut run = sample_run();
        run.spec.resources.push(binding(
            "skills",
            "skill-catalog/v1",
            "revision-1",
            BindingRequirement::Required,
            ResourceBindingMode::Snapshot,
        ));
        let run = AgentRunEnvelope::seal(run.spec).expect("resource run seals");
        let rejection = descriptor()
            .check_run_compatibility(&run)
            .expect_err("required unsupported resource must reject start");
        assert_eq!(rejection.code, AgentRejectionCode::UnsupportedResource);
        assert_eq!(rejection.details["code"], "unsupported_kind");
        assert_eq!(rejection.details["binding_id"], "skills");
    }

    #[test]
    fn run_spec_has_no_effect_authority_or_provider_native_fields() {
        fn collect_keys(value: &Value, keys: &mut BTreeSet<String>) {
            match value {
                Value::Object(object) => {
                    for (key, value) in object {
                        keys.insert(key.clone());
                        collect_keys(value, keys);
                    }
                }
                Value::Array(values) => {
                    for value in values {
                        collect_keys(value, keys);
                    }
                }
                _ => {}
            }
        }

        let value = serde_json::to_value(&sample_run().spec).expect("spec serializes");
        let mut keys = BTreeSet::new();
        collect_keys(&value, &mut keys);
        for forbidden in [
            "authority_ref",
            "approval",
            "sandbox",
            "secret",
            "native_session",
            "mcp",
        ] {
            assert!(!keys.contains(forbidden), "forbidden field: {forbidden}");
        }
    }

    #[test]
    fn command_shape_prevents_request_and_approval_ambiguity() {
        let run_id = RunId::new("run-1");
        assert_eq!(
            AgentCommandEnvelope::new(
                CommandId::new("resolve-without-request"),
                run_id.clone(),
                None,
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Input {
                        content: vec![Content::text("answer")],
                    },
                },
            )
            .expect_err("ResolveRequest must correlate a request")
            .code,
            AgentProtocolErrorCode::InvalidSpec
        );
        assert!(AgentCommandEnvelope::new(
            CommandId::new("forged-approval"),
            run_id.clone(),
            Some(RequestId::new("approval-1")),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: None,
                },
            },
        )
        .is_err());
        assert!(AgentCommandEnvelope::new(
            CommandId::new("cancel-with-request"),
            run_id,
            Some(RequestId::new("request-1")),
            AgentCommand::Cancel {
                reason: "stop".to_owned(),
            },
        )
        .is_err());
    }

    #[test]
    fn command_extensions_are_digest_bound_and_empty_extensions_stay_wire_compatible() {
        let legacy_shape = AgentCommandEnvelope::new(
            CommandId::new("command-legacy"),
            RunId::new("run-1"),
            None,
            AgentCommand::Steer {
                content: vec![Content::text("continue")],
            },
        )
        .unwrap();
        let encoded = serde_json::to_value(&legacy_shape).unwrap();
        assert!(encoded.get("extensions").is_none());
        serde_json::from_value::<AgentCommandEnvelope>(encoded)
            .unwrap()
            .verify_digest()
            .unwrap();

        let mut extensions = Extensions::new();
        extensions.insert(
            "orchestral.dev/session-history-anchor".to_owned(),
            serde_json::json!({"after_activity_id": "activity-1"}),
        );
        let mut anchored = AgentCommandEnvelope::new_with_extensions(
            CommandId::new("command-anchored"),
            RunId::new("run-1"),
            None,
            AgentCommand::Steer {
                content: vec![Content::text("continue")],
            },
            extensions,
        )
        .unwrap();
        anchored.verify_digest().unwrap();
        anchored
            .extensions
            .get_mut("orchestral.dev/session-history-anchor")
            .unwrap()["after_activity_id"] = Value::String("activity-2".to_owned());
        assert_eq!(
            anchored.verify_digest().unwrap_err().code,
            AgentProtocolErrorCode::InvalidDigest
        );
    }

    #[test]
    fn descriptor_digest_binds_start_and_execution_contract() {
        let descriptor = AgentDescriptorEnvelope::seal(descriptor()).expect("descriptor seals");
        let run = sample_run();
        let request =
            AgentStartRequest::new(run, ProviderBindingRef::new("binding-1"), &descriptor)
                .expect("start request binds the selected descriptor");
        let execution =
            AgentExecutionRef::for_start(&request, &descriptor).expect("execution binds start");
        assert!(execution.validate_for(&request, &descriptor).is_ok());

        let mut mismatched = request;
        mismatched.expected_descriptor_digest = Digest::sha256("different-descriptor");
        assert_eq!(
            execution
                .validate_for(&mismatched, &descriptor)
                .expect_err("TOCTOU descriptor mismatch must reject")
                .code,
            AgentProtocolErrorCode::RunIdConflict
        );
    }

    #[test]
    fn strict_wire_types_reject_unknown_core_fields() {
        let mut version = serde_json::to_value(super::super::AGENT_PROTOCOL_V1)
            .expect("protocol version serializes");
        version["future_core_field"] = Value::Bool(true);
        assert!(serde_json::from_value::<ProtocolVersion>(version).is_err());

        let mut descriptor_value =
            serde_json::to_value(descriptor()).expect("descriptor serializes");
        descriptor_value["capabilities"]["controls"]["future_core_field"] = Value::Bool(true);
        assert!(serde_json::from_value::<AgentDescriptor>(descriptor_value).is_err());

        let descriptor = AgentDescriptorEnvelope::seal(descriptor()).expect("descriptor seals");
        let request = AgentStartRequest::new(
            sample_run(),
            ProviderBindingRef::new("binding-1"),
            &descriptor,
        )
        .expect("request binds descriptor");
        let execution =
            AgentExecutionRef::for_start(&request, &descriptor).expect("execution binds start");
        let mut execution_value = serde_json::to_value(execution).expect("execution serializes");
        execution_value["native_session"] = Value::String("must-not-leak".to_owned());
        assert!(serde_json::from_value::<AgentExecutionRef>(execution_value).is_err());

        let telemetry = AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new("telemetry-1"),
            run_id: request.run.spec.run_id,
            provider_seq: None,
            payload: AgentTelemetry::ProgressReported {
                message: "working".to_owned(),
                fraction: Some(0.5),
            },
        };
        let mut telemetry_value = serde_json::to_value(telemetry).expect("telemetry serializes");
        telemetry_value["future_core_field"] = Value::Bool(true);
        assert!(serde_json::from_value::<AgentTelemetryEnvelope>(telemetry_value).is_err());
    }

    #[test]
    fn tool_activity_telemetry_is_typed_and_validated() {
        let telemetry = AgentTelemetry::ToolActivity {
            activity_id: ToolActivityId::new("activity-1"),
            tool_name: "file_read".to_owned(),
            state: ToolActivityState::Succeeded,
            evidence: vec![ToolActivityEvidence::File {
                operation: ToolFileActivityKind::Read,
                path: "core/src/lib.rs".to_owned(),
                diff: Vec::new(),
                diff_omitted: 0,
            }],
        };
        telemetry
            .validate_integrity()
            .expect("typed Tool activity is valid");
        let value = serde_json::to_value(&telemetry).expect("Tool activity serializes");
        assert_eq!(value["type"], "tool_activity");
        assert_eq!(value["state"], "succeeded");
        assert_eq!(value["evidence"][0]["type"], "file");
        assert_eq!(value["evidence"][0]["path"], "core/src/lib.rs");
        assert!(serde_json::from_value::<AgentTelemetry>(value)
            .expect("Tool activity deserializes")
            .validate_integrity()
            .is_ok());
        assert!(AgentTelemetry::ToolActivity {
            activity_id: ToolActivityId::new(""),
            tool_name: "file_read".to_owned(),
            state: ToolActivityState::Running,
            evidence: Vec::new(),
        }
        .validate_integrity()
        .is_err());
        assert!(AgentTelemetry::ToolActivity {
            activity_id: ToolActivityId::new("activity-2"),
            tool_name: "exec_command".to_owned(),
            state: ToolActivityState::Running,
            evidence: vec![ToolActivityEvidence::Command {
                command: "x".repeat(513),
            }],
        }
        .validate_integrity()
        .is_err());
    }

    #[test]
    fn tool_error_evidence_carries_bounded_recovery_details() {
        let evidence = ToolActivityEvidence::Error {
            code: "mcp_tool_error".to_owned(),
            message: "repo must be alphanumeric".to_owned(),
            details: vec![ToolActivityErrorDetail {
                label: "how_to_get".to_owned(),
                value: "Call search_capabilities".to_owned(),
            }],
        };
        evidence
            .validate_integrity()
            .expect("structured recovery details are valid");
        let value = serde_json::to_value(&evidence).expect("Tool error serializes");
        assert_eq!(value["details"][0]["label"], "how_to_get");
        assert_eq!(value["details"][0]["value"], "Call search_capabilities");
        assert!(serde_json::from_value::<ToolActivityEvidence>(value)
            .expect("Tool error deserializes")
            .validate_integrity()
            .is_ok());
    }

    #[test]
    fn recovery_replay_policy_is_provider_neutral_and_state_based() {
        let request = |id: &str| AgentEvent::RequestOpened {
            request: PendingRequest {
                request_id: RequestId::new(id),
                blocking: true,
                payload: PendingRequestPayload::Input {
                    prompt: vec![Content::text("answer")],
                    input_schema: None,
                },
            },
        };
        let pending = BTreeSet::from([RequestId::new("still-pending")]);

        assert_eq!(
            request("still-pending").recovery_replay_policy(&pending),
            RecoveryReplayPolicy::ProviderEvidenceRequired
        );
        assert_eq!(
            request("already-closed").recovery_replay_policy(&pending),
            RecoveryReplayPolicy::HostJournalSufficient
        );
        for event in [
            AgentEvent::RequestClosed {
                request_id: RequestId::new("already-closed"),
                reason: "resolved by the native owner".to_owned(),
            },
            AgentEvent::CommandDispositionRecorded {
                command_id: CommandId::new("command-1"),
                outcome: ProviderCommandOutcome::Accepted,
            },
            AgentEvent::StopRequested {
                reason: "user requested cancellation".to_owned(),
            },
        ] {
            assert_eq!(
                event.recovery_replay_policy(&pending),
                RecoveryReplayPolicy::HostJournalSufficient
            );
        }
        for event in [
            AgentEvent::RunStarted,
            AgentEvent::InputCommitted {
                content: vec![Content::text("durable input")],
            },
        ] {
            assert_eq!(
                event.recovery_replay_policy(&pending),
                RecoveryReplayPolicy::ProviderEvidenceRequired
            );
        }
    }
}
