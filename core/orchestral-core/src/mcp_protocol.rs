//! Immutable MCP Tool discovery snapshots used by the MCP adapter.
//!
//! MCP is an external Tool provider. These types describe a pinned discovery
//! result; they do not carry Agent context or effect authority.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::agent_protocol::wire::Digest;
use crate::tool_protocol::EffectScope;

pub const MCP_ADAPTER_PROTOCOL_V1: &str = "orchestral.mcp-tools/v1";
pub const MCP_STATELESS_PROTOCOL_2026_07_28: &str = "2026-07-28";
pub const MCP_LATEST_LEGACY_PROTOCOL: &str = "2025-11-25";

macro_rules! string_id {
    ($name:ident) => {
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

string_id!(McpServerId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum McpTransportKind {
    Stdio,
    StreamableHttp,
}

/// Immutable Host authority consumed by one MCP transport factory.
///
/// Implementations may hold resolved secret values internally, but only their
/// opaque references are exposed here and compared with Tool policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpTransportAuthority {
    pub kind: McpTransportKind,
    /// Digest of non-secret binding identity (endpoint/program, arguments,
    /// credential references, fixed header names, and transport limits).
    pub binding_digest: Digest,
    pub effect_scopes: BTreeSet<EffectScope>,
    pub process_programs: BTreeSet<String>,
    pub network_targets: BTreeSet<String>,
    pub environment_variables: BTreeSet<String>,
    pub credential_references: BTreeSet<String>,
}

impl McpTransportAuthority {
    pub fn validate(&self) -> Result<(), McpProtocolError> {
        let invalid = self
            .process_programs
            .iter()
            .chain(self.network_targets.iter())
            .chain(self.environment_variables.iter())
            .chain(self.credential_references.iter())
            .any(|value| value.trim().is_empty() || value.chars().any(char::is_control));
        if invalid || !self.binding_digest.is_sha256() {
            return Err(McpProtocolError::Invalid(
                "MCP transport authority contains an invalid target or reference".to_owned(),
            ));
        }
        let required_effects = match self.kind {
            McpTransportKind::Stdio => BTreeSet::from([
                EffectScope::Process,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
                EffectScope::ExternalSideEffect,
            ]),
            McpTransportKind::StreamableHttp => {
                BTreeSet::from([EffectScope::Network, EffectScope::ExternalSideEffect])
            }
        };
        if !required_effects.is_subset(&self.effect_scopes)
            || (!self.credential_references.is_empty()
                && !self.effect_scopes.contains(&EffectScope::SecretRead))
        {
            return Err(McpProtocolError::Invalid(
                "MCP transport authority omits a mandatory effect scope".to_owned(),
            ));
        }
        match self.kind {
            McpTransportKind::Stdio
                if self.process_programs.len() != 1 || !self.network_targets.is_empty() =>
            {
                Err(McpProtocolError::Invalid(
                    "stdio MCP transport authority requires one program and no network target"
                        .to_owned(),
                ))
            }
            McpTransportKind::StreamableHttp
                if self.network_targets.len() != 1
                    || !self.process_programs.is_empty()
                    || !self.environment_variables.is_empty() =>
            {
                Err(McpProtocolError::Invalid(
                    "Streamable HTTP MCP authority requires one network target and no process or environment authority"
                        .to_owned(),
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Transport-specific metadata derived by the trusted MCP adapter. Values are
/// raw canonical primitive strings; concrete HTTP transports perform the
/// required header-safe encoding.
#[derive(Debug, Clone, PartialEq)]
pub struct McpTransportRequest {
    pub id: u64,
    pub method: String,
    pub params: Value,
    pub parameter_headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpTransportCancellation {
    /// The transport uses `notifications/cancelled` before the connection is closed.
    ProtocolNotification,
    /// Dropping the in-flight exchange is the cancellation signal.
    DropExchange,
}

#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum McpTransportError {
    #[error("MCP JSON-RPC error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("MCP transport error: {0}")]
    Transport(String),
    #[error("MCP protocol error: {0}")]
    Protocol(String),
}

impl McpTransportError {
    pub fn permits_legacy_fallback(&self) -> bool {
        matches!(
            self,
            Self::Rpc {
                code: -32601 | -32022,
                ..
            }
        )
    }

    pub fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

/// One connected MCP message transport. Protocol negotiation, catalog
/// semantics, Tool policy, and effect journaling remain owned by runtime.
#[async_trait]
pub trait McpTransportConnection: Send + Sync {
    fn kind(&self) -> McpTransportKind;

    fn cancellation(&self) -> McpTransportCancellation;

    async fn request(
        &self,
        request: McpTransportRequest,
        cancellation: CancellationToken,
    ) -> Result<Value, McpTransportError>;

    async fn notification(
        &self,
        method: &str,
        params: Value,
        cancellation: CancellationToken,
    ) -> Result<(), McpTransportError>;

    async fn close(&self) -> Result<(), McpTransportError>;
}

/// Host-composed extension point for concrete stdio/HTTP implementations.
#[async_trait]
pub trait McpTransportFactory: Send + Sync {
    fn authority(&self) -> &McpTransportAuthority;

    async fn connect(&self) -> Result<Box<dyn McpTransportConnection>, McpTransportError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum McpProtocolEra {
    /// Per-request metadata; no initialize/initialized protocol session.
    Stateless,
    /// initialize/initialized compatibility mode for older servers.
    LegacyHandshake,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolSnapshot {
    pub server_id: McpServerId,
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
    pub schema_digest: Digest,
}

#[derive(Serialize)]
struct McpToolDigestView<'a> {
    protocol: &'a str,
    server_id: &'a McpServerId,
    name: &'a str,
    description: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Option<Value>,
}

impl McpToolSnapshot {
    pub fn seal(
        server_id: McpServerId,
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        output_schema: Option<Value>,
    ) -> Result<Self, McpProtocolError> {
        let mut snapshot = Self {
            server_id,
            name: name.into(),
            description: description.into(),
            input_schema,
            output_schema,
            schema_digest: Digest::sha256([]),
        };
        snapshot.schema_digest = snapshot.computed_digest()?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn computed_digest(&self) -> Result<Digest, McpProtocolError> {
        canonical_digest(&McpToolDigestView {
            protocol: MCP_ADAPTER_PROTOCOL_V1,
            server_id: &self.server_id,
            name: &self.name,
            description: &self.description,
            input_schema: &self.input_schema,
            output_schema: &self.output_schema,
        })
    }

    pub fn validate(&self) -> Result<(), McpProtocolError> {
        if self.server_id.is_empty()
            || self.name.trim().is_empty()
            || !self.input_schema.is_object()
            || self
                .output_schema
                .as_ref()
                .is_some_and(|schema| !schema.is_object())
            || !self.schema_digest.is_sha256()
            || self.computed_digest()? != self.schema_digest
        {
            return Err(McpProtocolError::Invalid(
                "MCP Tool snapshot has an invalid identity, schema, or digest".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSnapshot {
    pub protocol: String,
    pub server_id: McpServerId,
    pub transport: McpTransportKind,
    pub transport_binding_digest: Digest,
    pub mcp_protocol_version: String,
    pub mcp_protocol_era: McpProtocolEra,
    pub tools: Vec<McpToolSnapshot>,
    pub revision: Digest,
}

#[derive(Serialize)]
struct McpServerDigestView<'a> {
    protocol: &'a str,
    server_id: &'a McpServerId,
    transport: McpTransportKind,
    transport_binding_digest: &'a Digest,
    mcp_protocol_version: &'a str,
    mcp_protocol_era: McpProtocolEra,
    tools: &'a [McpToolSnapshot],
}

impl McpServerSnapshot {
    pub fn seal(
        server_id: McpServerId,
        transport: McpTransportKind,
        transport_binding_digest: Digest,
        mcp_protocol_version: impl Into<String>,
        mcp_protocol_era: McpProtocolEra,
        mut tools: Vec<McpToolSnapshot>,
    ) -> Result<Self, McpProtocolError> {
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        let mut snapshot = Self {
            protocol: MCP_ADAPTER_PROTOCOL_V1.to_owned(),
            server_id,
            transport,
            transport_binding_digest,
            mcp_protocol_version: mcp_protocol_version.into(),
            mcp_protocol_era,
            tools,
            revision: Digest::sha256([]),
        };
        snapshot.revision = snapshot.computed_digest()?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn computed_digest(&self) -> Result<Digest, McpProtocolError> {
        canonical_digest(&McpServerDigestView {
            protocol: &self.protocol,
            server_id: &self.server_id,
            transport: self.transport,
            transport_binding_digest: &self.transport_binding_digest,
            mcp_protocol_version: &self.mcp_protocol_version,
            mcp_protocol_era: self.mcp_protocol_era,
            tools: &self.tools,
        })
    }

    pub fn validate(&self) -> Result<(), McpProtocolError> {
        if self.protocol != MCP_ADAPTER_PROTOCOL_V1
            || self.server_id.is_empty()
            || !self.transport_binding_digest.is_sha256()
            || !is_protocol_version(&self.mcp_protocol_version)
            || (self.mcp_protocol_era == McpProtocolEra::Stateless
                && self.mcp_protocol_version != MCP_STATELESS_PROTOCOL_2026_07_28)
            || (self.mcp_protocol_era == McpProtocolEra::LegacyHandshake
                && self.mcp_protocol_version.as_str() >= MCP_STATELESS_PROTOCOL_2026_07_28)
            || !self.revision.is_sha256()
        {
            return Err(McpProtocolError::Invalid(
                "MCP server snapshot has an invalid identity or revision".to_owned(),
            ));
        }
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            tool.validate()?;
            if tool.server_id != self.server_id || !names.insert(tool.name.clone()) {
                return Err(McpProtocolError::Invalid(
                    "MCP Tool names must be unique inside one server snapshot".to_owned(),
                ));
            }
        }
        if self.computed_digest()? != self.revision {
            return Err(McpProtocolError::Invalid(
                "MCP server revision does not match its pinned Tool schemas".to_owned(),
            ));
        }
        Ok(())
    }
}

fn is_protocol_version(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum McpProtocolError {
    #[error("invalid MCP adapter protocol value: {0}")]
    Invalid(String),
    #[error("could not serialize MCP adapter protocol value: {0}")]
    Serialization(String),
}

fn canonical_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, McpProtocolError> {
    let bytes = serde_jcs::to_vec(value)
        .map_err(|error| McpProtocolError::Serialization(error.to_string()))?;
    Ok(Digest::sha256(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn server_revision_is_order_independent_and_schema_bound() {
        let server = McpServerId::new("demo");
        let left = McpToolSnapshot::seal(
            server.clone(),
            "left",
            "left tool",
            json!({"type": "object"}),
            None,
        )
        .unwrap();
        let right = McpToolSnapshot::seal(
            server.clone(),
            "right",
            "right tool",
            json!({"type": "object", "properties": {"value": {"type": "string"}}}),
            Some(json!({"type": "object"})),
        )
        .unwrap();
        let first = McpServerSnapshot::seal(
            server.clone(),
            McpTransportKind::Stdio,
            Digest::sha256("stdio-binding"),
            MCP_STATELESS_PROTOCOL_2026_07_28,
            McpProtocolEra::Stateless,
            vec![left.clone(), right.clone()],
        )
        .unwrap();
        let second = McpServerSnapshot::seal(
            server,
            McpTransportKind::Stdio,
            Digest::sha256("stdio-binding"),
            MCP_STATELESS_PROTOCOL_2026_07_28,
            McpProtocolEra::Stateless,
            vec![right, left],
        )
        .unwrap();
        assert_eq!(first, second);
    }
}
