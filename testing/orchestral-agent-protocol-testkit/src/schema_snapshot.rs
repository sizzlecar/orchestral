//! Deterministic JSON Schema snapshot for the public Agent Protocol wire
//! allowlist. SPI traits and Host reference projections are deliberately absent.

use orchestral_core::agent_protocol::wire::{
    AgentAdmission, AgentCommandEnvelope, AgentDescriptorEnvelope, AgentExecutionRef,
    AgentJournalRecord, AgentProtocolError, AgentProviderStreamItem, AgentRejection,
    AgentRunEnvelope, AgentRunState, AgentRunView, AgentStartRequest, CommandAck, CommandAckState,
    ProviderCommandDisposition,
};
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, Schema};

/// Path relative to this crate's manifest directory.
pub const WIRE_SCHEMA_SNAPSHOT_PATH: &str = "snapshots/agent_protocol_v1.schema.json";

/// A schema-only root that gathers the independently transported v1 messages
/// into one deterministic document and one shared `$defs` namespace.
#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(rename = "OrchestralAgentProtocolV1WireBundle", deny_unknown_fields)]
struct AgentProtocolV1WireBundle {
    run_envelope: AgentRunEnvelope,
    descriptor_envelope: AgentDescriptorEnvelope,
    start_request: AgentStartRequest,
    command_envelope: AgentCommandEnvelope,
    provider_stream_item: AgentProviderStreamItem,
    journal_record: AgentJournalRecord,
    command_ack: CommandAck,
    command_ack_state: CommandAckState,
    provider_command_disposition: ProviderCommandDisposition,
    protocol_error: AgentProtocolError,
    rejection: AgentRejection,
    admission: AgentAdmission,
    execution_ref: AgentExecutionRef,
    run_state: AgentRunState,
    run_view: AgentRunView,
}

pub fn wire_schema_bundle() -> Schema {
    SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator()
        .into_root_schema_for::<AgentProtocolV1WireBundle>()
}

pub fn render_wire_schema_bundle() -> Result<String, serde_json::Error> {
    let mut rendered = serde_json::to_string_pretty(&wire_schema_bundle())?;
    rendered.push('\n');
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use orchestral_core::agent_protocol::wire::{
        AgentRunEnvelope, AgentSessionId, Content, Extensions, RunId,
    };
    use orchestral_core::agent_protocol::AGENT_PROTOCOL_V1;
    use serde_json::{json, Value};

    use super::*;

    const CHECKED_IN_SCHEMA: &str = include_str!("../snapshots/agent_protocol_v1.schema.json");

    #[test]
    fn wire_schema_matches_checked_in_snapshot() {
        let generated = render_wire_schema_bundle().expect("wire schema must serialize");

        assert_eq!(
            generated, CHECKED_IN_SCHEMA,
            "Agent Protocol wire schema drifted; review compatibility, then explicitly run: \
             cargo run -p orchestral-agent-protocol-testkit --bin update-agent-protocol-wire-schema"
        );
    }

    #[test]
    fn schema_keeps_closed_core_objects_and_namespaced_extensions() {
        let schema = serde_json::to_value(wire_schema_bundle()).expect("schema is JSON");
        let definitions = schema
            .get("$defs")
            .and_then(Value::as_object)
            .expect("bundle has shared definitions");

        for type_name in [
            "AgentRunEnvelope",
            "AgentDescriptorEnvelope",
            "AgentStartRequest",
            "AgentCommandEnvelope",
            "AgentJournalRecord",
            "CommandAck",
            "AgentRunView",
        ] {
            assert_eq!(
                definitions[type_name].get("additionalProperties"),
                Some(&Value::Bool(false)),
                "{type_name} must reject unknown core fields"
            );
        }

        for type_name in ["AgentRunSpec", "AgentDescriptor", "Provenance"] {
            let extensions = &definitions[type_name]["properties"]["extensions"];
            assert_eq!(
                extensions.get("additionalProperties"),
                Some(&Value::Bool(false)),
                "{type_name}.extensions must not admit unnamespaced map keys"
            );
            assert!(
                extensions["patternProperties"]
                    .get(r"^[^/][^/]*/[\s\S]+$")
                    .is_some(),
                "{type_name}.extensions must encode the namespace/name key invariant"
            );
        }

        let telemetry_extension = definitions["AgentTelemetry"]["oneOf"]
            .as_array()
            .expect("telemetry is a tagged enum")
            .iter()
            .find(|variant| {
                variant["properties"]["type"]["const"] == Value::String("extension".to_owned())
            })
            .expect("telemetry extension variant is present");
        assert_eq!(
            telemetry_extension["properties"]["namespace"]["pattern"],
            Value::String(r"^[^/][^/]*/[\s\S]+$".to_owned()),
            "telemetry extension namespaces must retain namespace/name semantics"
        );
    }

    #[test]
    fn serde_and_runtime_validation_match_the_snapshotted_constraints() {
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("schema-session"),
            RunId::new("schema-run"),
            vec![Content::text("schema fixture")],
        )
        .expect("fixture run seals");

        let mut unknown_envelope = serde_json::to_value(&run).expect("run serializes");
        unknown_envelope
            .as_object_mut()
            .expect("envelope is an object")
            .insert("unknown_core_field".to_owned(), json!(true));
        assert!(serde_json::from_value::<AgentRunEnvelope>(unknown_envelope).is_err());

        let mut unknown_spec = serde_json::to_value(&run).expect("run serializes");
        unknown_spec["spec"]
            .as_object_mut()
            .expect("spec is an object")
            .insert("unknown_core_field".to_owned(), json!(true));
        assert!(serde_json::from_value::<AgentRunEnvelope>(unknown_spec).is_err());

        let mut invalid_spec = run.spec.clone();
        invalid_spec.extensions =
            Extensions::from(BTreeMap::from([("unnamespaced".to_owned(), json!(true))]));
        let invalid_run = AgentRunEnvelope::seal(invalid_spec).expect("invalid shape still seals");
        assert!(invalid_run.validate_integrity().is_err());

        let mut valid_spec = run.spec;
        valid_spec.extensions =
            Extensions::from(BTreeMap::from([("example/flag".to_owned(), json!(true))]));
        AgentRunEnvelope::seal(valid_spec)
            .expect("valid extension seals")
            .validate_integrity()
            .expect("namespaced extension remains valid");
    }
}
