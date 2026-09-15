//! Negotiated v1 commands for input consumed at a model boundary.
use serde::{Deserialize, Serialize};

use super::types::*;

const NAMESPACE: &str = "orchestral/input-queue.v1";

/// A queued Steer is accepted now and committed before the next model call.
/// Editing or withdrawal is valid only while the target remains queued.
/// The command envelope retains ordinary Steer content and digest binding;
/// providers must explicitly advertise this extension before receiving it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueuedInputOperation {
    Enqueue,
    Replace { target: CommandId },
    Withdraw { target: CommandId },
}

impl QueuedInputOperation {
    pub fn descriptor_extensions() -> Extensions {
        Extensions::from([(NAMESPACE.to_owned(), serde_json::json!({"version": 1}))])
    }

    pub fn is_supported(descriptor: &AgentDescriptor) -> bool {
        descriptor
            .extensions
            .get(NAMESPACE)
            .and_then(|value| value.get("version"))
            .and_then(serde_json::Value::as_u64)
            == Some(1)
    }

    pub fn command(
        self,
        command_id: CommandId,
        run_id: RunId,
        content: Vec<Content>,
    ) -> Result<AgentCommandEnvelope, AgentProtocolError> {
        let command = AgentCommandEnvelope::new_with_extensions(
            command_id,
            run_id,
            None,
            AgentCommand::Steer { content },
            Extensions::from([(NAMESPACE.to_owned(), serde_json::json!(self))]),
        )?;
        Self::from_command(&command)?;
        Ok(command)
    }

    pub fn from_command(
        command: &AgentCommandEnvelope,
    ) -> Result<Option<Self>, AgentProtocolError> {
        let Some(value) = command.extensions.get(NAMESPACE) else {
            return Ok(None);
        };
        let operation: Self = serde_json::from_value(value.clone())
            .map_err(|_| invalid("invalid queued input operation"))?;
        if !matches!(command.payload, AgentCommand::Steer { .. }) || command.request_id.is_some() {
            return Err(invalid("queued input operations require a Steer command"));
        }
        if let Self::Replace { target } | Self::Withdraw { target } = &operation {
            if target.is_empty() || target == &command.command_id {
                return Err(invalid(
                    "queued input edits require a distinct target command",
                ));
            }
        }
        Ok(Some(operation))
    }
}

fn invalid(message: &str) -> AgentProtocolError {
    AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_scheduling_is_bound_to_command_identity_and_retains_v1_wire_shape() {
        let command = QueuedInputOperation::Enqueue
            .command(
                CommandId::new("queue"),
                RunId::new("run"),
                vec![Content::text("Additional requirement")],
            )
            .unwrap();
        command.verify_digest().unwrap();
        assert_eq!(
            QueuedInputOperation::from_command(&command).unwrap(),
            Some(QueuedInputOperation::Enqueue)
        );
        let mut modified = command.clone();
        modified.extensions.clear();
        assert!(modified.verify_digest().is_err());
        let decoded: AgentCommandEnvelope =
            serde_json::from_value(serde_json::to_value(command).unwrap()).unwrap();
        decoded.verify_digest().unwrap();
    }
}
