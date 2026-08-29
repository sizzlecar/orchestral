//! Host-owned bridge from Agent approval requests to Tool capabilities.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::{ApprovalGrantRef, RequestId};
use orchestral_core::tool_protocol::{
    ApprovalBinding, ApprovalCapability, HostApprovalIssuer, ToolProtocolError,
};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ApprovalBridgeError {
    #[error("invalid approval bridge configuration: {0}")]
    Invalid(String),
    #[error("approval request binding conflict: {0}")]
    RequestConflict(RequestId),
    #[error("approval request was not staged: {0}")]
    RequestNotFound(RequestId),
    #[error("approval grant was not found or did not match its operation: {0}")]
    GrantMismatch(ApprovalGrantRef),
    #[error("approval bridge state is unavailable")]
    Unavailable,
}

impl From<ToolProtocolError> for ApprovalBridgeError {
    fn from(error: ToolProtocolError) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// Provider-side view of a Host approval service. The Agent can stage and
/// resolve exact bindings but cannot issue grants or choose Allow itself.
#[async_trait]
pub trait AgentApprovalBridge: Send + Sync {
    async fn stage(
        &self,
        request_id: &RequestId,
        binding: ApprovalBinding,
    ) -> Result<(), ApprovalBridgeError>;

    async fn resolve(
        &self,
        request_id: &RequestId,
        grant_ref: &ApprovalGrantRef,
        expected: &ApprovalBinding,
    ) -> Result<ApprovalCapability, ApprovalBridgeError>;

    async fn clear(&self, request_id: &RequestId) -> Result<(), ApprovalBridgeError>;
}

struct StoredGrant {
    request_id: RequestId,
    binding: ApprovalBinding,
    capability: ApprovalCapability,
}

#[derive(Default)]
struct BrokerState {
    pending: BTreeMap<RequestId, ApprovalBinding>,
    issued_by_request: BTreeMap<RequestId, ApprovalGrantRef>,
    grants: BTreeMap<ApprovalGrantRef, StoredGrant>,
}

/// Minimal embedded Host implementation used by CLI and tests.
///
/// `approve` is deliberately absent from [`AgentApprovalBridge`]; only the
/// Host-facing concrete type can turn a user decision into a signed grant.
pub struct InMemoryHostApprovalBroker {
    issuer: HostApprovalIssuer,
    state: Mutex<BrokerState>,
}

impl InMemoryHostApprovalBroker {
    pub fn new(signing_key: impl AsRef<[u8]>) -> Result<Self, ApprovalBridgeError> {
        Ok(Self {
            issuer: HostApprovalIssuer::new(signing_key)?,
            state: Mutex::new(BrokerState::default()),
        })
    }

    pub fn approve(
        &self,
        request_id: &RequestId,
        expires_at_unix_ms: i64,
    ) -> Result<ApprovalGrantRef, ApprovalBridgeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApprovalBridgeError::Unavailable)?;
        if let Some(existing) = state.issued_by_request.get(request_id) {
            return Ok(existing.clone());
        }
        let binding = state
            .pending
            .get(request_id)
            .cloned()
            .ok_or_else(|| ApprovalBridgeError::RequestNotFound(request_id.clone()))?;
        let capability = self.issuer.issue(binding.clone(), expires_at_unix_ms)?;
        let grant_ref = ApprovalGrantRef::new(format!(
            "approval:{}:{}",
            request_id.as_str(),
            capability.claims.nonce.as_str()
        ));
        state
            .issued_by_request
            .insert(request_id.clone(), grant_ref.clone());
        state.grants.insert(
            grant_ref.clone(),
            StoredGrant {
                request_id: request_id.clone(),
                binding,
                capability,
            },
        );
        Ok(grant_ref)
    }
}

#[async_trait]
impl AgentApprovalBridge for InMemoryHostApprovalBroker {
    async fn stage(
        &self,
        request_id: &RequestId,
        binding: ApprovalBinding,
    ) -> Result<(), ApprovalBridgeError> {
        if request_id.is_empty() {
            return Err(ApprovalBridgeError::Invalid(
                "approval request_id must not be empty".to_owned(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApprovalBridgeError::Unavailable)?;
        match state.pending.get(request_id) {
            Some(existing) if existing == &binding => Ok(()),
            Some(_) => Err(ApprovalBridgeError::RequestConflict(request_id.clone())),
            None => {
                state.pending.insert(request_id.clone(), binding);
                Ok(())
            }
        }
    }

    async fn resolve(
        &self,
        request_id: &RequestId,
        grant_ref: &ApprovalGrantRef,
        expected: &ApprovalBinding,
    ) -> Result<ApprovalCapability, ApprovalBridgeError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ApprovalBridgeError::Unavailable)?;
        let grant = state
            .grants
            .get(grant_ref)
            .ok_or_else(|| ApprovalBridgeError::GrantMismatch(grant_ref.clone()))?;
        if grant.request_id != *request_id
            || grant.binding != *expected
            || grant.capability.claims.binding != *expected
        {
            return Err(ApprovalBridgeError::GrantMismatch(grant_ref.clone()));
        }
        Ok(grant.capability.clone())
    }

    async fn clear(&self, request_id: &RequestId) -> Result<(), ApprovalBridgeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApprovalBridgeError::Unavailable)?;
        state.pending.remove(request_id);
        if let Some(grant_ref) = state.issued_by_request.remove(request_id) {
            state.grants.remove(&grant_ref);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::agent_protocol::wire::{Digest, RunId};
    use orchestral_core::tool_protocol::{CapabilityRequest, EffectScope, ToolCallId, ToolId};
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn only_host_facing_api_can_issue_an_exact_staged_grant() {
        let broker = InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap();
        let request_id = RequestId::new("request-1");
        let binding = ApprovalBinding {
            run_id: RunId::new("run-1"),
            call_id: ToolCallId::new("call-1"),
            tool_id: ToolId::new("tool-1"),
            args_digest: Digest::sha256("args"),
            operation_digest: Digest::sha256("operation"),
            permission_digest: Digest::sha256("permission"),
            requested_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                EffectScope::Process,
            ])),
            policy_digest: Digest::sha256("policy"),
        };
        broker.stage(&request_id, binding.clone()).await.unwrap();
        let grant_ref = broker.approve(&request_id, i64::MAX).unwrap();
        let capability = broker
            .resolve(&request_id, &grant_ref, &binding)
            .await
            .unwrap();
        assert_eq!(capability.claims.binding, binding);
    }

    #[tokio::test]
    async fn grant_refs_cannot_cross_requests_and_clear_revokes_the_ref() {
        let broker = InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap();
        let first_request = RequestId::new("request-1");
        let second_request = RequestId::new("request-2");
        let first = ApprovalBinding {
            run_id: RunId::new("run-1"),
            call_id: ToolCallId::new("call-1"),
            tool_id: ToolId::new("tool-1"),
            args_digest: Digest::sha256("args-1"),
            operation_digest: Digest::sha256("operation-1"),
            permission_digest: Digest::sha256("permission"),
            requested_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                EffectScope::Process,
            ])),
            policy_digest: Digest::sha256("policy"),
        };
        let second = ApprovalBinding {
            call_id: ToolCallId::new("call-2"),
            args_digest: Digest::sha256("args-2"),
            operation_digest: Digest::sha256("operation-2"),
            ..first.clone()
        };
        broker.stage(&first_request, first.clone()).await.unwrap();
        broker.stage(&second_request, second.clone()).await.unwrap();
        let grant_ref = broker.approve(&first_request, i64::MAX).unwrap();

        assert!(matches!(
            broker.resolve(&second_request, &grant_ref, &second).await,
            Err(ApprovalBridgeError::GrantMismatch(_))
        ));
        broker.clear(&first_request).await.unwrap();
        assert!(matches!(
            broker.resolve(&first_request, &grant_ref, &first).await,
            Err(ApprovalBridgeError::GrantMismatch(_))
        ));
    }
}
