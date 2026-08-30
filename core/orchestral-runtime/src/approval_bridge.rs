//! Host-owned bridge from Agent approval requests to Tool capabilities.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::{ApprovalGrantRef, Digest, RequestId};
use orchestral_core::tool_protocol::{
    ApprovalBinding, ApprovalCapability, HostApprovalIssuer, ToolId, ToolProtocolError,
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
    #[error("approval request cannot be remembered for this session: {0}")]
    SessionScopeUnavailable(RequestId),
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

/// A remembered decision is narrower than a Tool and wider than one exact
/// invocation. Tool planners opt in with a sealed review scope; the Host also
/// binds policy and capability shape so a wider operation cannot inherit it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionApprovalKey {
    tool_id: ToolId,
    review_scope: Digest,
    capabilities_digest: Digest,
    permission_digest: Digest,
    policy_digest: Digest,
}

fn session_approval_key(
    binding: &ApprovalBinding,
) -> Result<Option<SessionApprovalKey>, ApprovalBridgeError> {
    let Some(review_scope) = binding.session_approval_scope.clone() else {
        return Ok(None);
    };
    if !review_scope.is_sha256() {
        return Err(ApprovalBridgeError::Invalid(
            "session approval scope must be a SHA-256 digest".to_owned(),
        ));
    }
    Ok(Some(SessionApprovalKey {
        tool_id: binding.tool_id.clone(),
        review_scope,
        capabilities_digest: binding.requested_capabilities.digest()?,
        permission_digest: binding.permission_digest.clone(),
        policy_digest: binding.policy_digest.clone(),
    }))
}

#[derive(Default)]
struct BrokerState {
    pending: BTreeMap<RequestId, ApprovalBinding>,
    issued_by_request: BTreeMap<RequestId, ApprovalGrantRef>,
    grants: BTreeMap<ApprovalGrantRef, StoredGrant>,
    session_approvals: BTreeSet<SessionApprovalKey>,
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
        self.issue_exact(&mut state, request_id, expires_at_unix_ms)
    }

    /// Remembers the Host user's decision for this Tool-defined review class,
    /// then issues a fresh exact grant for the current request.
    pub fn approve_for_session(
        &self,
        request_id: &RequestId,
        expires_at_unix_ms: i64,
    ) -> Result<ApprovalGrantRef, ApprovalBridgeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApprovalBridgeError::Unavailable)?;
        let binding = state
            .pending
            .get(request_id)
            .cloned()
            .ok_or_else(|| ApprovalBridgeError::RequestNotFound(request_id.clone()))?;
        let key = session_approval_key(&binding)?
            .ok_or_else(|| ApprovalBridgeError::SessionScopeUnavailable(request_id.clone()))?;
        let grant_ref = self.issue_exact(&mut state, request_id, expires_at_unix_ms)?;
        state.session_approvals.insert(key);
        Ok(grant_ref)
    }

    /// Issues a new exact single-use grant when an equivalent review class was
    /// approved earlier in this Host session.
    pub fn approve_if_remembered(
        &self,
        request_id: &RequestId,
        expires_at_unix_ms: i64,
    ) -> Result<Option<ApprovalGrantRef>, ApprovalBridgeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApprovalBridgeError::Unavailable)?;
        let binding = state
            .pending
            .get(request_id)
            .cloned()
            .ok_or_else(|| ApprovalBridgeError::RequestNotFound(request_id.clone()))?;
        let Some(key) = session_approval_key(&binding)? else {
            return Ok(None);
        };
        if !state.session_approvals.contains(&key) {
            return Ok(None);
        }
        self.issue_exact(&mut state, request_id, expires_at_unix_ms)
            .map(Some)
    }

    fn issue_exact(
        &self,
        state: &mut BrokerState,
        request_id: &RequestId,
        expires_at_unix_ms: i64,
    ) -> Result<ApprovalGrantRef, ApprovalBridgeError> {
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
            session_approval_scope: None,
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
            session_approval_scope: None,
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

    #[tokio::test]
    async fn remembered_decision_issues_fresh_exact_grants_for_the_same_review_scope() {
        let broker = InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap();
        let first_request = RequestId::new("request-1");
        let second_request = RequestId::new("request-2");
        let first = ApprovalBinding {
            run_id: RunId::new("run-1"),
            call_id: ToolCallId::new("call-1"),
            tool_id: ToolId::new("mcp/seekee/run/v1"),
            args_digest: Digest::sha256("args-1"),
            operation_digest: Digest::sha256("operation-1"),
            permission_digest: Digest::sha256("permission"),
            requested_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                EffectScope::ExternalSideEffect,
            ])),
            session_approval_scope: Some(Digest::sha256("seekee/run/schema")),
            policy_digest: Digest::sha256("policy"),
        };
        broker.stage(&first_request, first.clone()).await.unwrap();
        let first_ref = broker
            .approve_for_session(&first_request, i64::MAX)
            .unwrap();
        let first_capability = broker
            .resolve(&first_request, &first_ref, &first)
            .await
            .unwrap();

        let second = ApprovalBinding {
            run_id: RunId::new("run-2"),
            call_id: ToolCallId::new("call-2"),
            args_digest: Digest::sha256("args-2"),
            operation_digest: Digest::sha256("operation-2"),
            ..first.clone()
        };
        broker.stage(&second_request, second.clone()).await.unwrap();
        let second_ref = broker
            .approve_if_remembered(&second_request, i64::MAX)
            .unwrap()
            .expect("same review scope should be remembered");
        let second_capability = broker
            .resolve(&second_request, &second_ref, &second)
            .await
            .unwrap();

        assert_eq!(second_capability.claims.binding, second);
        assert_ne!(
            first_capability.claims.nonce, second_capability.claims.nonce,
            "remembered decisions must not reuse a capability"
        );

        let changed_scope_request = RequestId::new("request-3");
        let changed_scope = ApprovalBinding {
            run_id: RunId::new("run-3"),
            call_id: ToolCallId::new("call-3"),
            args_digest: Digest::sha256("args-3"),
            operation_digest: Digest::sha256("operation-3"),
            session_approval_scope: Some(Digest::sha256("seekee/run/changed-schema")),
            ..first
        };
        broker
            .stage(&changed_scope_request, changed_scope)
            .await
            .unwrap();
        assert!(
            broker
                .approve_if_remembered(&changed_scope_request, i64::MAX)
                .unwrap()
                .is_none(),
            "a changed Tool schema must require a new review"
        );
    }
}
