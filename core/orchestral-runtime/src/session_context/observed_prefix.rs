//! Immutable, Run-scoped evidence for soft planning after a completed request.

use super::*;
use orchestral_core::model_protocol::ModelRequestId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextInputSignature {
    pub messages_len: usize,
    pub messages_digest: Digest,
    pub tools_digest: Digest,
    /// Always the unadjusted estimate, including full request/tool overhead.
    pub raw_estimate_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedPrefixAnchor {
    pub run_id: RunId,
    pub config_digest: Digest,
    pub source_request_id: ModelRequestId,
    pub input: ContextInputSignature,
    pub observed_input_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPlanningTrace {
    pub input: ContextInputSignature,
    /// The prior observation supplied to this projection, not its successor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<ObservedPrefixAnchor>,
}

fn digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, ModelError> {
    serde_jcs::to_vec(value)
        .map(Digest::sha256)
        .map_err(|error| ModelError::invalid_request(error.to_string()))
}

impl ContextInputSignature {
    pub(crate) fn validate(&self) -> bool {
        self.messages_len > 0 && self.messages_digest.is_sha256() && self.tools_digest.is_sha256()
    }
}

impl ObservedPrefixAnchor {
    pub(crate) fn validate(&self) -> bool {
        !self.run_id.is_empty()
            && self.config_digest.is_sha256()
            && !self.source_request_id.is_empty()
            && self.input.validate()
            && self.observed_input_tokens > 0
    }

    fn estimate(
        &self,
        meter: &dyn ModelTokenMeter,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        let raw = meter.estimate_context_input(messages, tools)?;
        let Some(prefix) = messages.get(..self.input.messages_len) else {
            return Ok(raw);
        };
        if raw.accounting != ModelTokenAccounting::Estimated
            || digest(prefix)? != self.input.messages_digest
            || digest(tools)? != self.input.tools_digest
            || messages[self.input.messages_len..]
                .iter()
                .any(|message| !matches!(message.role, ModelRole::Assistant | ModelRole::Tool))
        {
            return Ok(raw);
        }
        let Some(tokens) = raw
            .tokens
            .checked_sub(self.input.raw_estimate_tokens)
            .and_then(|delta| self.observed_input_tokens.checked_add(delta))
        else {
            return Ok(raw);
        };
        if tokens > meter.count_request_input(messages, tools)? {
            return Ok(raw);
        }
        Ok(ModelContextEstimate {
            tokens,
            accounting: ModelTokenAccounting::Estimated,
        })
    }
}

// A single projection/compaction operation owns this immutable view. The
// provider's original meter and every hard accounting call remain unchanged.
struct ObservedPrefixMeter {
    inner: Arc<dyn ModelTokenMeter>,
    anchor: ObservedPrefixAnchor,
}

impl ModelTokenMeter for ObservedPrefixMeter {
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        self.inner.meter_descriptor()
    }

    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        self.inner.count_request_input(messages, tools)
    }

    fn estimate_context_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        self.anchor.estimate(self.inner.as_ref(), messages, tools)
    }
}

impl AgentSessionContextEngine {
    pub(crate) fn with_observed_prefix(
        &self,
        run_id: &RunId,
        config_digest: &Digest,
        anchor: Option<&ObservedPrefixAnchor>,
    ) -> Self {
        let anchor = anchor.filter(|anchor| {
            self.token_meter.supports_observed_prefix_estimation()
                && anchor.validate()
                && &anchor.run_id == run_id
                && &anchor.config_digest == config_digest
        });
        let token_meter = match anchor {
            Some(anchor) => Arc::new(ObservedPrefixMeter {
                inner: self.token_meter.clone(),
                anchor: anchor.clone(),
            }) as Arc<dyn ModelTokenMeter>,
            None => self.token_meter.clone(),
        };
        Self {
            journal: self.journal.clone(),
            token_meter,
        }
    }

    pub(crate) fn planning_trace(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
        anchor: Option<&ObservedPrefixAnchor>,
    ) -> Result<Option<ContextPlanningTrace>, SessionContextError> {
        if !self.token_meter.supports_observed_prefix_estimation() {
            return Ok(None);
        }
        let trace = || -> Result<_, ModelError> {
            let raw = self.token_meter.estimate_context_input(messages, tools)?;
            if raw.accounting != ModelTokenAccounting::Estimated {
                return Ok(None);
            }
            Ok(Some(ContextPlanningTrace {
                input: ContextInputSignature {
                    messages_len: messages.len(),
                    messages_digest: digest(messages)?,
                    tools_digest: digest(tools)?,
                    raw_estimate_tokens: raw.tokens,
                },
                anchor: anchor.cloned(),
            }))
        };
        trace().map_err(|error| SessionContextError::InvalidRequest(error.to_string()))
    }
}

#[cfg(test)]
mod tests;
