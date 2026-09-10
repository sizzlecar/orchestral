//! Bounded policy for retrying a model attempt before any content arrives.

use serde::{Deserialize, Serialize};

use crate::model_protocol::{ModelError, ModelErrorCode};

/// Host-owned retries within one logical model step. A zero retry count disables
/// retries. Once an attempt has produced text, a Tool call, or Finish, it cannot
/// be retried by this policy. Usage-only failures retain their reported usage;
/// strict token or cost ceilings still prohibit retrying incomplete accounting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelRetryPolicy {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for ModelRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 500,
            max_delay_ms: 8_000,
        }
    }
}

impl ModelRetryPolicy {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.base_delay_ms == 0
            || self.max_delay_ms < self.base_delay_ms
            || self.max_delay_ms > 60_000
        {
            return Err(ModelError::invalid_request(
                "model retry delays must satisfy 0 < base_delay_ms <= max_delay_ms <= 60000",
            ));
        }
        Ok(())
    }

    /// Returns a delay for a one-based retry number, or None when the failure
    /// is permanent or the retry budget is exhausted.
    pub fn delay_ms(&self, error: &ModelError, retry_number: u32) -> Option<u64> {
        if !error.retryable
            || !matches!(
                error.code,
                ModelErrorCode::RateLimited | ModelErrorCode::Unavailable
            )
            || retry_number == 0
            || retry_number > self.max_retries
        {
            return None;
        }
        Some(
            self.base_delay_ms
                .saturating_mul(1_u64.checked_shl(retry_number - 1).unwrap_or(u64::MAX))
                .min(self.max_delay_ms),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_is_bounded_and_only_accepts_transient_errors() {
        let policy = ModelRetryPolicy {
            max_retries: 4,
            base_delay_ms: 500,
            max_delay_ms: 1_500,
        };
        let transient =
            ModelError::new(ModelErrorCode::Unavailable, "temporary").with_retryable(true);
        assert_eq!(
            (1..=5)
                .map(|number| policy.delay_ms(&transient, number))
                .collect::<Vec<_>>(),
            [Some(500), Some(1_000), Some(1_500), Some(1_500), None]
        );
        for code in [
            ModelErrorCode::Authentication,
            ModelErrorCode::InvalidRequest,
            ModelErrorCode::Protocol,
            ModelErrorCode::Cancelled,
            ModelErrorCode::Internal,
        ] {
            assert_eq!(
                policy.delay_ms(&ModelError::new(code, "permanent").with_retryable(true), 1),
                None
            );
        }
        assert_eq!(
            policy.delay_ms(
                &ModelError::new(ModelErrorCode::Unavailable, "not retryable"),
                1
            ),
            None
        );
        assert_eq!(
            ModelRetryPolicy {
                max_retries: 0,
                ..policy
            }
            .delay_ms(&transient, 1),
            None
        );
    }

    #[test]
    fn invalid_delay_configuration_is_rejected() {
        for (base_delay_ms, max_delay_ms) in [(0, 1), (2, 1), (1, 60_001)] {
            assert!(ModelRetryPolicy {
                base_delay_ms,
                max_delay_ms,
                ..Default::default()
            }
            .validate()
            .is_err());
        }
    }
}
