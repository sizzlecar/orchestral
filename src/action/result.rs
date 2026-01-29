//! ActionResult type definition

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// Action execution result with retry semantics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionResult {
    /// Execution succeeded
    /// exports can be empty (side-effect only / only updates ReferenceStore)
    Success {
        /// Output values to export to WorkingSet
        #[serde(default)]
        exports: HashMap<String, Value>,
    },

    /// Need user clarification to proceed
    NeedClarification {
        /// Question to ask the user
        question: String,
    },

    /// Retryable error (rate limit / timeout / external service flakiness)
    RetryableError {
        /// Error message
        message: String,
        /// Suggested wait time before retry
        #[serde(default, with = "optional_duration_serde")]
        retry_after: Option<Duration>,
        /// Current attempt number
        #[serde(default)]
        attempt: u32,
    },

    /// Non-recoverable error
    Error {
        /// Error message
        message: String,
    },
}

impl ActionResult {
    /// Convenience: create a success result with no exports
    pub fn success() -> Self {
        Self::Success {
            exports: HashMap::new(),
        }
    }

    /// Convenience: create a success result with exports
    pub fn success_with(exports: HashMap<String, Value>) -> Self {
        Self::Success { exports }
    }

    /// Convenience: create a success result with a single export
    pub fn success_with_one(key: impl Into<String>, value: Value) -> Self {
        let mut exports = HashMap::new();
        exports.insert(key.into(), value);
        Self::Success { exports }
    }

    /// Convenience: create a clarification request
    pub fn need_clarification(question: impl Into<String>) -> Self {
        Self::NeedClarification {
            question: question.into(),
        }
    }

    /// Convenience: create a retryable error
    pub fn retryable(message: impl Into<String>, retry_after: Option<Duration>, attempt: u32) -> Self {
        Self::RetryableError {
            message: message.into(),
            retry_after,
            attempt,
        }
    }

    /// Convenience: create a non-recoverable error
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }

    /// Check if the result is successful
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    /// Check if the result is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::RetryableError { .. })
    }

    /// Check if the result is a terminal error
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Check if the result needs user input
    pub fn needs_user_input(&self) -> bool {
        matches!(self, Self::NeedClarification { .. })
    }
}

/// Serde support for Option<Duration>
mod optional_duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => d.as_millis().serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis: Option<u64> = Option::deserialize(deserializer)?;
        Ok(millis.map(Duration::from_millis))
    }
}
