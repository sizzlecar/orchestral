use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Result returned by the mandatory run-scoped `StepExecutionPort`.
/// Input and approval remain Agent control-plane concerns, not DAG outcomes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepOutcome {
    Success {
        #[serde(default)]
        exports: HashMap<String, Value>,
    },
    RetryableError {
        message: String,
        #[serde(default, with = "optional_duration_serde")]
        retry_after: Option<Duration>,
        #[serde(default)]
        attempt: u32,
    },
    Error {
        message: String,
    },
}

impl StepOutcome {
    pub fn success_with(exports: HashMap<String, Value>) -> Self {
        Self::Success { exports }
    }

    pub fn retryable(
        message: impl Into<String>,
        retry_after: Option<Duration>,
        attempt: u32,
    ) -> Self {
        Self::RetryableError {
            message: message.into(),
            retry_after,
            attempt,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }
}

mod optional_duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .map(|duration| duration.as_millis() as u64)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<u64>::deserialize(deserializer).map(|value| value.map(Duration::from_millis))
    }
}
