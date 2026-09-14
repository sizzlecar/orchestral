use orchestral_core::model_protocol::ModelError;
use serde::{Deserialize, Serialize};

/// Optional Chat Completions sampling parameters. Omitted values retain the
/// endpoint's defaults; nonstandard fields require endpoint support.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpenAiSamplingConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

impl OpenAiSamplingConfig {
    pub fn validate(&self) -> Result<(), ModelError> {
        for (name, value, minimum, maximum) in [
            ("top_p", self.top_p, 0.0, 1.0),
            ("min_p", self.min_p, 0.0, 1.0),
            ("presence_penalty", self.presence_penalty, -2.0, 2.0),
            ("frequency_penalty", self.frequency_penalty, -2.0, 2.0),
        ] {
            if value.is_some_and(|value| !(minimum..=maximum).contains(&value)) {
                return Err(ModelError::invalid_request(format!(
                    "sampling.{name} must be finite and between {minimum} and {maximum}"
                )));
            }
        }
        if self.top_k.is_some_and(|value| value < -1) {
            return Err(ModelError::invalid_request(
                "sampling.top_k must be -1, 0, or a positive integer",
            ));
        }
        if self
            .repetition_penalty
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        {
            return Err(ModelError::invalid_request(
                "sampling.repetition_penalty must be finite and positive",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sampling_rejects_invalid_values_and_unknown_fields() {
        for value in [
            json!({"top_p": 1.01}),
            json!({"min_p": -0.1}),
            json!({"top_k": -2}),
            json!({"repetition_penalty": 0}),
            json!({"presence_penalty": 2.1}),
            json!({"frequency_penalty": -2.1}),
        ] {
            let sampling: OpenAiSamplingConfig = serde_json::from_value(value).unwrap();
            assert!(sampling.validate().is_err());
        }
        assert!(serde_json::from_value::<OpenAiSamplingConfig>(json!({"top_p": "0.9"})).is_err());
        assert!(serde_json::from_value::<OpenAiSamplingConfig>(json!({"topk": 20})).is_err());
        assert!(OpenAiSamplingConfig {
            top_p: Some(f32::NAN),
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(OpenAiSamplingConfig {
            repetition_penalty: Some(f32::INFINITY),
            ..Default::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn sampling_preserves_explicit_zero_and_disabled_top_k() {
        for top_k in [-1, 0, 20] {
            let value = json!({
                "top_p": 1.0, "top_k": top_k, "min_p": 0.0,
                "repetition_penalty": 1.0, "presence_penalty": 0.0,
                "frequency_penalty": 0.0, "seed": 0,
            });
            let sampling: OpenAiSamplingConfig = serde_json::from_value(value.clone()).unwrap();
            sampling.validate().unwrap();
            assert_eq!(serde_json::to_value(sampling).unwrap(), value);
        }
        assert_eq!(
            serde_json::to_value(OpenAiSamplingConfig::default()).unwrap(),
            json!({})
        );
    }
}
