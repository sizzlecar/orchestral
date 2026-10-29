//! Dimension-based soft planning, separate from wire-byte resource accounting.
//! Encoded image bytes are transport data, not text tokens. This generic patch
//! estimate is not a model capability or certified vision tokenizer; verified
//! provider usage calibrates subsequent context planning in the runtime.

use base64::{engine::general_purpose::STANDARD, Engine};
use orchestral_core::model_protocol::{ModelContent, ModelError, ModelMessage, ModelRole};
use serde_json::Value;

pub(super) const PLANNING_IDENTITY: &str = "openai-compatible/dimension-image-planning/v1";
const PATCH_SIDE: u64 = 32;
const PLANNING_TOKENS_PER_PATCH: u64 = 3;

pub(super) fn planning_size(messages: &[ModelMessage]) -> Result<(u64, u64), ModelError> {
    let mut bytes = 0_u64;
    let mut tokens = 0_u64;
    for message in messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
    {
        for content in &message.content {
            let ModelContent::Data {
                media_type,
                value: Value::String(data),
            } = content
            else {
                continue;
            };
            if !media_type.starts_with("image/") || data.trim().is_empty() {
                continue;
            }
            let encoded = if data.starts_with("data:") {
                data.split_once(',').map(|(_, data)| data).ok_or_else(|| {
                    ModelError::invalid_request("inline image data URI omitted its payload")
                })?
            } else {
                data.as_str()
            };
            let raw = STANDARD.decode(encoded).map_err(|_| {
                ModelError::invalid_request("inline image payload is not valid base64")
            })?;
            let size = imagesize::blob_size(&raw).map_err(|_| {
                ModelError::invalid_request("cannot inspect inline image dimensions")
            })?;
            if size.width == 0 || size.height == 0 {
                return Err(ModelError::invalid_request(
                    "inline image dimensions must be nonzero",
                ));
            }
            // Avoid model-name tables and preserve size sensitivity. Unknown
            // service resizing and multipliers remain estimated accounting.
            let patches = (size.width as u64)
                .div_ceil(PATCH_SIDE)
                .saturating_mul((size.height as u64).div_ceil(PATCH_SIDE));
            bytes = bytes.saturating_add(encoded.len() as u64);
            tokens = tokens.saturating_add(patches.saturating_mul(PLANNING_TOKENS_PER_PATCH));
        }
    }
    Ok((bytes, tokens))
}

#[cfg(test)]
mod tests;
