//! Stateless Responses API. Native output items belong to the Assistant
//! message, so encrypted reasoning and function identities survive recovery.

use super::*;

mod decode;
pub(super) use decode::Decoder;

pub(super) const NAMESPACE: &str = "openai-compatible/responses-output/v1";

pub(super) fn validate_reasoning(
    control: Option<&OpenAiReasoningControl>,
) -> Result<(), ModelError> {
    match control {
        Some(OpenAiReasoningControl::Thinking(_)) => Err(ModelError::new(
            ModelErrorCode::Unsupported,
            "Responses uses reasoning effort; select default or an explicit effort instead of on/off",
        )),
        Some(OpenAiReasoningControl::Effort(effort)) if effort.as_str().trim().is_empty() => {
            Err(ModelError::invalid_request("reasoning effort must be non-empty"))
        }
        _ => Ok(()),
    }
}

pub(super) fn request_body(
    backend: &OpenAiCompatibleBackend,
    request: &ModelRequest,
) -> Result<Value, ModelError> {
    validate_reasoning(backend.reasoning.as_ref())?;
    let mut body = json!({
        "model": backend.config.model,
        "input": encode_input(&request.messages, backend.tool_result_format)?,
        "stream": true,
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "max_output_tokens": request.max_output_tokens.unwrap_or(backend.config.default_max_output_tokens),
    });
    if let Some(OpenAiReasoningControl::Effort(effort)) = &backend.reasoning {
        body["reasoning"] = json!({"effort": effort});
        if *effort == OpenAiReasoningEffort::None {
            body["temperature"] = json!(backend.config.temperature);
        }
    }
    // A configured Chat Completions extension must not be silently discarded.
    let sampling = serde_json::to_value(&backend.sampling)
        .map_err(|error| ModelError::invalid_request(error.to_string()))?;
    for (key, value) in sampling.as_object().expect("sampling serializes as object") {
        if key != "top_p" {
            return Err(ModelError::new(ModelErrorCode::Unsupported,
                format!("Responses does not support sampling.{key}; use Chat Completions for this extension")));
        }
        if matches!(&backend.reasoning, Some(OpenAiReasoningControl::Effort(effort)) if *effort != OpenAiReasoningEffort::None)
        {
            return Err(ModelError::invalid_request(
                "Responses top_p requires reasoning effort none or the service default",
            ));
        }
        body[key] = value.clone();
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    Ok(json!({
                        "type": "function", "name": tool.name, "description": tool.description,
                        "parameters": tool_schema::parameters(&tool.input_schema)?,
                        // Preserve optional fields and Host-owned cross-field checks.
                        "strict": false,
                    }))
                })
                .collect::<Result<Vec<_>, ModelError>>()?,
        );
        body["tool_choice"] = json!("auto");
    }
    if let Some(schema) = &request.output_schema {
        if !backend.config.structured_output {
            return Err(ModelError::new(
                ModelErrorCode::Unsupported,
                "this Responses endpoint does not advertise structured output",
            ));
        }
        body["text"] = json!({"format": {
            "type": "json_schema", "name": "orchestral_response", "strict": true, "schema": schema,
        }});
    }
    Ok(body)
}

fn native_id<'a>(call_id: &'a ModelToolCallId, extensions: &'a BTreeMap<String, Value>) -> &'a str {
    extensions
        .get(OPENAI_TOOL_CALL_ID_EXTENSION)
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .unwrap_or(call_id.as_str())
}

fn encode_input(
    messages: &[ModelMessage],
    format: OpenAiToolResultFormat,
) -> Result<Vec<Value>, ModelError> {
    let mut native_ids = BTreeMap::new();
    for message in messages {
        for content in &message.content {
            if let ModelContent::ToolCall {
                call_id,
                extensions,
                ..
            } = content
            {
                native_ids.insert(call_id.as_str(), native_id(call_id, extensions));
            }
        }
    }
    let mut input = Vec::new();
    for message in messages {
        match message.role {
            ModelRole::System => {
                input.push(json!({"role": "system", "content": flatten_text(&message.content)?}))
            }
            ModelRole::User => {
                let content = encode_user_content(&message.content)?;
                let content = match content {
                    Value::Array(parts) => Value::Array(parts.into_iter().map(|part| match part["type"].as_str() {
                        Some("text") => json!({"type": "input_text", "text": part["text"]}),
                        Some("image_url") => json!({"type": "input_image", "image_url": part["image_url"]["url"]}),
                        _ => unreachable!("user content encoder has a closed set of part types"),
                    }).collect()),
                    content => content,
                };
                input.push(json!({"role": "user", "content": content}));
            }
            ModelRole::Assistant => {
                let continuation = message.content.iter().find_map(|part| match part {
                    ModelContent::Continuation { namespace, value } => Some((namespace, value)),
                    _ => None,
                });
                if let Some((namespace, value)) = continuation {
                    if namespace != NAMESPACE
                        || message
                            .content
                            .iter()
                            .filter(|part| matches!(part, ModelContent::Continuation { .. }))
                            .count()
                            != 1
                    {
                        return Err(ModelError::new(
                            ModelErrorCode::Unsupported,
                            "Responses cannot replay a foreign message continuation",
                        ));
                    }
                    let output = value.as_array().ok_or_else(|| {
                        ModelError::invalid_request("invalid Responses continuation")
                    })?;
                    validate_replay(message, output)?;
                    input.extend(output.iter().cloned());
                    continue;
                }
                // Canonical history without native output (e.g. imported text).
                for part in &message.content {
                    match part {
                        ModelContent::Text { text } => input.push(json!({"role": "assistant", "content": [{"type": "output_text", "text": text}]})),
                        ModelContent::Json { value } | ModelContent::Data { value, .. } => input.push(json!({"role": "assistant", "content": [{"type": "output_text", "text": value.to_string()}]})),
                        ModelContent::ToolCall { call_id, name, arguments, extensions } => input.push(json!({
                            "type": "function_call", "call_id": native_id(call_id, extensions), "name": name,
                            "arguments": serde_json::to_string(arguments).map_err(|error| ModelError::invalid_request(error.to_string()))?,
                        })),
                        _ => return Err(ModelError::invalid_request("unsupported Responses Assistant content")),
                    }
                }
            }
            ModelRole::Tool => {
                for part in &message.content {
                    let ModelContent::ToolResult {
                        call_id,
                        result,
                        is_error,
                    } = part
                    else {
                        return Err(ModelError::invalid_request(
                            "Tool messages may contain ToolResult blocks only",
                        ));
                    };
                    input.push(json!({
                    "type": "function_call_output",
                    "call_id": native_ids.get(call_id.as_str()).copied().unwrap_or(call_id.as_str()),
                    "output": tool_output(format, result, *is_error)?,
                }));
                }
            }
            _ => {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    "unsupported canonical model role",
                ))
            }
        }
    }
    Ok(input)
}

fn tool_output(
    format: OpenAiToolResultFormat,
    result: &Value,
    is_error: bool,
) -> Result<Value, ModelError> {
    let mut output = format.encode(result, is_error)?;
    if let Value::Array(parts) = &mut output {
        for part in parts {
            // The codec emits Chat Completions text parts; Responses names
            // these input_text in function_call_output arrays.
            part["type"] = json!("input_text");
        }
    }
    Ok(output)
}

fn validate_replay(message: &ModelMessage, output: &[Value]) -> Result<(), ModelError> {
    let mut calls = BTreeMap::new();
    for item in output {
        match item["type"].as_str() {
            Some("reasoning") => {}
            Some("message") if item["role"] == "assistant" => {}
            Some("function_call") => {
                let id = item["call_id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        ModelError::invalid_request("Responses continuation omitted call_id")
                    })?;
                if calls.insert(id, item).is_some() {
                    return Err(ModelError::invalid_request(
                        "duplicate Responses continuation call_id",
                    ));
                }
            }
            _ => {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    "unsupported Responses continuation item",
                ))
            }
        }
    }
    for part in &message.content {
        if let ModelContent::ToolCall {
            call_id,
            name,
            arguments,
            extensions,
        } = part
        {
            let raw = calls
                .remove(native_id(call_id, extensions))
                .ok_or_else(|| {
                    ModelError::invalid_request(
                        "Responses continuation lost its canonical Tool call",
                    )
                })?;
            let decoded = raw["arguments"]
                .as_str()
                .and_then(|value| serde_json::from_str::<Value>(value).ok());
            if raw["name"] != *name || decoded.as_ref() != Some(arguments) {
                return Err(ModelError::invalid_request(
                    "Responses continuation disagrees with canonical Tool arguments",
                ));
            }
        }
    }
    if !calls.is_empty() {
        return Err(ModelError::invalid_request(
            "Responses continuation contains unpaired Tool calls",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
