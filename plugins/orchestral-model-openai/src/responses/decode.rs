use super::*;

#[derive(Default)]
struct Item {
    id: String,
    kind: String,
    call: Option<Call>,
    text: BTreeMap<u64, String>,
    done: bool,
}

struct Call {
    native_id: String,
    name: String,
    arguments: String,
}

pub(crate) struct Decoder {
    request_id: ModelRequestId,
    items: BTreeMap<u64, Item>,
    limit: usize,
    content_bytes: usize,
    refusal: bool,
}

impl Decoder {
    pub(crate) fn new(request_id: ModelRequestId, limit: usize) -> Self {
        Self {
            request_id,
            items: BTreeMap::new(),
            limit,
            content_bytes: 0,
            refusal: false,
        }
    }

    pub(crate) fn handle_frame(&mut self, frame: &str) -> Result<Vec<ModelEvent>, ModelError> {
        let event: Value = serde_json::from_str(frame).map_err(|error| {
            ModelError::protocol(format!("invalid Responses SSE JSON: {error}"))
        })?;
        let kind = string(&event, "type")?;
        let mut events = Vec::new();
        match kind {
            "response.output_item.added" => {
                self.add_item(index(&event, "output_index")?, &event["item"], &mut events)?
            }
            "response.output_item.done" => {
                self.finish_item(index(&event, "output_index")?, &event["item"], &mut events)?
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                self.refusal |= kind == "response.refusal.delta";
                let delta = string(&event, "delta")?;
                self.text_delta(&event, delta, &mut events)?;
            }
            "response.output_text.done" | "response.refusal.done" => {
                self.refusal |= kind == "response.refusal.done";
                let field = if kind == "response.refusal.done" {
                    "refusal"
                } else {
                    "text"
                };
                let item_index = self.event_item(&event, "message")?;
                self.reconcile_text(
                    item_index,
                    index(&event, "content_index")?,
                    string(&event, field)?,
                    &mut events,
                )?;
            }
            "response.function_call_arguments.delta" => {
                let item_index = self.event_item(&event, "function_call")?;
                let delta = string(&event, "delta")?;
                self.account(delta.len())?;
                let call = self
                    .items
                    .get_mut(&item_index)
                    .and_then(|item| item.call.as_mut())
                    .expect("function item has a call");
                call.arguments.push_str(delta);
                if !delta.is_empty() {
                    events.push(ModelEvent::ToolCallArgumentsDelta {
                        call_id: scoped_tool_call_id(&self.request_id, &call.native_id),
                        delta: delta.to_owned(),
                    });
                }
            }
            "response.function_call_arguments.done" => {
                let item_index = self.event_item(&event, "function_call")?;
                self.reconcile_arguments(item_index, string(&event, "arguments")?, &mut events)?;
            }
            "response.reasoning_summary_text.delta" => {
                let delta = string(&event, "delta")?;
                if !delta.is_empty() {
                    events.push(ModelEvent::ReasoningSummaryDelta {
                        delta: delta.to_owned(),
                    });
                }
            }
            "response.completed" | "response.incomplete" => {
                let expected = if kind == "response.completed" {
                    "completed"
                } else {
                    "incomplete"
                };
                if event["response"]["status"] != expected {
                    return Err(ModelError::protocol(
                        "Responses terminal event disagrees with response status",
                    ));
                }
                return self.complete(&event["response"]);
            }
            "response.failed" => return Err(response_error(&event["response"]["error"])),
            "error" => return Err(response_error(event.get("error").unwrap_or(&event))),
            // Lifecycle/annotation events do not create canonical content.
            "response.created"
            | "response.queued"
            | "response.in_progress"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.annotation.added"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done" => {}
            _ => {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    format!("unsupported Responses event type '{kind}'"),
                ))
            }
        }
        self.check_events(events)
    }

    fn check_events(&self, events: Vec<ModelEvent>) -> Result<Vec<ModelEvent>, ModelError> {
        if events.len() > self.limit {
            Err(ModelError::protocol(
                "Responses canonical-event buffer limit exceeded",
            ))
        } else {
            Ok(events)
        }
    }

    fn account(&mut self, bytes: usize) -> Result<(), ModelError> {
        self.content_bytes = self.content_bytes.saturating_add(bytes);
        if self.content_bytes > MAX_SSE_FRAME_BYTES {
            return Err(ModelError::protocol(
                "Responses accumulated content exceeds the Host limit",
            ));
        }
        Ok(())
    }

    fn event_item(&self, event: &Value, kind: &str) -> Result<u64, ModelError> {
        let index = index(event, "output_index")?;
        let item = self
            .items
            .get(&index)
            .ok_or_else(|| ModelError::protocol("Responses delta arrived before output item"))?;
        if item.kind != kind || event["item_id"] != item.id || item.done {
            return Err(ModelError::protocol(
                "Responses delta has a conflicting or completed output item",
            ));
        }
        Ok(index)
    }

    fn add_item(
        &mut self,
        index: u64,
        raw: &Value,
        events: &mut Vec<ModelEvent>,
    ) -> Result<(), ModelError> {
        let id = string(raw, "id")?;
        let kind = string(raw, "type")?;
        if id.is_empty() {
            return Err(ModelError::protocol(
                "Responses output item omitted identity",
            ));
        }
        if let Some(item) = self.items.get(&index) {
            if item.id != id || item.kind != kind {
                return Err(ModelError::protocol(
                    "Responses changed output item identity",
                ));
            }
            if let Some(call) = &item.call {
                if raw["call_id"] != call.native_id || raw["name"] != call.name {
                    return Err(ModelError::protocol(
                        "Responses changed function call identity",
                    ));
                }
            }
            return Ok(());
        }
        if self.items.len() >= self.limit || self.items.values().any(|item| item.id == id) {
            return Err(ModelError::protocol(
                "Responses output item limit or duplicate identity",
            ));
        }
        let call = match kind {
            "function_call" => {
                let native_id = string(raw, "call_id")?;
                let name = string(raw, "name")?;
                if native_id.is_empty()
                    || name.is_empty()
                    || self
                        .items
                        .values()
                        .filter_map(|item| item.call.as_ref())
                        .any(|call| call.native_id == native_id)
                {
                    return Err(ModelError::protocol(
                        "Responses omitted or duplicated function identity",
                    ));
                }
                events.push(ModelEvent::ToolCallStart {
                    call_id: scoped_tool_call_id(&self.request_id, native_id),
                    name: name.to_owned(),
                    extensions: native_tool_call_extensions(native_id.to_owned()),
                });
                Some(Call {
                    native_id: native_id.to_owned(),
                    name: name.to_owned(),
                    arguments: String::new(),
                })
            }
            "message" if raw["role"] == "assistant" => None,
            "reasoning" => None,
            _ => {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    format!("unsupported Responses output item '{kind}'"),
                ))
            }
        };
        self.items.insert(
            index,
            Item {
                id: id.to_owned(),
                kind: kind.to_owned(),
                call,
                ..Default::default()
            },
        );
        Ok(())
    }

    fn text_delta(
        &mut self,
        event: &Value,
        delta: &str,
        events: &mut Vec<ModelEvent>,
    ) -> Result<(), ModelError> {
        let item_index = self.event_item(event, "message")?;
        let content_index = index(event, "content_index")?;
        self.account(delta.len())?;
        let item = self.items.get_mut(&item_index).expect("known message item");
        if !item.text.contains_key(&content_index) && item.text.len() >= self.limit {
            return Err(ModelError::protocol(
                "Responses message part limit exceeded",
            ));
        }
        item.text.entry(content_index).or_default().push_str(delta);
        if !delta.is_empty() {
            events.push(ModelEvent::TextDelta {
                delta: delta.to_owned(),
            });
        }
        Ok(())
    }

    fn reconcile_text(
        &mut self,
        item_index: u64,
        content_index: u64,
        full: &str,
        events: &mut Vec<ModelEvent>,
    ) -> Result<(), ModelError> {
        let previous = self.items[&item_index]
            .text
            .get(&content_index)
            .map(String::as_str)
            .unwrap_or("");
        let tail = full
            .strip_prefix(previous)
            .ok_or_else(|| {
                ModelError::protocol("Responses final text disagrees with streamed text")
            })?
            .to_owned();
        if self.items[&item_index].done && !tail.is_empty() {
            return Err(ModelError::protocol(
                "Responses changed completed message text",
            ));
        }
        self.account(tail.len())?;
        let item = self.items.get_mut(&item_index).expect("known message item");
        if !item.text.contains_key(&content_index) && item.text.len() >= self.limit {
            return Err(ModelError::protocol(
                "Responses message part limit exceeded",
            ));
        }
        item.text.insert(content_index, full.to_owned());
        if !tail.is_empty() {
            events.push(ModelEvent::TextDelta { delta: tail });
        }
        Ok(())
    }

    fn reconcile_arguments(
        &mut self,
        item_index: u64,
        full: &str,
        events: &mut Vec<ModelEvent>,
    ) -> Result<(), ModelError> {
        let call = self.items[&item_index]
            .call
            .as_ref()
            .expect("function item has call");
        let tail = full
            .strip_prefix(&call.arguments)
            .ok_or_else(|| {
                ModelError::protocol("Responses final arguments disagree with streamed arguments")
            })?
            .to_owned();
        let call_id = scoped_tool_call_id(&self.request_id, &call.native_id);
        if self.items[&item_index].done && !tail.is_empty() {
            return Err(ModelError::protocol(
                "Responses changed completed function arguments",
            ));
        }
        self.account(tail.len())?;
        self.items
            .get_mut(&item_index)
            .and_then(|item| item.call.as_mut())
            .expect("function item has call")
            .arguments = full.to_owned();
        if !tail.is_empty() {
            events.push(ModelEvent::ToolCallArgumentsDelta {
                call_id,
                delta: tail,
            });
        }
        Ok(())
    }

    fn finish_item(
        &mut self,
        index: u64,
        raw: &Value,
        events: &mut Vec<ModelEvent>,
    ) -> Result<(), ModelError> {
        self.add_item(index, raw, events)?;
        match self.items[&index].kind.as_str() {
            "function_call" => {
                self.reconcile_arguments(index, string(raw, "arguments")?, events)?;
                let item = self.items.get_mut(&index).expect("known item");
                if !item.done {
                    let call = item.call.as_ref().expect("function item has call");
                    events.push(ModelEvent::ToolCallEnd {
                        call_id: scoped_tool_call_id(&self.request_id, &call.native_id),
                    });
                }
            }
            "message" => {
                let parts = raw["content"]
                    .as_array()
                    .ok_or_else(|| ModelError::protocol("Responses message omitted content"))?;
                if parts.len() > self.limit
                    || self.items[&index]
                        .text
                        .keys()
                        .any(|part| *part >= parts.len() as u64)
                {
                    return Err(ModelError::protocol(
                        "Responses final message lost streamed content",
                    ));
                }
                for (part_index, part) in parts.iter().enumerate() {
                    let text = match part["type"].as_str() {
                        Some("output_text") => string(part, "text")?,
                        Some("refusal") => {
                            self.refusal = true;
                            string(part, "refusal")?
                        }
                        _ => {
                            return Err(ModelError::new(
                                ModelErrorCode::Unsupported,
                                "unsupported Responses message content",
                            ))
                        }
                    };
                    self.reconcile_text(index, part_index as u64, text, events)?;
                }
            }
            _ => {}
        }
        self.items.get_mut(&index).expect("known item").done = true;
        Ok(())
    }

    pub(crate) fn complete(&mut self, response: &Value) -> Result<Vec<ModelEvent>, ModelError> {
        let mut reason = match response["status"].as_str() {
            Some("completed") => ModelFinishReason::Stop,
            Some("incomplete") => match response
                .pointer("/incomplete_details/reason")
                .and_then(Value::as_str)
            {
                Some("max_output_tokens") => ModelFinishReason::Length,
                Some("content_filter") => ModelFinishReason::ContentFilter,
                _ => ModelFinishReason::Other,
            },
            Some("failed") => return Err(response_error(&response["error"])),
            _ => {
                return Err(ModelError::protocol(
                    "Responses JSON has no terminal status",
                ))
            }
        };
        let output = response["output"]
            .as_array()
            .ok_or_else(|| ModelError::protocol("Responses omitted output"))?;
        if output.len() > self.limit || self.items.keys().any(|index| *index >= output.len() as u64)
        {
            return Err(ModelError::protocol(
                "Responses terminal output lost streamed items or exceeds the Host limit",
            ));
        }
        let serialized =
            serde_json::to_vec(output).map_err(|error| ModelError::protocol(error.to_string()))?;
        if serialized.len() > MAX_SSE_FRAME_BYTES {
            return Err(ModelError::protocol(
                "Responses continuation exceeds the Host limit",
            ));
        }
        let mut events = Vec::new();
        for (index, item) in output.iter().enumerate() {
            self.finish_item(index as u64, item, &mut events)?;
        }
        if reason == ModelFinishReason::Stop {
            if self.refusal {
                reason = ModelFinishReason::ContentFilter;
            } else if self.items.values().any(|item| item.call.is_some()) {
                reason = ModelFinishReason::ToolCalls;
            } else if self
                .items
                .values()
                .all(|item| item.text.values().all(String::is_empty))
            {
                return Err(ModelError::protocol(
                    "Responses completed without deliverable text or Tool calls",
                ));
            }
        }
        if let Some(usage) = response.get("usage").filter(|value| !value.is_null()) {
            events.push(ModelEvent::Usage {
                usage: ModelUsage {
                    input_tokens: usage["input_tokens"].as_u64(),
                    output_tokens: usage["output_tokens"].as_u64(),
                },
            });
        }
        if !output.is_empty() {
            events.push(ModelEvent::Continuation {
                namespace: NAMESPACE.to_owned(),
                value: Value::Array(output.clone()),
            });
        }
        events.push(ModelEvent::Finish { reason });
        self.check_events(events)
    }
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, ModelError> {
    value[field]
        .as_str()
        .ok_or_else(|| ModelError::protocol(format!("Responses omitted string field '{field}'")))
}

fn index(value: &Value, field: &str) -> Result<u64, ModelError> {
    value[field]
        .as_u64()
        .ok_or_else(|| ModelError::protocol(format!("Responses omitted index '{field}'")))
}

fn response_error(error: &Value) -> ModelError {
    let code = match error["code"].as_str() {
        Some("context_length_exceeded") => ModelErrorCode::ContextLengthExceeded,
        Some("invalid_api_key" | "authentication_error") => ModelErrorCode::Authentication,
        Some("rate_limit_exceeded") => ModelErrorCode::RateLimited,
        Some("invalid_request_error" | "invalid_argument") => ModelErrorCode::InvalidRequest,
        _ => ModelErrorCode::Unavailable,
    };
    let retryable = matches!(
        code,
        ModelErrorCode::RateLimited | ModelErrorCode::Unavailable
    );
    ModelError::new(
        code,
        error["message"]
            .as_str()
            .unwrap_or("Responses request failed"),
    )
    .with_retryable(retryable)
}
