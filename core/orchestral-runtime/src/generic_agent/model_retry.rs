use super::*;
use orchestral_core::model_protocol::ModelStream;

pub(super) struct StartedModelStream {
    pub(super) stream: ModelStream,
    pub(super) expected_sequence: u64,
    pub(super) usage: Option<ModelUsage>,
}

/// Opens a logical attempt through its first non-usage event. Usage snapshots
/// are validated and retained without buffering an unbounded event prefix.
/// Failed attempts cannot reach the Session journal or the tool executor.
/// The caller races this entire future against cancellation and Steer.
pub(super) async fn start_model_with_retry(
    inner: &GenericInner,
    run: &AgentStartRequest,
    round: u64,
    request: &ModelRequest,
    cancellation: CancellationToken,
    total_usage: &mut ModelUsage,
) -> Result<StartedModelStream, AgentFailure> {
    let mut retry_number = 0_u32;
    loop {
        if cancellation.is_cancelled() {
            return Err(model_failure(ModelError::new(
                ModelErrorCode::Cancelled,
                "model request cancelled",
            )));
        }
        let attempt_cancellation = cancellation.child_token();
        let guard = attempt_cancellation.clone().drop_guard();
        let result = inner
            .backend
            .start(request.clone(), attempt_cancellation)
            .await;
        let mut usage = None;
        let mut expected_sequence = 1;
        let error = match result {
            Ok(mut stream) => loop {
                match stream.next().await {
                    Some(Ok(first)) => {
                        first
                            .validate_for(&request.request_id, expected_sequence)
                            .map_err(model_failure)?;
                        if let ModelEvent::Usage { usage: observed } = first.payload {
                            usage = Some(observed);
                            expected_sequence += 1;
                            continue;
                        }
                        return Ok(StartedModelStream {
                            stream: stream::once(std::future::ready(Ok(first)))
                                .chain(stream)
                                .map(move |event| {
                                    let _ = &guard;
                                    event
                                })
                                .boxed(),
                            expected_sequence,
                            usage,
                        });
                    }
                    Some(Err(error)) => break error,
                    None => {
                        break ModelError::new(
                            ModelErrorCode::Unavailable,
                            "model stream ended before any content or Finish",
                        )
                        .with_retryable(true)
                    }
                }
            },
            Err(error) => error,
        };
        drop(guard);
        retry_number = match retry_number.checked_add(1) {
            Some(number) => number,
            None => return Err(model_failure(error)),
        };
        // An unobserved transport failure may already have consumed paid
        // tokens. Without usage evidence, retrying cannot preserve a strict
        // cumulative usage ceiling. Usage snapshots before an error may also
        // be incomplete. Only a rate-limit rejection with no usage is safe
        // under a strict ceiling.
        let usage_limited = run.run.spec.limits.max_input_tokens.is_some()
            || run.run.spec.limits.max_output_tokens.is_some()
            || run.run.spec.limits.max_cost.is_some();
        let delay_ms = match inner.config.model_retry.delay_ms(&error, retry_number) {
            Some(delay)
                if !usage_limited
                    || (error.code == ModelErrorCode::RateLimited && usage.is_none()) =>
            {
                delay
            }
            _ => return Err(model_failure(error)),
        };
        let run_id = &run.run.spec.run_id;
        append_checkpoint(
            inner,
            run_id,
            GenericCheckpointEventId::new(format!(
                "generic-{}-model-retry-{round}-{retry_number}",
                run_id.as_str()
            )),
            GenericCheckpointEvent::ModelRetryScheduled {
                round,
                request_id: request.request_id.clone(),
                retry_number,
                delay_ms,
                error: error.clone(),
                observed_usage: usage.clone(),
            },
        )?;
        if let Some(usage) = usage {
            merge_usage(total_usage, usage);
        }
        publish_telemetry(
            inner,
            run_id,
            AgentTelemetryEnvelope {
                telemetry_id: TelemetryId::new(format!(
                    "generic-{}-model-retry-{round}-{retry_number}",
                    run_id.as_str()
                )),
                run_id: run_id.clone(),
                provider_seq: None,
                payload: AgentTelemetry::ProgressReported {
                    message: format!(
                        "{}; retry {retry_number}/{} in {delay_ms} ms",
                        if error.code == ModelErrorCode::RateLimited {
                            "Model rate limit reached"
                        } else {
                            "Model temporarily unavailable"
                        },
                        inner.config.model_retry.max_retries
                    ),
                    fraction: None,
                },
            },
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}
