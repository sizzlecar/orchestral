use super::*;
use orchestral_core::model_protocol::ModelStream;

/// Opens a logical attempt through its first event. Nothing from a failed
/// attempt reaches the model loop, the Session journal, or the tool executor.
/// The caller races this entire future against cancellation and Steer.
pub(super) async fn start_model_with_retry(
    inner: &GenericInner,
    run: &AgentStartRequest,
    round: u64,
    request: &ModelRequest,
    cancellation: CancellationToken,
) -> Result<ModelStream, AgentFailure> {
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
        let error = match result {
            Ok(mut stream) => match stream.next().await {
                Some(Ok(first)) => {
                    return Ok(stream::once(std::future::ready(Ok(first)))
                        .chain(stream)
                        .map(move |event| {
                            let _ = &guard;
                            event
                        })
                        .boxed());
                }
                Some(Err(error)) => error,
                None => ModelError::new(
                    ModelErrorCode::Unavailable,
                    "model stream ended before its first event",
                )
                .with_retryable(true),
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
        // cumulative usage ceiling. An explicit rate-limit rejection is safe.
        let usage_limited = run.run.spec.limits.max_input_tokens.is_some()
            || run.run.spec.limits.max_output_tokens.is_some()
            || run.run.spec.limits.max_cost.is_some();
        let delay_ms = match inner.config.model_retry.delay_ms(&error, retry_number) {
            Some(delay) if !usage_limited || error.code == ModelErrorCode::RateLimited => delay,
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
            },
        )?;
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
