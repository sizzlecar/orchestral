use super::*;
use orchestral_core::model_protocol::{ModelStream, ModelStreamEvent};
use serde_json::{json, Value};

pub(super) struct StartedModelStream {
    pub(super) stream: ModelStream,
    pub(super) expected_sequence: u64,
    pub(super) usage: Option<ModelUsage>,
    pub(super) timing: ModelAttemptTiming,
}

pub(super) enum ModelStartFailure {
    Failure(AgentFailure),
    ContextRejected(ModelError),
}

impl From<AgentFailure> for ModelStartFailure {
    fn from(failure: AgentFailure) -> Self {
        Self::Failure(failure)
    }
}

#[derive(Clone, Copy)]
pub(super) struct ModelAttemptTiming {
    request_started: tokio::time::Instant,
    attempt_started: tokio::time::Instant,
    attempt_number: u64,
    max_attempts: u64,
}

impl ModelAttemptTiming {
    fn attach(self, mut error: ModelError) -> ModelError {
        let mut details = match error.details {
            Value::Object(details) => details,
            Value::Null => serde_json::Map::new(),
            original => serde_json::Map::from_iter([("cause".to_owned(), original)]),
        };
        details.extend([
            ("attempt_number".to_owned(), json!(self.attempt_number)),
            ("max_attempts".to_owned(), json!(self.max_attempts)),
            (
                "attempt_elapsed_ms".to_owned(),
                json!(self.attempt_started.elapsed().as_millis() as u64),
            ),
            (
                "logical_request_elapsed_ms".to_owned(),
                json!(self.request_started.elapsed().as_millis() as u64),
            ),
        ]);
        error.details = Value::Object(details);
        error
    }
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
) -> Result<StartedModelStream, ModelStartFailure> {
    let mut retry_number = 0_u32;
    // This clock covers every attempt and backoff for the same ModelRequest.
    // Per-read wait notices must not reset the user-visible cumulative wait.
    let request_started = tokio::time::Instant::now();
    loop {
        if cancellation.is_cancelled() {
            return Err(model_failure(ModelError::new(
                ModelErrorCode::Cancelled,
                "model request cancelled",
            ))
            .into());
        }
        let attempt_cancellation = cancellation.child_token();
        let guard = attempt_cancellation.clone().drop_guard();
        let max_retries = inner.config.model_retry.max_retries;
        let timing = ModelAttemptTiming {
            request_started,
            attempt_started: tokio::time::Instant::now(),
            attempt_number: u64::from(retry_number) + 1,
            max_attempts: u64::from(max_retries) + 1,
        };
        let result = with_wait_notice(
            inner.backend.start(request.clone(), attempt_cancellation),
            |elapsed| report_model_wait(inner, run, "response", elapsed, timing),
        )
        .await;
        let mut usage = None;
        let mut expected_sequence = 1;
        let error = match result {
            Ok(mut stream) => loop {
                match next_model_event(inner, run, &mut stream, timing).await {
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
                                    event.map_err(|error| timing.attach(error))
                                })
                                .boxed(),
                            expected_sequence,
                            usage,
                            timing,
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
        let error = timing.attach(error);
        drop(guard);
        // Only an explicit rejection before any usage/content is known safe
        // to reproject. A late error cannot certify that generation was absent.
        if error.code == ModelErrorCode::ContextLengthExceeded && usage.is_none() {
            return Err(ModelStartFailure::ContextRejected(error));
        }
        retry_number = match retry_number.checked_add(1) {
            Some(number) => number,
            None => return Err(model_failure(error).into()),
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
            _ => return Err(model_failure(error).into()),
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
                        "{}; retry {retry_number}/{} in {delay_ms} ms · total {}s",
                        if error.code == ModelErrorCode::RateLimited {
                            "Model rate limit reached"
                        } else {
                            "Model temporarily unavailable"
                        },
                        inner.config.model_retry.max_retries,
                        request_started.elapsed().as_secs()
                    ),
                    fraction: None,
                },
            },
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

/// Keep reporting silence after streaming has begun as well as before the
/// first event. The pending read stays alive; this never retries inference.
pub(super) async fn next_model_event(
    inner: &GenericInner,
    run: &AgentStartRequest,
    stream: &mut ModelStream,
    timing: ModelAttemptTiming,
) -> Option<Result<ModelStreamEvent, ModelError>> {
    with_wait_notice(stream.next(), |elapsed| {
        report_model_wait(inner, run, "output", elapsed, timing)
    })
    .await
}

// Keep the original request future alive. Reporting progress must never cause
// a second inference request, change retry policy, or delay cancellation.
async fn with_wait_notice<T>(
    future: impl std::future::Future<Output = T>,
    mut notify: impl FnMut(Duration),
) -> T {
    let started = tokio::time::Instant::now();
    let period = Duration::from_secs(10);
    let mut timer = tokio::time::interval_at(started + period, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = timer.tick() => notify(started.elapsed()),
        }
    }
}

fn report_model_wait(
    inner: &GenericInner,
    run: &AgentStartRequest,
    phase: &str,
    elapsed: Duration,
    timing: ModelAttemptTiming,
) {
    let run_id = &run.run.spec.run_id;
    let waiting = format!("Waiting for model {phase} ({}s)", elapsed.as_secs());
    let message = if timing.attempt_number > 1 {
        format!(
            "Retry {}/{} · total {}s · {waiting}",
            timing.attempt_number - 1,
            timing.max_attempts.saturating_sub(1),
            timing.request_started.elapsed().as_secs(),
        )
    } else {
        waiting
    };
    publish_telemetry(
        inner,
        run_id,
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("model-wait-{}", uuid::Uuid::new_v4())),
            run_id: run_id.clone(),
            provider_seq: None,
            payload: AgentTelemetry::ProgressReported {
                message,
                fraction: None,
            },
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn waiting_reports_progress_without_restarting_or_delaying_the_request() {
        let mut notices = Vec::new();
        let result = with_wait_notice(
            async {
                tokio::time::sleep(Duration::from_secs(25)).await;
                7
            },
            |elapsed| notices.push(elapsed.as_secs()),
        )
        .await;
        assert_eq!(result, 7);
        assert_eq!(notices, vec![10, 20]);
        notices.clear();
        with_wait_notice(std::future::ready(()), |elapsed| {
            notices.push(elapsed.as_secs())
        })
        .await;
        assert!(notices.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_wait_drops_its_notifications_too() {
        let mut notices = Vec::new();
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            with_wait_notice(std::future::pending::<()>(), |elapsed| {
                notices.push(elapsed.as_secs())
            }),
        )
        .await;
        assert!(result.is_err());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(notices, vec![10]);
    }
}
