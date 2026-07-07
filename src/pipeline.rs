use crate::config::PipelineConfig;
use crate::dto::RuuviTelemetry;
use crate::error::{PipelineError, SinkError, SourceError};
use crate::sink::SensorValuesSink;
use futures::{future, Future, Stream};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_stream::StreamExt;

pub async fn run_pipeline(
    source: impl Stream<Item = Result<RuuviTelemetry, SourceError>>,
    sink: &dyn SensorValuesSink,
) -> Result<(), PipelineError> {
    run_pipeline_with_config(source, sink, &PipelineConfig::default()).await
}

pub async fn run_pipeline_with_config(
    source: impl Stream<Item = Result<RuuviTelemetry, SourceError>>,
    sink: &dyn SensorValuesSink,
    config: &PipelineConfig,
) -> Result<(), PipelineError> {
    run_pipeline_until(source, sink, config, future::pending()).await
}

pub async fn run_pipeline_until(
    source: impl Stream<Item = Result<RuuviTelemetry, SourceError>>,
    sink: &dyn SensorValuesSink,
    config: &PipelineConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), PipelineError> {
    let result = pipeline_loop(source, sink, config, shutdown).await;
    if result.is_err() {
        cleanup_after_failure(sink, Duration::from_secs(config.shutdown_timeout_seconds)).await;
    }
    result
}

async fn pipeline_loop(
    source: impl Stream<Item = Result<RuuviTelemetry, SourceError>>,
    sink: &dyn SensorValuesSink,
    config: &PipelineConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), PipelineError> {
    let batch_size = sink.desired_batch_size();
    let max_latency = sink.desired_max_batch_latency();
    if batch_size == 0 || max_latency.is_zero() {
        return Err(PipelineError::Sink(SinkError::ConfigError(
            "batch size and latency must be greater than zero".into(),
        )));
    }

    let shutdown_timeout = Duration::from_secs(config.shutdown_timeout_seconds);
    tokio::pin!(source);
    tokio::pin!(shutdown);
    // Armed once a shutdown is requested; every later await is bounded by it.
    // After it is set the shutdown future is never polled again.
    let mut shutdown_deadline: Option<Instant> = None;

    bounded(
        retry_initialize(sink, config),
        shutdown.as_mut(),
        &mut shutdown_deadline,
        shutdown_timeout,
    )
    .await?;

    let mut batch = Vec::with_capacity(batch_size);
    let mut deadline: Option<Instant> = None;
    let mut parse_errors: u64 = 0;

    loop {
        if shutdown_deadline.is_some() {
            tracing::info!("Shutdown requested; flushing pending telemetry");
            return finish(
                &mut batch,
                sink,
                config,
                shutdown.as_mut(),
                &mut shutdown_deadline,
                shutdown_timeout,
            )
            .await;
        }
        tokio::select! {
            _ = shutdown.as_mut() => {
                shutdown_deadline = Some(Instant::now() + shutdown_timeout);
            }
            item = source.next() => {
                match item {
                    Some(Ok(telemetry)) => {
                        if batch.is_empty() {
                            deadline = Some(Instant::now() + max_latency);
                        }
                        batch.push(telemetry);
                        if batch.len() >= batch_size {
                            bounded(
                                flush(&mut batch, sink, config),
                                shutdown.as_mut(),
                                &mut shutdown_deadline,
                                shutdown_timeout,
                            )
                            .await?;
                            deadline = None;
                        }
                    }
                    Some(Err(SourceError::ParseError(message))) => {
                        parse_errors += 1;
                        // A garbage stream must not flood the log: after the
                        // first few errors, report only every hundredth.
                        if parse_errors <= 10 || parse_errors.is_multiple_of(100) {
                            tracing::error!(
                                "Error parsing telemetry (error #{parse_errors}): {message}"
                            );
                        } else {
                            tracing::debug!(
                                "Error parsing telemetry (error #{parse_errors}): {message}"
                            );
                        }
                    }
                    Some(Err(SourceError::StreamShutdown)) | None => {
                        tracing::info!("Stream completed - shutting down");
                        return finish(
                            &mut batch,
                            sink,
                            config,
                            shutdown.as_mut(),
                            &mut shutdown_deadline,
                            shutdown_timeout,
                        )
                        .await;
                    }
                    Some(Err(error @ SourceError::IoError(_))) => {
                        tracing::error!("Source failed: {error}; flushing buffered telemetry");
                        if let Err(flush_error) = bounded(
                            flush(&mut batch, sink, config),
                            shutdown.as_mut(),
                            &mut shutdown_deadline,
                            shutdown_timeout,
                        )
                        .await
                        {
                            tracing::warn!(
                                "Flushing buffered telemetry after source failure failed: {flush_error}"
                            );
                        }
                        return Err(PipelineError::Source(error));
                    }
                }
            }
            _ = wait_for_deadline(deadline), if deadline.is_some() => {
                bounded(
                    flush(&mut batch, sink, config),
                    shutdown.as_mut(),
                    &mut shutdown_deadline,
                    shutdown_timeout,
                )
                .await?;
                deadline = None;
            }
        }
    }
}

/// Flush the remaining batch and close the sink, bounded by the shutdown
/// deadline once a shutdown has been requested.
async fn finish<S: Future<Output = ()>>(
    batch: &mut Vec<RuuviTelemetry>,
    sink: &dyn SensorValuesSink,
    config: &PipelineConfig,
    mut shutdown: Pin<&mut S>,
    shutdown_deadline: &mut Option<Instant>,
    shutdown_timeout: Duration,
) -> Result<(), PipelineError> {
    bounded(
        flush(batch, sink, config),
        shutdown.as_mut(),
        shutdown_deadline,
        shutdown_timeout,
    )
    .await?;
    bounded(
        async { sink.shutdown().await.map_err(PipelineError::Sink) },
        shutdown.as_mut(),
        shutdown_deadline,
        shutdown_timeout,
    )
    .await
}

/// Await `op` while also watching for a shutdown request. Once shutdown is
/// requested (in this call or an earlier one), the operation is bounded by a
/// single absolute deadline shared across the pipeline and aborts with
/// `ShutdownTimeout` when the deadline passes. An abandoned in-flight database
/// command cannot be cancelled mid-call; its outcome is unknown, matching the
/// semantics of `SinkError::TransactionOutcomeUnknown`.
async fn bounded<S>(
    op: impl Future<Output = Result<(), PipelineError>>,
    mut shutdown: Pin<&mut S>,
    shutdown_deadline: &mut Option<Instant>,
    shutdown_timeout: Duration,
) -> Result<(), PipelineError>
where
    S: Future<Output = ()>,
{
    tokio::pin!(op);
    if shutdown_deadline.is_none() {
        tokio::select! {
            result = &mut op => return result,
            _ = &mut shutdown => {
                *shutdown_deadline = Some(Instant::now() + shutdown_timeout);
                tracing::info!(
                    "Shutdown requested; bounding in-flight work by {}s",
                    shutdown_timeout.as_secs()
                );
            }
        }
    }
    let deadline = shutdown_deadline.expect("shutdown deadline must be set");
    match tokio::time::timeout_at(deadline, &mut op).await {
        Ok(result) => result,
        Err(_) => Err(PipelineError::ShutdownTimeout),
    }
}

/// Best-effort, time-bounded sink cleanup after a pipeline failure so the
/// database worker is joined even on error exits.
async fn cleanup_after_failure(sink: &dyn SensorValuesSink, timeout: Duration) {
    match tokio::time::timeout(timeout, sink.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!("Sink cleanup after pipeline failure failed: {error}"),
        Err(_) => tracing::warn!("Sink cleanup after pipeline failure timed out"),
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => future::pending().await,
    }
}

async fn flush(
    batch: &mut Vec<RuuviTelemetry>,
    sink: &dyn SensorValuesSink,
    config: &PipelineConfig,
) -> Result<(), PipelineError> {
    if batch.is_empty() {
        return Ok(());
    }
    let capacity = batch.capacity();
    let records: Arc<[RuuviTelemetry]> = std::mem::take(batch).into();
    *batch = Vec::with_capacity(capacity);
    retry_write(sink, records, config).await?;
    Ok(())
}

async fn retry_initialize(
    sink: &dyn SensorValuesSink,
    config: &PipelineConfig,
) -> Result<(), PipelineError> {
    let mut attempt = 0;
    loop {
        match sink.initialize().await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_retryable() && attempt < config.max_write_retries => {
                attempt += 1;
                let delay = retry_delay(config, attempt);
                tracing::warn!(
                    "Sink initialization failed (attempt {attempt}/{}): {error}; retrying in {}ms",
                    config.max_write_retries,
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(PipelineError::Sink(error)),
        }
    }
}

async fn retry_write(
    sink: &dyn SensorValuesSink,
    batch: Arc<[RuuviTelemetry]>,
    config: &PipelineConfig,
) -> Result<(), PipelineError> {
    let mut attempt = 0;
    loop {
        match sink.write_batch(Arc::clone(&batch)).await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_retryable() && attempt < config.max_write_retries => {
                attempt += 1;
                let delay = retry_delay(config, attempt);
                tracing::warn!(
                    "Sink write failed for {} records (attempt {attempt}/{}): {error}; retrying in {}ms",
                    batch.len(),
                    config.max_write_retries,
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(PipelineError::Sink(error)),
        }
    }
}

fn retry_delay(config: &PipelineConfig, retry_number: u32) -> Duration {
    let multiplier = 1_u64
        .checked_shl(retry_number.saturating_sub(1))
        .unwrap_or(u64::MAX);
    let millis = config
        .initial_retry_delay_ms
        .saturating_mul(multiplier)
        .min(config.max_retry_delay_ms);
    Duration::from_millis(millis)
}
