use crate::config::PipelineConfig;
use crate::dto::RuuviTelemetry;
use crate::error::{PipelineError, SinkError, SourceError};
use crate::sink::SensorValuesSink;
use futures::{future, Future, Stream};
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
    retry_initialize(sink, config).await?;

    let batch_size = sink.desired_batch_size();
    let max_latency = sink.desired_max_batch_latency();
    if batch_size == 0 || max_latency.is_zero() {
        return Err(PipelineError::Sink(SinkError::ConfigError(
            "batch size and latency must be greater than zero".into(),
        )));
    }

    let mut batch = Vec::with_capacity(batch_size);
    let mut deadline: Option<Instant> = None;
    tokio::pin!(source);
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("Shutdown requested; flushing pending telemetry");
                let timeout = Duration::from_secs(config.shutdown_timeout_seconds);
                tokio::time::timeout(timeout, async {
                    flush(&mut batch, sink, config).await?;
                    sink.shutdown().await.map_err(PipelineError::Sink)
                })
                .await
                .map_err(|_| PipelineError::ShutdownTimeout)??;
                return Ok(());
            }
            item = source.next() => {
                match item {
                    Some(Ok(telemetry)) => {
                        if batch.is_empty() {
                            deadline = Some(Instant::now() + max_latency);
                        }
                        batch.push(telemetry);
                        if batch.len() >= batch_size {
                            flush(&mut batch, sink, config).await?;
                            deadline = None;
                        }
                    }
                    Some(Err(SourceError::ParseError(message))) => {
                        tracing::error!("Error parsing telemetry: {message}");
                    }
                    Some(Err(SourceError::StreamShutdown)) | None => {
                        flush(&mut batch, sink, config).await?;
                        sink.shutdown().await?;
                        tracing::info!("Stream completed - shutting down");
                        return Ok(());
                    }
                    Some(Err(error @ SourceError::IoError(_))) => {
                        return Err(PipelineError::Source(error));
                    }
                }
            }
            _ = wait_for_deadline(deadline), if deadline.is_some() => {
                flush(&mut batch, sink, config).await?;
                deadline = None;
            }
        }
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
