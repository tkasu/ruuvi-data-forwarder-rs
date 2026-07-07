mod common;

use async_trait::async_trait;
use ruuvi_data_forwarder_rs::config::PipelineConfig;
use ruuvi_data_forwarder_rs::dto::RuuviTelemetry;
use ruuvi_data_forwarder_rs::error::{PipelineError, SinkError, SourceError};
use ruuvi_data_forwarder_rs::pipeline::{run_pipeline_until, run_pipeline_with_config};
use ruuvi_data_forwarder_rs::sink::SensorValuesSink;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct RecordingSink {
    batch_size: usize,
    latency: Duration,
    batches: Mutex<Vec<Vec<RuuviTelemetry>>>,
    initialize_calls: AtomicUsize,
    write_calls: AtomicUsize,
    shutdown_calls: AtomicUsize,
    initialize_failures: AtomicUsize,
    write_failures: AtomicUsize,
    uncertain_failure: AtomicBool,
    hang_writes: AtomicBool,
    hang_initialize: AtomicBool,
}

impl RecordingSink {
    fn new(batch_size: usize, latency: Duration) -> Self {
        Self {
            batch_size,
            latency,
            batches: Mutex::new(Vec::new()),
            initialize_calls: AtomicUsize::new(0),
            write_calls: AtomicUsize::new(0),
            shutdown_calls: AtomicUsize::new(0),
            initialize_failures: AtomicUsize::new(0),
            write_failures: AtomicUsize::new(0),
            uncertain_failure: AtomicBool::new(false),
            hang_writes: AtomicBool::new(false),
            hang_initialize: AtomicBool::new(false),
        }
    }

    fn retryable_error() -> SinkError {
        SinkError::IoError(std::io::Error::other("transient test failure"))
    }
}

#[async_trait]
impl SensorValuesSink for RecordingSink {
    async fn initialize(&self) -> Result<(), SinkError> {
        self.initialize_calls.fetch_add(1, Ordering::SeqCst);
        if self.hang_initialize.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.initialize_failures.load(Ordering::SeqCst) > 0 {
            self.initialize_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(Self::retryable_error());
        }
        Ok(())
    }

    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        if self.hang_writes.load(Ordering::SeqCst) {
            std::future::pending().await
        } else if self.uncertain_failure.load(Ordering::SeqCst) {
            Err(SinkError::TransactionOutcomeUnknown("test".into()))
        } else if self.write_failures.load(Ordering::SeqCst) > 0 {
            self.write_failures.fetch_sub(1, Ordering::SeqCst);
            Err(Self::retryable_error())
        } else {
            self.batches.lock().unwrap().push(batch.to_vec());
            Ok(())
        }
    }

    async fn shutdown(&self) -> Result<(), SinkError> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn desired_batch_size(&self) -> usize {
        self.batch_size
    }

    fn desired_max_batch_latency(&self) -> Duration {
        self.latency
    }
}

fn fast_config() -> PipelineConfig {
    PipelineConfig {
        max_write_retries: 3,
        initial_retry_delay_ms: 10,
        max_retry_delay_ms: 40,
        shutdown_timeout_seconds: 1,
    }
}

fn receiver_stream(
    receiver: tokio::sync::mpsc::Receiver<Result<RuuviTelemetry, SourceError>>,
) -> impl futures::Stream<Item = Result<RuuviTelemetry, SourceError>> {
    futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    })
}

#[tokio::test(start_paused = true)]
async fn partial_batch_waits_for_full_latency() {
    let sink = Arc::new(RecordingSink::new(5, Duration::from_secs(1)));
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let task_sink = Arc::clone(&sink);
    let task = tokio::spawn(async move {
        run_pipeline_with_config(
            receiver_stream(receiver),
            task_sink.as_ref(),
            &fast_config(),
        )
        .await
    });

    sender.send(Ok(common::telemetry1())).await.unwrap();
    tokio::task::yield_now().await;
    assert!(sink.batches.lock().unwrap().is_empty());
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(sink.batches.lock().unwrap().is_empty());
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(sink.batches.lock().unwrap().len(), 1);

    drop(sender);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn size_flush_resets_next_batch_deadline() {
    let sink = Arc::new(RecordingSink::new(2, Duration::from_secs(1)));
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let task_sink = Arc::clone(&sink);
    let task = tokio::spawn(async move {
        run_pipeline_with_config(
            receiver_stream(receiver),
            task_sink.as_ref(),
            &fast_config(),
        )
        .await
    });

    sender.send(Ok(common::telemetry1())).await.unwrap();
    sender.send(Ok(common::telemetry2())).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(sink.batches.lock().unwrap().len(), 1);

    tokio::time::advance(Duration::from_millis(500)).await;
    sender.send(Ok(common::telemetry1())).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert_eq!(sink.batches.lock().unwrap().len(), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(sink.batches.lock().unwrap().len(), 2);

    drop(sender);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn parse_errors_are_skipped_without_reordering_valid_records() {
    let sink = RecordingSink::new(10, Duration::from_secs(30));
    let source = tokio_stream::iter(vec![
        Ok(common::telemetry1()),
        Err(SourceError::ParseError("bad json".into())),
        Ok(common::telemetry2()),
    ]);
    run_pipeline_with_config(source, &sink, &fast_config())
        .await
        .unwrap();
    assert_eq!(
        sink.batches.lock().unwrap().as_slice(),
        &[vec![common::telemetry1(), common::telemetry2()]]
    );
}

#[tokio::test(start_paused = true)]
async fn retryable_initialization_and_write_failures_recover() {
    let sink = RecordingSink::new(1, Duration::from_secs(30));
    sink.initialize_failures.store(1, Ordering::SeqCst);
    sink.write_failures.store(2, Ordering::SeqCst);
    let source = tokio_stream::iter(vec![Ok(common::telemetry1())]);
    run_pipeline_with_config(source, &sink, &fast_config())
        .await
        .unwrap();
    assert_eq!(sink.initialize_calls.load(Ordering::SeqCst), 2);
    assert_eq!(sink.write_calls.load(Ordering::SeqCst), 3);
    assert_eq!(sink.batches.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_transaction_outcome_is_not_retried() {
    let sink = RecordingSink::new(1, Duration::from_secs(30));
    sink.uncertain_failure.store(true, Ordering::SeqCst);
    let source = tokio_stream::iter(vec![Ok(common::telemetry1())]);
    let error = run_pipeline_with_config(source, &sink, &fast_config())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PipelineError::Sink(SinkError::TransactionOutcomeUnknown(_))
    ));
    assert_eq!(sink.write_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn retryable_write_failure_exits_after_configured_retries() {
    let sink = RecordingSink::new(1, Duration::from_secs(30));
    sink.write_failures.store(10, Ordering::SeqCst);
    let source = tokio_stream::iter(vec![Ok(common::telemetry1())]);
    let error = run_pipeline_with_config(source, &sink, &fast_config())
        .await
        .unwrap_err();
    assert!(matches!(error, PipelineError::Sink(SinkError::IoError(_))));
    assert_eq!(sink.write_calls.load(Ordering::SeqCst), 4);
    assert!(sink.batches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn source_io_errors_are_fatal() {
    let sink = RecordingSink::new(5, Duration::from_secs(30));
    let source = tokio_stream::iter(vec![Err(SourceError::IoError(std::io::Error::other(
        "stdin failed",
    )))]);
    let error = run_pipeline_with_config(source, &sink, &fast_config())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PipelineError::Source(SourceError::IoError(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_flushes_pending_batch() {
    let sink = Arc::new(RecordingSink::new(5, Duration::from_secs(30)));
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    sender.send(Ok(common::telemetry1())).await.unwrap();
    let task_sink = Arc::clone(&sink);
    let task = tokio::spawn(async move {
        run_pipeline_until(
            receiver_stream(receiver),
            task_sink.as_ref(),
            &fast_config(),
            tokio::time::sleep(Duration::from_millis(1)),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(1)).await;
    task.await.unwrap().unwrap();
    assert_eq!(sink.batches.lock().unwrap().len(), 1);
    assert_eq!(sink.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_hung_initialization_times_out() {
    let sink = Arc::new(RecordingSink::new(5, Duration::from_secs(30)));
    sink.hang_initialize.store(true, Ordering::SeqCst);
    let task_sink = Arc::clone(&sink);
    let task = tokio::spawn(async move {
        run_pipeline_until(
            futures::stream::pending(),
            task_sink.as_ref(),
            &fast_config(),
            tokio::time::sleep(Duration::from_millis(1)),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, PipelineError::ShutdownTimeout));
    assert_eq!(sink.initialize_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_hung_write_times_out() {
    let sink = Arc::new(RecordingSink::new(1, Duration::from_secs(30)));
    sink.hang_writes.store(true, Ordering::SeqCst);
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    sender.send(Ok(common::telemetry1())).await.unwrap();
    let task_sink = Arc::clone(&sink);
    let task = tokio::spawn(async move {
        run_pipeline_until(
            receiver_stream(receiver),
            task_sink.as_ref(),
            &fast_config(),
            tokio::time::sleep(Duration::from_millis(1)),
        )
        .await
    });
    // The size-triggered write is already hung when the shutdown signal fires.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, PipelineError::ShutdownTimeout));
    assert_eq!(sink.write_calls.load(Ordering::SeqCst), 1);
    drop(sender);
}

#[tokio::test]
async fn source_io_error_flushes_buffered_records() {
    let sink = RecordingSink::new(10, Duration::from_secs(30));
    let source = tokio_stream::iter(vec![
        Ok(common::telemetry1()),
        Ok(common::telemetry2()),
        Err(SourceError::IoError(std::io::Error::other("stdin failed"))),
    ]);
    let error = run_pipeline_with_config(source, &sink, &fast_config())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PipelineError::Source(SourceError::IoError(_))
    ));
    assert_eq!(
        sink.batches.lock().unwrap().as_slice(),
        &[vec![common::telemetry1(), common::telemetry2()]]
    );
    assert_eq!(sink.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_timeout_is_reported() {
    let sink = Arc::new(RecordingSink::new(5, Duration::from_secs(30)));
    sink.hang_writes.store(true, Ordering::SeqCst);
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    sender.send(Ok(common::telemetry1())).await.unwrap();
    let task_sink = Arc::clone(&sink);
    let task = tokio::spawn(async move {
        run_pipeline_until(
            receiver_stream(receiver),
            task_sink.as_ref(),
            &fast_config(),
            tokio::time::sleep(Duration::from_millis(1)),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, PipelineError::ShutdownTimeout));
}
