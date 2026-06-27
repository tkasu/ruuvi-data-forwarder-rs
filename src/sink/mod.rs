pub mod console;
pub mod duckdb;
pub mod ducklake;
mod worker;

use crate::dto::RuuviTelemetry;
use crate::error::SinkError;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Trait for all sensor value sinks.
///
/// Implement this trait to add a new sink. The pipeline will:
/// 1. Call `initialize()` once before processing begins.
/// 2. Accumulate records in batches based on `desired_batch_size()` and `desired_max_batch_latency()`.
/// 3. Call `write_batch()` for each accumulated batch.
#[async_trait]
pub trait SensorValuesSink: Send + Sync {
    /// Process a batch of telemetry records.
    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError>;

    /// One-time initialization (create tables, directories, etc.). Default is a no-op.
    async fn initialize(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Close owned resources after EOF or a graceful shutdown.
    async fn shutdown(&self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Desired number of records per batch before flushing.
    fn desired_batch_size(&self) -> usize;

    /// Maximum time to wait before flushing a partial batch.
    fn desired_max_batch_latency(&self) -> Duration;
}
