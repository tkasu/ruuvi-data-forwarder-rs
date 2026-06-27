use crate::dto::RuuviTelemetry;
use crate::error::SinkError;
use crate::sink::SensorValuesSink;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// Sink that writes telemetry records as JSON lines to stdout.
pub struct ConsoleSink;

#[async_trait]
impl SensorValuesSink for ConsoleSink {
    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError> {
        let mut output = Vec::new();
        for telemetry in batch.iter() {
            let json = serde_json::to_string(telemetry)
                .map_err(|e| SinkError::SerializationError(e.to_string()))?;
            output.extend_from_slice(json.as_bytes());
            output.push(b'\n');
        }
        let mut stdout = tokio::io::stdout();
        stdout.write_all(&output).await?;
        stdout.flush().await?;
        Ok(())
    }

    fn desired_batch_size(&self) -> usize {
        1
    }

    fn desired_max_batch_latency(&self) -> Duration {
        Duration::from_secs(86400)
    }
}
