use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Stream shutdown")]
    StreamShutdown,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("DuckDB error: {0}")]
    DuckDBError(#[from] duckdb::Error),
    #[error("transaction outcome is unknown after commit or rollback failed: {0}")]
    TransactionOutcomeUnknown(String),
    #[error("Invalid table name '{0}': expected a letter or underscore followed by alphanumeric characters or underscores")]
    InvalidTableName(String),
    #[error("configuration error: {0}")]
    ConfigError(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("database worker is unavailable")]
    WorkerUnavailable,
    #[error("database worker failed: {0}")]
    WorkerFailed(String),
}

impl SinkError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::IoError(_) | Self::DuckDBError(_))
    }
}

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("source failed: {0}")]
    Source(#[from] SourceError),
    #[error("sink failed: {0}")]
    Sink(#[from] SinkError),
    #[error("shutdown timed out while flushing pending telemetry")]
    ShutdownTimeout,
}
