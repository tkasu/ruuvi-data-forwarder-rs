use crate::dto::RuuviTelemetry;
use crate::error::SinkError;
use std::sync::{mpsc, Arc, Mutex};

type ConnectionFactory = Box<dyn Fn() -> Result<duckdb::Connection, SinkError> + Send + 'static>;

enum Command {
    Initialize(tokio::sync::oneshot::Sender<Result<(), SinkError>>),
    Write(
        Arc<[RuuviTelemetry]>,
        tokio::sync::oneshot::Sender<Result<(), SinkError>>,
    ),
    Shutdown(tokio::sync::oneshot::Sender<Result<(), SinkError>>),
}

pub(crate) struct DatabaseWorker {
    sender: mpsc::Sender<Command>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl DatabaseWorker {
    pub(crate) fn start(factory: ConnectionFactory, insert_sql: String) -> Result<Self, SinkError> {
        let (sender, receiver) = mpsc::channel();
        let join = std::thread::Builder::new()
            .name("ruuvi-duckdb".into())
            .spawn(move || worker_loop(receiver, factory, insert_sql))?;
        Ok(Self {
            sender,
            join: Mutex::new(Some(join)),
        })
    }

    pub(crate) async fn initialize(&self) -> Result<(), SinkError> {
        self.request(Command::Initialize).await
    }

    pub(crate) async fn write(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Command::Write(batch, response_tx))
            .map_err(|_| SinkError::WorkerUnavailable)?;
        response_rx
            .await
            .map_err(|_| SinkError::WorkerUnavailable)?
    }

    /// Stop the worker and join its thread. Idempotent: later calls are no-ops.
    pub(crate) async fn shutdown(&self) -> Result<(), SinkError> {
        let join = self
            .join
            .lock()
            .map_err(|_| SinkError::WorkerFailed("worker join lock was poisoned".into()))?
            .take();
        let Some(join) = join else {
            return Ok(());
        };
        // If the worker already exited (channel closed), joining is all that is left.
        let result = match self.request(Command::Shutdown).await {
            Err(SinkError::WorkerUnavailable) => Ok(()),
            other => other,
        };
        tokio::task::spawn_blocking(move || join.join())
            .await
            .map_err(|e| SinkError::WorkerFailed(e.to_string()))?
            .map_err(|_| SinkError::WorkerFailed("database worker panicked".into()))?;
        result
    }

    async fn request(
        &self,
        command: impl FnOnce(tokio::sync::oneshot::Sender<Result<(), SinkError>>) -> Command,
    ) -> Result<(), SinkError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(command(response_tx))
            .map_err(|_| SinkError::WorkerUnavailable)?;
        response_rx
            .await
            .map_err(|_| SinkError::WorkerUnavailable)?
    }
}

impl Drop for DatabaseWorker {
    /// Fallback for sinks dropped without `shutdown()`: stop the worker and join
    /// it so process exit cannot race native DuckDB connection teardown. Joining
    /// blocks briefly (the worker drains queued commands first); the pipeline's
    /// time-bounded async `shutdown()` remains the primary path.
    fn drop(&mut self) {
        let Ok(mut guard) = self.join.lock() else {
            return;
        };
        if let Some(join) = guard.take() {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let _ = self.sender.send(Command::Shutdown(response_tx));
            drop(response_rx);
            let _ = join.join();
        }
    }
}

fn worker_loop(receiver: mpsc::Receiver<Command>, factory: ConnectionFactory, insert_sql: String) {
    let mut connection: Option<duckdb::Connection> = None;
    while let Ok(command) = receiver.recv() {
        match command {
            Command::Initialize(response) => {
                let result = ensure_connection(&mut connection, &factory).map(|_| ());
                let _ = response.send(result);
            }
            Command::Write(batch, response) => {
                let result = ensure_connection(&mut connection, &factory)
                    .and_then(|conn| super::duckdb::insert_batch(conn, &insert_sql, &batch));
                if result.is_err() {
                    // The connection may be broken; drop it so a retried write
                    // reconnects through the factory instead of reusing it.
                    connection = None;
                }
                let _ = response.send(result);
            }
            Command::Shutdown(response) => {
                drop(connection.take());
                let _ = response.send(Ok(()));
                break;
            }
        }
    }
}

fn ensure_connection<'a>(
    connection: &'a mut Option<duckdb::Connection>,
    factory: &ConnectionFactory,
) -> Result<&'a duckdb::Connection, SinkError> {
    if connection.is_none() {
        *connection = Some(factory()?);
    }
    connection.as_ref().ok_or(SinkError::WorkerUnavailable)
}
