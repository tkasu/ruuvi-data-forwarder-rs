use crate::config::ResourceLimits;
use crate::dto::RuuviTelemetry;
use crate::error::SinkError;
use crate::sink::worker::DatabaseWorker;
use crate::sink::SensorValuesSink;
use async_trait::async_trait;
use regex::Regex;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

pub(crate) const CREATE_TABLE_SQL: &str = "
    CREATE TABLE IF NOT EXISTS {table} (
        temperature_millicelsius INTEGER NOT NULL,
        humidity INTEGER NOT NULL,
        pressure INTEGER NOT NULL,
        battery_potential INTEGER NOT NULL,
        tx_power INTEGER NOT NULL,
        movement_counter INTEGER NOT NULL,
        measurement_sequence_number INTEGER NOT NULL,
        measurement_ts_ms BIGINT NOT NULL,
        mac_address VARCHAR NOT NULL
    )
";

pub(crate) const INSERT_SQL: &str = "
    INSERT INTO {table} (
        temperature_millicelsius,
        humidity,
        pressure,
        battery_potential,
        tx_power,
        movement_counter,
        measurement_sequence_number,
        measurement_ts_ms,
        mac_address
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
";

static TABLE_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("valid table regex"));

pub(crate) fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn apply_resource_limits(
    conn: &duckdb::Connection,
    limits: &ResourceLimits,
) -> Result<(), SinkError> {
    if let Some(limit) = &limits.memory_limit {
        conn.execute_batch(&format!("SET memory_limit = '{}'", sql_string(limit)))?;
    }
    if let Some(threads) = limits.threads {
        if threads == 0 {
            return Err(SinkError::ConfigError(
                "DuckDB threads must be greater than zero".into(),
            ));
        }
        conn.execute_batch(&format!("SET threads = {threads}"))?;
    }
    Ok(())
}

pub fn validate_table_name(name: &str) -> Result<(), SinkError> {
    if TABLE_NAME_RE.is_match(name) {
        Ok(())
    } else {
        Err(SinkError::InvalidTableName(name.to_string()))
    }
}

pub(crate) fn validate_batch_settings(batch_size: usize, latency: u64) -> Result<(), SinkError> {
    if batch_size == 0 {
        return Err(SinkError::ConfigError(
            "batch size must be greater than zero".into(),
        ));
    }
    if latency == 0 {
        return Err(SinkError::ConfigError(
            "batch latency must be greater than zero".into(),
        ));
    }
    Ok(())
}

pub struct DuckDBSink {
    table_name: String,
    batch_size: usize,
    max_latency: Duration,
    debug_logging: bool,
    worker: DatabaseWorker,
}

impl DuckDBSink {
    pub fn new(
        db_path: impl Into<String>,
        table_name: impl Into<String>,
        batch_size: usize,
        max_latency_seconds: u64,
        resource_limits: ResourceLimits,
    ) -> Result<Self, SinkError> {
        Self::new_with_debug(
            db_path,
            table_name,
            batch_size,
            max_latency_seconds,
            resource_limits,
            false,
        )
    }

    pub fn new_with_debug(
        db_path: impl Into<String>,
        table_name: impl Into<String>,
        batch_size: usize,
        max_latency_seconds: u64,
        resource_limits: ResourceLimits,
        debug_logging: bool,
    ) -> Result<Self, SinkError> {
        let db_path = db_path.into();
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        validate_batch_settings(batch_size, max_latency_seconds)?;
        let create_sql = CREATE_TABLE_SQL.replace("{table}", &table_name);
        let insert_sql = INSERT_SQL.replace("{table}", &table_name);
        let factory = move || {
            let path = PathBuf::from(&db_path);
            if db_path != ":memory:" {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
            }
            let conn = duckdb::Connection::open(&db_path)?;
            apply_resource_limits(&conn, &resource_limits)?;
            conn.execute_batch(&create_sql)?;
            Ok(conn)
        };
        Ok(Self {
            table_name,
            batch_size,
            max_latency: Duration::from_secs(max_latency_seconds),
            debug_logging,
            worker: DatabaseWorker::start(Box::new(factory), insert_sql)?,
        })
    }
}

#[async_trait]
impl SensorValuesSink for DuckDBSink {
    async fn initialize(&self) -> Result<(), SinkError> {
        self.worker.initialize().await
    }

    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }
        if self.debug_logging {
            tracing::debug!(
                "Inserting batch of {} records into DuckDB table {}",
                batch.len(),
                self.table_name
            );
        }
        self.worker.write(batch).await
    }

    async fn shutdown(&self) -> Result<(), SinkError> {
        self.worker.shutdown().await
    }

    fn desired_batch_size(&self) -> usize {
        self.batch_size
    }

    fn desired_max_batch_latency(&self) -> Duration {
        self.max_latency
    }
}

pub(crate) fn insert_batch(
    conn: &duckdb::Connection,
    insert_sql: &str,
    batch: &[RuuviTelemetry],
) -> Result<(), SinkError> {
    conn.execute_batch("BEGIN TRANSACTION")?;
    let inserts = (|| {
        let mut stmt = conn.prepare(insert_sql)?;
        for telemetry in batch {
            stmt.execute(duckdb::params![
                telemetry.temperature_millicelsius,
                telemetry.humidity,
                telemetry.pressure,
                telemetry.battery_potential,
                telemetry.tx_power,
                telemetry.movement_counter,
                telemetry.measurement_sequence_number,
                telemetry.measurement_ts_ms,
                telemetry.mac_address_hex(),
            ])?;
        }
        Ok::<(), duckdb::Error>(())
    })();

    if let Err(error) = inserts {
        return match conn.execute_batch("ROLLBACK") {
            Ok(()) => Err(SinkError::DuckDBError(error)),
            Err(rollback) => Err(SinkError::TransactionOutcomeUnknown(format!(
                "insert failed ({error}); rollback failed ({rollback})"
            ))),
        };
    }

    conn.execute_batch("COMMIT")
        .map_err(|error| SinkError::TransactionOutcomeUnknown(format!("commit failed ({error})")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telemetry(humidity: i32) -> RuuviTelemetry {
        RuuviTelemetry {
            temperature_millicelsius: 1,
            humidity,
            pressure: 1,
            battery_potential: 1,
            tx_power: 1,
            movement_counter: 1,
            measurement_sequence_number: 1,
            measurement_ts_ms: 1,
            mac_address: vec![1, 2, 3, 4, 5, 6],
        }
    }

    #[test]
    fn failed_batch_is_rolled_back_atomically() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE telemetry (
                temperature_millicelsius INTEGER NOT NULL,
                humidity INTEGER NOT NULL CHECK (humidity > 0),
                pressure INTEGER NOT NULL,
                battery_potential INTEGER NOT NULL,
                tx_power INTEGER NOT NULL,
                movement_counter INTEGER NOT NULL,
                measurement_sequence_number INTEGER NOT NULL,
                measurement_ts_ms BIGINT NOT NULL,
                mac_address VARCHAR NOT NULL
            )",
        )
        .unwrap();
        let error = insert_batch(
            &conn,
            &INSERT_SQL.replace("{table}", "telemetry"),
            &[telemetry(1), telemetry(-1)],
        )
        .unwrap_err();
        assert!(matches!(error, SinkError::DuckDBError(_)));
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM telemetry", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn resource_limits_are_applied_and_zero_threads_are_rejected() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        apply_resource_limits(
            &conn,
            &ResourceLimits {
                memory_limit: Some("64MB".into()),
                threads: Some(1),
            },
        )
        .unwrap();
        let threads: i64 = conn
            .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(threads, 1);
        assert!(apply_resource_limits(
            &conn,
            &ResourceLimits {
                memory_limit: None,
                threads: Some(0),
            }
        )
        .is_err());
    }
}
