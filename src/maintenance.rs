use crate::config::ResourceLimits;
use crate::error::SinkError;
use crate::sink::ducklake::{open_ducklake_connection, CatalogType, DuckLakeConfig};
use std::time::Instant;

pub fn run_ducklake_maintenance(
    config: &DuckLakeConfig,
    limits: &ResourceLimits,
    expire_older_than: &str,
) -> Result<(), SinkError> {
    if config.catalog_type == CatalogType::DuckDB {
        return Err(SinkError::ConfigError(
            "DuckDB-backed DuckLake catalogs do not support a concurrent maintenance client".into(),
        ));
    }
    if expire_older_than.trim().is_empty() {
        return Err(SinkError::ConfigError(
            "expire_older_than must not be empty".into(),
        ));
    }

    tracing::info!(
        catalog_type = ?config.catalog_type,
        expire_older_than,
        "Starting DuckLake maintenance"
    );
    let started = Instant::now();
    let connection = open_ducklake_connection(config, limits, false)?;
    connection.execute(
        "CALL ducklake.set_option('expire_older_than', ?)",
        duckdb::params![expire_older_than],
    )?;
    connection.execute_batch("USE ducklake; CHECKPOINT;")?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis(),
        "DuckLake maintenance completed"
    );
    Ok(())
}
