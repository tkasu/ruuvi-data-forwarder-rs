use crate::config::{CatalogTypeCfg, ResourceLimits};
use crate::dto::RuuviTelemetry;
use crate::error::SinkError;
use crate::sink::duckdb::{
    apply_resource_limits, sql_string, validate_batch_settings, validate_table_name,
    CREATE_TABLE_SQL, INSERT_SQL,
};
use crate::sink::worker::DatabaseWorker;
use crate::sink::SensorValuesSink;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogType {
    DuckDB,
    SQLite,
    Postgres,
}

impl From<CatalogTypeCfg> for CatalogType {
    fn from(catalog_type: CatalogTypeCfg) -> Self {
        match catalog_type {
            CatalogTypeCfg::DuckDB => Self::DuckDB,
            CatalogTypeCfg::SQLite => Self::SQLite,
            CatalogTypeCfg::Postgres => Self::Postgres,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DuckLakeConfig {
    pub catalog_type: CatalogType,
    pub catalog_path: String,
    pub data_path: String,
}

pub struct DuckLakeSink {
    full_table_name: String,
    batch_size: usize,
    max_latency: Duration,
    debug_logging: bool,
    worker: DatabaseWorker,
}

impl DuckLakeSink {
    pub fn new(
        table_name: impl Into<String>,
        batch_size: usize,
        max_latency_seconds: u64,
        ducklake_config: DuckLakeConfig,
        resource_limits: ResourceLimits,
    ) -> Result<Self, SinkError> {
        Self::new_with_debug(
            table_name,
            batch_size,
            max_latency_seconds,
            ducklake_config,
            resource_limits,
            false,
        )
    }

    pub fn new_with_debug(
        table_name: impl Into<String>,
        batch_size: usize,
        max_latency_seconds: u64,
        ducklake_config: DuckLakeConfig,
        resource_limits: ResourceLimits,
        debug_logging: bool,
    ) -> Result<Self, SinkError> {
        let table_name = table_name.into();
        validate_table_name(&table_name)?;
        validate_batch_settings(batch_size, max_latency_seconds)?;
        let full_table_name = format!("ducklake.{table_name}");
        let create_sql = CREATE_TABLE_SQL.replace("{table}", &full_table_name);
        let insert_sql = INSERT_SQL.replace("{table}", &full_table_name);
        let factory = move || {
            let conn = open_ducklake_connection(&ducklake_config, &resource_limits, true)?;
            conn.execute_batch(&create_sql)?;
            Ok(conn)
        };
        Ok(Self {
            full_table_name,
            batch_size,
            max_latency: Duration::from_secs(max_latency_seconds),
            debug_logging,
            worker: DatabaseWorker::start(Box::new(factory), insert_sql)?,
        })
    }
}

fn to_absolute(path: &str) -> Result<PathBuf, SinkError> {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn attach_sql(config: &DuckLakeConfig, data_path: &str) -> Result<String, SinkError> {
    let data_path = sql_string(data_path);
    let catalog = match config.catalog_type {
        CatalogType::Postgres => format!("postgres:{}", config.catalog_path),
        CatalogType::DuckDB => to_absolute(&config.catalog_path)?
            .to_string_lossy()
            .into_owned(),
        CatalogType::SQLite => format!(
            "sqlite:{}",
            to_absolute(&config.catalog_path)?.to_string_lossy()
        ),
    };
    Ok(format!(
        "ATTACH 'ducklake:{}' AS ducklake (DATA_PATH '{}', AUTOMATIC_MIGRATION)",
        sql_string(&catalog),
        data_path
    ))
}

pub(crate) fn open_ducklake_connection(
    config: &DuckLakeConfig,
    limits: &ResourceLimits,
    create_missing_paths: bool,
) -> Result<duckdb::Connection, SinkError> {
    let absolute_data = to_absolute(&config.data_path)?;
    if create_missing_paths {
        std::fs::create_dir_all(&absolute_data)?;
    } else if !absolute_data.is_dir() {
        return Err(SinkError::ConfigError(format!(
            "DuckLake data directory does not exist: {}",
            absolute_data.display()
        )));
    }
    if config.catalog_type != CatalogType::Postgres {
        let absolute_catalog = to_absolute(&config.catalog_path)?;
        if create_missing_paths {
            if let Some(parent) = absolute_catalog.parent() {
                std::fs::create_dir_all(parent)?;
            }
        } else if !absolute_catalog.is_file() {
            return Err(SinkError::ConfigError(format!(
                "DuckLake catalog does not exist: {}",
                absolute_catalog.display()
            )));
        }
    }

    let conn = duckdb::Connection::open_in_memory()?;
    apply_resource_limits(&conn, limits)?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    match config.catalog_type {
        CatalogType::SQLite => conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?,
        CatalogType::Postgres => conn.execute_batch("INSTALL postgres; LOAD postgres;")?,
        CatalogType::DuckDB => {}
    }
    let sql = attach_sql(config, &absolute_data.to_string_lossy())?;
    conn.execute_batch(&sql)?;
    Ok(conn)
}

#[async_trait]
impl SensorValuesSink for DuckLakeSink {
    async fn initialize(&self) -> Result<(), SinkError> {
        self.worker.initialize().await
    }

    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }
        if self.debug_logging {
            tracing::debug!(
                "Inserting batch of {} records into DuckLake table {}",
                batch.len(),
                self.full_table_name
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_attach_uses_catalog_prefix_and_escapes_literals() {
        let config = DuckLakeConfig {
            catalog_type: CatalogType::Postgres,
            catalog_path: "dbname=ruuvi password='secret'".into(),
            data_path: "unused".into(),
        };
        let sql = attach_sql(&config, "/tmp/it's-data").unwrap();
        assert_eq!(
            sql,
            "ATTACH 'ducklake:postgres:dbname=ruuvi password=''secret''' AS ducklake (DATA_PATH '/tmp/it''s-data', AUTOMATIC_MIGRATION)"
        );
    }
}
