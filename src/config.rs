use config::{Config, ConfigError, File, FileFormat};
use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;

const EMBEDDED_DEFAULTS: &str = include_str!("../config/default.toml");

static MEMORY_LIMIT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*[1-9][0-9]*(?:\.[0-9]+)?\s*(?:b|kb|mb|gb|tb|kib|mib|gib|tib)\s*$")
        .expect("valid memory-limit regex")
});

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SinkType {
    Console,
    DuckDB,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DuckLakeConfig {
    pub catalog_type: CatalogTypeCfg,
    pub catalog_path: String,
    pub data_path: String,
    pub maintenance: Option<DuckLakeMaintenanceConfig>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DuckLakeMaintenanceConfig {
    pub expire_older_than: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogTypeCfg {
    DuckDB,
    SQLite,
    Postgres,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimits {
    pub memory_limit: Option<String>,
    pub threads: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DuckDBConfig {
    pub path: String,
    pub table_name: String,
    #[serde(default)]
    pub debug_logging: bool,
    pub desired_batch_size: usize,
    pub desired_max_batch_latency_seconds: u64,
    pub ducklake_enabled: bool,
    pub ducklake: Option<DuckLakeConfig>,
    #[serde(default)]
    pub resource_limits: ResourceLimits,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    pub sink_type: SinkType,
    pub duckdb: Option<DuckDBConfig>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub max_write_retries: u32,
    pub initial_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
    pub shutdown_timeout_seconds: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_write_retries: 3,
            initial_retry_delay_ms: 250,
            max_retry_delay_ms: 5_000,
            shutdown_timeout_seconds: 30,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub sink: SinkConfig,
    pub pipeline: PipelineConfig,
}

#[derive(Debug, Clone)]
pub struct MaintenanceSettings {
    pub ducklake: DuckLakeConfig,
    pub resource_limits: ResourceLimits,
    pub expire_older_than: String,
}

impl AppConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let pipeline = &self.pipeline;
        if pipeline.initial_retry_delay_ms == 0 {
            return invalid("pipeline.initial_retry_delay_ms must be greater than zero");
        }
        if pipeline.max_retry_delay_ms < pipeline.initial_retry_delay_ms {
            return invalid(
                "pipeline.max_retry_delay_ms must be greater than or equal to initial_retry_delay_ms",
            );
        }
        if pipeline.shutdown_timeout_seconds == 0 {
            return invalid("pipeline.shutdown_timeout_seconds must be greater than zero");
        }

        if self.sink.sink_type == SinkType::DuckDB {
            let db = self.sink.duckdb.as_ref().ok_or_else(|| {
                ConfigError::Message("DuckDB sink selected but configuration is missing".into())
            })?;
            if db.path.trim().is_empty() && !db.ducklake_enabled {
                return invalid("sink.duckdb.path must not be empty");
            }
            if db.desired_batch_size == 0 {
                return invalid("sink.duckdb.desired_batch_size must be greater than zero");
            }
            if db.desired_max_batch_latency_seconds == 0 {
                return invalid(
                    "sink.duckdb.desired_max_batch_latency_seconds must be greater than zero",
                );
            }
            if matches!(db.resource_limits.threads, Some(0)) {
                return invalid("sink.duckdb.resource_limits.threads must be greater than zero");
            }
            if let Some(limit) = &db.resource_limits.memory_limit {
                if !MEMORY_LIMIT_RE.is_match(limit) {
                    return invalid(
                        "sink.duckdb.resource_limits.memory_limit must be a positive size such as 200MB or 2GiB",
                    );
                }
            }
            if db.ducklake_enabled {
                let lake = db.ducklake.as_ref().ok_or_else(|| {
                    ConfigError::Message(
                        "DuckLake enabled but ducklake configuration is missing".into(),
                    )
                })?;
                if lake.catalog_path.trim().is_empty() {
                    return invalid("sink.duckdb.ducklake.catalog_path must not be empty");
                }
                if lake.data_path.trim().is_empty() {
                    return invalid("sink.duckdb.ducklake.data_path must not be empty");
                }
                if lake.catalog_type == CatalogTypeCfg::Postgres {
                    let catalog = lake.catalog_path.trim();
                    if !catalog.contains('=')
                        && !catalog.starts_with("postgres://")
                        && !catalog.starts_with("postgresql://")
                    {
                        return invalid(
                            "PostgreSQL DuckLake catalog_path must be a libpq connection string or PostgreSQL URI",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn maintenance_settings(&self) -> Result<MaintenanceSettings, ConfigError> {
        if self.sink.sink_type != SinkType::DuckDB {
            return invalid("DuckLake maintenance requires sink.sink_type = 'duckdb'");
        }
        let db = self.sink.duckdb.as_ref().ok_or_else(|| {
            ConfigError::Message("DuckDB sink selected but configuration is missing".into())
        })?;
        if !db.ducklake_enabled {
            return invalid("DuckLake maintenance requires sink.duckdb.ducklake_enabled = true");
        }
        let ducklake = db.ducklake.as_ref().ok_or_else(|| {
            ConfigError::Message("DuckLake maintenance configuration is missing".into())
        })?;
        if ducklake.catalog_type == CatalogTypeCfg::DuckDB {
            return invalid(
                "DuckLake maintenance does not support DuckDB catalogs because they allow only one client; use SQLite or PostgreSQL",
            );
        }
        let maintenance = ducklake.maintenance.as_ref().ok_or_else(|| {
            ConfigError::Message(
                "sink.duckdb.ducklake.maintenance.expire_older_than must be configured".into(),
            )
        })?;
        let expire_older_than = maintenance.expire_older_than.trim();
        if expire_older_than.is_empty() {
            return invalid("sink.duckdb.ducklake.maintenance.expire_older_than must not be empty");
        }
        Ok(MaintenanceSettings {
            ducklake: ducklake.clone(),
            resource_limits: db.resource_limits.clone(),
            expire_older_than: expire_older_than.to_owned(),
        })
    }
}

fn invalid<T>(message: &str) -> Result<T, ConfigError> {
    Err(ConfigError::Message(message.into()))
}

pub fn load_config() -> Result<AppConfig, ConfigError> {
    let mut builder = Config::builder()
        .add_source(File::from_str(EMBEDDED_DEFAULTS, FileFormat::Toml))
        .add_source(File::with_name("config/default").required(false));

    if let Ok(path) = std::env::var("RUUVI_CONFIG_FILE") {
        builder = builder.add_source(File::with_name(&path).required(true));
    }

    macro_rules! env_override {
        ($env:expr, $key:expr) => {
            if let Ok(val) = std::env::var($env) {
                builder = builder.set_override($key, val)?;
            }
        };
    }

    env_override!("RUUVI_SINK_TYPE", "sink.sink_type");
    env_override!("RUUVI_DUCKDB_PATH", "sink.duckdb.path");
    env_override!("RUUVI_DUCKDB_TABLE_NAME", "sink.duckdb.table_name");
    env_override!("RUUVI_DUCKDB_DEBUG_LOGGING", "sink.duckdb.debug_logging");
    env_override!(
        "RUUVI_DUCKDB_DESIRED_BATCH_SIZE",
        "sink.duckdb.desired_batch_size"
    );
    env_override!(
        "RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS",
        "sink.duckdb.desired_max_batch_latency_seconds"
    );
    env_override!(
        "RUUVI_DUCKDB_DUCKLAKE_ENABLED",
        "sink.duckdb.ducklake_enabled"
    );
    env_override!(
        "RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE",
        "sink.duckdb.ducklake.catalog_type"
    );
    env_override!(
        "RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH",
        "sink.duckdb.ducklake.catalog_path"
    );
    env_override!(
        "RUUVI_DUCKDB_DUCKLAKE_DATA_PATH",
        "sink.duckdb.ducklake.data_path"
    );
    env_override!(
        "RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN",
        "sink.duckdb.ducklake.maintenance.expire_older_than"
    );
    env_override!(
        "RUUVI_DUCKDB_MEMORY_LIMIT",
        "sink.duckdb.resource_limits.memory_limit"
    );
    env_override!(
        "RUUVI_DUCKDB_THREADS",
        "sink.duckdb.resource_limits.threads"
    );
    env_override!("RUUVI_MAX_WRITE_RETRIES", "pipeline.max_write_retries");
    env_override!(
        "RUUVI_INITIAL_RETRY_DELAY_MS",
        "pipeline.initial_retry_delay_ms"
    );
    env_override!("RUUVI_MAX_RETRY_DELAY_MS", "pipeline.max_retry_delay_ms");
    env_override!(
        "RUUVI_SHUTDOWN_TIMEOUT_SECONDS",
        "pipeline.shutdown_timeout_seconds"
    );

    let config: AppConfig = builder.build()?.try_deserialize()?;
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_defaults_are_valid() {
        let config: AppConfig = Config::builder()
            .add_source(File::from_str(EMBEDDED_DEFAULTS, FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        config.validate().unwrap();
        assert_eq!(config.sink.sink_type, SinkType::Console);
        assert_eq!(config.pipeline.max_write_retries, 3);
    }
}
