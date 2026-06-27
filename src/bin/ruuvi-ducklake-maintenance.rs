use ruuvi_data_forwarder_rs::config::{load_config, CatalogTypeCfg};
use ruuvi_data_forwarder_rs::maintenance::run_ducklake_maintenance;
use ruuvi_data_forwarder_rs::sink::ducklake::{CatalogType, DuckLakeConfig};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = load_config().map_err(|error| format!("Failed to load config: {error}"))?;
    let settings = config
        .maintenance_settings()
        .map_err(|error| format!("Invalid maintenance config: {error}"))?;
    let catalog_type = match settings.ducklake.catalog_type {
        CatalogTypeCfg::DuckDB => CatalogType::DuckDB,
        CatalogTypeCfg::SQLite => CatalogType::SQLite,
        CatalogTypeCfg::Postgres => CatalogType::Postgres,
    };
    let ducklake = DuckLakeConfig {
        catalog_type,
        catalog_path: settings.ducklake.catalog_path,
        data_path: settings.ducklake.data_path,
    };

    run_ducklake_maintenance(
        &ducklake,
        &settings.resource_limits,
        &settings.expire_older_than,
    )?;
    Ok(())
}
