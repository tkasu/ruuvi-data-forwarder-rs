use ruuvi_data_forwarder_rs::config::{load_config, CatalogTypeCfg, ResourceLimits, SinkType};
use ruuvi_data_forwarder_rs::sink::console::ConsoleSink;
use ruuvi_data_forwarder_rs::sink::duckdb::DuckDBSink;
use ruuvi_data_forwarder_rs::sink::ducklake::{DuckLakeConfig, DuckLakeSink};
use ruuvi_data_forwarder_rs::sink::SensorValuesSink;

fn log_resource_limits(limits: &ResourceLimits) {
    if let Some(ref limit) = limits.memory_limit {
        tracing::info!("Memory limit: {limit}");
    }
    if let Some(threads) = limits.threads {
        tracing::info!("Thread limit: {threads}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cfg = load_config().map_err(|e| format!("Failed to load config: {}", e))?;

    tracing::info!("Starting Ruuvi Data Forwarder");
    tracing::info!("Reading from StdIn");

    let sink: Box<dyn SensorValuesSink> = match cfg.sink.sink_type {
        SinkType::Console => {
            tracing::info!("Using Console sink (stdout)");
            Box::new(ConsoleSink)
        }
        SinkType::DuckDB => {
            let duckdb_cfg = cfg
                .sink
                .duckdb
                .ok_or("DuckDB sink selected but configuration is missing")?;

            if duckdb_cfg.ducklake_enabled {
                let dl_cfg = duckdb_cfg
                    .ducklake
                    .ok_or("DuckLake enabled but ducklake configuration is missing")?;
                tracing::info!("Using DuckLake sink");
                tracing::info!("Catalog type: {:?}", dl_cfg.catalog_type);
                // Postgres catalog paths are connection strings that may carry credentials.
                if dl_cfg.catalog_type == CatalogTypeCfg::Postgres {
                    tracing::info!("Catalog path: <postgres connection string redacted>");
                } else {
                    tracing::info!("Catalog path: {}", dl_cfg.catalog_path);
                }
                tracing::info!("Data path: {}", dl_cfg.data_path);
                tracing::info!("Table name: {}", duckdb_cfg.table_name);
                tracing::info!("Batch size: {}", duckdb_cfg.desired_batch_size);
                tracing::info!(
                    "Batch latency: {}s",
                    duckdb_cfg.desired_max_batch_latency_seconds
                );
                log_resource_limits(&duckdb_cfg.resource_limits);
                Box::new(DuckLakeSink::new_with_debug(
                    duckdb_cfg.table_name,
                    duckdb_cfg.desired_batch_size,
                    duckdb_cfg.desired_max_batch_latency_seconds,
                    DuckLakeConfig {
                        catalog_type: dl_cfg.catalog_type.into(),
                        catalog_path: dl_cfg.catalog_path,
                        data_path: dl_cfg.data_path,
                    },
                    duckdb_cfg.resource_limits,
                    duckdb_cfg.debug_logging,
                )?)
            } else {
                tracing::info!("Using DuckDB sink: {}", duckdb_cfg.path);
                tracing::info!("Table name: {}", duckdb_cfg.table_name);
                tracing::info!("Batch size: {}", duckdb_cfg.desired_batch_size);
                tracing::info!(
                    "Batch latency: {}s",
                    duckdb_cfg.desired_max_batch_latency_seconds
                );
                log_resource_limits(&duckdb_cfg.resource_limits);
                Box::new(DuckDBSink::new_with_debug(
                    duckdb_cfg.path,
                    duckdb_cfg.table_name,
                    duckdb_cfg.desired_batch_size,
                    duckdb_cfg.desired_max_batch_latency_seconds,
                    duckdb_cfg.resource_limits,
                    duckdb_cfg.debug_logging,
                )?)
            }
        }
    };

    let source = ruuvi_data_forwarder_rs::source::stdin_source();
    let result = ruuvi_data_forwarder_rs::pipeline::run_pipeline_until(
        source,
        sink.as_ref(),
        &cfg.pipeline,
        shutdown_signal(),
    )
    .await;

    // tokio's stdin performs an uncancellable blocking read; letting main return
    // would make the runtime drop wait on that read, hanging exit while the input
    // pipe is open but idle. All batches are flushed and the database worker is
    // joined by this point, so exit immediately instead.
    match result {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            tracing::error!("Pipeline failed: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        tracing::error!("SIGINT handler failed: {error}");
                    }
                }
                _ = terminate.recv() => {}
            }
        }
        Err(error) => {
            tracing::error!("SIGTERM handler failed: {error}");
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!("SIGINT handler failed: {error}");
            }
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!("Shutdown signal handler failed: {error}");
    }
}
