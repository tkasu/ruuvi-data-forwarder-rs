mod common;

use ruuvi_data_forwarder_rs::config::ResourceLimits;
use ruuvi_data_forwarder_rs::dto::RuuviTelemetry;
use ruuvi_data_forwarder_rs::sink::ducklake::{CatalogType, DuckLakeConfig, DuckLakeSink};
use ruuvi_data_forwarder_rs::sink::SensorValuesSink;
use std::sync::Arc;
use tempfile::TempDir;

fn open_test_ducklake_conn(catalog_path: &str, data_path: &str) -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .unwrap();
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:{catalog_path}' AS ducklake (DATA_PATH '{data_path}')"
    ))
    .unwrap();
    conn
}

fn open_sqlite_ducklake_conn(catalog_path: &str, data_path: &str) -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        .unwrap();
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:sqlite:{catalog_path}' AS ducklake (DATA_PATH '{data_path}')"
    ))
    .unwrap();
    conn
}

fn read_ducklake_record(catalog_path: &str, data_path: &str) -> RuuviTelemetry {
    let conn = open_test_ducklake_conn(catalog_path, data_path);
    let mut stmt = conn
        .prepare("SELECT * FROM ducklake.telemetry LIMIT 1")
        .unwrap();
    stmt.query_row([], |row| {
        Ok(RuuviTelemetry {
            temperature_millicelsius: row.get(0)?,
            humidity: row.get(1)?,
            pressure: row.get(2)?,
            battery_potential: row.get(3)?,
            tx_power: row.get(4)?,
            movement_counter: row.get(5)?,
            measurement_sequence_number: row.get(6)?,
            measurement_ts_ms: row.get(7)?,
            mac_address: {
                let s: String = row.get(8)?;
                common::parse_mac_hex(&s)
            },
        })
    })
    .unwrap()
}

fn count_ducklake_records(catalog_path: &str, data_path: &str) -> i64 {
    let conn = open_test_ducklake_conn(catalog_path, data_path);
    conn.query_row("SELECT COUNT(*) FROM ducklake.telemetry", [], |row| {
        row.get(0)
    })
    .unwrap()
}

#[tokio::test]
async fn test_ducklake_write_with_duckdb_catalog() {
    let dir = TempDir::new().unwrap();
    let catalog = dir.path().join("catalog.ducklake");
    let data = dir.path().join("data");

    let sink = DuckLakeSink::new(
        "telemetry",
        5,
        30,
        DuckLakeConfig {
            catalog_type: CatalogType::DuckDB,
            catalog_path: catalog.to_str().unwrap().to_string(),
            data_path: data.to_str().unwrap().to_string(),
        },
        ResourceLimits::default(),
    )
    .unwrap();

    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();

    let record = read_ducklake_record(catalog.to_str().unwrap(), data.to_str().unwrap());
    assert_eq!(record, common::telemetry1());
}

#[tokio::test]
async fn test_ducklake_append() {
    let dir = TempDir::new().unwrap();
    let catalog = dir.path().join("catalog.ducklake");
    let data = dir.path().join("data");

    let config = DuckLakeConfig {
        catalog_type: CatalogType::DuckDB,
        catalog_path: catalog.to_str().unwrap().to_string(),
        data_path: data.to_str().unwrap().to_string(),
    };

    let sink1 = DuckLakeSink::new(
        "telemetry",
        5,
        30,
        config.clone(),
        ResourceLimits::default(),
    )
    .unwrap();
    sink1.initialize().await.unwrap();
    sink1
        .write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();

    let sink2 = DuckLakeSink::new(
        "telemetry",
        5,
        30,
        config.clone(),
        ResourceLimits::default(),
    )
    .unwrap();
    sink2.initialize().await.unwrap();
    sink2
        .write_batch(Arc::from(vec![common::telemetry2()]))
        .await
        .unwrap();

    let count = count_ducklake_records(catalog.to_str().unwrap(), data.to_str().unwrap());
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_ducklake_relative_path_resolution() {
    // Create TempDir inside CWD so we can compute genuine relative paths
    let cwd = std::env::current_dir().unwrap();
    let dir = tempfile::Builder::new()
        .prefix("test-ducklake-rel-")
        .tempdir_in(&cwd)
        .unwrap();

    let rel_base = dir.path().strip_prefix(&cwd).unwrap();
    let rel_catalog = rel_base.join("catalog.ducklake");
    let rel_data = rel_base.join("data");
    let abs_catalog = dir.path().join("catalog.ducklake");
    let abs_data = dir.path().join("data");

    let sink = DuckLakeSink::new(
        "telemetry",
        5,
        30,
        DuckLakeConfig {
            catalog_type: CatalogType::DuckDB,
            catalog_path: rel_catalog.to_str().unwrap().to_string(),
            data_path: rel_data.to_str().unwrap().to_string(),
        },
        ResourceLimits::default(),
    )
    .unwrap();

    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();

    let count = count_ducklake_records(abs_catalog.to_str().unwrap(), abs_data.to_str().unwrap());
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_ducklake_write_with_sqlite_catalog() {
    let dir = TempDir::new().unwrap();
    let catalog = dir.path().join("catalog.sqlite");
    let data = dir.path().join("data");
    let sink = DuckLakeSink::new(
        "telemetry",
        5,
        30,
        DuckLakeConfig {
            catalog_type: CatalogType::SQLite,
            catalog_path: catalog.to_string_lossy().into_owned(),
            data_path: data.to_string_lossy().into_owned(),
        },
        ResourceLimits::default(),
    )
    .unwrap();

    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();
    sink.shutdown().await.unwrap();

    let conn = open_sqlite_ducklake_conn(&catalog.to_string_lossy(), &data.to_string_lossy());
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ducklake.telemetry", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_ducklake_write_with_postgres_catalog_when_configured() {
    let Ok(postgres_url) = std::env::var("RUUVI_TEST_POSTGRES_URL") else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("data");
    let sink = DuckLakeSink::new(
        "telemetry",
        5,
        30,
        DuckLakeConfig {
            catalog_type: CatalogType::Postgres,
            catalog_path: postgres_url.clone(),
            data_path: data.to_string_lossy().into_owned(),
        },
        ResourceLimits::default(),
    )
    .unwrap();

    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();
    sink.shutdown().await.unwrap();

    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;")
        .unwrap();
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:postgres:{postgres_url}' AS ducklake (DATA_PATH '{}')",
        data.to_string_lossy()
    ))
    .unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ducklake.telemetry", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
}
