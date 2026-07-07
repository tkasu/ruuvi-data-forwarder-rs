mod common;

use ruuvi_data_forwarder_rs::config::ResourceLimits;
use ruuvi_data_forwarder_rs::dto::RuuviTelemetry;
use ruuvi_data_forwarder_rs::sink::duckdb::DuckDBSink;
use ruuvi_data_forwarder_rs::sink::SensorValuesSink;
use std::sync::Arc;
use tempfile::TempDir;

/// Read all records from a DuckDB file, ordered by measurement_ts_ms.
fn read_records(db_path: &str, table: &str) -> Vec<RuuviTelemetry> {
    let conn = duckdb::Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT * FROM {} ORDER BY measurement_ts_ms",
            table
        ))
        .unwrap();
    stmt.query_map([], |row| {
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
    .map(|r| r.unwrap())
    .collect()
}

fn count_records(db_path: &str, table: &str) -> i64 {
    let conn = duckdb::Connection::open(db_path).unwrap();
    conn.query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| {
        row.get(0)
    })
    .unwrap()
}

#[tokio::test]
async fn test_write_telemetry_to_file_db_and_read_back() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("telemetry.db");

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "test_telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1(), common::telemetry2()]))
        .await
        .unwrap();

    let records = read_records(db_path.to_str().unwrap(), "test_telemetry");
    assert_eq!(records.len(), 2);

    let mut expected = vec![common::telemetry1(), common::telemetry2()];
    expected.sort_by_key(|t| t.measurement_ts_ms);
    assert_eq!(records, expected);
}

#[tokio::test]
async fn test_create_database_file_and_table() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("new.db");
    assert!(!db_path.exists());

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();

    assert!(db_path.exists());
    let records = read_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(records, vec![common::telemetry1()]);
}

#[tokio::test]
async fn test_create_parent_directories() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nested/path/telemetry.db");
    assert!(!db_path.exists());

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();

    assert!(db_path.exists());
    let records = read_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(records, vec![common::telemetry1()]);
}

#[tokio::test]
async fn test_append_to_existing_database() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("append.db");

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();

    // Second write - should append
    let sink2 = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink2.initialize().await.unwrap();
    sink2
        .write_batch(Arc::from(vec![common::telemetry2()]))
        .await
        .unwrap();

    let records = read_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(records.len(), 2);

    let mut expected = vec![common::telemetry1(), common::telemetry2()];
    expected.sort_by_key(|t| t.measurement_ts_ms);
    assert_eq!(records, expected);
}

#[tokio::test]
async fn test_reject_invalid_table_names() {
    let invalid_names = vec![
        "telemetry; DROP TABLE users--",
        "telemetry'--",
        "telemetry OR 1=1",
        "123invalid",
        "table-name",
        "table.name",
    ];

    for name in invalid_names {
        let result = DuckDBSink::new(":memory:", name, 5, 30, ResourceLimits::default());
        assert!(result.is_err(), "Expected error for table name: '{}'", name);
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("Invalid table name"),
            "Expected 'Invalid table name' in error for '{}', got: {}",
            name,
            err_msg
        );
    }
}

#[tokio::test]
async fn test_batch_10_records_efficiently() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batch10.db");

    let batch: Vec<RuuviTelemetry> = (1..=10)
        .map(|i| RuuviTelemetry {
            battery_potential: 2000 + i,
            humidity: 500000 + i * 1000,
            mac_address: vec![254, 38, 136, 122, 102, i as i16],
            measurement_ts_ms: 1693460525699 + i as i64,
            measurement_sequence_number: 53300 + i,
            movement_counter: i,
            pressure: 100000 + i * 100,
            temperature_millicelsius: 20000 + i * 100,
            tx_power: 4,
        })
        .collect();

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(batch.into()).await.unwrap();

    let count = count_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(count, 10);
}

#[tokio::test]
async fn test_insert_3_records_in_single_batch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batch3.db");

    let batch = vec![
        common::telemetry1(),
        common::telemetry2(),
        RuuviTelemetry {
            battery_potential: 2500,
            humidity: 600000,
            mac_address: vec![100, 200, 50, 75, 125, 150i16],
            measurement_ts_ms: 1693460525702,
            measurement_sequence_number: 2000,
            movement_counter: 100,
            pressure: 101000,
            temperature_millicelsius: 25000,
            tx_power: 4,
        },
    ];

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(batch.clone().into()).await.unwrap();

    let records = read_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(records.len(), 3);

    let mut expected = batch.clone();
    expected.sort_by_key(|t| t.measurement_ts_ms);
    assert_eq!(records, expected);
}

#[tokio::test]
async fn test_pipeline_flushes_partial_batch_at_eof() {
    use ruuvi_data_forwarder_rs::pipeline::run_pipeline;
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("pipeline.db");

    // A finite stream with fewer records than the batch size must flush at EOF.
    let batch = [
        common::telemetry1(),
        common::telemetry2(),
        RuuviTelemetry {
            battery_potential: 2500,
            humidity: 600000,
            mac_address: vec![100, 200, 50, 75, 125, 150i16],
            measurement_ts_ms: 1693460525702,
            measurement_sequence_number: 2000,
            movement_counter: 100,
            pressure: 101000,
            temperature_millicelsius: 25000,
            tx_power: 4,
        },
    ];

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        1,
        ResourceLimits::default(),
    )
    .unwrap();

    let items: Vec<Result<RuuviTelemetry, ruuvi_data_forwarder_rs::error::SourceError>> =
        batch.iter().cloned().map(Ok).collect();
    let source = tokio_stream::iter(items);

    run_pipeline(source, &sink).await.unwrap();

    let count = count_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_in_memory_database_persists_across_worker_commands() {
    let sink = DuckDBSink::new(":memory:", "telemetry", 5, 30, ResourceLimits::default()).unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry2()]))
        .await
        .unwrap();
    sink.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shutdown_is_idempotent() {
    let sink = DuckDBSink::new(":memory:", "telemetry", 5, 30, ResourceLimits::default()).unwrap();
    sink.initialize().await.unwrap();
    sink.write_batch(Arc::from(vec![common::telemetry1()]))
        .await
        .unwrap();
    sink.shutdown().await.unwrap();
    sink.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_repeated_create_write_drop_without_shutdown() {
    // Dropping a sink without shutdown() must still stop and join the worker
    // thread so its DuckDB connection cannot race process teardown.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("churn.db");
    for _ in 0..10 {
        let sink = DuckDBSink::new(
            db_path.to_str().unwrap(),
            "telemetry",
            5,
            30,
            ResourceLimits::default(),
        )
        .unwrap();
        sink.initialize().await.unwrap();
        sink.write_batch(Arc::from(vec![common::telemetry1()]))
            .await
            .unwrap();
        drop(sink);
    }
    let count = count_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(count, 10);
}

#[tokio::test]
async fn test_failed_write_reconnects_on_next_attempt() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("reconnect.db");

    // Pre-create the table with a CHECK constraint the sink's own DDL lacks,
    // so a write can be made to fail deterministically.
    {
        let conn = duckdb::Connection::open(db_path.to_str().unwrap()).unwrap();
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
    }

    let sink = DuckDBSink::new(
        db_path.to_str().unwrap(),
        "telemetry",
        5,
        30,
        ResourceLimits::default(),
    )
    .unwrap();
    sink.initialize().await.unwrap();

    let mut invalid = common::telemetry1();
    invalid.humidity = -1;
    let error = sink
        .write_batch(Arc::from(vec![invalid]))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ruuvi_data_forwarder_rs::error::SinkError::DuckDBError(_)
    ));

    // The worker recycled its connection; the next write must succeed.
    sink.write_batch(Arc::from(vec![common::telemetry2()]))
        .await
        .unwrap();
    sink.shutdown().await.unwrap();

    let records = read_records(db_path.to_str().unwrap(), "telemetry");
    assert_eq!(records, vec![common::telemetry2()]);
}

#[test]
fn test_reject_zero_batch_settings() {
    assert!(DuckDBSink::new(":memory:", "telemetry", 0, 30, ResourceLimits::default()).is_err());
    assert!(DuckDBSink::new(":memory:", "telemetry", 5, 0, ResourceLimits::default()).is_err());
}
