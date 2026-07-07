mod common;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

fn clean_command(directory: &TempDir) -> Command {
    let mut command = Command::cargo_bin("ruuvi-data-forwarder-rs").unwrap();
    command
        .current_dir(directory.path())
        .env_remove("RUUVI_CONFIG_FILE")
        .env_remove("RUUVI_SINK_TYPE")
        .env_remove("RUUVI_DUCKDB_PATH")
        .env_remove("RUUVI_DUCKDB_DESIRED_BATCH_SIZE")
        .env_remove("RUUVI_DUCKDB_DUCKLAKE_ENABLED")
        .env_remove("RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE")
        .env_remove("RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH")
        .env_remove("RUUVI_DUCKDB_DUCKLAKE_DATA_PATH")
        .env_remove("RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN")
        .env_remove("RUUVI_DUCKDB_DEBUG_LOGGING")
        .env_remove("RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS")
        .write_stdin("");
    command
}

#[test]
fn embedded_defaults_work_outside_project_directory() {
    let directory = TempDir::new().unwrap();
    clean_command(&directory)
        .assert()
        .success()
        .stderr(predicate::str::contains("Using Console sink"));
}

#[test]
fn explicit_config_file_overlays_embedded_defaults() {
    let directory = TempDir::new().unwrap();
    let config_path = directory.path().join("deployment.toml");
    let database_path = directory.path().join("configured.db");
    std::fs::write(
        &config_path,
        format!(
            "[sink]\nsink_type = \"duckdb\"\n[sink.duckdb]\npath = {:?}\n",
            database_path.to_string_lossy()
        ),
    )
    .unwrap();

    clean_command(&directory)
        .env("RUUVI_CONFIG_FILE", &config_path)
        .assert()
        .success();
    assert!(database_path.exists());
}

#[test]
fn environment_overrides_explicit_file() {
    let directory = TempDir::new().unwrap();
    let config_path = directory.path().join("deployment.toml");
    let database_path = directory.path().join("must-not-exist.db");
    std::fs::write(
        &config_path,
        format!(
            "[sink]\nsink_type = \"duckdb\"\n[sink.duckdb]\npath = {:?}\n",
            database_path.to_string_lossy()
        ),
    )
    .unwrap();

    clean_command(&directory)
        .env("RUUVI_CONFIG_FILE", &config_path)
        .env("RUUVI_SINK_TYPE", "console")
        .assert()
        .success();
    assert!(!database_path.exists());
}

#[test]
fn unknown_configuration_keys_are_rejected() {
    // A typo must fail startup instead of silently keeping an embedded default.
    let cases = [
        "[pipelinee]\nmax_write_retries = 1\n",
        "[pipeline]\nmax_write_retriess = 1\n",
        "[sink]\nsink_typee = \"console\"\n",
        "[sink.duckdb]\npathh = \"x.db\"\n",
        "[sink.duckdb.resource_limits]\nmemory_limitt = \"64MB\"\n",
        "[sink.duckdb.ducklake]\ncatalog_pathh = \"x.sqlite\"\n",
        "[sink.duckdb.ducklake.maintenance]\nexpire_older_thann = \"1 week\"\n",
    ];
    for contents in cases {
        let directory = TempDir::new().unwrap();
        let config_path = directory.path().join("deployment.toml");
        std::fs::write(&config_path, contents).unwrap();
        clean_command(&directory)
            .env("RUUVI_CONFIG_FILE", &config_path)
            .assert()
            .failure()
            .stderr(predicate::str::contains("unknown field"));
    }
}

#[test]
fn missing_explicit_config_file_is_fatal() {
    let directory = TempDir::new().unwrap();
    clean_command(&directory)
        .env("RUUVI_CONFIG_FILE", directory.path().join("missing.toml"))
        .assert()
        .failure()
        .stderr(predicate::str::contains("configuration file"));
}

#[test]
fn invalid_batch_configuration_is_rejected_before_startup() {
    let directory = TempDir::new().unwrap();
    clean_command(&directory)
        .env("RUUVI_SINK_TYPE", "duckdb")
        .env("RUUVI_DUCKDB_DESIRED_BATCH_SIZE", "0")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "desired_batch_size must be greater than zero",
        ));
}

#[test]
fn ducklake_env_configuration_starts_the_ducklake_sink() {
    let directory = TempDir::new().unwrap();
    let catalog_path = directory.path().join("catalog.sqlite");
    let data_path = directory.path().join("ducklake_files");
    let input = format!(
        "{}\n",
        serde_json::to_string(&common::telemetry1()).unwrap()
    );

    let output = clean_command(&directory)
        .env("RUUVI_SINK_TYPE", "duckdb")
        .env("RUUVI_DUCKDB_DUCKLAKE_ENABLED", "true")
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE", "sqlite")
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH", &catalog_path)
        .env("RUUVI_DUCKDB_DUCKLAKE_DATA_PATH", &data_path)
        .env("RUUVI_DUCKDB_DESIRED_BATCH_SIZE", "1")
        .env("RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS", "1")
        .write_stdin(input)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Using DuckLake sink"), "stderr: {stderr}");
    assert!(stderr.contains("Catalog type: SQLite"), "stderr: {stderr}");
    assert!(catalog_path.exists(), "catalog file was not created");
    assert!(
        data_path.exists(),
        "ducklake data directory was not created"
    );

    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        .unwrap();
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:sqlite:{}' AS ducklake (DATA_PATH '{}')",
        catalog_path.to_string_lossy().replace('\'', "''"),
        data_path.to_string_lossy().replace('\'', "''"),
    ))
    .unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ducklake.telemetry", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
}
