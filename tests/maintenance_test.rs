use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

fn maintenance_command(directory: &TempDir) -> Command {
    let mut command = Command::cargo_bin("ruuvi-ducklake-maintenance-rs").unwrap();
    command
        .current_dir(directory.path())
        .env_remove("RUUVI_CONFIG_FILE")
        .env_remove("RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN")
        .env("RUUVI_SINK_TYPE", "duckdb")
        .env("RUUVI_DUCKDB_DUCKLAKE_ENABLED", "true")
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE", "sqlite");
    command
}

fn seed_ducklake(directory: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let catalog = directory.path().join("catalog.sqlite");
    let data = directory.path().join("ducklake_files");
    std::fs::create_dir(&data).unwrap();
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        .unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS ducklake (DATA_PATH '{}'); \
             CREATE TABLE ducklake.telemetry (value INTEGER); \
             INSERT INTO ducklake.telemetry VALUES (1);",
            sql_path(&catalog),
            sql_path(&data)
        ))
        .unwrap();
    (catalog, data)
}

fn row_count(catalog: &std::path::Path, data: &std::path::Path) -> i64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        .unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS ducklake (DATA_PATH '{}')",
            sql_path(catalog),
            sql_path(data)
        ))
        .unwrap();
    connection
        .query_row("SELECT COUNT(*) FROM ducklake.telemetry", [], |row| {
            row.get(0)
        })
        .unwrap()
}

fn sql_path(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn snapshot_count(catalog: &std::path::Path, data: &std::path::Path) -> i64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        .unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS ducklake (DATA_PATH '{}')",
            sql_path(catalog),
            sql_path(data)
        ))
        .unwrap();
    connection
        .query_row(
            "SELECT COUNT(*) FROM ducklake_snapshots('ducklake')",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn checkpoint_runs_against_an_existing_sqlite_ducklake() {
    let directory = TempDir::new().unwrap();
    let (catalog, data) = seed_ducklake(&directory);

    maintenance_command(&directory)
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH", &catalog)
        .env("RUUVI_DUCKDB_DUCKLAKE_DATA_PATH", &data)
        .env(
            "RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN",
            "1 week",
        )
        .assert()
        .success()
        .stderr(predicate::str::contains("DuckLake maintenance completed"));

    assert_eq!(row_count(&catalog, &data), 1);
}

#[test]
fn checkpoint_expires_old_snapshots() {
    let directory = TempDir::new().unwrap();
    let (catalog, data) = seed_ducklake(&directory);

    // Additional inserts create additional snapshots.
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        .unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS ducklake (DATA_PATH '{}'); \
             INSERT INTO ducklake.telemetry VALUES (2); \
             INSERT INTO ducklake.telemetry VALUES (3);",
            sql_path(&catalog),
            sql_path(&data)
        ))
        .unwrap();
    drop(connection);

    let before = snapshot_count(&catalog, &data);
    assert!(before >= 3, "expected at least 3 snapshots, got {before}");

    // Age every snapshot past the retention used below.
    std::thread::sleep(std::time::Duration::from_millis(1500));

    maintenance_command(&directory)
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH", &catalog)
        .env("RUUVI_DUCKDB_DUCKLAKE_DATA_PATH", &data)
        .env(
            "RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN",
            "1 second",
        )
        .assert()
        .success()
        .stderr(predicate::str::contains("DuckLake maintenance completed"));

    let after = snapshot_count(&catalog, &data);
    assert!(
        after < before,
        "expected snapshots to be expired: before={before}, after={after}"
    );
    // The latest state must survive expiry.
    assert_eq!(row_count(&catalog, &data), 3);
}

#[test]
fn explicit_retention_is_required() {
    let directory = TempDir::new().unwrap();
    maintenance_command(&directory)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "maintenance.expire_older_than must be configured",
        ));
}

#[test]
fn duckdb_catalogs_are_rejected() {
    let directory = TempDir::new().unwrap();
    maintenance_command(&directory)
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE", "duckdb")
        .env(
            "RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN",
            "1 week",
        )
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "DuckDB catalogs because they allow only one client",
        ));
}

#[test]
fn missing_paths_fail_without_creating_storage() {
    let directory = TempDir::new().unwrap();
    let catalog = directory.path().join("missing/catalog.sqlite");
    let data = directory.path().join("missing/data");

    maintenance_command(&directory)
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH", &catalog)
        .env("RUUVI_DUCKDB_DUCKLAKE_DATA_PATH", &data)
        .env(
            "RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN",
            "1 week",
        )
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "DuckLake data directory does not exist",
        ));

    assert!(!catalog.exists());
    assert!(!data.exists());
}

#[test]
fn retention_is_bound_as_data_not_executed_as_sql() {
    let directory = TempDir::new().unwrap();
    let (catalog, data) = seed_ducklake(&directory);

    maintenance_command(&directory)
        .env("RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH", &catalog)
        .env("RUUVI_DUCKDB_DUCKLAKE_DATA_PATH", &data)
        .env(
            "RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN",
            "1 week'); DROP TABLE ducklake.telemetry; --",
        )
        .assert()
        .failure();

    assert_eq!(row_count(&catalog, &data), 1);
}
