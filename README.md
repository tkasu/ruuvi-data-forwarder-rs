# Ruuvi Data Forwarder (Rust)

An asynchronous newline-delimited JSON forwarder for Ruuvi telemetry. It reads stdin and writes to console, DuckDB, or DuckLake sinks.

## Run

```bash
cargo build --release
cat telemetry.jsonl | ./target/release/ruuvi-data-forwarder-rs
```

Select DuckDB with `RUUVI_SINK_TYPE=duckdb`. Enable DuckLake with `RUUVI_DUCKDB_DUCKLAKE_ENABLED=true`.

## Configuration

Configuration precedence, from lowest to highest, is:

1. Defaults embedded in the binary from `config/default.toml`.
2. An optional `config/default.toml` under the current working directory.
3. The file named by `RUUVI_CONFIG_FILE`; a named file must exist.
4. `RUUVI_*` environment variables.

Core variables:

- `RUUVI_SINK_TYPE`: `console` or `duckdb`.
- `RUUVI_CONFIG_FILE`: optional TOML deployment configuration.
- `RUUVI_MAX_WRITE_RETRIES`, `RUUVI_INITIAL_RETRY_DELAY_MS`, `RUUVI_MAX_RETRY_DELAY_MS`.
- `RUUVI_SHUTDOWN_TIMEOUT_SECONDS`.

DuckDB variables:

- `RUUVI_DUCKDB_PATH`, `RUUVI_DUCKDB_TABLE_NAME`, `RUUVI_DUCKDB_DEBUG_LOGGING`.
- `RUUVI_DUCKDB_DESIRED_BATCH_SIZE`, `RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS`.
- `RUUVI_DUCKDB_MEMORY_LIMIT`, `RUUVI_DUCKDB_THREADS`.

DuckLake variables:

- `RUUVI_DUCKDB_DUCKLAKE_ENABLED`.
- `RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE`: `duckdb`, `sqlite`, or `postgres`.
- `RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH`: a file path or PostgreSQL connection string.
- `RUUVI_DUCKDB_DUCKLAKE_DATA_PATH`.
- `RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN`: required snapshot retention for the maintenance binary, for example `1 week`.

Batch size, latency, retry delays, shutdown timeout, memory limits, and thread counts are validated at startup.

## Processing guarantees

- Malformed JSON lines are logged and skipped; stdin I/O failures are fatal.
- A partial batch is flushed after its configured latency, measured from its first record.
- SIGINT, SIGTERM, and EOF flush pending telemetry and close the database connection.
- Retry-safe initialization and write failures use bounded exponential backoff.
- A commit or rollback failure has an unknown transaction outcome and is not retried, avoiding an automatic duplicate write.
- Other retried writes provide at-least-once delivery and consumers should tolerate duplicates.

DuckDB and DuckLake use one persistent connection on a dedicated blocking worker. This supports `:memory:` DuckDB and avoids loading and attaching DuckLake for every batch. DuckLake extensions must be available to DuckDB on first use.

## DuckLake maintenance

`ruuvi-ducklake-maintenance-rs` runs one maintenance cycle and exits. It sets the explicitly configured snapshot retention and runs DuckLake `CHECKPOINT`, which performs snapshot expiry, file compaction, and old-file cleanup. The streaming forwarder does not run maintenance internally.

Configure retention in the deployment TOML:

```toml
[sink]
sink_type = "duckdb"

[sink.duckdb]
ducklake_enabled = true

[sink.duckdb.ducklake]
catalog_type = "sqlite"
catalog_path = "/var/lib/ruuvi/ruuvidb.sqlite"
data_path = "/var/lib/ruuvi/ducklake_files"

[sink.duckdb.ducklake.maintenance]
expire_older_than = "1 week"
```

The catalog and data directory must already exist. SQLite and PostgreSQL catalogs are supported. DuckDB catalogs are rejected because a DuckDB-backed DuckLake permits only one client and cannot safely overlap the forwarder.

Run one cycle with the same deployment configuration as the forwarder:

```bash
RUUVI_CONFIG_FILE=/etc/ruuvi/forwarder.toml \
  ./target/release/ruuvi-ducklake-maintenance-rs
```

A systemd service and timer can schedule non-overlapping runs:

```ini
# /etc/systemd/system/ruuvi-ducklake-maintenance.service
[Service]
Type=oneshot
Environment=RUUVI_CONFIG_FILE=/etc/ruuvi/forwarder.toml
ExecStart=/usr/local/bin/ruuvi-ducklake-maintenance-rs

# /etc/systemd/system/ruuvi-ducklake-maintenance.timer
[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

For cron, use a lock to prevent overlapping runs:

```cron
0 * * * * flock -n /run/lock/ruuvi-ducklake-maintenance env RUUVI_CONFIG_FILE=/etc/ruuvi/forwarder.toml /usr/local/bin/ruuvi-ducklake-maintenance-rs
```

Success exits with status 0. Configuration, catalog locking, extension loading, or maintenance failures return a nonzero status for the scheduler to report or retry.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo build --release --all-features
```

The release build produces `ruuvi-data-forwarder-rs` and `ruuvi-ducklake-maintenance-rs`.

The PostgreSQL DuckLake integration test runs when `RUUVI_TEST_POSTGRES_URL` is set. CI supplies a disposable PostgreSQL instance.

### Potential CI optimizations

- Use a prebuilt DuckDB dynamic library for Clippy and unit tests. This requires making `duckdb/bundled` an application feature, disabling it for those jobs, and setting `DUCKDB_DOWNLOAD_LIB=1`. Release builds should keep the bundled feature enabled, and binary integration tests should continue exercising that bundled release artifact.
- Tune cross-job Rust caching if repeat builds remain slow. `Swatinem/rust-cache` includes the GitHub job ID in its cache key by default, so a `shared-key` or `add-job-id-key: false` may improve reuse across later workflow runs. Concurrent jobs cannot consume a cache produced during the same run, so build artifacts should still be passed explicitly between dependent jobs.
