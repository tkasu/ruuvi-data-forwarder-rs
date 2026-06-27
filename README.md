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

Batch size, latency, retry delays, shutdown timeout, memory limits, and thread counts are validated at startup.

## Processing guarantees

- Malformed JSON lines are logged and skipped; stdin I/O failures are fatal.
- A partial batch is flushed after its configured latency, measured from its first record.
- SIGINT, SIGTERM, and EOF flush pending telemetry and close the database connection.
- Retry-safe initialization and write failures use bounded exponential backoff.
- A commit or rollback failure has an unknown transaction outcome and is not retried, avoiding an automatic duplicate write.
- Other retried writes provide at-least-once delivery and consumers should tolerate duplicates.

DuckDB and DuckLake use one persistent connection on a dedicated blocking worker. This supports `:memory:` DuckDB and avoids loading and attaching DuckLake for every batch. DuckLake extensions must be available to DuckDB on first use.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo build --release --all-features
```

The PostgreSQL DuckLake integration test runs when `RUUVI_TEST_POSTGRES_URL` is set. CI supplies a disposable PostgreSQL instance.
