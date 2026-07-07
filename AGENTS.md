# ruuvi-data-forwarder-rs

## Overview

**ruuvi-data-forwarder-rs** is a Rust application built with Tokio that acts as middleware in the Ruuvitag telemetry pipeline. It processes sensor data from various sources and forwards it to configurable targets. Currently implements Console (stdout), DuckDB (database), and DuckLake (lakehouse) sinks with full configuration support.

**Status:** Active Development

**Language:** Rust (edition 2021)

**Runtime:** Tokio 1.x (async I/O and task scheduling)

## Purpose

This utility serves as the data processing and routing layer by:
- Reading sensor telemetry from configurable sources (currently stdin)
- Parsing and validating JSON data into type-safe models
- Batching records by size or time for efficient writes
- Forwarding data to multiple sinks (Console, DuckDB, DuckLake)
- Providing robust error handling and stream processing capabilities

## Architecture

### Tech Stack

- **Async Runtime:** Tokio 1.x (async I/O, timers, spawn_blocking)
- **Serialization:** serde + serde_json (derive macros, snake_case JSON)
- **Database:** duckdb crate with bundled DuckDB C library
- **Configuration:** config crate 0.14 (TOML file + env var overrides)
- **Logging:** tracing + tracing-subscriber (env-filter, writes to stderr)
- **Error Handling:** thiserror 2 (derive macros for error types)
- **Streaming:** tokio-stream + tokio-util (length-capped `LinesCodec` for stdin), futures
- **Build Tool:** Cargo

### Project Structure

```
ruuvi-data-forwarder-rs/
├── Cargo.toml                   # Dependencies and build targets
├── Makefile                     # Build & test commands
├── CLAUDE.md                    # Points to @AGENTS.md
├── AGENTS.md                    # Full project reference (this file)
├── .gitignore                   # Git ignore rules
├── .github/workflows/
│   ├── ci.yml                   # Lint, Test, Build, Integration Tests
│   └── claude.yml               # Claude Code Action
├── config/
│   └── default.toml             # Default configuration (also embedded in the binary)
├── data/
│   └── .gitkeep                 # Keep data directory in git
├── perf_test.sh                 # Single DuckLake performance run
├── perf_matrix.sh               # RPS x batch-size performance matrix
├── test-data.jsonl              # Test data for integration tests
├── src/
│   ├── main.rs                  # Forwarder entry point + sink selection
│   ├── lib.rs                   # Public module exports
│   ├── bin/
│   │   └── ruuvi-ducklake-maintenance.rs  # One-shot DuckLake maintenance binary
│   ├── config.rs                # Config structs + TOML loading + env var overrides
│   ├── dto.rs                   # RuuviTelemetry struct with serde
│   ├── error.rs                 # SourceError + SinkError + PipelineError (thiserror)
│   ├── maintenance.rs           # DuckLake snapshot expiry + CHECKPOINT
│   ├── source.rs                # stdin_source() with line-length cap + validation
│   ├── pipeline.rs              # deadline batching, retries, and bounded graceful shutdown
│   └── sink/
│       ├── mod.rs               # SensorValuesSink trait
│       ├── console.rs           # ConsoleSink (JSON to stdout)
│       ├── duckdb.rs            # DuckDBSink (file-based, batch insert)
│       ├── ducklake.rs          # DuckLakeSink (in-memory DuckDB + ATTACH)
│       └── worker.rs            # DatabaseWorker (dedicated blocking DB thread)
└── tests/
    ├── common/mod.rs            # Shared telemetry fixtures
    ├── config_test.rs           # Config precedence, validation, unknown-key rejection
    ├── console_test.rs          # Console pass-through + broken-pipe (binary subprocess)
    ├── duckdb_test.rs           # DuckDB sink + worker lifecycle tests
    ├── ducklake_test.rs         # DuckLake sink tests (DuckDB/SQLite/Postgres catalogs)
    ├── maintenance_test.rs      # Maintenance binary tests (incl. snapshot expiry)
    └── pipeline_test.rs         # Batching, retry, and shutdown tests (paused clock)
```

### Key Components

**src/main.rs** - Entry point
- Initializes `tracing_subscriber` with env filter (defaults to "info", writes to stderr)
- Loads config via `load_config()`
- Matches on `sink_type` to construct the appropriate sink
- Redacts PostgreSQL catalog paths in startup logs (connection strings may carry credentials)
- Creates `stdin_source()` and runs `run_pipeline_until(source, sink, config, shutdown_signal())`
- Exits via `std::process::exit` after the pipeline finishes: tokio's stdin uses an
  uncancellable blocking read, and letting the runtime drop would hang exit while the
  input pipe is open but idle

**src/dto.rs** - Data model
- `RuuviTelemetry` struct: 9 fields matching Ruuvi RAWv2 format
- All fields snake_case: `temperature_millicelsius`, `humidity`, `pressure`, `battery_potential`, `tx_power`, `movement_counter`, `measurement_sequence_number`, `measurement_ts_ms`, `mac_address`
- `mac_address: Vec<i16>` (matches Scala `Seq[Short]`)
- `mac_address_hex()` method: formats as `"FE:26:88:7A:66:66"` using `(*b as u8)` cast
- Derives `Serialize`, `Deserialize`, `Debug`, `Clone`, `PartialEq`

**src/error.rs** - Error types
- `SourceError`: `ParseError(String)`, `StreamShutdown`, `IoError`
- `SinkError`: `IoError`, `DuckDBError`, `TransactionOutcomeUnknown`, `InvalidTableName`,
  `ConfigError`, `SerializationError`, `WorkerUnavailable`, `WorkerFailed`
- `PipelineError`: `Source`, `Sink`, `ShutdownTimeout`
- `SinkError::is_retryable()`: DuckDB errors and transient IO errors retry;
  `BrokenPipe`/`UnexpectedEof` and unknown transaction outcomes do not
- Uses `thiserror` for `Display`/`Error` derives

**src/sink/mod.rs** - Sink trait
```rust
#[async_trait]
pub trait SensorValuesSink: Send + Sync {
    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError>;
    async fn initialize(&self) -> Result<(), SinkError> { Ok(()) }
    async fn shutdown(&self) -> Result<(), SinkError> { Ok(()) }
    fn desired_batch_size(&self) -> usize;
    fn desired_max_batch_latency(&self) -> Duration;
}
```

**src/sink/console.rs** - Console sink
- Serializes each telemetry to JSON via `serde_json::to_string`, prints to stdout
- `batch_size = 1`, `latency = 86400s` (passthrough, no batching)

**src/sink/duckdb.rs** - DuckDB sink
- `validate_table_name()`: regex `^[a-zA-Z_][a-zA-Z0-9_]*$` for SQL injection protection
- A dedicated blocking worker owns one persistent connection per sink
- `initialize()`: creates parent dirs, opens the connection, and creates the table
- `write_batch()`: sends the batch to the worker without blocking Tokio threads
  - Opens connection, `BEGIN TRANSACTION`
  - Prepared statement with 9 params, iterates batch calling `stmt.execute()`
  - `COMMIT` on success, `ROLLBACK` on failure
  - MAC address stored as hex string via `mac_address_hex()`
- Schema: 9 columns (see `initialize()` for DDL)

**src/sink/ducklake.rs** - DuckLake sink
- `CatalogType` enum: `DuckDB`, `SQLite`, `Postgres`
- `DuckLakeConfig` struct: `catalog_type`, `catalog_path`, `data_path`
- `open_ducklake_connection()`:
  - Resolves paths to absolute using `std::env::current_dir().join()` (not `canonicalize` — paths may not exist yet)
  - Creates parent dirs for catalog and data dir
  - Opens in-memory DuckDB: `Connection::open_in_memory()`
  - `INSTALL ducklake; LOAD ducklake;` (and sqlite/postgres if needed)
  - `ATTACH 'ducklake:[prefix:]<abs_path>' AS ducklake (DATA_PATH '<abs_data_path>')`
- All table refs prefixed with `ducklake.` (e.g., `ducklake.telemetry`)
- Same schema and batch logic as DuckDB sink

**src/sink/worker.rs** - Database worker
- `DatabaseWorker`: a dedicated blocking OS thread owns one DuckDB connection per sink
- Commands (`Initialize`, `Write`, `Shutdown`) are sent over a channel; responses via oneshot
- A failed write drops the cached connection so a retried write reconnects through the factory
- `shutdown()` is idempotent and joins the thread; a `Drop` fallback also stops and joins it
  so process exit never races native DuckDB teardown

**src/maintenance.rs + src/bin/ruuvi-ducklake-maintenance.rs** - DuckLake maintenance
- One-shot binary: sets `expire_older_than` (persisted in the catalog) and runs `CHECKPOINT`
- Requires a SQLite or PostgreSQL catalog (DuckDB catalogs allow only one client)
- Requires existing catalog/data paths; never creates storage

**src/source.rs** - Stdin source
- `stdin_source()`: returns `impl Stream<Item = Result<RuuviTelemetry, SourceError>>`
- Length-capped line framing (`MAX_LINE_BYTES` = 64 KiB); an oversized line is discarded,
  reported as `ParseError`, and the stream recovers at the next newline
- Parses each line as JSON and validates `mac_address` (exactly 6 elements, each 0..=255)
- Yields `SourceError::StreamShutdown` when stdin closes

**src/pipeline.rs** - Batching pipeline
- `run_pipeline_until(source, sink, config, shutdown)` async function
- Deadline-based grouping using `tokio::select!`:
  - Start a deadline when the first record enters an empty batch
  - Stream item: add to batch and flush if `>= batch_size`
  - Parse errors: log and continue (rate-limited after the first few)
  - Stream shutdown / EOF: flush remaining batch, close the sink, exit
  - Retry safe sink failures with bounded exponential backoff
- Shutdown is observed during initialization, in-flight writes, and retry sleeps;
  a single absolute deadline (`shutdown_timeout_seconds`) is armed when the signal
  arrives and bounds all remaining work (`PipelineError::ShutdownTimeout` on overrun)
- A source I/O error flushes buffered records best-effort before returning the source error
- Every error exit performs a time-bounded best-effort sink shutdown

**src/config.rs** - Configuration
- Embedded defaults (`include_str!` of `config/default.toml`) overlaid by an optional
  `config/default` file in the CWD, then `RUUVI_CONFIG_FILE`, then `RUUVI_*` env vars
- Explicit env var overrides for each `RUUVI_*` variable (no HOCON `${?ENV}` equivalent)
- All config structs use `#[serde(deny_unknown_fields)]`: typo'd keys fail startup
- `SinkType`: `Console`, `DuckDB` (lowercase in TOML via serde rename_all)
- `validate()` checks batch settings, retry delays, resource limits, and DuckLake paths

## Data Format

### Input (from stdin)

Newline-delimited JSON matching ruuvi-reader-rs output:

```json
{
  "mac_address": [213, 18, 52, 102, 20, 20],
  "humidity": 570925,
  "temperature_millicelsius": 22005,
  "pressure": 100621,
  "battery_potential": 1941,
  "tx_power": 4,
  "movement_counter": 79,
  "measurement_sequence_number": 559,
  "measurement_ts_ms": 1693460275133
}
```

### Output (Console sink, stdout)

Same JSON format with snake_case field names (validated and re-serialized):

```json
{"temperature_millicelsius":22005,"humidity":570925,"pressure":100621,"battery_potential":1941,"tx_power":4,"movement_counter":79,"measurement_sequence_number":559,"measurement_ts_ms":1693460275133,"mac_address":[213,18,52,102,20,20]}
```

## Building and Running

### Prerequisites

1. **Rust toolchain** (stable)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   rustc --version
   ```

### Build Commands

This project includes a Makefile with standard targets. Run `make help` to see all available commands.

```bash
# Using Make (recommended)
make build           # Compile project (debug)
make build-release   # Build optimized release binary
make test            # Run all unit tests
make lint            # Check formatting (rustfmt) and clippy
make format          # Format code with rustfmt
make clean           # Remove build artifacts
make run             # Run application (reads from stdin)

# Or use Cargo directly
cargo build
cargo build --release
cargo test
cargo fmt --check && cargo clippy -- -D warnings
cargo fmt
```

**Build Output:**
- Release binary: `target/release/ruuvi-data-forwarder-rs`

### Running

**Console Sink (default):**
```bash
# With test data
echo '{"battery_potential":2335,"humidity":653675,"measurement_ts_ms":1693460525701,"mac_address":[254,38,136,122,102,102],"measurement_sequence_number":53300,"movement_counter":2,"pressure":100755,"temperature_millicelsius":-29020,"tx_power":4}' | cargo run

# With ruuvi-reader-rs (live data)
../ruuvi-reader-rs/target/release/ruuvi-reader-rs | ./target/release/ruuvi-data-forwarder-rs

# With file input
cat test-data.jsonl | ./target/release/ruuvi-data-forwarder-rs
```

**DuckDB Sink (Standard Mode):**
```bash
# Write to default DuckDB database (data/telemetry.db)
cat test-data.jsonl | RUUVI_SINK_TYPE=duckdb cargo run

# With custom path and table name
cat test-data.jsonl | \
  RUUVI_SINK_TYPE=duckdb \
  RUUVI_DUCKDB_PATH=/var/lib/ruuvi/telemetry.db \
  RUUVI_DUCKDB_TABLE_NAME=sensor_data \
  ./target/release/ruuvi-data-forwarder-rs

# With custom batch settings
cat test-data.jsonl | \
  RUUVI_SINK_TYPE=duckdb \
  RUUVI_DUCKDB_DESIRED_BATCH_SIZE=100 \
  RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS=60 \
  ./target/release/ruuvi-data-forwarder-rs
```

**DuckLake Sink (Lakehouse Mode):**
```bash
# Write to DuckLake with DuckDB catalog (default for local use)
cat test-data.jsonl | \
  RUUVI_SINK_TYPE=duckdb \
  RUUVI_DUCKDB_DUCKLAKE_ENABLED=true \
  ./target/release/ruuvi-data-forwarder-rs

# With custom catalog and data paths (DuckDB catalog)
cat test-data.jsonl | \
  RUUVI_SINK_TYPE=duckdb \
  RUUVI_DUCKDB_DUCKLAKE_ENABLED=true \
  RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE=duckdb \
  RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH=/var/lib/ruuvi/catalog.ducklake \
  RUUVI_DUCKDB_DUCKLAKE_DATA_PATH=/var/lib/ruuvi/parquet_files/ \
  ./target/release/ruuvi-data-forwarder-rs

# With SQLite catalog (multi-client, local)
cat test-data.jsonl | \
  RUUVI_SINK_TYPE=duckdb \
  RUUVI_DUCKDB_DUCKLAKE_ENABLED=true \
  RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE=sqlite \
  RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH=/var/lib/ruuvi/catalog.sqlite \
  RUUVI_DUCKDB_DUCKLAKE_DATA_PATH=/var/lib/ruuvi/parquet_files/ \
  ./target/release/ruuvi-data-forwarder-rs

# With PostgreSQL catalog (multi-host, distributed)
cat test-data.jsonl | \
  RUUVI_SINK_TYPE=duckdb \
  RUUVI_DUCKDB_DUCKLAKE_ENABLED=true \
  RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE=postgres \
  RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH="dbname=ruuvi_catalog host=localhost user=postgres" \
  RUUVI_DUCKDB_DUCKLAKE_DATA_PATH=/shared/ruuvi/data/ \
  ./target/release/ruuvi-data-forwarder-rs
```

### Testing

**Unit Tests:**
```bash
# Using Make (recommended)
make test            # Run all tests (unit + integration)
make lint            # Check formatting and clippy

# Or use Cargo directly
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```

**Integration Tests:**
```bash
# Test Console sink
make test-console-sink

# Test DuckDB sink
make test-duckdb-sink

# Test DuckLake sink (requires duckdb CLI)
make test-ducklake-sink

# Test all sinks
make test-sinks
```

**Data Management:**
```bash
# Remove all data files (DB, DuckLake)
make clean-data

# Remove only integration test data
make clean-test-data

# Remove only DuckLake catalog and data files
make clean-ducklake-data
```

**Test Coverage:**
- Unit tests: MAC hex formatting, config defaults, error retryability, source
  parsing/validation (line cap, MAC shape), DuckDB rollback and resource limits
- `console_test` - pass-through and broken-pipe handling (binary subprocess)
- `config_test` - config precedence (embedded/file/env), validation failures,
  unknown-key rejection, DuckLake env startup
- `pipeline_test` - batching by size and latency (paused clock), retry recovery
  and exhaustion, unknown-transaction handling, shutdown flush, shutdown during
  hung initialization/writes, source-failure flush
- `duckdb_test` - write/read-back, file and directory creation, append,
  invalid table names, batching, idempotent shutdown, drop-without-shutdown churn,
  reconnect after a failed write
- `ducklake_test` - DuckDB/SQLite catalogs, append, relative paths, and a
  PostgreSQL catalog test gated on `RUUVI_TEST_POSTGRES_URL`
- `maintenance_test` - checkpoint run, snapshot expiry, retention validation,
  DuckDB-catalog rejection, missing-path handling, SQL-injection resistance

## Configuration

Configuration is loaded from `config/default.toml` with environment variable overrides.

**Default Configuration (`config/default.toml`):**
```toml
[pipeline]
max_write_retries = 3
initial_retry_delay_ms = 250
max_retry_delay_ms = 5000
shutdown_timeout_seconds = 30

[sink]
sink_type = "console"

[sink.duckdb]
path = "data/telemetry.db"
table_name = "telemetry"
debug_logging = false
desired_batch_size = 5
desired_max_batch_latency_seconds = 30
ducklake_enabled = false

[sink.duckdb.resource_limits]
# memory_limit = "200MB"
# threads = 2

[sink.duckdb.ducklake]
catalog_type = "sqlite"
catalog_path = "data/ruuvidb.sqlite"
data_path = "data/ducklake_files/"

# Required only by ruuvi-ducklake-maintenance-rs.
# [sink.duckdb.ducklake.maintenance]
# expire_older_than = "1 week"
```

**Environment Variables:**

Core:
- `RUUVI_SINK_TYPE` - Sink type: `console` or `duckdb`
- `RUUVI_CONFIG_FILE` - Optional TOML deployment configuration (must exist if set)

Pipeline:
- `RUUVI_MAX_WRITE_RETRIES`, `RUUVI_INITIAL_RETRY_DELAY_MS`, `RUUVI_MAX_RETRY_DELAY_MS`
- `RUUVI_SHUTDOWN_TIMEOUT_SECONDS` - Bound on flush/close work after a shutdown signal

DuckDB (Standard Mode):
- `RUUVI_DUCKDB_PATH` - Path to DuckDB database file
- `RUUVI_DUCKDB_TABLE_NAME` - Table name for storing telemetry
- `RUUVI_DUCKDB_DEBUG_LOGGING` - Enable/disable debug logging (`true`/`false`)
- `RUUVI_DUCKDB_DESIRED_BATCH_SIZE` - Number of records per batch
- `RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS` - Max seconds before flushing batch
- `RUUVI_DUCKDB_MEMORY_LIMIT` - DuckDB memory limit, e.g. `200MB`
- `RUUVI_DUCKDB_THREADS` - DuckDB thread count

DuckLake (Lakehouse Mode):
- `RUUVI_DUCKDB_DUCKLAKE_ENABLED` - Enable DuckLake mode (`true`/`false`)
- `RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE` - Catalog database: `duckdb`, `sqlite`, or `postgres`
- `RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH` - Path to catalog database file (or connection string for PostgreSQL)
- `RUUVI_DUCKDB_DUCKLAKE_DATA_PATH` - Directory for Parquet data files
- `RUUVI_DUCKDB_DUCKLAKE_MAINTENANCE_EXPIRE_OLDER_THAN` - Snapshot retention for the
  maintenance binary, e.g. `1 week`. Persisted in the catalog by `set_option`, so it
  also applies to any other client that runs `CHECKPOINT`

**DuckLake Catalog Database Options:**
- **DuckDB**: Single-client, file-based. Best for local single-process usage.
- **SQLite**: Multi-client with retry logic. Best for local multi-process usage.
- **PostgreSQL**: Fully distributed. Best for multi-user/multi-host environments.

## Design Decisions

1. **Dedicated DuckDB worker**: One blocking OS thread owns the connection for the sink lifetime, keeping database work off Tokio and supporting `:memory:`.
2. **Deadline-based grouping**: `tokio::select!` flushes a partial batch after a deadline measured from its first record.
3. **`async_trait`**: Enables `async fn` in the `SensorValuesSink` trait while keeping it object-safe for `Box<dyn SensorValuesSink>`.
4. **Separate `DuckDBSink`/`DuckLakeSink` structs**: More idiomatic Rust than a boolean flag; each struct is simpler, shared SQL logic via helper functions.
5. **Explicit env var overrides**: No HOCON `${?ENV}` equivalent in Rust; each env var mapped explicitly in `load_config()`.
6. **Library + binary targets**: `src/lib.rs` exposes modules for integration tests; `src/main.rs` is the binary entry point.

## Adding New Sinks

1. Create `src/sink/<name>.rs` implementing the `SensorValuesSink` trait:

```rust
use async_trait::async_trait;
use std::time::Duration;
use crate::dto::RuuviTelemetry;
use crate::error::SinkError;
use super::SensorValuesSink;

pub struct MySink { /* fields */ }

#[async_trait]
impl SensorValuesSink for MySink {
    async fn initialize(&self) -> Result<(), SinkError> {
        // one-time setup
        Ok(())
    }

    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError> {
        // write records
        Ok(())
    }

    fn desired_batch_size(&self) -> usize { 100 }

    fn desired_max_batch_latency(&self) -> Duration {
        Duration::from_secs(30)
    }
}
```

2. Add `pub mod <name>;` to `src/sink/mod.rs`
3. Add a variant to `SinkType` enum in `src/config.rs` and handle it in `src/main.rs`
4. Add tests in `tests/<name>_test.rs`

## Integration

### Upstream: ruuvi-reader-rs

```bash
../ruuvi-reader-rs/target/release/ruuvi-reader-rs | \
  ./target/release/ruuvi-data-forwarder-rs
```

Receives newline-delimited JSON from BLE scanner.

### Downstream: Analytics

Read DuckDB database directly:
```sql
SELECT * FROM telemetry ORDER BY measurement_ts_ms DESC LIMIT 10;
```

Or query DuckLake parquet files via any DuckDB client.

## Related Projects

- **ruuvi-reader-rs** - Upstream BLE scanner that feeds this forwarder
- **ruuvi-data-forwarder** - Scala/ZIO version of this forwarder

## Contributing

When committing changes made with the help of an AI assistant, use the `Co-authored-by:` trailer:

```
feat: Add new sink type

Implement S3 sink for cloud storage.

Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>
```
