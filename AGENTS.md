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
- **Streaming:** tokio-stream (LinesStream for stdin), futures
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
│   └── default.toml             # Default configuration
├── data/
│   └── .gitkeep                 # Keep data directory in git
├── test-data.jsonl              # Test data for integration tests
├── src/
│   ├── main.rs                  # Entry point + sink selection
│   ├── lib.rs                   # Public module exports
│   ├── config.rs                # Config structs + TOML loading + env var overrides
│   ├── dto.rs                   # RuuviTelemetry struct with serde
│   ├── error.rs                 # SourceError + SinkError (thiserror)
│   ├── source.rs                # stdin_source() using tokio BufReader + LinesStream
│   ├── pipeline.rs              # deadline batching, retries, and graceful shutdown
│   └── sink/
│       ├── mod.rs               # SensorValuesSink trait
│       ├── console.rs           # ConsoleSink (JSON to stdout)
│       ├── duckdb.rs            # DuckDBSink (file-based, batch insert)
│       └── ducklake.rs          # DuckLakeSink (in-memory DuckDB + ATTACH)
└── tests/
    ├── console_test.rs          # Console pass-through (binary subprocess)
    ├── duckdb_test.rs           # 8 DuckDB tests
    └── ducklake_test.rs         # 3 DuckLake tests
```

### Key Components

**src/main.rs** - Entry point
- Initializes `tracing_subscriber` with env filter (defaults to "info", writes to stderr)
- Loads config via `load_config()`
- Matches on `sink_type` to construct the appropriate sink
- Creates `stdin_source()` and runs `run_pipeline(source, sink)`

**src/dto.rs** - Data model
- `RuuviTelemetry` struct: 9 fields matching Ruuvi RAWv2 format
- All fields snake_case: `temperature_millicelsius`, `humidity`, `pressure`, `battery_potential`, `tx_power`, `movement_counter`, `measurement_sequence_number`, `measurement_ts_ms`, `mac_address`
- `mac_address: Vec<i16>` (matches Scala `Seq[Short]`)
- `mac_address_hex()` method: formats as `"FE:26:88:7A:66:66"` using `(*b as u8)` cast
- Derives `Serialize`, `Deserialize`, `Debug`, `Clone`, `PartialEq`

**src/error.rs** - Error types
- `SourceError`: `ParseError(String)`, `StreamShutdown`, `IoError`
- `SinkError`: `IoError`, `DuckDBError`, `InvalidTableName`, `ConfigError`, `JoinError`
- Uses `thiserror` for `Display`/`Error` derives

**src/sink/mod.rs** - Sink trait
```rust
#[async_trait]
pub trait SensorValuesSink: Send + Sync {
    async fn write_batch(&self, batch: Arc<[RuuviTelemetry]>) -> Result<(), SinkError>;
    async fn initialize(&self) -> Result<(), SinkError> { Ok(()) }
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

**src/source.rs** - Stdin source
- `stdin_source()`: returns `impl Stream<Item = Result<RuuviTelemetry, SourceError>>`
- Uses `tokio::io::BufReader + LinesStream`
- Parses each line as JSON, yields `SourceError::ParseError` on failure
- Yields `SourceError::StreamShutdown` when stdin closes

**src/pipeline.rs** - Batching pipeline
- `run_pipeline_until(source, sink, config, shutdown)` async function
- Deadline-based grouping using `tokio::select!`:
  - Start a deadline when the first record enters an empty batch
  - Stream item: add to batch and flush if `>= batch_size`
  - Parse errors: log and continue
  - Stream shutdown: flush remaining batch, break
  - Deadline, EOF, or shutdown: flush if non-empty
  - Retry safe sink failures with bounded exponential backoff

**src/config.rs** - Configuration
- Loads `config/default.toml` via `config` crate
- Explicit env var overrides for each `RUUVI_*` variable (no HOCON `${?ENV}` equivalent)
- `SinkType`: `Console`, `DuckDB` (lowercase in TOML via serde rename_all)

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
- Unit and integration coverage for configuration, console, pipeline, DuckDB, and DuckLake
- `dto::tests::test_mac_address_hex` - MAC address hex formatting
- `console_sink_passes_through_values` - Console sink binary subprocess test
- `duckdb_test` - 8 tests:
  - Write and read back telemetry (verify all fields)
  - Create database file and table automatically
  - Create parent directories for nested DB path
  - Append to existing database
  - Reject invalid table names (SQL injection protection)
  - Batch 10 records efficiently
  - Insert 3 records in single batch
  - Pipeline batching by size and time (timeout-based flush)
- `ducklake_test` - 3 tests:
  - Write with DuckDB catalog, read back and verify
  - Append to existing DuckLake catalog
  - Relative path resolution

## Configuration

Configuration is loaded from `config/default.toml` with environment variable overrides.

**Default Configuration (`config/default.toml`):**
```toml
[sink]
sink_type = "console"

[sink.duckdb]
path = "data/telemetry.db"
table_name = "telemetry"
debug_logging = false
desired_batch_size = 5
desired_max_batch_latency_seconds = 30
ducklake_enabled = false

[sink.duckdb.ducklake]
catalog_type = "duckdb"
catalog_path = "data/catalog.ducklake"
data_path = "data/ducklake_files/"
```

**Environment Variables:**

Core:
- `RUUVI_SINK_TYPE` - Sink type: `console` or `duckdb`

DuckDB (Standard Mode):
- `RUUVI_DUCKDB_PATH` - Path to DuckDB database file
- `RUUVI_DUCKDB_TABLE_NAME` - Table name for storing telemetry
- `RUUVI_DUCKDB_DEBUG_LOGGING` - Enable/disable debug logging (`true`/`false`)
- `RUUVI_DUCKDB_DESIRED_BATCH_SIZE` - Number of records per batch
- `RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS` - Max seconds before flushing batch

DuckLake (Lakehouse Mode):
- `RUUVI_DUCKDB_DUCKLAKE_ENABLED` - Enable DuckLake mode (`true`/`false`)
- `RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE` - Catalog database: `duckdb`, `sqlite`, or `postgres`
- `RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH` - Path to catalog database file (or connection string for PostgreSQL)
- `RUUVI_DUCKDB_DUCKLAKE_DATA_PATH` - Directory for Parquet data files

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
