.PHONY: help build build-release test lint format clean run run-maintenance create-test-data check-binary test-console-sink test-duckdb-sink test-ducklake-sink test-sinks clean-data clean-test-data clean-ducklake-data

BINARY := target/release/ruuvi-data-forwarder-rs
TEST_DATA := test-data.jsonl

help:
	@echo "Ruuvi Data Forwarder (Rust) - Available targets:"
	@echo ""
	@echo "Build & Development:"
	@echo "  make build           - Compile project (debug)"
	@echo "  make build-release   - Build optimized release binary"
	@echo "  make test            - Run all unit tests"
	@echo "  make lint            - Check code formatting and clippy"
	@echo "  make format          - Format code with rustfmt"
	@echo "  make clean           - Remove build artifacts"
	@echo "  make run             - Run application (reads from stdin)"
	@echo "  make run-maintenance - Run one DuckLake maintenance cycle"
	@echo ""
	@echo "Integration Testing:"
	@echo "  make create-test-data       - Create test data file for integration tests"
	@echo "  make check-binary           - Verify release binary exists"
	@echo "  make test-console-sink      - Test Console sink (stdout)"
	@echo "  make test-duckdb-sink       - Test DuckDB sink (database output)"
	@echo "  make test-ducklake-sink     - Test DuckLake sink (lakehouse output)"
	@echo "  make test-sinks             - Test all sink types"
	@echo ""
	@echo "Data Management:"
	@echo "  make clean-data             - Remove all data files (DB, DuckLake)"
	@echo "  make clean-test-data        - Remove integration test data only"
	@echo "  make clean-ducklake-data    - Remove DuckLake catalog and data files"

build:
	cargo build

build-release:
	cargo build --release

test:
	cargo test --all-targets --all-features

lint:
	cargo fmt --all --check
	cargo clippy --all-targets --all-features -- -D warnings

format:
	cargo fmt

clean:
	cargo clean

run:
	cargo run

run-maintenance:
	cargo run --bin ruuvi-ducklake-maintenance-rs

# Helper target to check if release binary exists
check-binary:
	@if [ ! -f "$(BINARY)" ]; then \
		echo "Release binary not found. Run 'make build-release' first."; \
		exit 1; \
	fi

# Create test data for integration tests
create-test-data:
	@echo "Creating test data file: $(TEST_DATA)"
	@echo '{"battery_potential":2335,"humidity":653675,"measurement_ts_ms":1693460525701,"mac_address":[254,38,136,122,102,102],"measurement_sequence_number":53300,"movement_counter":2,"pressure":100755,"temperature_millicelsius":-29020,"tx_power":4}' > $(TEST_DATA)
	@echo '{"battery_potential":1941,"humidity":570925,"measurement_ts_ms":1693460275133,"mac_address":[213,18,52,102,20,20],"measurement_sequence_number":559,"movement_counter":79,"pressure":100621,"temperature_millicelsius":22005,"tx_power":4}' >> $(TEST_DATA)
	@echo "Test data created: $(TEST_DATA)"

# Integration test targets
test-console-sink: check-binary create-test-data
	@echo "Testing Console sink (stdout)..."
	@cat $(TEST_DATA) | RUUVI_SINK_TYPE=console $(BINARY)
	@echo "Console sink test complete"

test-duckdb-sink: check-binary create-test-data
	@echo "Testing DuckDB sink..."
	@rm -f data/telemetry.db
	@cat $(TEST_DATA) | RUUVI_SINK_TYPE=duckdb $(BINARY)
	@if [ -f data/telemetry.db ]; then \
		echo "Database created: data/telemetry.db"; \
	else \
		echo "Database not created"; \
		exit 1; \
	fi

test-ducklake-sink: check-binary create-test-data
	@echo "Testing DuckLake sink with DuckDB catalog..."
	@echo "Cleaning up old test data..."
	@rm -f data/test_catalog.ducklake
	@rm -rf data/test_ducklake_files/
	@cat $(TEST_DATA) | \
		RUUVI_SINK_TYPE=duckdb \
		RUUVI_DUCKDB_DUCKLAKE_ENABLED=true \
		RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE=duckdb \
		RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH=data/test_catalog.ducklake \
		RUUVI_DUCKDB_DUCKLAKE_DATA_PATH=data/test_ducklake_files/ \
		$(BINARY)
	@if [ -f data/test_catalog.ducklake ]; then \
		echo "Catalog created: data/test_catalog.ducklake"; \
		if [ -d data/test_ducklake_files ]; then \
			echo "Data directory created: data/test_ducklake_files/"; \
			ROW_COUNT=$$(duckdb :memory: -csv -noheader -c "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:$(CURDIR)/data/test_catalog.ducklake' AS my_ducklake (DATA_PATH '$(CURDIR)/data/test_ducklake_files/'); SELECT COUNT(*) FROM my_ducklake.telemetry;" 2>/dev/null); \
			if [ "$$ROW_COUNT" = "2" ]; then \
				echo "Data verified: $$ROW_COUNT rows found in telemetry table"; \
			else \
				echo "Expected 2 rows, but found: $$ROW_COUNT"; \
				exit 1; \
			fi; \
		else \
			echo "Data directory not created"; \
			exit 1; \
		fi; \
	else \
		echo "Catalog not created"; \
		exit 1; \
	fi

test-sinks: test-console-sink test-duckdb-sink test-ducklake-sink
	@echo ""
	@echo "====================================="
	@echo "All sink tests passed successfully"
	@echo "====================================="

# Data cleanup commands
clean-data:
	@echo "Cleaning all data files..."
	@rm -f data/telemetry.db data/catalog.ducklake
	@rm -rf data/ducklake_files/
	@rm -f data/test_catalog.ducklake
	@rm -rf data/test_ducklake_files/
	@echo "All data files removed"

clean-test-data:
	@echo "Cleaning integration test data..."
	@rm -f data/test_catalog.ducklake
	@rm -rf data/test_ducklake_files/
	@echo "Integration test data removed"

clean-ducklake-data:
	@echo "Cleaning DuckLake catalog and data files..."
	@rm -f data/catalog.ducklake
	@rm -rf data/ducklake_files/
	@echo "DuckLake data removed"
