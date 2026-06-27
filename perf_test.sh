#!/bin/bash
# Performance test: configurable RPS, DuckLake sink
#
# Usage: ./perf_test.sh [OPTIONS]
#   -r, --rps           Events per second (default: 1)
#   -d, --duration      Test duration in seconds (default: 130)
#   -b, --batch-size    Batch size (default: 100)
#   -l, --latency       Max batch latency in seconds (default: 60)
#   -c, --catalog-type  DuckLake catalog type: duckdb|sqlite|postgres (default: duckdb)
#   -h, --help          Show this help

set -e

# Defaults
RPS=1
DURATION=130
BATCH_SIZE=100
BATCH_LATENCY=60
CATALOG_TYPE="duckdb"
MEMORY_LIMIT=""
THREADS=""
CSV_OUTPUT=false   # when true: suppress progress output, print one CSV line at end

usage() {
    sed -n '2,8p' "$0" | sed 's/^# //'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--rps)           RPS="$2";          shift 2 ;;
        -d|--duration)      DURATION="$2";     shift 2 ;;
        -b|--batch-size)    BATCH_SIZE="$2";   shift 2 ;;
        -l|--latency)       BATCH_LATENCY="$2"; shift 2 ;;
        -c|--catalog-type)  CATALOG_TYPE="$2"; shift 2 ;;
        -m|--memory-limit)  MEMORY_LIMIT="$2"; shift 2 ;;
        -t|--threads)       THREADS="$2";      shift 2 ;;
        --csv)              CSV_OUTPUT=true; shift ;;
        -h|--help)          usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Derived values
SLEEP_INTERVAL=$(awk "BEGIN { printf \"%.6f\", 1/$RPS }")

BINARY="target/release/ruuvi-data-forwarder-rs"
TEST_EVENT='{"battery_potential":2335,"humidity":653675,"measurement_ts_ms":1693460525701,"mac_address":[254,38,136,122,102,102],"measurement_sequence_number":53300,"movement_counter":2,"pressure":100755,"temperature_millicelsius":-29020,"tx_power":4}'

# Use a unique run ID so parallel invocations don't share paths
RUN_ID="${RPS}_${BATCH_SIZE}_$$"
CATALOG_PATH="data/perf_test_${RUN_ID}_catalog.ducklake"
DATA_PATH="data/perf_test_${RUN_ID}_ducklake/"
LOG_FILE="/tmp/ruuvi_perf_${RUN_ID}.log"

if ! $CSV_OUTPUT; then
echo "=== Ruuvi DuckLake Performance Test ==="
echo "Rate:           ${RPS} RPS (sleep ${SLEEP_INTERVAL}s)"
echo "Duration:       ${DURATION}s"
echo "Batch size:     ${BATCH_SIZE} items"
echo "Batch latency:  ${BATCH_LATENCY}s"
echo "Catalog type:   ${CATALOG_TYPE}"
echo "Memory limit:   ${MEMORY_LIMIT:-none}"
echo "Threads:        ${THREADS:-none}"
echo ""
fi

# Cleanup previous test artifacts
rm -f "$CATALOG_PATH"
rm -rf "$DATA_PATH"
rm -f "$LOG_FILE"

if [ ! -f "$BINARY" ]; then
    echo "ERROR: Binary not found at $BINARY. Run 'make build-release' first."
    exit 1
fi

# Create a named pipe for stdin
FIFO=$(mktemp -u /tmp/ruuvi_perf_XXXXXX)
mkfifo "$FIFO"

# Start the binary (conditionally set resource limits)
env_args=(
    RUUVI_SINK_TYPE=duckdb
    RUUVI_DUCKDB_DUCKLAKE_ENABLED=true
    RUUVI_DUCKDB_DUCKLAKE_CATALOG_TYPE="$CATALOG_TYPE"
    RUUVI_DUCKDB_DUCKLAKE_CATALOG_PATH="$CATALOG_PATH"
    RUUVI_DUCKDB_DUCKLAKE_DATA_PATH="$DATA_PATH"
    RUUVI_DUCKDB_DESIRED_BATCH_SIZE="$BATCH_SIZE"
    RUUVI_DUCKDB_DESIRED_MAX_BATCH_LATENCY_SECONDS="$BATCH_LATENCY"
    RUST_LOG=info
)
[[ -n "$MEMORY_LIMIT" ]] && env_args+=(RUUVI_DUCKDB_MEMORY_LIMIT="$MEMORY_LIMIT")
[[ -n "$THREADS" ]]       && env_args+=(RUUVI_DUCKDB_THREADS="$THREADS")

env "${env_args[@]}" "$BINARY" < "$FIFO" >"$LOG_FILE" 2>&1 &

BINARY_PID=$!
$CSV_OUTPUT || echo "Binary PID: $BINARY_PID"

# Open FIFO for writing
exec 3>"$FIFO"

# Give binary a moment to initialize
sleep 0.5

if ! kill -0 "$BINARY_PID" 2>/dev/null; then
    echo "ERROR: Binary failed to start. Log:"
    cat "$LOG_FILE"
    rm -f "$FIFO"
    exit 1
fi

$CSV_OUTPUT || { echo "Starting event loop..."; echo ""; }

declare -a RSS_SAMPLES=()
declare -a VSZ_SAMPLES=()
declare -a CPU_SAMPLES=()
EVENTS_SENT=0
START_TIME=$(date +%s)
ITERATION=0

while true; do
    ITERATION=$((ITERATION + 1))
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TIME))
    (( ELAPSED >= DURATION )) && break

    # Send one event
    echo "$TEST_EVENT" >&3
    EVENTS_SENT=$((EVENTS_SENT + 1))

    # Sample metrics
    if kill -0 "$BINARY_PID" 2>/dev/null; then
        read -r RSS VSZ CPU <<< "$(ps -p "$BINARY_PID" -o rss=,vsz=,pcpu= 2>/dev/null || echo '0 0 0')"
        RSS_SAMPLES+=("${RSS:-0}")
        VSZ_SAMPLES+=("${VSZ:-0}")
        CPU_SAMPLES+=("${CPU:-0}")
    fi

    # Progress update every 10 events (suppressed in CSV mode)
    if ! $CSV_OUTPUT && (( EVENTS_SENT % 10 == 0 )); then
        CURRENT_RSS=${RSS_SAMPLES[-1]:-0}
        echo "  t=${ELAPSED}s | events: ${EVENTS_SENT} | RSS: $((CURRENT_RSS / 1024)) MB | CPU: ${CPU}%"
    fi

    sleep "$SLEEP_INTERVAL"
done

$CSV_OUTPUT || echo ""
$CSV_OUTPUT || echo "Closing stdin (sending EOF)..."
exec 3>&-

wait "$BINARY_PID" 2>/dev/null || true
END_TIME=$(date +%s)
rm -f "$FIFO"

TOTAL_TIME=$((END_TIME - START_TIME))

# ---- Statistics ----

# Integer stats (RSS, VSZ in KB)
int_stats() {
    local -n arr=$1
    local count=${#arr[@]}
    (( count == 0 )) && { echo "0 0 0"; return; }
    local min="${arr[0]}" max="${arr[0]}" sum=0
    for v in "${arr[@]}"; do
        (( v < min )) && min=$v
        (( v > max )) && max=$v
        sum=$((sum + v))
    done
    echo "$min $max $((sum / count))"
}

read -r RSS_MIN RSS_MAX RSS_AVG <<< "$(int_stats RSS_SAMPLES)"
read -r VSZ_MIN VSZ_MAX VSZ_AVG <<< "$(int_stats VSZ_SAMPLES)"

CPU_STATS=$(printf '%s\n' "${CPU_SAMPLES[@]:-0}" | awk '
    NR==1 { min=$1; max=$1; sum=$1; n=1 }
    NR>1  { if($1<min) min=$1; if($1>max) max=$1; sum+=$1; n++ }
    END   { if(n>0) printf "%.1f %.1f %.1f", min, max, sum/n; else print "0 0 0" }
')
read -r CPU_MIN CPU_MAX CPU_AVG <<< "$CPU_STATS"

# Row count verification
ROW_COUNT="?"
if command -v duckdb &>/dev/null && [ -f "$CATALOG_PATH" ]; then
    ROW_COUNT=$(duckdb :memory: -csv -noheader -c \
        "INSTALL ducklake; LOAD ducklake; \
         ATTACH 'ducklake:$(pwd)/${CATALOG_PATH}' AS dl (DATA_PATH '$(pwd)/${DATA_PATH}'); \
         SELECT COUNT(*) FROM dl.telemetry;" 2>/dev/null || echo "?")
fi

if $CSV_OUTPUT; then
    # Machine-readable: rps,batch,dur,rss_min_mb,rss_max_mb,rss_avg_mb,cpu_min,cpu_max,cpu_avg,rows
    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$RPS" "$BATCH_SIZE" "$TOTAL_TIME" \
        "$((RSS_MIN / 1024))" "$((RSS_MAX / 1024))" "$((RSS_AVG / 1024))" \
        "$CPU_MIN" "$CPU_MAX" "$CPU_AVG" \
        "$ROW_COUNT"
else
    echo "=== Results ==="
    echo "Config:         ${RPS} RPS | batch=${BATCH_SIZE} or ${BATCH_LATENCY}s | catalog=${CATALOG_TYPE} | mem=${MEMORY_LIMIT:-unlimited} | threads=${THREADS:-unlimited}"
    echo "Total time:     ${TOTAL_TIME}s"
    echo "Events sent:    ${EVENTS_SENT}"
    echo "Samples taken:  ${#RSS_SAMPLES[@]}"
    echo ""
    echo "Memory RSS (resident/physical):"
    printf "  Min: %d MB (%d KB)\n"  "$((RSS_MIN / 1024))" "$RSS_MIN"
    printf "  Max: %d MB (%d KB)\n"  "$((RSS_MAX / 1024))" "$RSS_MAX"
    printf "  Avg: %d MB (%d KB)\n"  "$((RSS_AVG / 1024))" "$RSS_AVG"
    echo ""
    echo "Memory VSZ (virtual):"
    printf "  Min: %d MB (%d KB)\n"  "$((VSZ_MIN / 1024))" "$VSZ_MIN"
    printf "  Max: %d MB (%d KB)\n"  "$((VSZ_MAX / 1024))" "$VSZ_MAX"
    printf "  Avg: %d MB (%d KB)\n"  "$((VSZ_AVG / 1024))" "$VSZ_AVG"
    echo ""
    echo "CPU usage (%):"
    echo "  Min: ${CPU_MIN}%"
    echo "  Max: ${CPU_MAX}%"
    echo "  Avg: ${CPU_AVG}%"
    echo ""
    echo "DuckLake row count: ${ROW_COUNT} (expected ~${EVENTS_SENT})"
    echo ""
    echo "Binary stderr (last 5 lines):"
    tail -5 "$LOG_FILE" 2>/dev/null || echo "(empty)"
fi
