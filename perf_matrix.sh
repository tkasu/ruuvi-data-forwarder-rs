#!/bin/bash
# Run perf_test.sh across RPS x batch-size matrix, print summary table.
#
# Usage: ./perf_matrix.sh [--memory-limit 200MB] [--threads 2]
set -e

MEMORY_LIMIT=""
THREADS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--memory-limit)  MEMORY_LIMIT="$2"; shift 2 ;;
        -t|--threads)       THREADS="$2";      shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

RPSS=(1 5 50)
BATCH_SIZES=(100 200 1000)
BATCH_LATENCY=60
MAX_DURATION=90  # cap so no single test runs > 90s

# Duration: enough to see at least one batch flush (by size or by timer),
# but capped so slow-RPS / large-batch combos don't run forever.
calc_duration() {
    local rps=$1 batch=$2
    local by_size=$(( (batch / rps) + 15 ))
    local by_time=$(( BATCH_LATENCY + 30 ))
    local dur
    (( by_size > by_time )) && dur=$by_size || dur=$by_time
    (( dur > MAX_DURATION )) && dur=$MAX_DURATION
    echo "$dur"
}

LIMIT_ARGS=()
[[ -n "$MEMORY_LIMIT" ]] && LIMIT_ARGS+=(--memory-limit "$MEMORY_LIMIT")
[[ -n "$THREADS" ]]       && LIMIT_ARGS+=(--threads "$THREADS")

echo ""
echo "=== DuckLake Performance Matrix ==="
echo "Limits: mem=${MEMORY_LIMIT:-none} | threads=${THREADS:-none}"
echo ""
printf "%-5s  %-10s  %-8s  %-10s  %-10s  %-10s  %-9s  %-9s  %-9s  %-6s\n" \
    "RPS" "BatchSize" "Dur(s)" "RSS_min" "RSS_max" "RSS_avg" "CPU_min" "CPU_max" "CPU_avg" "Rows"
printf '%0.s-' {1..100}; echo

ERR_FILE=$(mktemp /tmp/ruuvi_perf_matrix_XXXXXX)
trap 'rm -f "$ERR_FILE"' EXIT

for rps in "${RPSS[@]}"; do
    for batch in "${BATCH_SIZES[@]}"; do
        dur=$(calc_duration "$rps" "$batch")
        printf "  [running RPS=%-2s batch=%-4s dur=%-3ss]...\r" "$rps" "$batch" "$dur" >&2

        if ! csv=$(bash perf_test.sh \
            --rps "$rps" \
            --duration "$dur" \
            --batch-size "$batch" \
            --latency "$BATCH_LATENCY" \
            "${LIMIT_ARGS[@]}" \
            --csv 2>"$ERR_FILE"); then
            echo "" >&2
            echo "ERROR: perf_test.sh failed for RPS=$rps batch=$batch:" >&2
            cat "$ERR_FILE" >&2
            exit 1
        fi

        IFS=',' read -r _rps _batch _dur rss_min rss_max rss_avg cpu_min cpu_max cpu_avg rows <<< "$csv"

        printf "%-5s  %-10s  %-8s  %-10s  %-10s  %-10s  %-9s  %-9s  %-9s  %-6s\n" \
            "${rps}" "${batch}" "${_dur}" \
            "${rss_min}MB" "${rss_max}MB" "${rss_avg}MB" \
            "${cpu_min}%" "${cpu_max}%" "${cpu_avg}%" \
            "${rows}"
    done
done

echo ""
