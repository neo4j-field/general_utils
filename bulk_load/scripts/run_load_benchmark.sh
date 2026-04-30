#!/usr/bin/env bash
#
# run_load_benchmark.sh
# End-to-end Neo4j bulk load benchmark: schema -> generate -> load -> report.
#
# Mirrors the credentials-file convention used by the rest of general_utils.
# All knobs are env vars so you can sweep without editing the script.
#
# Usage:
#   ./run_load_benchmark.sh <credentials_file>
#   ./run_load_benchmark.sh <username> <password> <uri>
#
# Tunable env vars:
#   PARQUET_DIR     Default: $HOME/data/parquet
#   BATCH_SIZE      Default: 5000
#   PARTITIONS      Default: 8
#   NODE_MODE       Default: merge   (merge | create)
#   SCALE           Default: 1.0     (multiplier on all volumes; 0.01 for a smoke test)
#   SKIP_GENERATE   Default: false   (re-use existing parquet)
#   SKIP_SCHEMA     Default: false
#   SKIP_LOAD       Default: false
#   RESULTS_DIR     Default: $HOME/results
#   JAR             Default: $NEO4J_SPARK_CONNECTOR_JAR (set by provision_vm.sh)
#   VENV            Default: $HOME/.venv-bulkload
#   CONFIG          Default: <repo>/bulk_load/config/data_model.yaml

set -euo pipefail

# ---------- Argument parsing (same shape as load_benchmark_data.sh) ----------
if [[ "$#" -eq 1 ]]; then
    CRED_FILE="$1"
    [[ -f "$CRED_FILE" ]] || { echo "Error: credentials file '$CRED_FILE' not found." >&2; exit 1; }
    parse_cred() {
        grep "^$1=" "$CRED_FILE" | head -n 1 | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g'
    }
    URI=$(parse_cred "NEO4J_URI")
    USERNAME=$(parse_cred "NEO4J_USERNAME")
    PASSWORD=$(parse_cred "NEO4J_PASSWORD")
    DATABASE=$(parse_cred "NEO4J_DATABASE"); DATABASE="${DATABASE:-neo4j}"
elif [[ "$#" -eq 3 ]]; then
    USERNAME="$1"; PASSWORD="$2"; URI="$3"
    DATABASE="${DATABASE:-neo4j}"
    CRED_FILE=""
else
    echo "Usage: $0 <credentials_file>   OR   $0 <username> <password> <aura_uri>" >&2
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MODULE_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

PARQUET_DIR="${PARQUET_DIR:-$HOME/data/parquet}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
PARTITIONS="${PARTITIONS:-8}"
REL_PARTITIONS="${REL_PARTITIONS:-1}"
HOT_REL_THRESHOLD="${HOT_REL_THRESHOLD:-1000}"
NODE_MODE="${NODE_MODE:-merge}"
SCALE="${SCALE:-1.0}"
SKIP_GENERATE="${SKIP_GENERATE:-false}"
SKIP_SCHEMA="${SKIP_SCHEMA:-false}"
SKIP_LOAD="${SKIP_LOAD:-false}"
RESULTS_DIR="${RESULTS_DIR:-$HOME/results}"
JAR="${JAR:-${NEO4J_SPARK_CONNECTOR_JAR:-}}"
VENV="${VENV:-$HOME/.venv-bulkload}"
CONFIG="${CONFIG:-$MODULE_DIR/config/data_model.yaml}"

if [[ -z "$JAR" || ! -f "$JAR" ]]; then
    echo "Error: Spark Connector jar not found. Set JAR or NEO4J_SPARK_CONNECTOR_JAR." >&2
    exit 1
fi
if [[ ! -f "$VENV/bin/python" ]]; then
    echo "Error: virtualenv not found at $VENV. Run provision_vm.sh first." >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)_b${BATCH_SIZE}_p${PARTITIONS}_${NODE_MODE}"
RESULTS_CSV="$RESULTS_DIR/load_${RUN_ID}.csv"
RUN_LOG="$RESULTS_DIR/load_${RUN_ID}.log"

echo "============================================================"
echo "Neo4j bulk load benchmark"
echo "============================================================"
echo "  URI:          $URI"
echo "  Database:     $DATABASE"
echo "  Config:       $CONFIG"
echo "  Parquet dir:  $PARQUET_DIR"
echo "  Batch size:   $BATCH_SIZE"
echo "  Node parts:   $PARTITIONS"
echo "  Rel parts:    $REL_PARTITIONS  (hot threshold: $HOT_REL_THRESHOLD)"
echo "  Node mode:    $NODE_MODE"
echo "  Scale:        $SCALE"
echo "  Results CSV:  $RESULTS_CSV"
echo "  Run log:      $RUN_LOG"
echo

PYTHON="$VENV/bin/python"
TOTAL_START=$SECONDS

# ---------- 1. Generate ----------
if [[ "$SKIP_GENERATE" != "true" ]]; then
    echo "[1/3] Generating synthetic parquet..."
    GEN_START=$SECONDS
    "$PYTHON" "$MODULE_DIR/src/generate_synthetic_data.py" \
        --config "$CONFIG" \
        --output "$PARQUET_DIR" \
        --scale "$SCALE" 2>&1 | tee -a "$RUN_LOG"
    echo "  generation: $((SECONDS - GEN_START))s"
    echo
else
    echo "[1/3] SKIP generate (using existing $PARQUET_DIR)"
fi

# ---------- 2. Schema ----------
if [[ "$SKIP_SCHEMA" != "true" ]]; then
    echo "[2/3] Setting up schema (unique constraints)..."
    SCHEMA_START=$SECONDS
    if [[ -n "${CRED_FILE:-}" ]]; then
        "$PYTHON" "$MODULE_DIR/src/schema_setup.py" \
            --config "$CONFIG" \
            --credentials "$CRED_FILE" 2>&1 | tee -a "$RUN_LOG"
    else
        "$PYTHON" "$MODULE_DIR/src/schema_setup.py" \
            --config "$CONFIG" \
            --uri "$URI" --user "$USERNAME" --password "$PASSWORD" \
            --database "$DATABASE" 2>&1 | tee -a "$RUN_LOG"
    fi
    echo "  schema setup: $((SECONDS - SCHEMA_START))s"
    echo
else
    echo "[2/3] SKIP schema setup"
fi

# ---------- 3. Load ----------
if [[ "$SKIP_LOAD" != "true" ]]; then
    echo "[3/3] Loading parquet -> Neo4j..."
    LOAD_START=$SECONDS
    LOAD_CMD=(
        "$PYTHON" "$MODULE_DIR/src/load_to_neo4j.py"
        --config "$CONFIG"
        --parquet-dir "$PARQUET_DIR"
        --batch-size "$BATCH_SIZE"
        --partitions "$PARTITIONS"
        --rel-partitions "$REL_PARTITIONS"
        --hot-rel-threshold "$HOT_REL_THRESHOLD"
        --node-mode "$NODE_MODE"
        --jar "$JAR"
        --results-csv "$RESULTS_CSV"
    )
    if [[ -n "${CRED_FILE:-}" ]]; then
        LOAD_CMD+=( --credentials "$CRED_FILE" )
    else
        LOAD_CMD+=( --uri "$URI" --user "$USERNAME" --password "$PASSWORD" --database "$DATABASE" )
    fi
    "${LOAD_CMD[@]}" 2>&1 | tee -a "$RUN_LOG"
    echo "  load: $((SECONDS - LOAD_START))s"
    echo
else
    echo "[3/3] SKIP load"
fi

TOTAL_ELAPSED=$((SECONDS - TOTAL_START))
echo "============================================================"
echo "Total wall time: ${TOTAL_ELAPSED}s"
echo "Results CSV:     $RESULTS_CSV"
echo "Run log:         $RUN_LOG"
echo "============================================================"
