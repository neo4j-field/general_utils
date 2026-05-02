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
#   PARQUET_DIR          Default: $HOME/data/parquet
#   NODE_BATCH_SIZE      Default: 5000   (Spark Connector batch.size for node writes)
#   REL_BATCH_SIZE       Default: 5000   (Spark Connector batch.size for rel writes.
#                                         Empirically validated: 50000 produced no
#                                         measurable speedup vs 5000 on the test
#                                         workload, since per-transaction work
#                                         scales linearly with batch size on the
#                                         receiver. Surfaced for tuning, not for
#                                         default sweeping.)
#   PARTITIONS           Default: 8      (Spark partitions for node writes)
#   REL_PARTITIONS       Default: 1      (single-thread rel writes; raise only after
#                                         empirical FK-distribution testing)
#   HOT_REL_THRESHOLD    Default: 1000
#   NODE_MODE            Default: merge  (merge | create)
#   SCALE                Default: 1.0    (multiplier on all volumes; 0.01 for smoke test)
#   SKIP_GENERATE        Default: false  (re-use existing parquet)
#   SKIP_SCHEMA          Default: false
#   SKIP_LOAD            Default: false
#   RESULTS_DIR          Default: $HOME/results
#   JAR                  Default: $NEO4J_SPARK_CONNECTOR_JAR (set by provision_vm.sh)
#   VENV                 Default: $HOME/.venv-bulkload
#   CONFIG               Default: <repo>/bulk_load/config/data_model.yaml
#
# Aura instance lifecycle (opt-in; destructive):
#   RECREATE_INSTANCE    Default: false  Set to true to delete the current
#                                        Aura instance and create a fresh one
#                                        before the load. Requires the three
#                                        env vars below.
#   AURA_API_CREDENTIALS Path to a file with CLIENT_ID and CLIENT_SECRET for
#                        the Aura public API. Required when RECREATE_INSTANCE=true.
#   AURA_INSTANCE_ID     The instance ID to delete and recreate (e.g. 27ad415a).
#                        Required when RECREATE_INSTANCE=true.
#   AURA_CUSTOM_ENDPOINT Optional. Printed in the recreate summary as a
#                        reminder to manually rebind in the Aura console
#                        (the public API does not expose this operation to
#                        ordinary OAuth keys).
#   FRESH_CREDS_FILE     Default: $HOME/Neo4j-fresh-credentials.txt
#                        Where the new instance's URI/username/password are
#                        written. Used as the credentials source for the
#                        rest of this run, regardless of the positional
#                        credentials argument passed in.
#
# Pre-load wipe (defaults to ON; required for benchmark validity):
#   WIPE_BEFORE_LOAD     Default: auto. auto | true | false.
#                        auto: wipe unless RECREATE_INSTANCE=true (a fresh
#                        instance is already empty), and unless both
#                        SKIP_SCHEMA and SKIP_LOAD are true (no DB writes
#                        coming, so no need to wipe).
#                        true:  always wipe.
#                        false: skip wipe (e.g. when appending to an
#                        existing graph).
#   RESET_SCRIPT         Default: ../reset_to_blank_neo4j_db.sh relative
#                        to this module. The script is part of the parent
#                        general_utils/ directory and drops constraints,
#                        deletes rels, then deletes nodes — in that order,
#                        because index maintenance is the dominant cost
#                        of DETACH DELETE at scale.

set -euo pipefail

# Reads NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD / NEO4J_DATABASE from a
# credentials file and sets the corresponding shell vars. Used both for the
# initial credentials input and (when RECREATE_INSTANCE=true) for re-reading
# the file written by recreate_instance.py.
load_creds_from_file() {
    local file="$1"
    [[ -f "$file" ]] || { echo "Error: credentials file '$file' not found." >&2; exit 1; }
    parse_cred() {
        grep "^$1=" "$file" | head -n 1 | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g'
    }
    URI=$(parse_cred "NEO4J_URI")
    USERNAME=$(parse_cred "NEO4J_USERNAME")
    PASSWORD=$(parse_cred "NEO4J_PASSWORD")
    DATABASE=$(parse_cred "NEO4J_DATABASE"); DATABASE="${DATABASE:-neo4j}"
    CRED_FILE="$file"
}

# ---------- Argument parsing (same shape as load_benchmark_data.sh) ----------
if [[ "$#" -eq 1 ]]; then
    load_creds_from_file "$1"
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
NODE_BATCH_SIZE="${NODE_BATCH_SIZE:-5000}"
REL_BATCH_SIZE="${REL_BATCH_SIZE:-5000}"
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
WIPE_BEFORE_LOAD="${WIPE_BEFORE_LOAD:-auto}"
RESET_SCRIPT="${RESET_SCRIPT:-$MODULE_DIR/../reset_to_blank_neo4j_db.sh}"

if [[ -z "$JAR" || ! -f "$JAR" ]]; then
    echo "Error: Spark Connector jar not found. Set JAR or NEO4J_SPARK_CONNECTOR_JAR." >&2
    exit 1
fi
if [[ ! -f "$VENV/bin/python" ]]; then
    echo "Error: virtualenv not found at $VENV. Run provision_vm.sh first." >&2
    exit 1
fi

PYTHON="$VENV/bin/python"

# ---------- Optional: recreate Aura instance before the load ----------
# Destructive. Opt-in only. Replaces the current Aura instance with a
# fresh one of the same config. The fresh credentials are written to
# $FRESH_CREDS_FILE and used for the rest of this run.
if [[ "${RECREATE_INSTANCE:-false}" == "true" ]]; then
    [[ -n "${AURA_API_CREDENTIALS:-}" ]] || {
        echo "Error: RECREATE_INSTANCE=true requires AURA_API_CREDENTIALS env var." >&2
        exit 1
    }
    [[ -n "${AURA_INSTANCE_ID:-}" ]] || {
        echo "Error: RECREATE_INSTANCE=true requires AURA_INSTANCE_ID env var." >&2
        exit 1
    }
    FRESH_CREDS_FILE="${FRESH_CREDS_FILE:-$HOME/Neo4j-fresh-credentials.txt}"

    echo "============================================================"
    echo "Recreating Aura instance before load"
    echo "============================================================"
    echo "  Instance ID:          $AURA_INSTANCE_ID"
    echo "  API credentials:      $AURA_API_CREDENTIALS"
    echo "  Output credentials:   $FRESH_CREDS_FILE"
    [[ -n "${AURA_CUSTOM_ENDPOINT:-}" ]] && echo "  Custom endpoint:      $AURA_CUSTOM_ENDPOINT"
    echo

    RECREATE_CMD=(
        "$PYTHON" "$MODULE_DIR/src/recreate_instance.py"
        --api-credentials "$AURA_API_CREDENTIALS"
        --instance-id "$AURA_INSTANCE_ID"
        --output-credentials "$FRESH_CREDS_FILE"
        --yes
    )
    [[ -n "${AURA_CUSTOM_ENDPOINT:-}" ]] && RECREATE_CMD+=( --custom-endpoint "$AURA_CUSTOM_ENDPOINT" )
    "${RECREATE_CMD[@]}"

    echo
    echo "[recreate] Switching to fresh credentials at $FRESH_CREDS_FILE"
    load_creds_from_file "$FRESH_CREDS_FILE"
    echo
fi

mkdir -p "$RESULTS_DIR"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)_n${NODE_BATCH_SIZE}r${REL_BATCH_SIZE}_p${PARTITIONS}_${NODE_MODE}"
RESULTS_CSV="$RESULTS_DIR/load_${RUN_ID}.csv"
RUN_LOG="$RESULTS_DIR/load_${RUN_ID}.log"

# Resolve WIPE_BEFORE_LOAD=auto into a concrete decision based on the rest
# of the run shape. We only resolve here (not at env-var read time) because
# RECREATE_INSTANCE may have just changed the credentials we're targeting.
case "$WIPE_BEFORE_LOAD" in
    auto)
        if [[ "${RECREATE_INSTANCE:-false}" == "true" ]]; then
            RUN_WIPE=false
            WIPE_REASON="fresh instance from recreate (already empty)"
        elif [[ "$SKIP_LOAD" == "true" && "$SKIP_SCHEMA" == "true" ]]; then
            RUN_WIPE=false
            WIPE_REASON="SKIP_LOAD and SKIP_SCHEMA both set (no DB writes coming)"
        else
            RUN_WIPE=true
            WIPE_REASON="benchmark validity: existing instance, load is happening"
        fi
        ;;
    true)  RUN_WIPE=true;  WIPE_REASON="WIPE_BEFORE_LOAD=true (explicit)" ;;
    false) RUN_WIPE=false; WIPE_REASON="WIPE_BEFORE_LOAD=false (explicit; e.g. appending)" ;;
    *) echo "Error: WIPE_BEFORE_LOAD must be auto, true, or false (got '$WIPE_BEFORE_LOAD')" >&2; exit 1 ;;
esac

echo "============================================================"
echo "Neo4j bulk load benchmark"
echo "============================================================"
echo "  URI:          $URI"
echo "  Database:     $DATABASE"
echo "  Config:       $CONFIG"
echo "  Parquet dir:  $PARQUET_DIR"
echo "  Node batch:   $NODE_BATCH_SIZE"
echo "  Rel batch:    $REL_BATCH_SIZE"
echo "  Node parts:   $PARTITIONS"
echo "  Rel parts:    $REL_PARTITIONS  (hot threshold: $HOT_REL_THRESHOLD)"
echo "  Node mode:    $NODE_MODE"
echo "  Scale:        $SCALE"
echo "  Wipe first:   $RUN_WIPE  ($WIPE_REASON)"
echo "  Results CSV:  $RESULTS_CSV"
echo "  Run log:      $RUN_LOG"
echo

TOTAL_START=$SECONDS

# ---------- 1. Wipe ----------
if [[ "$RUN_WIPE" == "true" ]]; then
    [[ -x "$RESET_SCRIPT" ]] || {
        echo "Error: reset script not found or not executable at $RESET_SCRIPT" >&2
        echo "Set RESET_SCRIPT to a valid path or set WIPE_BEFORE_LOAD=false." >&2
        exit 1
    }
    echo "[1/4] Wiping database to clean state via $(basename "$RESET_SCRIPT")..."
    WIPE_START=$SECONDS
    if [[ -n "${CRED_FILE:-}" ]]; then
        "$RESET_SCRIPT" "$CRED_FILE" 2>&1 | tee -a "$RUN_LOG"
    else
        "$RESET_SCRIPT" "$USERNAME" "$PASSWORD" "$URI" 2>&1 | tee -a "$RUN_LOG"
    fi
    echo "  wipe: $((SECONDS - WIPE_START))s"
    echo
else
    echo "[1/4] SKIP wipe ($WIPE_REASON)"
    echo
fi

# ---------- 2. Generate ----------
if [[ "$SKIP_GENERATE" != "true" ]]; then
    echo "[2/4] Generating synthetic parquet..."
    GEN_START=$SECONDS
    "$PYTHON" "$MODULE_DIR/src/generate_synthetic_data.py" \
        --config "$CONFIG" \
        --output "$PARQUET_DIR" \
        --scale "$SCALE" 2>&1 | tee -a "$RUN_LOG"
    echo "  generation: $((SECONDS - GEN_START))s"
    echo
else
    echo "[2/4] SKIP generate (using existing $PARQUET_DIR)"
fi

# ---------- 3. Schema ----------
if [[ "$SKIP_SCHEMA" != "true" ]]; then
    echo "[3/4] Setting up schema (unique constraints)..."
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
    echo "[3/4] SKIP schema setup"
fi

# ---------- 4. Load ----------
if [[ "$SKIP_LOAD" != "true" ]]; then
    echo "[4/4] Loading parquet -> Neo4j..."
    LOAD_START=$SECONDS
    LOAD_CMD=(
        "$PYTHON" "$MODULE_DIR/src/load_to_neo4j.py"
        --config "$CONFIG"
        --parquet-dir "$PARQUET_DIR"
        --node-batch-size "$NODE_BATCH_SIZE"
        --rel-batch-size "$REL_BATCH_SIZE"
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
    echo "[4/4] SKIP load"
fi

TOTAL_ELAPSED=$((SECONDS - TOTAL_START))
echo "============================================================"
echo "Total wall time: ${TOTAL_ELAPSED}s"
echo "Results CSV:     $RESULTS_CSV"
echo "Run log:         $RUN_LOG"
echo "============================================================"
