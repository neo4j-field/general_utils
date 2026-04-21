#!/usr/bin/env bash
# Orchestrates the parallelism benchmark matrix used to derive the
# PARALLEL_THRESHOLD default in reset_to_blank_neo4j_db.sh.
#
# For each volume tier:
#   for each strategy in [serial, parallel]:
#     load data; run reset with strategy; record wall-clock time.
#
# Writes a single results CSV and prints it at the end.
#
# Usage:
#   ./run_matrix.sh <credentials_file>

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
    echo "Usage: $0 <credentials_file>" >&2
    exit 1
fi

CREDS="$1"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RESET="$SCRIPT_DIR/reset_to_blank_neo4j_db.sh"
LOADER="$SCRIPT_DIR/load_benchmark_data.sh"
RESULTS="${RESULTS:-/tmp/matrix_results.csv}"

# Tier definitions: persons|companies|products|transactions|works_at|purchased|owns|connected
# node total = P + C + Pr + T,   rel total = W + Pu + O + Co
declare -a TIERS=(
    "small:10000:5000:20000:15000:50000:100000:50000:50000"
    "medium:100000:50000:200000:150000:500000:1000000:500000:500000"
    "large:200000:100000:400000:300000:1000000:2000000:1000000:1000000"
)

echo "tier,nodes,rels,strategy,reset_seconds,load_seconds" > "$RESULTS"

for tier_def in "${TIERS[@]}"; do
    IFS=':' read -r TIER PERSONS COMPANIES PRODUCTS TRANSACTIONS WORKS_AT PURCHASED OWNS CONNECTED <<< "$tier_def"
    NODES=$((PERSONS + COMPANIES + PRODUCTS + TRANSACTIONS))
    RELS=$((WORKS_AT + PURCHASED + OWNS + CONNECTED))
    echo ""
    echo "==========================================================="
    echo "TIER: $TIER   (target: ${NODES} nodes, ~${RELS} rels)"
    echo "==========================================================="

    for STRATEGY in serial parallel; do
        echo ""
        echo ">>> Loading $TIER ..."
        LOAD_START=$(python3 -c 'import time; print(time.time())')
        PERSONS=$PERSONS COMPANIES=$COMPANIES PRODUCTS=$PRODUCTS TRANSACTIONS=$TRANSACTIONS \
        WORKS_AT_RELS=$WORKS_AT PURCHASED_RELS=$PURCHASED OWNS_RELS=$OWNS CONNECTED_RELS=$CONNECTED \
            bash "$LOADER" "$CREDS" > /tmp/load_${TIER}_${STRATEGY}.log 2>&1
        LOAD_END=$(python3 -c 'import time; print(time.time())')
        LOAD_ELAPSED=$(python3 -c "print(f'{${LOAD_END} - ${LOAD_START}:.2f}')")
        echo "    load: ${LOAD_ELAPSED}s"

        echo ">>> Resetting with strategy=$STRATEGY"
        if [[ "$STRATEGY" == "serial" ]]; then
            RESET_PARALLEL="false"
        else
            RESET_PARALLEL="true"
        fi

        RESET_START=$(python3 -c 'import time; print(time.time())')
        PARALLEL_RELS="$RESET_PARALLEL" bash "$RESET" "$CREDS" > /tmp/reset_${TIER}_${STRATEGY}.log 2>&1
        RESET_END=$(python3 -c 'import time; print(time.time())')
        RESET_ELAPSED=$(python3 -c "print(f'{${RESET_END} - ${RESET_START}:.2f}')")
        echo "    reset: ${RESET_ELAPSED}s (strategy=$STRATEGY)"

        echo "${TIER},${NODES},${RELS},${STRATEGY},${RESET_ELAPSED},${LOAD_ELAPSED}" >> "$RESULTS"
    done
done

echo ""
echo "==========================================================="
echo "MATRIX COMPLETE"
echo "==========================================================="
column -s ',' -t < "$RESULTS"
