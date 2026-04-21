#!/usr/bin/env bash
#
# reset_to_blank_neo4j_db.sh
# High-performance reset script that wipes any Neo4j database reachable
# via cypher-shell to a clean state (no data, no schema).
#
# Works against:
#   - Neo4j Aura (all tiers, via neo4j+s://)
#   - Neo4j Enterprise / Community self-hosted (via bolt:// or neo4j://)
#   - Causal cluster members (writes auto-forward to leader)
#
# Designed for databases with significant data volume where the naive
# "MATCH (n) DETACH DELETE n" approach becomes prohibitively slow due to
# index-maintenance overhead and serial transaction execution.
#
# Performance strategy (in order of impact):
#   1. Drop constraints/indexes BEFORE data deletes. Every delete on an
#      indexed property would otherwise trigger an index update; removing
#      the schema first makes the hot delete path free of that work.
#   2. Delete relationships before nodes. Relationships are usually the
#      largest cardinality, and isolated relationship deletes parallelize
#      cleanly. Once they are gone, node deletes have nothing to detach.
#   3. Use apoc.periodic.iterate with parallel:true for relationships when
#      APOC is available (it is preinstalled on Aura). Falls back to
#      CALL { ... } IN TRANSACTIONS when APOC is absent.
#   4. Read counts from the count store (apoc.meta.stats) instead of a
#      full MATCH scan.
#   5. Minimize cypher-shell invocations (each is a fresh TLS handshake)
#      and pass the password via NEO4J_PASSWORD env so it never appears
#      in the process list.
#
# Usage:
#   ./reset_to_blank_neo4j_db.sh <credentials_file>
#   ./reset_to_blank_neo4j_db.sh <username> <password> <uri>
#
# Tunable environment overrides (all optional):
#   BATCH_SIZE           Rows per sub-transaction.                        Default: 50000
#   PARALLEL_RELS        auto | true | false. 'auto' picks by rel count.  Default: auto
#   PARALLEL_THRESHOLD   Relationship count above which auto uses parallel. Default: 100000
#   SKIP_STATS           Skip pre-counts for fastest path.                Default: false
#   DATABASE             Target database name.                            Default: neo4j
#
# Strategy selection (PARALLEL_RELS=auto):
#   - Below threshold: serial (CALL IN TRANSACTIONS). Parallel startup
#     overhead outweighs benefit on small graphs.
#   - At or above threshold: parallel apoc.periodic.iterate. Wins scale
#     with CPU count; see benchmark matrix in repo README.

set -euo pipefail

# ---------- Argument parsing ----------
if [[ "$#" -eq 1 ]]; then
    CRED_FILE="$1"
    if [[ ! -f "$CRED_FILE" ]]; then
        echo "Error: credentials file '$CRED_FILE' not found." >&2
        exit 1
    fi

    parse_cred() {
        grep "^$1=" "$CRED_FILE" | head -n 1 | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g'
    }
    URI=$(parse_cred "NEO4J_URI")
    USERNAME=$(parse_cred "NEO4J_USERNAME")
    PASSWORD=$(parse_cred "NEO4J_PASSWORD")

    if [[ -z "$URI" || -z "$USERNAME" || -z "$PASSWORD" ]]; then
        echo "Error: Could not parse NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD from $CRED_FILE." >&2
        echo "Expected format:" >&2
        echo "  NEO4J_URI=neo4j+s://..." >&2
        echo "  NEO4J_USERNAME=neo4j" >&2
        echo "  NEO4J_PASSWORD=..." >&2
        exit 1
    fi
elif [[ "$#" -eq 3 ]]; then
    USERNAME="$1"
    PASSWORD="$2"
    URI="$3"
else
    echo "Usage: $0 <credentials_file>"
    echo "   OR: $0 <username> <password> <uri>"
    exit 1
fi

# ---------- Tunables ----------
BATCH_SIZE="${BATCH_SIZE:-50000}"
PARALLEL_RELS="${PARALLEL_RELS:-auto}"
PARALLEL_THRESHOLD="${PARALLEL_THRESHOLD:-100000}"
SKIP_STATS="${SKIP_STATS:-false}"
DATABASE="${DATABASE:-neo4j}"

# Keep the password out of the process list; cypher-shell picks this up automatically.
export NEO4J_PASSWORD="$PASSWORD"

# All cypher-shell calls share this base. Note: no -p flag.
cs() {
    cypher-shell -a "$URI" -u "$USERNAME" -d "$DATABASE" --format plain "$@"
}

echo "Starting Neo4j database reset"
echo "  URI:               $URI"
echo "  Database:          $DATABASE"
echo "  Batch size:        $BATCH_SIZE"
echo "  Parallel strategy: $PARALLEL_RELS (threshold: $PARALLEL_THRESHOLD rels)"
echo "  Skip stats:        $SKIP_STATS"
START_TIME=$SECONDS

# ---------- 1. Detect APOC (one round trip) ----------
HAS_APOC="false"
if APOC_VER=$(cs "RETURN apoc.version() AS v;" 2>/dev/null | tail -n 1 | tr -d ' "'); then
    if [[ -n "$APOC_VER" && "$APOC_VER" != "v" ]]; then
        HAS_APOC="true"
        echo "  APOC:        detected ($APOC_VER)"
    fi
fi
if [[ "$HAS_APOC" != "true" ]]; then
    echo "  APOC:        not available (falling back to CALL IN TRANSACTIONS)"
fi

# ---------- 2. Gather stats from the count store (O(1)) ----------
# Needed for both the summary and (when strategy is 'auto') to pick
# serial vs parallel. If SKIP_STATS=true and strategy is 'auto', we
# default to parallel since we can't measure.
NODE_COUNT="(skipped)"
REL_COUNT="(skipped)"
REL_COUNT_NUM=0
if [[ "$SKIP_STATS" != "true" ]]; then
    echo ""
    echo "Reading count store..."
    T0=$SECONDS
    if [[ "$HAS_APOC" == "true" ]]; then
        STATS=$(cs "CALL apoc.meta.stats() YIELD nodeCount, relCount RETURN nodeCount + ',' + relCount AS s;" \
                | tail -n 1 | tr -d ' "')
        NODE_COUNT="${STATS%,*}"
        REL_COUNT="${STATS#*,}"
    else
        NODE_COUNT=$(cs "MATCH (n) RETURN count(n);" | tail -n 1 | tr -d ' "')
        REL_COUNT=$(cs "MATCH ()-[r]->() RETURN count(r);" | tail -n 1 | tr -d ' "')
    fi
    REL_COUNT_NUM="${REL_COUNT:-0}"
    echo "  Nodes: $NODE_COUNT  |  Relationships: $REL_COUNT  ($((SECONDS - T0))s)"
fi

# ---------- Resolve AUTO strategy ----------
USE_PARALLEL="false"
case "$PARALLEL_RELS" in
    true)  USE_PARALLEL="true"  ;;
    false) USE_PARALLEL="false" ;;
    auto)
        if [[ "$HAS_APOC" != "true" ]]; then
            USE_PARALLEL="false"  # no parallelism without APOC
        elif [[ "$SKIP_STATS" == "true" ]]; then
            USE_PARALLEL="true"   # no measurement — default to parallel for safety at scale
        elif [[ "$REL_COUNT_NUM" =~ ^[0-9]+$ ]] && [[ "$REL_COUNT_NUM" -ge "$PARALLEL_THRESHOLD" ]]; then
            USE_PARALLEL="true"
        else
            USE_PARALLEL="false"
        fi
        echo "  Auto-selected: parallel=$USE_PARALLEL (rels=$REL_COUNT_NUM, threshold=$PARALLEL_THRESHOLD)"
        ;;
    *)
        echo "Error: PARALLEL_RELS must be one of auto|true|false (got: $PARALLEL_RELS)" >&2
        exit 1
        ;;
esac

# ---------- 3. Drop schema BEFORE data deletes ----------
# Index maintenance during DELETE is the single biggest hidden cost on
# large graphs. Removing constraints and indexes first eliminates it.
echo ""
echo "Dropping schema (constraints + indexes) before data delete..."
T0=$SECONDS
SCHEMA_FILE=$(mktemp -t reset_aura_schema.XXXXXX)
trap 'rm -f "$SCHEMA_FILE"' EXIT

# Use backticks to quote names that contain special characters.
cs "SHOW CONSTRAINTS YIELD name RETURN 'DROP CONSTRAINT \`' + name + '\`;' AS stmt;" \
    | tail -n +2 | tr -d '"' > "$SCHEMA_FILE"

cs "SHOW INDEXES YIELD name, type, owningConstraint
    WHERE type <> 'LOOKUP' AND owningConstraint IS NULL
    RETURN 'DROP INDEX \`' + name + '\`;' AS stmt;" \
    | tail -n +2 | tr -d '"' >> "$SCHEMA_FILE"

SCHEMA_COUNT=0
if [[ -s "$SCHEMA_FILE" ]] && grep -q "DROP" "$SCHEMA_FILE"; then
    SCHEMA_COUNT=$(grep -c "^DROP" "$SCHEMA_FILE" || true)
    # --fail-at-end: one bad drop should not block the rest
    cs --fail-at-end -f "$SCHEMA_FILE" > /dev/null
    echo "  Dropped $SCHEMA_COUNT schema objects ($((SECONDS - T0))s)"
else
    echo "  No user-defined constraints or indexes to drop."
fi

# ---------- 4. Delete relationships first ----------
echo ""
echo "Deleting relationships (batch size $BATCH_SIZE, parallel=$USE_PARALLEL)..."
T0=$SECONDS
if [[ "$HAS_APOC" == "true" ]]; then
    cs "CALL apoc.periodic.iterate(
            'MATCH ()-[r]->() RETURN id(r) AS rid',
            'MATCH ()-[r]->() WHERE id(r) = rid DELETE r',
            {batchSize: $BATCH_SIZE, parallel: $USE_PARALLEL, retries: 3}
        ) YIELD batches, total, timeTaken, failedOperations, errorMessages
        RETURN batches, total, timeTaken, failedOperations;" > /dev/null
else
    cs "MATCH ()-[r]->() CALL { WITH r DELETE r } IN TRANSACTIONS OF $BATCH_SIZE ROWS;" > /dev/null
fi
echo "  Relationships deleted ($((SECONDS - T0))s)"

# ---------- 5. Delete nodes (now unattached) ----------
# Parallel:false on nodes — even with relationships gone, concurrent node
# deletes on the same label can contend on token-store locks. Serial is
# the safer default; the work is already small because there's no
# relationship traversal to do.
echo ""
echo "Deleting nodes (batch size $BATCH_SIZE)..."
T0=$SECONDS
if [[ "$HAS_APOC" == "true" ]]; then
    cs "CALL apoc.periodic.iterate(
            'MATCH (n) RETURN id(n) AS nid',
            'MATCH (n) WHERE id(n) = nid DELETE n',
            {batchSize: $BATCH_SIZE, parallel: false, retries: 3}
        ) YIELD batches, total, timeTaken, failedOperations, errorMessages
        RETURN batches, total, timeTaken, failedOperations;" > /dev/null
else
    cs "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF $BATCH_SIZE ROWS;" > /dev/null
fi
echo "  Nodes deleted ($((SECONDS - T0))s)"

# ---------- 6. Summary ----------
ELAPSED=$(( SECONDS - START_TIME ))
echo ""
echo "========================================"
echo "Reset complete."
echo "  Total time:       ${ELAPSED}s"
echo "  Nodes removed:    $NODE_COUNT"
echo "  Relationships:    $REL_COUNT"
echo "  Schema objects:   $SCHEMA_COUNT"
echo "========================================"
