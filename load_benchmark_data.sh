#!/usr/bin/env bash
#
# load_benchmark_data.sh
# Generates a synthetic dataset to exercise the Aura reset script.
#
# Default sizing targets an 8GB / 16GB Aura instance:
#   ~500,000 nodes
#   ~2,500,000 relationships
#   6 constraints + 6 range indexes + 1 fulltext index
#
# All data is schema-only synthetic (no PII) so it's safe to wipe.
#
# Usage:
#   ./load_benchmark_data.sh <credentials_file>
#   ./load_benchmark_data.sh <username> <password> <aura_uri>
#
# Tunable env vars:
#   PERSONS          Default: 100000
#   COMPANIES        Default: 50000
#   PRODUCTS         Default: 200000
#   TRANSACTIONS     Default: 150000
#   WORKS_AT_RELS    Default: 500000
#   PURCHASED_RELS   Default: 1000000
#   OWNS_RELS        Default: 500000
#   CONNECTED_RELS   Default: 500000
#   BATCH_SIZE       Default: 10000
#   DATABASE         Default: neo4j

set -euo pipefail

# ---------- Argument parsing (same shape as reset script) ----------
if [[ "$#" -eq 1 ]]; then
    CRED_FILE="$1"
    [[ -f "$CRED_FILE" ]] || { echo "Error: credentials file '$CRED_FILE' not found." >&2; exit 1; }
    parse_cred() {
        grep "^$1=" "$CRED_FILE" | head -n 1 | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g'
    }
    URI=$(parse_cred "NEO4J_URI")
    USERNAME=$(parse_cred "NEO4J_USERNAME")
    PASSWORD=$(parse_cred "NEO4J_PASSWORD")
elif [[ "$#" -eq 3 ]]; then
    USERNAME="$1"; PASSWORD="$2"; URI="$3"
else
    echo "Usage: $0 <credentials_file>   OR   $0 <username> <password> <aura_uri>"
    exit 1
fi

PERSONS="${PERSONS:-100000}"
COMPANIES="${COMPANIES:-50000}"
PRODUCTS="${PRODUCTS:-200000}"
TRANSACTIONS="${TRANSACTIONS:-150000}"
WORKS_AT_RELS="${WORKS_AT_RELS:-500000}"
PURCHASED_RELS="${PURCHASED_RELS:-1000000}"
OWNS_RELS="${OWNS_RELS:-500000}"
CONNECTED_RELS="${CONNECTED_RELS:-500000}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
DATABASE="${DATABASE:-neo4j}"

export NEO4J_PASSWORD="$PASSWORD"
cs() { cypher-shell -a "$URI" -u "$USERNAME" -d "$DATABASE" --format plain "$@"; }

echo "Loading benchmark data into: $URI"
echo "  Nodes:  ${PERSONS} Persons, ${COMPANIES} Companies, ${PRODUCTS} Products, ${TRANSACTIONS} Transactions"
echo "  Rels:   ${WORKS_AT_RELS} WORKS_AT, ${PURCHASED_RELS} PURCHASED, ${OWNS_RELS} OWNS, ${CONNECTED_RELS} CONNECTED_TO"
echo "  Batch:  $BATCH_SIZE"
START=$SECONDS

# ---------- 1. Schema ----------
echo ""
echo "[1/3] Creating constraints & indexes..."
cs <<'CYPHER' > /dev/null
CREATE CONSTRAINT person_id          IF NOT EXISTS FOR (n:Person)      REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT company_id         IF NOT EXISTS FOR (n:Company)     REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT product_id         IF NOT EXISTS FOR (n:Product)     REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT transaction_id     IF NOT EXISTS FOR (n:Transaction) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT location_code      IF NOT EXISTS FOR (n:Location)    REQUIRE n.code IS UNIQUE;
CREATE CONSTRAINT category_name      IF NOT EXISTS FOR (n:Category)    REQUIRE n.name IS UNIQUE;
CREATE INDEX person_name             IF NOT EXISTS FOR (n:Person)      ON (n.name);
CREATE INDEX company_industry        IF NOT EXISTS FOR (n:Company)     ON (n.industry);
CREATE INDEX product_price           IF NOT EXISTS FOR (n:Product)     ON (n.price);
CREATE INDEX transaction_date        IF NOT EXISTS FOR (n:Transaction) ON (n.date);
CREATE FULLTEXT INDEX search_text    IF NOT EXISTS FOR (n:Person|Company|Product) ON EACH [n.name, n.description];
CYPHER
echo "  Schema created."

# ---------- 2. Nodes ----------
echo ""
echo "[2/3] Creating nodes..."

# Persons
T0=$SECONDS
cs "
UNWIND range(1, $PERSONS) AS i
CALL {
  WITH i
  CREATE (p:Person {
    id: i,
    name: 'Person_' + i,
    email: 'person' + i + '@example.com',
    createdAt: datetime(),
    score: (i % 100) * 1.0,
    description: 'Synthetic person record ' + i
  })
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  Persons: $PERSONS ($((SECONDS - T0))s)"

# Companies
T0=$SECONDS
cs "
UNWIND range(1, $COMPANIES) AS i
CALL {
  WITH i
  CREATE (c:Company {
    id: i,
    name: 'Company_' + i,
    industry: ['Tech','Finance','Retail','Healthcare','Manufacturing'][i % 5],
    employees: (i * 7) % 10000,
    description: 'Synthetic company record ' + i
  })
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  Companies: $COMPANIES ($((SECONDS - T0))s)"

# Products
T0=$SECONDS
cs "
UNWIND range(1, $PRODUCTS) AS i
CALL {
  WITH i
  CREATE (p:Product {
    id: i,
    name: 'Product_' + i,
    price: (i % 1000) * 1.5,
    sku: 'SKU-' + i,
    description: 'Synthetic product record ' + i
  })
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  Products: $PRODUCTS ($((SECONDS - T0))s)"

# Transactions
T0=$SECONDS
cs "
UNWIND range(1, $TRANSACTIONS) AS i
CALL {
  WITH i
  CREATE (t:Transaction {
    id: i,
    amount: (i % 10000) * 1.25,
    date: date() - duration({days: i % 365}),
    status: ['completed','pending','refunded'][i % 3]
  })
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  Transactions: $TRANSACTIONS ($((SECONDS - T0))s)"

# ---------- 3. Relationships ----------
echo ""
echo "[3/3] Creating relationships..."

# WORKS_AT: Person -> Company
T0=$SECONDS
cs "
UNWIND range(1, $WORKS_AT_RELS) AS i
CALL {
  WITH i
  MATCH (p:Person   {id: ((i * 2654435761) % $PERSONS) + 1})
  MATCH (c:Company  {id: ((i * 40503)      % $COMPANIES) + 1})
  CREATE (p)-[:WORKS_AT {since: 2015 + (i % 10)}]->(c)
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  WORKS_AT: $WORKS_AT_RELS ($((SECONDS - T0))s)"

# PURCHASED: Person -> Product
T0=$SECONDS
cs "
UNWIND range(1, $PURCHASED_RELS) AS i
CALL {
  WITH i
  MATCH (p:Person   {id: ((i * 2654435761) % $PERSONS) + 1})
  MATCH (pr:Product {id: ((i * 19349663)   % $PRODUCTS) + 1})
  CREATE (p)-[:PURCHASED {qty: (i % 5) + 1}]->(pr)
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  PURCHASED: $PURCHASED_RELS ($((SECONDS - T0))s)"

# OWNS: Company -> Product
T0=$SECONDS
cs "
UNWIND range(1, $OWNS_RELS) AS i
CALL {
  WITH i
  MATCH (c:Company  {id: ((i * 40503)    % $COMPANIES) + 1})
  MATCH (pr:Product {id: ((i * 19349663) % $PRODUCTS) + 1})
  CREATE (c)-[:OWNS]->(pr)
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  OWNS: $OWNS_RELS ($((SECONDS - T0))s)"

# CONNECTED_TO: Person -> Person (social graph)
T0=$SECONDS
cs "
UNWIND range(1, $CONNECTED_RELS) AS i
CALL {
  WITH i
  MATCH (a:Person {id: ((i * 2654435761) % $PERSONS) + 1})
  MATCH (b:Person {id: ((i * 83492791)   % $PERSONS) + 1})
  WHERE a <> b
  CREATE (a)-[:CONNECTED_TO {weight: (i % 100) * 0.01}]->(b)
} IN TRANSACTIONS OF $BATCH_SIZE ROWS;
" > /dev/null
echo "  CONNECTED_TO: ~$CONNECTED_RELS ($((SECONDS - T0))s)"

# ---------- Summary ----------
ELAPSED=$(( SECONDS - START ))
echo ""
echo "========================================"
echo "Data load complete in ${ELAPSED}s."
cs "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypesCount RETURN nodeCount, relCount;"
echo "========================================"
