#!/bin/bash
# Usage: ./reset_aura.sh <credentials_file>
#    OR: ./reset_aura.sh <username> <password> <aura_uri>

if [ "$#" -eq 1 ]; then
    CRED_FILE=$1
    if [ ! -f "$CRED_FILE" ]; then
        echo "Error: Credentials file '$CRED_FILE' not found."
        exit 1
    fi

    echo "Parsing credentials from $CRED_FILE..."
    # Extract values, handling potential carriage returns and stripping quotes if present
    URI=$(grep "^NEO4J_URI=" "$CRED_FILE" | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g')
    USERNAME=$(grep "^NEO4J_USERNAME=" "$CRED_FILE" | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g')
    PASSWORD=$(grep "^NEO4J_PASSWORD=" "$CRED_FILE" | cut -d'=' -f2- | tr -d '\r' | sed 's/^"//;s/"$//;s/'\''//g')

    if [[ -z "$URI" || -z "$USERNAME" || -z "$PASSWORD" ]]; then
        echo "Error: Could not parse NEO4J_URI, NEO4J_USERNAME, or NEO4J_PASSWORD from $CRED_FILE."
        echo "Expected format:"
        echo "NEO4J_URI=neo4j+s://..."
        echo "NEO4J_USERNAME=neo4j"
        echo "NEO4J_PASSWORD=..."
        exit 1
    fi
elif [ "$#" -eq 3 ]; then
    USERNAME=$1
    PASSWORD=$2
    URI=$3
else
    echo "Usage: $0 <credentials_file>"
    echo "   OR: $0 <username> <password> <aura_uri>"
    exit 1
fi

echo "Starting database wipe for: $URI"
START_TIME=$SECONDS

# 1. Count existing data for the summary
echo "Gathering database statistics..."
NODE_COUNT=$(cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "MATCH (n) RETURN count(n);" | tail -n +2 | tr -d ' "')
REL_COUNT=$(cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "MATCH ()-[r]->() RETURN count(r);" | tail -n +2 | tr -d ' "')

# 2. Delete all data
echo "Deleting ${NODE_COUNT} nodes and ${REL_COUNT} relationships..."
cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;" > /dev/null

# 3. Fetch schema and dynamically generate DROP statements
echo "Fetching schema drops..."
cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "SHOW CONSTRAINTS YIELD name RETURN 'DROP CONSTRAINT ' + name + ';';" | tail -n +2 | tr -d '"' > drop_schema.cypher
cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "SHOW INDEXES YIELD name, type, owningConstraint WHERE type <> 'LOOKUP' AND owningConstraint IS NULL RETURN 'DROP INDEX ' + name + ';';" | tail -n +2 | tr -d '"' >> drop_schema.cypher

# 4. Execute the dynamically generated DROP statements
if [ -s drop_schema.cypher ] && grep -q "DROP" drop_schema.cypher; then
    echo "Executing schema drops..."
    SCHEMA_COUNT=$(wc -l < drop_schema.cypher | tr -d ' ')
    cypher-shell -a $URI -u $USERNAME -p $PASSWORD -f drop_schema.cypher > /dev/null
else
    echo "No custom indexes or constraints found to drop."
    SCHEMA_COUNT=0
fi

# 5. Clean up
rm drop_schema.cypher

# Calculate elapsed time
ELAPSED_TIME=$(($SECONDS - $START_TIME))

# Print Summary
echo ""
echo "========================================"
echo "Database reset successfully completed!"
echo "Execution time: ${ELAPSED_TIME} seconds"
echo "Summary of removed items:"
echo "   - Nodes: ${NODE_COUNT}"
echo "   - Relationships: ${REL_COUNT}"
echo "   - Schema Objects (Indexes/Constraints): ${SCHEMA_COUNT}"
echo "========================================"