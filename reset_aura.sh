#!/bin/bash
# Usage: ./reset_aura.sh <username> <password> <aura_uri>

USERNAME=$1
PASSWORD=$2
URI=$3

echo "Starting database wipe..."

echo "Deleting nodes and relationships..."
cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;"

echo "Fetching schema drops..."

cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "SHOW CONSTRAINTS YIELD name RETURN 'DROP CONSTRAINT ' + name + ';';" | tail -n +2 | tr -d '"' > drop_schema.cypher

cypher-shell -a $URI -u $USERNAME -p $PASSWORD --format plain "SHOW INDEXES YIELD name, type, owningConstraint WHERE type <> 'LOOKUP' AND owningConstraint IS NULL RETURN 'DROP INDEX ' + name + ';';" | tail -n +2 | tr -d '"' >> drop_schema.cypher

if [ -s drop_schema.cypher ] && grep -q "DROP" drop_schema.cypher; then
    echo "Executing schema drops..."
    cypher-shell -a $URI -u $USERNAME -p $PASSWORD -f drop_schema.cypher
else
    echo "No custom indexes or constraints found to drop."
fi

rm drop_schema.cypher

echo "Database reset complete."