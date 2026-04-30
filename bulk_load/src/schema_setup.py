"""
schema_setup.py

Idempotently creates the unique constraints implied by the data_model.yaml.
Run this BEFORE the load. Without unique constraints on node primary keys,
relationship MERGE/MATCH lookups degenerate to label scans and the loader
will hit the transaction memory ceiling.

This is the single biggest performance lever in the whole pipeline.

Usage:
    python schema_setup.py \
        --config bulk_load/config/data_model.yaml \
        --uri neo4j+s://<aura>.databases.neo4j.io \
        --user neo4j \
        --password "$NEO4J_PASSWORD" \
        --database neo4j

  Or with a credentials file (same shape used by the rest of general_utils):
    python schema_setup.py \
        --config bulk_load/config/data_model.yaml \
        --credentials /path/to/Neo4j-xxxx.txt
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import yaml
from neo4j import GraphDatabase


def parse_credentials_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    pattern = re.compile(r"^([A-Z_][A-Z0-9_]*)=(.*)$")
    for line in path.read_text().splitlines():
        m = pattern.match(line.strip())
        if m:
            out[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, type=Path)
    p.add_argument("--credentials", type=Path,
                   help="Aura credentials file (NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD).")
    p.add_argument("--uri")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--database", default="neo4j")
    p.add_argument("--drop", action="store_true",
                   help="Drop all existing constraints/indexes before recreating.")
    return p.parse_args()


def resolve_creds(args: argparse.Namespace) -> tuple[str, str, str, str]:
    if args.credentials:
        c = parse_credentials_file(args.credentials)
        return (
            args.uri or c["NEO4J_URI"],
            args.user or c.get("NEO4J_USERNAME", "neo4j"),
            args.password or c["NEO4J_PASSWORD"],
            args.database or c.get("NEO4J_DATABASE", "neo4j"),
        )
    pw = args.password or os.environ.get("NEO4J_PASSWORD")
    if not all([args.uri, args.user, pw]):
        sys.exit("Either --credentials FILE or all of --uri --user --password must be provided.")
    return args.uri, args.user, pw, args.database


def main() -> int:
    args = parse_args()
    uri, user, password, database = resolve_creds(args)
    cfg = yaml.safe_load(args.config.read_text())
    nodes = cfg["nodes"]

    print(f"Target:   {uri}  db={database}")
    print(f"Nodes:    {len(nodes)} unique constraints to ensure")
    print()

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=database) as session:
            if args.drop:
                print("Dropping existing constraints and indexes...")
                t0 = time.time()
                for rec in session.run("SHOW CONSTRAINTS YIELD name").data():
                    session.run(f"DROP CONSTRAINT {rec['name']} IF EXISTS")
                for rec in session.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP'").data():
                    session.run(f"DROP INDEX {rec['name']} IF EXISTS")
                print(f"  done in {time.time() - t0:.1f}s")
                print()

            print("Creating unique constraints...")
            t0 = time.time()
            for n in nodes:
                label = n["label"]
                pk = n["primary_key"]
                cname = f"{label.lower()}_{pk}_uniq"
                cypher = (
                    f"CREATE CONSTRAINT {cname} IF NOT EXISTS "
                    f"FOR (n:{label}) REQUIRE n.{pk} IS UNIQUE"
                )
                session.run(cypher).consume()
                print(f"  {cname:<40}  {label}({pk})")
            print(f"  done in {time.time() - t0:.1f}s")
            print()

            constraints = session.run(
                "SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties"
            ).data()
            print(f"Constraints in place: {len(constraints)}")
            for c in constraints:
                print(f"  {c['name']:<40}  {c['labelsOrTypes']}({c['properties']})")
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
