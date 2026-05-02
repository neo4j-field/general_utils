"""
load_to_neo4j.py

Loads parquet files produced by generate_synthetic_data.py into Neo4j Aura
(or any Neo4j 5.x instance) via the Neo4j Spark Connector.

Why this loader matters for the customer's transaction-memory problem:

  1. We rely on schema_setup.py having pre-created unique constraints
     on every node primary key. Without them, every relationship MERGE
     does a label scan and the per-batch transaction balloons.

  2. For relationships we set `relationship.save.strategy=keys`. This
     replaces the connector's default "rebuild full source/target
     nodes" path with a thin MATCH(src{k}) MATCH(tgt{k}) CREATE
     pattern. Per-transaction memory drops by 5-10x.

  3. `batch.size` is parametrized SEPARATELY for nodes and relationships,
     but empirically both default to 5000. We initially shipped 50000 for
     rels on the hypothesis that the single-thread rel writer's bottleneck
     was sync round-trip count over Bolt, so bigger batches would compress
     the rel phase. Measured on a 32 GB business-critical Aura with the
     same data and settings, 50000 vs 5000 rel batches produced essentially
     identical throughput (~10K rows/s either way). Per-transaction work
     on the receiver (the MATCH+MATCH+CREATE per row) scales linearly
     with batch size, so bigger batches just take proportionally longer.
     Both flags are still surfaced for tuning on workloads that may
     behave differently, but the default is 5000.

  4. Each rel dataframe is repartitioned on the source key. This
     groups writes for the same source node into the same Spark
     task and reduces lock contention on shared adjacent nodes.

Usage:
    python load_to_neo4j.py \
        --config bulk_load/config/data_model.yaml \
        --parquet-dir /var/data/parquet \
        --credentials /path/to/Neo4j-xxxx.txt \
        --node-batch-size 5000 \
        --rel-batch-size 5000 \
        --partitions 8 \
        --jar $HOME/jars/neo4j-connector-apache-spark_2.12-5.3.10_for_spark_3.jar \
        --node-mode merge \
        --results-csv /tmp/load_results.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from pathlib import Path

import yaml


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
    p.add_argument("--parquet-dir", required=True, type=Path)
    p.add_argument("--credentials", type=Path)
    p.add_argument("--uri")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--database", default="neo4j")
    p.add_argument("--node-batch-size", type=int, default=5000,
                   help="Connector batch.size for NODE writes. Default 5000. "
                        "Node rows can be wide (up to 33 columns); larger "
                        "batches risk per-transaction memory pressure on the "
                        "receiver and do not improve throughput meaningfully.")
    p.add_argument("--rel-batch-size", type=int, default=5000,
                   help="Connector batch.size for RELATIONSHIP writes. "
                        "Default 5000. We tested 50000 vs 5000 empirically "
                        "on a 32 GB BC Aura with the rest of the config "
                        "fixed; throughput was effectively unchanged "
                        "(~10K rows/sec either way). Per-transaction work "
                        "scales linearly with batch size, so bigger "
                        "batches just take proportionally longer. Surface "
                        "this flag for workloads that may behave "
                        "differently (very wide rel properties, vector "
                        "rel attributes, etc.).")
    p.add_argument("--partitions", type=int, default=8,
                   help="Spark partitions for NODE writes. Default 8.")
    p.add_argument("--rel-partitions", type=int, default=1,
                   help="Spark partitions for RELATIONSHIP writes. "
                        "Default 1 (single-writer, no deadlocks). "
                        "Random FK distribution causes Forseti deadlocks on "
                        "target-node relationship groups under any non-trivial "
                        "parallelism, regardless of how the dataframe is "
                        "repartitioned. Raise this for sparse target data "
                        "(rels per target ~ 1) or after empirical testing.")
    p.add_argument("--hot-rel-threshold", type=int, default=1000,
                   help="Rel types whose TARGET node count is below this "
                        "force single-partition writes (no parallelism) "
                        "to avoid deadlocks on small dimension tables. "
                        "Default 1000.")
    p.add_argument("--jar", type=Path,
                   default=Path(os.environ.get("NEO4J_SPARK_CONNECTOR_JAR", "")),
                   help="Path to neo4j-connector-apache-spark JAR.")
    p.add_argument("--node-mode", choices=["create", "merge"], default="merge",
                   help="'create' is fastest for empty databases (no upsert "
                        "check). 'merge' is idempotent (default).")
    p.add_argument("--skip-nodes", action="store_true",
                   help="Skip node loads (assume already loaded).")
    p.add_argument("--skip-rels", action="store_true",
                   help="Skip relationship loads (e.g. for node-only timing).")
    p.add_argument("--only", nargs="+", default=None,
                   help="Only load these labels/rel-types.")
    p.add_argument("--results-csv", type=Path, default=Path("/tmp/load_results.csv"))
    p.add_argument("--driver-memory", default="6g",
                   help="Spark driver heap. Default 6g.")
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
        sys.exit("Either --credentials FILE or --uri/--user/--password must be provided.")
    return args.uri, args.user, pw, args.database


def build_spark(args: argparse.Namespace):
    from pyspark.sql import SparkSession
    if not args.jar or not args.jar.exists():
        sys.exit(f"Spark Connector jar not found: {args.jar}. "
                 f"Set NEO4J_SPARK_CONNECTOR_JAR or pass --jar.")
    # local[*,10] = use all cores AND retry each task up to 10 times.
    # The plain `local[*]` master silently ignores spark.task.maxFailures
    # and treats any task failure (including TransientException from a
    # Forseti deadlock) as fatal. The local[K,F] form is the documented
    # way to get task retries in local mode.
    builder = (
        SparkSession.builder
        .appName("neo4j-bulk-load-benchmark")
        .master("local[*,10]")
        .config("spark.jars", str(args.jar))
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", str(max(args.partitions, 4)))
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.task.maxFailures", "10")
    )
    return builder.getOrCreate()


def common_options(uri: str, user: str, password: str, database: str,
                   batch_size: int) -> dict[str, str]:
    return {
        "url": uri,
        "authentication.type": "basic",
        "authentication.basic.username": user,
        "authentication.basic.password": password,
        "database": database,
        "batch.size": str(batch_size),
        # Connector-level retry on transient errors (deadlocks, conn drops).
        # Bumped from default 3 because Forseti deadlocks on rel writes
        # are common under any non-trivial parallelism.
        "transaction.retries.max": "10",
        "transaction.retry.timeout.max": "60000",
    }


def load_node(spark, parquet_path: Path, label: str, pk: str,
              creds: dict[str, str], partitions: int, node_mode: str) -> dict:
    print(f"  Loading node :{label}  pk={pk}  mode={node_mode}")
    t0 = time.time()
    df = spark.read.parquet(str(parquet_path))
    row_count = df.count()
    if partitions:
        df = df.repartition(partitions)

    writer = (df.write
              .format("org.neo4j.spark.DataSource")
              .mode("Overwrite" if node_mode == "merge" else "Append")
              .options(**creds)
              .option("labels", f":{label}"))
    if node_mode == "merge":
        writer = writer.option("node.keys", pk)
    writer.save()

    dt = time.time() - t0
    rate = row_count / dt if dt > 0 else 0
    print(f"    wrote {row_count:,} rows in {dt:.1f}s  ({rate:,.0f} rows/sec)")
    return {"kind": "node", "label": label, "rows": row_count,
            "seconds": round(dt, 2), "rows_per_sec": round(rate, 0)}


def load_rel(spark, parquet_path: Path, rel_type: str,
             src_label: str, src_key: str,
             tgt_label: str, tgt_key: str,
             tgt_volume: int,
             creds: dict[str, str], partitions: int,
             hot_rel_threshold: int) -> dict:
    # Hot-rel detection: if the target node count is small (e.g. Channel=20,
    # RiskRating=5), every parallel writer will fight for exclusive locks
    # on the same target nodes. Force single-partition write for these.
    effective_partitions = partitions
    if tgt_volume < hot_rel_threshold:
        effective_partitions = 1
        hot_note = f"  (hot rel: tgt_volume={tgt_volume:,} < {hot_rel_threshold}, forcing 1 partition)"
    else:
        hot_note = ""
    print(f"  Loading rel :{rel_type}  ({src_label})-[]->({tgt_label}){hot_note}")
    t0 = time.time()
    df = spark.read.parquet(str(parquet_path))
    row_count = df.count()
    src_col = f"src_{src_key}"
    tgt_col = f"tgt_{tgt_key}"

    if effective_partitions and effective_partitions > 1:
        # repartition on src_col so writes for the same source node group together,
        # minimizing adjacent-node lock contention.
        df = df.repartition(effective_partitions, src_col)
    elif effective_partitions == 1:
        df = df.coalesce(1)

    (df.write
       .format("org.neo4j.spark.DataSource")
       .mode("Overwrite")
       .options(**creds)
       .option("relationship", rel_type)
       .option("relationship.save.strategy", "keys")
       .option("relationship.source.labels", f":{src_label}")
       .option("relationship.source.save.mode", "Match")
       .option("relationship.source.node.keys", f"{src_col}:{src_key}")
       .option("relationship.target.labels", f":{tgt_label}")
       .option("relationship.target.save.mode", "Match")
       .option("relationship.target.node.keys", f"{tgt_col}:{tgt_key}")
       .save())

    dt = time.time() - t0
    rate = row_count / dt if dt > 0 else 0
    print(f"    wrote {row_count:,} rows in {dt:.1f}s  ({rate:,.0f} rows/sec)")
    return {"kind": "rel", "label": rel_type, "rows": row_count,
            "seconds": round(dt, 2), "rows_per_sec": round(rate, 0)}


def main() -> int:
    args = parse_args()
    uri, user, password, database = resolve_creds(args)
    cfg = yaml.safe_load(args.config.read_text())

    node_creds = common_options(uri, user, password, database, args.node_batch_size)
    rel_creds = common_options(uri, user, password, database, args.rel_batch_size)

    print(f"Target:        {uri}  db={database}")
    print(f"Parquet dir:   {args.parquet_dir}")
    print(f"Node batch:    {args.node_batch_size}")
    print(f"Rel batch:     {args.rel_batch_size}")
    print(f"Node parts:    {args.partitions}")
    print(f"Rel parts:     {args.rel_partitions}  (hot threshold: {args.hot_rel_threshold})")
    print(f"Node mode:     {args.node_mode}")
    print(f"Spark jar:     {args.jar}")
    print()

    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    only = set(args.only) if args.only else None
    results: list[dict] = []
    grand_t0 = time.time()

    if not args.skip_nodes:
        print("=== NODES ===")
        for n in cfg["nodes"]:
            label = n["label"]
            if only is not None and label not in only:
                continue
            pq_path = args.parquet_dir / "nodes" / f"{label}.parquet"
            if not pq_path.exists():
                print(f"  SKIP (missing): {pq_path}")
                continue
            results.append(load_node(spark, pq_path, label, n["primary_key"],
                                     node_creds, args.partitions, args.node_mode))
        print()

    if not args.skip_rels:
        print("=== RELATIONSHIPS ===")
        # Build label -> volume lookup for hot-rel detection
        node_volumes = {n["label"]: int(n["volume"]) for n in cfg["nodes"]}
        for r in cfg["relationships"]:
            rel_type = r["type"]
            if only is not None and rel_type not in only:
                continue
            pq_path = args.parquet_dir / "relationships" / f"{rel_type}.parquet"
            if not pq_path.exists():
                print(f"  SKIP (missing): {pq_path}")
                continue
            tgt_label = r["target"]["label"]
            tgt_volume = node_volumes.get(tgt_label, args.hot_rel_threshold + 1)
            results.append(load_rel(
                spark, pq_path, rel_type,
                r["source"]["label"], r["source"]["key"],
                tgt_label, r["target"]["key"],
                tgt_volume,
                rel_creds, args.rel_partitions, args.hot_rel_threshold,
            ))
        print()

    grand_dt = time.time() - grand_t0
    total_rows = sum(r["rows"] for r in results)

    args.results_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.results_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "label", "rows", "seconds", "rows_per_sec"])
        w.writeheader()
        for row in results:
            w.writerow(row)
        w.writerow({"kind": "TOTAL", "label": "all",
                    "rows": total_rows,
                    "seconds": round(grand_dt, 2),
                    "rows_per_sec": round(total_rows / grand_dt if grand_dt > 0 else 0, 0)})

    print(f"Total rows:    {total_rows:,}")
    print(f"Total time:    {grand_dt:.1f}s")
    print(f"Effective:     {total_rows / grand_dt if grand_dt > 0 else 0:,.0f} rows/sec")
    print(f"Results CSV:   {args.results_csv}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
