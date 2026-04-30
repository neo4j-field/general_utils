# Neo4j bulk load module

High-throughput parquet → Neo4j Aura loader, designed to fix the four most
common causes of transaction-memory pressure on large narrow-relationship
loads. Built on top of the official
[Neo4j Spark Connector](https://neo4j.com/docs/spark/current/installation/).

## Problem statement

A Neo4j customer is loading their graph from parquet and consistently
hitting the database's per-transaction memory limit on the relationship
phase. Their shape:

| | Customer | This benchmark (1/4 ratio) |
|---|---:|---:|
| Memory (Aura) | 128 GB | 32 GB |
| Node types | 22 | 22 |
| Total node rows | 136 M | 34 M |
| Relationship types | 33 | 33 |
| Total rel rows | 213 M | 53 M |
| Rel shape | 2-4 keys + insert_dtm | same |
| Node columns | up to 33 | up to 33 |

Node ingestion is fine for them. The pain is in relationships.

## Why transaction memory blows up on rel loads (and how this loader fixes it)

| Root cause | Fix in this loader |
|---|---|
| No unique constraint on node primary keys → every rel MERGE does a label scan, which serializes into the transaction. | `schema_setup.py` creates a unique constraint on every node PK before any data load. |
| Spark Connector default rel-write rebuilds full source/target nodes per batch. | `relationship.save.strategy=keys` forces a thin `MATCH(src{k}) MATCH(tgt{k}) CREATE` pattern. |
| `batch.size` set too high (or default left unchanged after schema growth). | `--batch-size` exposed as a CLI flag, default 5000. |
| Forseti deadlocks on adjacent-node locks under any non-trivial rel-write parallelism. | Default `rel-partitions=1`. See **Two non-obvious gotchas** below. |
| Hot dimension targets (e.g. 5 RiskRatings shared by millions of rels) deadlock instantly. | `--hot-rel-threshold` (default 1000): rels whose target volume is below this auto-fall back to single-partition writes. |

## Two non-obvious gotchas (smoke test surfaced both)

These two bit us in the smoke test and would bite the customer too. Worth
calling out explicitly because neither is documented in the connector docs.

### 1. Random FK distribution causes Forseti deadlocks at ANY parallelism > 1

Source-key repartitioning isolates which Spark partition owns writes for a
given source node, but it does NOT prevent two partitions from writing rels
that share a target node. Every relationship write acquires
`EXCLUSIVE NODE_RELATIONSHIP_GROUP_DELETE` on both source AND target. With
random FK distribution, target overlap across partitions is statistically
guaranteed, and Forseti's deadlock detector kills one of the writers.

**Default to `rel-partitions=1` and only raise it after empirical testing
on your specific FK distribution.** Sparse rels (where each target has
~1 incoming rel) are safe to parallelize. Dense rels (where targets are
shared across many sources) are not.

### 2. Plain `local[*]` master silently ignores `spark.task.maxFailures`

Setting `spark.task.maxFailures=10` does nothing in `local[*]` mode. Any
task failure (including retriable `TransientException`) immediately aborts
the job. The fix is the documented but rarely-used `local[*,N]` master URL
syntax (where N is the per-task retry count). This loader uses `local[*,10]`.

In cluster mode (YARN, k8s, standalone) `spark.task.maxFailures` works as
expected, so this gotcha is local-mode-only.

## Architecture

```
        bulk_load/
        ├── config/data_model.yaml         (single source of truth — 22 nodes, 33 rels)
        │
        ├── src/generate_synthetic_data.py (reads YAML → writes parquet, vectorized numpy)
        ├── src/schema_setup.py            (reads YAML → creates unique constraints)
        ├── src/load_to_neo4j.py           (reads YAML + parquet → Spark Connector write)
        │
        └── scripts/
            ├── provision_vm.sh            (idempotent VM bootstrap: Java, Python, jars)
            └── run_load_benchmark.sh      (orchestrates: generate → schema → load → CSV)
```

The YAML drives everything. Change a column type or volume in one place
and all three Python scripts pick it up.

## Trade-offs and design decisions

1. **Spark Connector over PyIngest.** At 87M rows, partition-level
   parallelism matters more than ops simplicity. PyIngest is a solid
   choice up to ~50M rows but leaves performance on the table above that.
2. **Local Spark mode, not EMR or a cluster.** Aura is the receiver and
   has 6 CPUs / one WAL. A bigger Spark cluster cannot push more
   throughput through the receiver. Local mode on a same-region VM
   gives full performance with zero cluster ops.
3. **Same-region VM, not laptop.** RTT to Aura drops from 30-80 ms to
   <1 ms. At 5K-row batches that is the difference between hours of
   network wait and minutes of actual database work.
4. **Generate parquet on the same VM, not S3/GCS.** The data is
   throwaway and the VM's local SSD is faster and cheaper.
5. **Unique constraints created BEFORE any load, not after.** Creating
   constraints after the data lands forces a full re-scan and is the
   slowest possible path.
6. **`batch.size=5000` default, not larger.** Counterintuitive for
   nodes (where bigger is faster) but correct for narrow relationships
   on a memory-constrained instance. Customer's pain is solved by a
   modest value here, not a large one.
7. **Synthetic data is intentionally `garbage-in`.** No faker, no real
   PII. The point is to exercise the load path at volume; the data
   semantics don't matter.

## Quick start (on the GCP us-east1 VM)

The VM provisioning command is in [`scripts/provision_vm.sh`](scripts/provision_vm.sh).
Once it has run on a fresh Debian 12 VM:

```bash
# Source the env updates (Spark Connector jar path, venv alias)
source ~/.bashrc
activate-bulkload

# Smoke test at 1% volume (~340K nodes, ~530K rels) first
SCALE=0.01 ./bulk_load/scripts/run_load_benchmark.sh ~/Neo4j-*.txt

# Full benchmark at 1.0x
./bulk_load/scripts/run_load_benchmark.sh ~/Neo4j-*.txt
```

## Tunable knobs (all surface as env vars)

| Var | Default | What it controls |
|---|---|---|
| `BATCH_SIZE` | `5000` | Rows per Bolt transaction. The single most important fix for transaction memory. |
| `PARTITIONS` | `8` | Spark partitions for NODE writes. Nodes don't lock-contend, so saturate the writer side. |
| `REL_PARTITIONS` | `1` | Spark partitions for RELATIONSHIP writes. Default 1 to avoid Forseti deadlocks. Raise only after testing. |
| `HOT_REL_THRESHOLD` | `1000` | Rel types whose TARGET node count is below this force-fall back to 1-partition writes. |
| `NODE_MODE` | `merge` | `merge` is idempotent (uses upsert via `node.keys`); `create` skips upsert check, ~2x faster, fresh-load only. |
| `SCALE` | `1.0` | Multiplier on every YAML volume. Use `0.01` for a smoke test. |
| `SKIP_GENERATE` | `false` | Reuse existing parquet. |
| `SKIP_SCHEMA` | `false` | Skip constraint creation. |
| `SKIP_LOAD` | `false` | Stop after generation/schema (e.g. for size-on-disk inspection). |
| `PARQUET_DIR` | `$HOME/data/parquet` | Where parquet lives. |
| `RESULTS_DIR` | `$HOME/results` | Per-run CSV + log directory. |

## Output

Each run writes:
- `results/load_<timestamp>_b<batch>_p<parts>_<mode>.csv` — per-table rows / seconds / rows-per-sec, plus a TOTAL row.
- `results/load_<timestamp>_*.log` — full run output for post-hoc analysis.

## Smoke test results (1% scale, ~931K rows, n2-standard-8 -> 32GB Aura us-east1)

Captured 2026-04-30, end-to-end (generate + schema + load):

| Phase | Rows | Wall time |
|---|---:|---:|
| Generate parquet | 931K | ~5s |
| Schema setup (22 unique constraints) | n/a | ~3s |
| Load nodes (22 types) | 340K | ~17s |
| Load relationships (33 types) | 591K | ~54s |
| **Total** | **931K** | **~76s** |

Effective rate: **13,090 rows/sec** (single-partition rel writes, mixed
node parallelism). At full 87M-row scale this projects to ~110 minutes.
Real-world throughput typically improves at scale because per-table
overhead amortizes — the 1% smoke test pays the same per-table fixed
cost on tables with as few as 1 row, which drags the average down.

## Recommendations applicable to the customer's production load

These translate directly to their 128 GB / 24 CPU / 213 M rel environment:

1. **Run the loader from a VM in the same region/zone as the Aura instance.** Pre-test, this alone is often a 5-10x speedup.
2. **Pre-create unique constraints on all node PKs before any load.** Even if the loader does it, surface this as an explicit pre-step so it isn't accidentally skipped.
3. **Always set `relationship.save.strategy=keys`.** This is the single biggest connector option for narrow-rel loads.
4. **Start `batch.size` at 5000 and only raise it if monitoring shows transaction memory has headroom.** Bigger is not always better.
5. **Repartition on the source key before write.** A few extra seconds of shuffle pays itself back many times in reduced lock contention.
6. **Load all nodes before any relationships.** The MATCH on the rel side is only fast if the source/target nodes already exist.

## Not in scope (for now)

- PyIngest variant. Documented as an alternative but not implemented.
- Online deltas / CDC. This is initial-load only.
- Multi-database write. Single Aura instance.
- neo4j-admin import. Not available on Aura.
