# Neo4j General Utilities

Production-grade utility scripts for operating Neo4j databases. Safe to
run against Neo4j Aura, self-hosted Enterprise, or Community.

## Scripts

### `reset_to_blank_neo4j_db.sh`

Wipes any Neo4j database reachable via cypher-shell back to a clean
state (no data, no schema). Designed for large-volume resets where a
naive `MATCH (n) DETACH DELETE n` would either be painfully slow or
fail outright.

**Works against:**

- Neo4j Aura (any tier, `neo4j+s://` URI)
- Neo4j Enterprise / Community, self-hosted (`bolt://` or `neo4j://`)
- Causal cluster members (writes auto-forward to leader)

**Design decisions, in order of impact:**

1. **Drop constraints and indexes before deleting data.** Index
   maintenance is the hidden tax on `DETACH DELETE` at scale.
2. **Delete relationships before nodes.** Isolated relationship deletes
   are small, parallelizable units; once they're gone, node deletes have
   nothing to detach.
3. **Adaptive parallelism via `apoc.periodic.iterate`.** The `auto`
   strategy picks serial or parallel from live relationship counts.
   Above ~100K relationships, parallelism wins — see
   [BENCHMARK.md](./BENCHMARK.md) for the full matrix.
4. **Count-store stats (`apoc.meta.stats`)** instead of a full `MATCH`
   scan, so the reset doesn't spend minutes counting before doing any
   work.
5. **Password via `NEO4J_PASSWORD` env var**, never on the command line.

#### Usage

```bash
# A. Using a credentials file (recommended)
./reset_to_blank_neo4j_db.sh <path_to_credentials_file.txt>

# B. Passing arguments directly
./reset_to_blank_neo4j_db.sh <username> <password> <uri>
```

See [Neo4j-Aura-Credentials-Sample.txt](./Neo4j-Aura-Credentials-Sample.txt)
for the expected credentials-file format.

#### Tuning

| Env var | Default | Effect |
|---|---|---|
| `BATCH_SIZE` | `50000` | Rows per sub-transaction. |
| `PARALLEL_RELS` | `auto` | `auto` / `true` / `false`. |
| `PARALLEL_THRESHOLD` | `100000` | Rel count above which `auto` picks parallel. |
| `SKIP_STATS` | `false` | Skip pre-counts for fastest path on known-dirty instances. |
| `DATABASE` | `neo4j` | Target database name. |

#### Performance (AuraDB Professional, 3 CPU / 16 GB)

| Relationships | Serial (s) | Parallel (s) | Speedup |
|---:|---:|---:|---:|
|   250,000 | 18.04 | 14.95 | 1.21× |
| 2,500,000 | 33.20 | 26.24 | 1.27× |
| 5,000,000 | 57.80 | 40.88 | 1.41× |

Full methodology, interpretation, and notes on sub-linear CPU scaling
are in [BENCHMARK.md](./BENCHMARK.md).

### `load_benchmark_data.sh`

Generates a synthetic graph (persons, companies, products, transactions,
plus four relationship types) with constraints and indexes, sized for a
target volume. Used to produce reproducible reset benchmarks. Safe to
run against a disposable instance only.

### `run_matrix.sh`

Runs the full volume-by-strategy benchmark matrix used to derive
`PARALLEL_THRESHOLD`. Writes CSV results to `/tmp/matrix_results.csv`.

```bash
./run_matrix.sh <credentials_file>
```

## License

MIT — see [LICENSE](./LICENSE).
