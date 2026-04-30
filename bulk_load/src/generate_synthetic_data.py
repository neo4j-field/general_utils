"""
generate_synthetic_data.py

Reads bulk_load/config/data_model.yaml and writes one parquet file per node
label and per relationship type into the configured output directory.

Design goals:
  * No per-row Python loops. Everything vectorized via numpy + pyarrow.
  * Streaming: never holds more than `rows_per_row_group` rows in memory.
  * Reproducible: a single seed propagates to every per-table generator.
  * Schema-faithful: pyarrow schema is built from the YAML so the loader's
    Spark Connector reads the exact types we wrote.

Performance reference (n2-standard-8, Debian 12):
  * 34M nodes + 53M rels ~= 87M rows
  * Expected wall time: 4-7 minutes total
  * Output size: ~5-7 GB parquet (snappy)

Usage:
    python generate_synthetic_data.py \
        --config bulk_load/config/data_model.yaml \
        --output /var/data/parquet
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


EPOCH_DATE = date(1970, 1, 1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, type=Path,
                   help="Path to data_model.yaml")
    p.add_argument("--output", required=True, type=Path,
                   help="Output directory for parquet files")
    p.add_argument("--scale", type=float, default=1.0,
                   help="Multiplier on every volume in the YAML (default 1.0). "
                        "Use e.g. 0.01 for a smoke test.")
    p.add_argument("--only", nargs="+", default=None,
                   help="If set, only generate these labels/rel-types.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Per-column generators. Each returns a numpy array of length `n`.
# ---------------------------------------------------------------------------

def _gen_column(col: dict, n: int, row_offset: int, rng: np.random.Generator,
                date_min_days: int, date_max_days: int,
                ts_min_micros: int, ts_max_micros: int) -> np.ndarray:
    t = col["type"]
    if t == "sequential_id":
        return np.arange(row_offset + 1, row_offset + 1 + n, dtype=np.int64)
    if t == "int":
        return rng.integers(col["low"], col["high"] + 1, size=n, dtype=np.int64)
    if t == "bigint":
        return rng.integers(col["low"], col["high"] + 1, size=n, dtype=np.int64)
    if t == "float":
        return rng.uniform(col["low"], col["high"], size=n).astype(np.float64)
    if t == "bool":
        return rng.integers(0, 2, size=n, dtype=np.int8).astype(np.bool_)
    if t == "category":
        values = col["values"]
        idx = rng.integers(0, len(values), size=n, dtype=np.int64)
        return np.asarray(values, dtype=object)[idx]
    if t == "date":
        return rng.integers(date_min_days, date_max_days + 1, size=n, dtype=np.int32)
    if t == "timestamp":
        return rng.integers(ts_min_micros, ts_max_micros + 1, size=n, dtype=np.int64)
    if t == "string_template":
        prefix = col["prefix"]
        ids = np.arange(row_offset + 1, row_offset + 1 + n, dtype=np.int64)
        return np.char.add(f"{prefix}_", ids.astype(str))
    raise ValueError(f"Unknown column type: {t}")


def _arrow_field(col: dict) -> pa.Field:
    t = col["type"]
    if t in ("sequential_id", "int", "bigint"):
        return pa.field(col["name"], pa.int64())
    if t == "float":
        return pa.field(col["name"], pa.float64())
    if t == "bool":
        return pa.field(col["name"], pa.bool_())
    if t == "category" or t == "string_template":
        return pa.field(col["name"], pa.string())
    if t == "date":
        return pa.field(col["name"], pa.date32())
    if t == "timestamp":
        return pa.field(col["name"], pa.timestamp("us"))
    raise ValueError(f"Unknown column type: {t}")


def _array_to_arrow(field: pa.Field, arr: np.ndarray) -> pa.Array:
    if pa.types.is_date32(field.type):
        return pa.array(arr, type=pa.date32())
    if pa.types.is_timestamp(field.type):
        return pa.array(arr, type=pa.timestamp("us"))
    return pa.array(arr, type=field.type)


# ---------------------------------------------------------------------------
# Streaming writer for one node label.
# ---------------------------------------------------------------------------

def _write_node_table(
    node: dict,
    output_dir: Path,
    rows_per_group: int,
    rng: np.random.Generator,
    date_min_days: int,
    date_max_days: int,
    ts_min_micros: int,
    ts_max_micros: int,
    compression: str,
) -> tuple[Path, int, float]:
    label: str = node["label"]
    volume: int = int(node["volume"])
    columns = node["columns"]

    schema = pa.schema([_arrow_field(c) for c in columns])
    out_path = output_dir / "nodes" / f"{label}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    written = 0
    with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
        while written < volume:
            chunk = min(rows_per_group, volume - written)
            arrays = []
            for col in columns:
                np_arr = _gen_column(
                    col, chunk, written, rng,
                    date_min_days, date_max_days,
                    ts_min_micros, ts_max_micros,
                )
                arrays.append(_array_to_arrow(schema.field(col["name"]), np_arr))
            writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
            written += chunk
    return out_path, written, time.time() - t0


# ---------------------------------------------------------------------------
# Streaming writer for one relationship type. Endpoints are uniform random
# FKs into the source/target node id ranges. This intentionally allows
# duplicate (src, tgt) pairs, mirroring real-world fact tables that haven't
# been deduplicated.
# ---------------------------------------------------------------------------

def _write_rel_table(
    rel: dict,
    output_dir: Path,
    node_volumes: dict[str, int],
    rows_per_group: int,
    rng: np.random.Generator,
    date_min_days: int,
    date_max_days: int,
    ts_min_micros: int,
    ts_max_micros: int,
    compression: str,
) -> tuple[Path, int, float]:
    rel_type: str = rel["type"]
    volume: int = int(rel["volume"])
    src = rel["source"]
    tgt = rel["target"]
    src_label = src["label"]
    tgt_label = tgt["label"]
    src_key = src["key"]
    tgt_key = tgt["key"]

    src_vol = node_volumes[src_label]
    tgt_vol = node_volumes[tgt_label]

    src_field_name = f"src_{src_key}"
    tgt_field_name = f"tgt_{tgt_key}"
    fields = [
        pa.field(src_field_name, pa.int64()),
        pa.field(tgt_field_name, pa.int64()),
    ]
    properties = rel.get("properties", []) or []
    for p in properties:
        fields.append(_arrow_field(p))
    schema = pa.schema(fields)

    out_path = output_dir / "relationships" / f"{rel_type}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    written = 0
    with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
        while written < volume:
            chunk = min(rows_per_group, volume - written)
            arrays = [
                pa.array(rng.integers(1, src_vol + 1, size=chunk, dtype=np.int64),
                         type=pa.int64()),
                pa.array(rng.integers(1, tgt_vol + 1, size=chunk, dtype=np.int64),
                         type=pa.int64()),
            ]
            for p in properties:
                np_arr = _gen_column(
                    p, chunk, written, rng,
                    date_min_days, date_max_days,
                    ts_min_micros, ts_max_micros,
                )
                arrays.append(_array_to_arrow(schema.field(p["name"]), np_arr))
            writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
            written += chunk
    return out_path, written, time.time() - t0


# ---------------------------------------------------------------------------
# Orchestration.
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    cfg = yaml.safe_load(args.config.read_text())
    g = cfg["global"]
    rng = np.random.default_rng(int(g["seed"]))
    rows_per_group = int(g["rows_per_row_group"])
    compression = g.get("compression", "snappy")

    date_min = date.fromisoformat(g["date_min"])
    date_max = date.fromisoformat(g["date_max"])
    date_min_days = (date_min - EPOCH_DATE).days
    date_max_days = (date_max - EPOCH_DATE).days
    ts_min_micros = date_min_days * 86_400_000_000
    ts_max_micros = date_max_days * 86_400_000_000

    args.output.mkdir(parents=True, exist_ok=True)

    only = set(args.only) if args.only else None
    scale = float(args.scale)

    nodes = cfg["nodes"]
    rels = cfg["relationships"]
    node_volumes: dict[str, int] = {n["label"]: max(1, int(int(n["volume"]) * scale)) for n in nodes}

    print(f"Output dir:   {args.output}")
    print(f"Scale:        {scale}")
    print(f"Compression:  {compression}")
    print(f"Row group:    {rows_per_group:,} rows")
    print(f"Nodes:        {len(nodes)}    Total rows: {sum(node_volumes.values()):,}")
    rel_volumes_total = sum(max(1, int(int(r['volume']) * scale)) for r in rels)
    print(f"Rels:         {len(rels)}    Total rows: {rel_volumes_total:,}")
    print()

    grand_t0 = time.time()
    grand_rows = 0

    print("=== NODES ===")
    for n in nodes:
        if only is not None and n["label"] not in only:
            continue
        n_scaled = dict(n)
        n_scaled["volume"] = node_volumes[n["label"]]
        path, rows, dt = _write_node_table(
            n_scaled, args.output, rows_per_group, rng,
            date_min_days, date_max_days, ts_min_micros, ts_max_micros, compression,
        )
        sz_mb = path.stat().st_size / 1024 / 1024
        print(f"  {n['label']:<14} rows={rows:>12,}  size={sz_mb:>8.1f} MB  time={dt:>6.1f}s  -> {path.name}")
        grand_rows += rows

    print()
    print("=== RELATIONSHIPS ===")
    for r in rels:
        if only is not None and r["type"] not in only:
            continue
        r_scaled = dict(r)
        r_scaled["volume"] = max(1, int(int(r["volume"]) * scale))
        path, rows, dt = _write_rel_table(
            r_scaled, args.output, node_volumes, rows_per_group, rng,
            date_min_days, date_max_days, ts_min_micros, ts_max_micros, compression,
        )
        sz_mb = path.stat().st_size / 1024 / 1024
        print(f"  {r['type']:<22} rows={rows:>12,}  size={sz_mb:>8.1f} MB  time={dt:>6.1f}s  -> {path.name}")
        grand_rows += rows

    grand_dt = time.time() - grand_t0
    rate = grand_rows / grand_dt if grand_dt > 0 else 0
    print()
    print(f"Total rows written: {grand_rows:,}")
    print(f"Total wall time:    {grand_dt:.1f}s")
    print(f"Effective rate:     {rate:,.0f} rows/sec")
    return 0


if __name__ == "__main__":
    sys.exit(main())
