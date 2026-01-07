#!/usr/bin/env python3
"""
Quick-and-dirty query path micro-benchmark.

It times `QueryEngine.run_sql` against a single metric and reports latency stats.
Uses an existing metric if available; otherwise seeds a synthetic gauge metric so
the benchmark can run against a local FoundationDB cluster.
"""

import argparse
import math
import statistics
import time
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
# Ensure repo root is importable when running from scripts/
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from query_engine import QueryEngine
from tsdb_fdb import FdbTsdb, get_db, init_tsdb


def ensure_metric_with_data(tsdb: FdbTsdb, points: int = 2_000, step: int = 1) -> int:
    """
    Return an existing metric_id if present; otherwise create a synthetic one with sample data.
    """
    existing = tsdb.list_metrics()
    if existing:
        return int(existing[0]["metric_id"])

    now = int(time.time())
    start_ts = now - points * step
    metric_id = tsdb.write_gauge(None, start_ts, 0.0, name="perf_metric", tags={"scenario": "perf"}, step=step)
    for idx in range(1, points):
        ts_val = start_ts + idx * step
        val = math.sin(idx / 25.0) * 100 + idx % 50
        tsdb.write_gauge(metric_id, ts_val, val, step=step)
    return metric_id


def time_query(engine: QueryEngine, metric_id: int, start_ts: int, end_ts: int, bucket: int, iterations: int) -> List[float]:
    sql = f"""
SELECT ts_bucket(ts, {bucket}) AS bucket, avg(value) AS value
FROM samples
WHERE metric_id = {metric_id} AND ts >= {start_ts} AND ts <= {end_ts}
GROUP BY bucket
ORDER BY bucket
""".strip()

    durations_ms: List[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        rows = engine.run_sql([metric_id], start_ts, end_ts, sql)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        durations_ms.append(elapsed_ms)
        row_count = len(rows)
    print(f"Last query returned {row_count} rows")
    return durations_ms


def summarize(label: str, samples: List[float]) -> None:
    if not samples:
        print(f"{label}: no samples")
        return
    sorted_samples = sorted(samples)
    p50 = statistics.median(sorted_samples)
    p95_idx = max(0, int(len(sorted_samples) * 0.95) - 1)
    p95 = sorted_samples[p95_idx]
    print(
        f"{label}: count={len(samples)}, min={sorted_samples[0]:.2f}ms, "
        f"p50={p50:.2f}ms, p95={p95:.2f}ms, max={sorted_samples[-1]:.2f}ms"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the query path against FoundationDB-backed samples.")
    parser.add_argument("--cluster-file", help="Path to fdb.cluster (defaults to env/FDB defaults)", default=None)
    parser.add_argument("--iterations", type=int, default=10, help="Number of timed query runs (default: 10)")
    parser.add_argument("--range-seconds", type=int, default=3600, help="Time window to query (default: last hour)")
    parser.add_argument("--bucket", type=int, default=60, help="Bucket size in seconds (default: 60)")
    args = parser.parse_args()

    db = get_db(args.cluster_file)
    tsdb = init_tsdb(db)
    engine = QueryEngine(tsdb)

    metric_id = ensure_metric_with_data(tsdb)
    now = int(time.time())
    start_ts = now - args.range_seconds
    end_ts = now

    print(f"Benchmarking metric_id={metric_id} over last {args.range_seconds}s with {args.bucket}s buckets")
    # Warmup run (not timed)
    engine.run_sql([metric_id], start_ts, end_ts, f"SELECT 1 FROM samples WHERE metric_id = {metric_id} LIMIT 1")

    durations = time_query(engine, metric_id, start_ts, end_ts, args.bucket, args.iterations)
    summarize("Query latency", durations)


if __name__ == "__main__":
    main()
