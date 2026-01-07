"""
Collect local system metrics and push them into the FoundationDB-backed TSDB.

Counters use raw monotonic byte counts so downstream readers can compute rates;
gauges track instantaneous utilization.
"""

import argparse
import math
import os
import socket
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import httpx
import psutil


@dataclass
class MetricDef:
    name: str
    metric_id: int
    kind: str  # "gauge" or "counter"
    extractor: Callable[[Dict[str, float]], Optional[float]]


METRICS = [
    MetricDef("cpu_percent", 3001, "gauge", lambda s: s.get("cpu_percent")),
    MetricDef("load_avg_1m", 3002, "gauge", lambda s: s.get("load_avg_1m")),
    MetricDef("mem_used_percent", 3003, "gauge", lambda s: s.get("mem_used_percent")),
    MetricDef("disk_used_percent", 3004, "gauge", lambda s: s.get("disk_used_percent")),
    MetricDef("disk_read_bytes", 3010, "counter", lambda s: s.get("disk_read_bytes")),
    MetricDef("disk_write_bytes", 3011, "counter", lambda s: s.get("disk_write_bytes")),
    MetricDef("net_bytes_sent", 3020, "counter", lambda s: s.get("net_bytes_sent")),
    MetricDef("net_bytes_recv", 3021, "counter", lambda s: s.get("net_bytes_recv")),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest basic system metrics into the TSDB.")
    parser.add_argument(
        "--api-base",
        default=os.environ.get("API_BASE", "http://127.0.0.1:8000"),
        help="Base URL for the FastAPI server (defaults to http://127.0.0.1:8000).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between samples. Use 0 to collect a single snapshot.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=0,
        help="Number of samples to collect (0 runs until interrupted).",
    )
    parser.add_argument(
        "--mountpoint",
        default="/",
        help="Filesystem path to measure for disk usage.",
    )
    parser.add_argument(
        "--cpu-window",
        type=float,
        default=0.2,
        help="Seconds to wait for CPU percent calculation.",
    )
    return parser.parse_args()


def collect_snapshot(mountpoint: str, cpu_window: float) -> Dict[str, float]:
    now = int(time.time())
    cpu_percent = psutil.cpu_percent(interval=max(cpu_window, 0.0))
    try:
        load_avg_1m = os.getloadavg()[0]
    except (AttributeError, OSError):
        load_avg_1m = float("nan")

    mem = psutil.virtual_memory()
    try:
        disk = psutil.disk_usage(mountpoint)
        disk_used_percent = float(disk.percent)
    except FileNotFoundError:
        disk_used_percent = float("nan")

    disk_io = psutil.disk_io_counters()
    net_io = psutil.net_io_counters()
    disk_read_bytes = float(disk_io.read_bytes) if disk_io else float("nan")
    disk_write_bytes = float(disk_io.write_bytes) if disk_io else float("nan")
    net_bytes_sent = float(net_io.bytes_sent) if net_io else float("nan")
    net_bytes_recv = float(net_io.bytes_recv) if net_io else float("nan")
    return {
        "ts": now,
        "cpu_percent": float(cpu_percent),
        "load_avg_1m": float(load_avg_1m),
        "mem_used_percent": float(mem.percent),
        "disk_used_percent": disk_used_percent,
        "disk_read_bytes": disk_read_bytes,
        "disk_write_bytes": disk_write_bytes,
        "net_bytes_sent": net_bytes_sent,
        "net_bytes_recv": net_bytes_recv,
    }


def _should_skip(value: Optional[float]) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def ingest_snapshot(
    client: httpx.Client,
    api_base: str,
    snapshot: Dict[str, float],
    base_tags: Optional[Dict[str, str]] = None,
) -> int:
    ts = int(snapshot["ts"])
    base_tags = base_tags or {}
    written = 0
    for metric in METRICS:
        value = metric.extractor(snapshot)
        if _should_skip(value):
            continue
        endpoint = "/ingest/gauge" if metric.kind == "gauge" else "/ingest/counter"
        payload = (
            {
                "metric_id": metric.metric_id,
                "ts": ts,
                "value": float(value),
                "name": metric.name,
                "tags": base_tags,
            }
            if metric.kind == "gauge"
            else {
                "metric_id": metric.metric_id,
                "ts": ts,
                "raw_value": float(value),
                "name": metric.name,
                "tags": base_tags,
            }
        )
        url = api_base.rstrip("/") + endpoint
        resp = client.post(url, json=payload, timeout=5.0)
        if resp.status_code != 200:
            raise RuntimeError(f"failed to write {metric.name} ({resp.status_code}): {resp.text}")
        written += 1
    return written


def main() -> None:
    args = parse_args()
    default_tags = {"host": socket.gethostname()}
    remaining = args.samples
    with httpx.Client() as client:
        while True:
            snapshot = collect_snapshot(args.mountpoint, cpu_window=args.cpu_window)
            written = ingest_snapshot(client, args.api_base, snapshot, base_tags=default_tags)
            ts_readable = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(snapshot["ts"]))
            print(f"[{ts_readable}] wrote {written} metrics to {args.api_base}")
            if args.interval <= 0 or (remaining and remaining <= 1):
                break
            if remaining:
                remaining -= 1
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
