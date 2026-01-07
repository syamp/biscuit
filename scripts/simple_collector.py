"""
Minimal collector that posts metrics to the FastAPI ingest endpoints.

Examples:
  python scripts/simple_collector.py --metric-name temp_c --value 22.4 --tag room=lab
  python scripts/simple_collector.py --metric-name requests --counter --value 1200 --samples 5 --interval 2
"""

import argparse
import os
import random
import time
from typing import Dict, List, Optional

import httpx


def parse_tags(tag_list: Optional[List[str]]) -> Dict[str, str]:
    tags: Dict[str, str] = {}
    if not tag_list:
        return tags
    for entry in tag_list:
        if "=" not in entry:
            continue
        key, val = entry.split("=", 1)
        if key and val:
            tags[key.strip()] = val.strip()
    return tags


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send simple metrics to the TSDB API.")
    parser.add_argument(
        "--api-base",
        default=os.environ.get("API_BASE", "http://127.0.0.1:8000"),
        help="Base URL for the FastAPI server.",
    )
    parser.add_argument("--metric-name", default="sample_metric", help="Metric name to write.")
    parser.add_argument("--metric-id", type=int, help="Metric ID to write (optional).")
    parser.add_argument("--value", type=float, help="Value to send (defaults to a random 0-100).")
    parser.add_argument(
        "--counter",
        action="store_true",
        help="Send as counter raw_value instead of gauge value.",
    )
    parser.add_argument(
        "--tag",
        action="append",
        help="Tag in key=value form. Repeat for multiple tags.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Seconds between samples; 0 sends a single sample.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=1,
        help="How many samples to send (0 = run forever).",
    )
    return parser.parse_args()


def send_sample(client: httpx.Client, api_base: str, args: argparse.Namespace) -> None:
    ts = int(time.time())
    value = args.value if args.value is not None else random.uniform(0, 100)
    tags = parse_tags(args.tag)
    endpoint = "/ingest/counter" if args.counter else "/ingest/gauge"
    payload = {
        "ts": ts,
        "name": args.metric_name,
        "tags": tags,
    }
    if args.metric_id is not None:
        payload["metric_id"] = args.metric_id
    if args.counter:
        payload["raw_value"] = value
    else:
        payload["value"] = value

    url = api_base.rstrip("/") + endpoint
    resp = client.post(url, json=payload, timeout=5.0)
    if resp.status_code != 200:
        raise RuntimeError(f"write failed ({resp.status_code}): {resp.text}")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))}] sent {value:.3f} to {url}")


def main() -> None:
    args = parse_args()
    remaining = args.samples
    with httpx.Client() as client:
        while True:
            send_sample(client, args.api_base, args)
            if args.interval <= 0 or (remaining and remaining <= 1):
                break
            if remaining:
                remaining -= 1
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
