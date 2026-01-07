#!/usr/bin/env bash
set -euo pipefail

exec 2>&1

# Environment knobs:
# - API_BASE: URL of the FastAPI server (defaults to local API service port)
# - COLLECT_INTERVAL: seconds between samples (default 5)
# - COLLECT_SAMPLES: number of samples to collect (0 = run forever)
# - COLLECT_MOUNTPOINT: filesystem path for disk usage stats (default "/")
# - COLLECT_CPU_WINDOW: seconds to wait for CPU percent calculation (default 0.2)

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
API_BASE="${API_BASE:-http://${API_HOST}:${API_PORT}}"
INTERVAL="${COLLECT_INTERVAL:-5}"
SAMPLES="${COLLECT_SAMPLES:-0}"
MOUNTPOINT="${COLLECT_MOUNTPOINT:-/}"
CPU_WINDOW="${COLLECT_CPU_WINDOW:-0.2}"

cd /workspace
exec python collect_metrics.py \
  --api-base "${API_BASE}" \
  --interval "${INTERVAL}" \
  --samples "${SAMPLES}" \
  --mountpoint "${MOUNTPOINT}" \
  --cpu-window "${CPU_WINDOW}"
