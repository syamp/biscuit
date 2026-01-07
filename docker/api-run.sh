#!/usr/bin/env bash
set -euo pipefail

exec 2>&1

API_HOST="${API_HOST:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"

cd /workspace
exec uvicorn api:app --host "$API_HOST" --port "$API_PORT"
