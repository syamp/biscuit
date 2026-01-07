#!/usr/bin/env bash
set -euo pipefail

IMAGE=${IMAGE:-tsdb-proto}
DATA_DIR=${DATA_DIR:-/data/fdb}
API_PORT=${API_PORT:-8000}
API_HOST=${API_HOST:-0.0.0.0}
FDB_PORT=${FDB_PORT:-4501}
FDB_LISTEN=${FDB_LISTEN:-127.0.0.1}

mkdir -p "$DATA_DIR"/{data,logs}

podman run --rm -it \
  --net=host \
  -v "$(pwd)":/workspace:z \
  -v "$DATA_DIR/data":/var/lib/foundationdb \
  -v "$DATA_DIR/logs":/var/log/foundationdb \
  -e FDB_LISTEN="$FDB_LISTEN" \
  -e FDB_PORT="$FDB_PORT" \
  -e API_HOST="$API_HOST" \
  -e API_PORT="$API_PORT" \
  "$IMAGE"
