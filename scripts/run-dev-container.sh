#!/usr/bin/env bash
set -euo pipefail

IMAGE=${IMAGE:-biscuit}
DATA_DIR=${DATA_DIR:-/fdb}
API_PORT=${API_PORT:-8000}
API_HOST=${API_HOST:-0.0.0.0}
FDB_PORT=${FDB_PORT:-4501}
FDB_LISTEN=${FDB_LISTEN:-127.0.0.1}

mkdir -p "$DATA_DIR"/{data,logs}

podman run --rm -it \
  --privileged \
  --security-opt seccomp=unconfined \
  -v "$(pwd)":/workspace:z \
  -v "/home/syam/.codex":/root/.codex:z \
  -v "$DATA_DIR/data":/var/lib/foundationdb \
  -v "$DATA_DIR/logs":/var/log/foundationdb \
  -e FDB_LISTEN="$FDB_LISTEN" \
  -e FDB_PORT="$FDB_PORT" \
  -e API_HOST="$API_HOST" \
  -e API_PORT="$API_PORT" \
  -p "$API_PORT:$API_PORT" \
  "$IMAGE"
