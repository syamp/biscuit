#!/usr/bin/env bash
set -euo pipefail

exec 2>&1

FDB_DATA_DIR=/var/lib/foundationdb
FDB_LOG_DIR=/var/log/foundationdb
CLUSTER_FILE=/etc/foundationdb/fdb.cluster
FDB_PORT="${FDB_PORT:-4501}"
FDB_LISTEN="${FDB_LISTEN:-127.0.0.1}"

mkdir -p "$FDB_DATA_DIR" "$FDB_LOG_DIR"

write_cluster_file() {
  echo "docker:docker@${FDB_LISTEN}:${FDB_PORT}" >"$CLUSTER_FILE"
}

write_cluster_file

exec fdbserver \
  -p "${FDB_LISTEN}:${FDB_PORT}" \
  -d "$FDB_DATA_DIR" \
  -C "$CLUSTER_FILE" \
  --logdir "$FDB_LOG_DIR"
