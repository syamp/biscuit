#!/usr/bin/env bash
set -euo pipefail

exec 2>&1

FDB_DATA_DIR=/var/lib/foundationdb
CLUSTER_FILE=/etc/foundationdb/fdb.cluster
FDB_PORT="${FDB_PORT:-4501}"
FDB_LISTEN="${FDB_LISTEN:-127.0.0.1}"
CONFIG_FLAG="$FDB_DATA_DIR/.configured"

mkdir -p "$FDB_DATA_DIR"

write_cluster_file() {
  echo "docker:docker@${FDB_LISTEN}:${FDB_PORT}" >"$CLUSTER_FILE"
}

write_cluster_file

while true; do
  if [ -f "$CONFIG_FLAG" ]; then
    sleep 3600
    continue
  fi

  if FDB_CLUSTER_FILE="$CLUSTER_FILE" fdbcli --exec "configure new single memory"; then
    touch "$CONFIG_FLAG"
    sleep 3600
  else
    sleep 1
  fi
done
