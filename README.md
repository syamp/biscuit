### Everything in this repo except this statement is 99% codex. Use at your own risk.

# Biscuit - a tsdb 

Minimal time-series storage built on FoundationDB with a ring buffer per metric and a DataFusion-powered SQL surface.

![Sample metrics UI](metrics.png)

## Zero-to-cURL quick start

This is the fastest way to experience the system: launch the Podman image, POST a few samples, and run a SQL query.


1. **Build and start the container**

   ```bash
   podman build -t tsdb-proto .
   podman run --rm -it \
     --privileged \
     --security-opt seccomp=unconfined \
     -p 8000:8000 \
     -v "$(pwd):/workspace:z" \
     tsdb-proto
   ```

   The container brings up FoundationDB, the FastAPI server, and the demo collector under runit. Port `8000` now exposes the API to your host. The first startup spends a few seconds auto-configuring FoundationDB (`configure new single memory`), so wait until the logs go quiet before sending requests.

2. **Send a gauge datapoint**

   ```bash
   API=http://127.0.0.1:8000
   curl -s "$API/ingest/gauge" \
     -H "Content-Type: application/json" \
     -d "{\"name\":\"cpu_percent\",\"ts\":$(date +%s),\"value\":37.5,\"tags\":{\"host\":\"demo\"}}"
   ```

3. **Look up the metric_id**

   Requires `jq`; alternatively inspect the JSON manually.

   ```bash
   metric_id=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"cpu_percent"}' | jq -r '.metrics[0].metric_id // empty')
   if [ -z "$metric_id" ]; then
     echo "metric cpu_percent not found (did ingest succeed?)"
     exit 1
   fi
   echo "metric_id=$metric_id"
   ```

4. **Query the samples with SQL**

   ```bash
   start_ts=$(( $(date +%s) - 600 ))
   end_ts=$(date +%s)
   curl -s "$API/query" \
     -H "Content-Type: application/json" \
     -d "{\"metric_ids\":[$metric_id],\"start_ts\":$start_ts,\"end_ts\":$end_ts,\"sql\":\"SELECT ts, value FROM samples WHERE metric_id = $metric_id ORDER BY ts\"}" \
     | jq
   ```

   Swap in your own SQL (`ts_bucket`, `bucket_rate`, and friends are registered) or hit `/metrics/$metric_id/series` for pre-bucketed JSON.

5. **Open the UI**

   Visit [http://127.0.0.1:8000/ui](http://127.0.0.1:8000/ui) in your browser to explore dashboards that read from the same API.

Shutdown the container with `Ctrl+C` when finished.

## How it works

The system stores metric samples in a bounded ring per metric (tuple key `(1, metric_id, slot)`), metadata in `(2, metric_id)`, and counter state in `(3, metric_id)`. Each metric keeps `slots` samples where `slot = (ts // step) % slots`, so storage stays constant over time. Gauges keep raw values, counters keep cumulative values, and `bucket_rate` derives per-bucket rates at query time. Retention is explicit (no TTLs); delete or rewrite metrics via the API when needed.

### Design highlights (see `DESIGN.md` for full rationale)

- Fixed-size rings mean storage is bounded by construction (`num_metrics × slots × record_size`).
- Writes overwrite existing slots instead of appending, so reducing traffic immediately reduces pressure.
- There are no TTLs/compactions/deletes—old data is naturally overwritten on schedule.
- FoundationDB handles durability/replication; DataFusion handles SQL, letting the two evolve independently.
- Backpressure is predictable: overload shows up as latency, not surprise disk or compaction spikes.

For deeper reasoning, tradeoffs, and comparisons to other TSDB models, read [`DESIGN.md`](DESIGN.md).

### Available endpoints

- `POST /ingest/gauge` – `{ "metric_id"?, "name"?, "ts", "value", "tags"?, "step"?, "slots"? }`
- `POST /ingest/counter` – `{ "metric_id"?, "name"?, "ts", "raw_value", "tags"?, "step"?, "slots"? }`
- `POST /query` – `{ "metric_ids": [...], "start_ts", "end_ts", "sql" }`; tables: `samples`, `metrics`, `metric_tags`; UDFs: `ts_bucket`, `bucket_rate`, `clamp`, `align_time`, `null_if_outside`, `series_add/sub/mul/div`
- `GET /metrics` – list metric metadata
- `POST /metrics/lookup` – match name/tags
- `GET /metrics/{metric_id}/series` – pre-bucketed time series (uses `bucket_rate` for counters)
- `POST /metrics/{metric_id}/retention` – gauges-only rewrite `{ "step": int, "slots": int }`
- `DELETE /metrics/{metric_id}` – drop metric + values
- Dashboards live under `/ui` with `GET/POST /dashboards`


## Sample queries

Every query is a two-step process:

### 1. Discover the metric IDs you care about

- UI: open `/metrics`, search for your metric, and copy the ID column.
- SQL: run `SELECT metric_id FROM metrics WHERE name = 'cpu_percent';` via the UI’s SQL tab or `/query`.
- API/CLI: call `/metrics/lookup`, for example:

  ```bash
  metric_id=$(curl -s "$API/metrics/lookup" \
    -H "Content-Type: application/json" \
    -d '{"name":"cpu_percent"}' | jq -r '.metrics[0].metric_id // empty')
  ```

### 2. Run SQL against those IDs

Send SQL from the command line with `curl` + `/query`; pass the metric IDs you just resolved plus time bounds. Example:

```bash
curl -s "$API/query" \
  -H "Content-Type: application/json" \
  -d "{\"metric_ids\":[$metric_id],\"start_ts\":1700000000,\"end_ts\":1700003600,\"sql\":\"SELECT ts, value FROM samples WHERE metric_id = $metric_id ORDER BY ts\"}" \
  | jq
```

That same payload is what the UI issues behind the scenes when you run a query, so you can prototype in the terminal and paste back into dashboards later.

### Example recipes

Each example shows the two exact commands you’d run: one to discover the metric ID, one to query it.

#### Inspect raw gauge samples

1. Find the metric ID (using the metric name `cpu_percent` as an example):

   ```bash
   CPU_METRIC_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"cpu_percent"}' \
     | jq -r '.metrics[0].metric_id // empty')
   ```

2. Pull the last 10 minutes of samples:

   ```bash
   START_TS=$(( $(date +%s) - 600 ))
   END_TS=$(date +%s)

   curl -s "$API/query" \
     -H "Content-Type: application/json" \
     -d "{\"metric_ids\":[${CPU_METRIC_ID}],\"start_ts\":$START_TS,\"end_ts\":$END_TS,\"sql\":\"SELECT ts, value FROM samples WHERE metric_id = ${CPU_METRIC_ID} ORDER BY ts\"}" \
     | jq
   ```

#### Bucket and average a gauge

1. Reuse the `CPU_METRIC_ID` lookup above (or run it again for a different metric).
2. Bucket readings into one-minute intervals and smooth them with `avg()`:

   ```bash
   curl -s "$API/query" \
     -H "Content-Type: application/json" \
     -d "{\"metric_ids\":[${CPU_METRIC_ID}],\"start_ts\":$START_TS,\"end_ts\":$END_TS,\"sql\":\"WITH selector_map AS (SELECT 'cpu' AS alias, ${CPU_METRIC_ID} AS metric_id), base_agg AS (SELECT ts_bucket(s.ts, 60) AS bucket, 'cpu' AS alias, avg(s.value) AS value FROM samples AS s JOIN selector_map AS sel ON s.metric_id = sel.metric_id WHERE s.ts BETWEEN $START_TS AND $END_TS GROUP BY bucket) SELECT bucket, alias, value FROM base_agg ORDER BY bucket\"}" \
     | jq
   ```

   (Same SQL written out for reference:)

   ```sql
   WITH selector_map AS (
     SELECT 'cpu' AS alias, 42 AS metric_id
   ),
   base_agg AS (
     SELECT ts_bucket(s.ts, 60) AS bucket, 'cpu' AS alias, avg(s.value) AS value
     FROM samples AS s
     JOIN selector_map AS sel ON s.metric_id = sel.metric_id
     WHERE s.ts BETWEEN 1700000000 AND 1700003600
     GROUP BY bucket
   )
   SELECT bucket, alias, value
   FROM base_agg
   ORDER BY bucket;
   ```

#### Counter throughput with `bucket_rate`

1. Resolve the counter’s metric ID (here using the baked-in `net_bytes_sent` metric):

   ```bash
   REQ_METRIC_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"net_bytes_sent"}' \
     | jq -r '.metrics[0].metric_id // empty')
   ```

2. Convert cumulative counter samples into per-second rates. `bucket_rate` expects both the current bucket value and the previous bucket value (`LAG(...)`) so it can compute the delta safely:

   ```bash
   curl -s "$API/query" \
     -H "Content-Type: application/json" \
     -d "{\"metric_ids\":[${REQ_METRIC_ID}],\"start_ts\":$START_TS,\"end_ts\":$END_TS,\"sql\":\"WITH selector_map AS (SELECT 'net_sent' AS alias, ${REQ_METRIC_ID} AS metric_id), base AS (SELECT ts_bucket(s.ts, 60) AS bucket, s.metric_id, avg(s.value) AS bucket_avg FROM samples AS s JOIN selector_map AS sel ON s.metric_id = sel.metric_id WHERE s.ts BETWEEN $START_TS AND $END_TS GROUP BY bucket, s.metric_id) SELECT b.bucket, sel.alias AS series, bucket_rate(b.bucket_avg, LAG(b.bucket_avg) OVER (ORDER BY b.bucket), 60) AS per_second FROM base AS b JOIN selector_map AS sel ON b.metric_id = sel.metric_id ORDER BY bucket\"}" \
     | jq
   ```

   SQL reference:

   ```sql
   WITH selector_map AS (
     SELECT 'net_sent' AS alias, 3020 AS metric_id
   ),
   base AS (
     SELECT ts_bucket(s.ts, 60) AS bucket, s.metric_id, avg(s.value) AS bucket_avg
     FROM samples AS s
     JOIN selector_map AS sel ON s.metric_id = sel.metric_id
     WHERE s.ts BETWEEN 1700000000 AND 1700003600
     GROUP BY bucket, s.metric_id
   )
   SELECT b.bucket, sel.alias AS series, bucket_rate(b.bucket_avg, LAG(b.bucket_avg) OVER (ORDER BY b.bucket), 60) AS per_second
   FROM base AS b
   JOIN selector_map AS sel ON b.metric_id = sel.metric_id
   ORDER BY bucket;
   ```

`QueryEngine` registers `ts_bucket`, `bucket_rate`, math helpers, and series operators automatically before executing SQL, so the functions above are always available.

#### Compare counter directions (sent vs recv)

1. Resolve the `net_bytes_sent` and `net_bytes_recv` metric IDs:

   ```bash
   SENT_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"net_bytes_sent"}' | jq -r '.metrics[0].metric_id // empty')

   RECV_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"net_bytes_recv"}' | jq -r '.metrics[0].metric_id // empty')
   ```

2. Compute per-second rates for each direction and subtract them to see bursts or imbalances:

   ```bash
   START_TS=$(( $(date +%s) - 3600 ))
   END_TS=$(date +%s)

   curl -s "$API/query" \
     -H "Content-Type: application/json" \
     -d "{\"metric_ids\":[${SENT_ID},${RECV_ID}],\"start_ts\":$START_TS,\"end_ts\":$END_TS,\"sql\":\"WITH selector_map AS (SELECT 'sent' AS alias, ${SENT_ID} AS metric_id UNION ALL SELECT 'recv' AS alias, ${RECV_ID} AS metric_id), bucketed AS (SELECT ts_bucket(s.ts, 60) AS bucket, sel.alias, avg(s.value) AS bucket_avg FROM samples AS s JOIN selector_map AS sel ON s.metric_id = sel.metric_id WHERE s.ts BETWEEN $START_TS AND $END_TS GROUP BY bucket, sel.alias), rates AS (SELECT bucket, alias, bucket_rate(bucket_avg, LAG(bucket_avg) OVER (PARTITION BY alias ORDER BY bucket), 60) AS rate FROM bucketed) SELECT bucket, max(CASE WHEN alias = 'sent' THEN rate END) AS sent_rate, max(CASE WHEN alias = 'recv' THEN rate END) AS recv_rate, max(CASE WHEN alias = 'sent' THEN rate END) - max(CASE WHEN alias = 'recv' THEN rate END) AS delta FROM rates GROUP BY bucket ORDER BY bucket\"}" \
     | jq '.[\"rows\"]? // .'
   ```

   SQL reference:

   ```sql
   WITH selector_map AS (
     SELECT 'sent' AS alias, 3020 AS metric_id
     UNION ALL
     SELECT 'recv' AS alias, 3021 AS metric_id
   ),
   bucketed AS (
     SELECT ts_bucket(s.ts, 60) AS bucket, sel.alias, avg(s.value) AS bucket_avg
     FROM samples AS s
     JOIN selector_map AS sel ON s.metric_id = sel.metric_id
     WHERE s.ts BETWEEN 1700000000 AND 1700003600
     GROUP BY bucket, sel.alias
   ),
   rates AS (
     SELECT bucket,
            alias,
            bucket_rate(bucket_avg, LAG(bucket_avg) OVER (PARTITION BY alias ORDER BY bucket), 60) AS rate
     FROM bucketed
   )
  SELECT bucket,
         max(CASE WHEN alias = 'sent' THEN rate END) AS sent_rate,
         max(CASE WHEN alias = 'recv' THEN rate END) AS recv_rate,
         max(CASE WHEN alias = 'sent' THEN rate END) - max(CASE WHEN alias = 'recv' THEN rate END) AS delta
  FROM rates
  GROUP BY bucket
  ORDER BY bucket;
  ```

#### Spot resource ceilings (CPU/memory/disk %)

1. Resolve the three percentage gauges:

   ```bash
   CPU_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"cpu_percent"}' | jq -r '.metrics[0].metric_id // empty')
   MEM_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"mem_used_percent"}' | jq -r '.metrics[0].metric_id // empty')
   DISK_ID=$(curl -s "$API/metrics/lookup" \
     -H "Content-Type: application/json" \
     -d '{"name":"disk_used_percent"}' | jq -r '.metrics[0].metric_id // empty')
   ```

2. Bucket all three into a single query so dashboards can plot them together and watch for anything approaching 100%:

   ```bash
   START_TS=$(( $(date +%s) - 3600 ))
   END_TS=$(date +%s)

   curl -s "$API/query" \
     -H "Content-Type: application/json" \
     -d "{\"metric_ids\":[${CPU_ID},${MEM_ID},${DISK_ID}],\"start_ts\":$START_TS,\"end_ts\":$END_TS,\"sql\":\"WITH selector_map AS (SELECT 'cpu' AS alias, ${CPU_ID} AS metric_id UNION ALL SELECT 'mem' AS alias, ${MEM_ID} AS metric_id UNION ALL SELECT 'disk' AS alias, ${DISK_ID} AS metric_id), bucketed AS (SELECT ts_bucket(s.ts, 60) AS bucket, sel.alias, avg(s.value) AS pct FROM samples AS s JOIN selector_map AS sel ON s.metric_id = sel.metric_id WHERE s.ts BETWEEN $START_TS AND $END_TS GROUP BY bucket, sel.alias) SELECT bucket, max(CASE WHEN alias = 'cpu' THEN pct END) AS cpu_percent, max(CASE WHEN alias = 'mem' THEN pct END) AS mem_used_percent, max(CASE WHEN alias = 'disk' THEN pct END) AS disk_used_percent FROM bucketed GROUP BY bucket ORDER BY bucket\"}\" \
     | jq '.[\"rows\"]? // .'
   ```

   SQL reference:

   ```sql
   WITH selector_map AS (
     SELECT 'cpu' AS alias, 3001 AS metric_id
     UNION ALL
     SELECT 'mem' AS alias, 3003 AS metric_id
     UNION ALL
     SELECT 'disk' AS alias, 3004 AS metric_id
   ),
   bucketed AS (
     SELECT ts_bucket(s.ts, 60) AS bucket, sel.alias, avg(s.value) AS pct
     FROM samples AS s
     JOIN selector_map AS sel ON s.metric_id = sel.metric_id
     WHERE s.ts BETWEEN 1700000000 AND 1700003600
     GROUP BY bucket, sel.alias
   )
   SELECT bucket,
          max(CASE WHEN alias = 'cpu' THEN pct END) AS cpu_percent,
          max(CASE WHEN alias = 'mem' THEN pct END) AS mem_used_percent,
          max(CASE WHEN alias = 'disk' THEN pct END) AS disk_used_percent
   FROM bucketed
   GROUP BY bucket
   ORDER BY bucket;
   ```

## Tests

```bash
.venv/bin/python tests.py
```

The suite covers ring-buffer round trips, counter-rate detection, and SQL queries. Tests skip automatically if FoundationDB is unreachable.

## Repository tips

- Root entry points: `api.py` (FastAPI), `demo.py` (ingestion/query flow), `tests.py` (unittests)
- Persistence logic lives in `tsdb_fdb.py`; SQL orchestration and UDF registration live in `query_engine.py`
- Shared config lives in `config/`; container assets under `docker/` and `Dockerfile`
- Store `fdb.cluster` in the repo root or reference it via `FDB_CLUSTER_FILE`
