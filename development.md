# Development Guide

## Environment
- Python 3.10+, FoundationDB client/server installed.
- Default retention: `step=1`, `slots=3600`. Override per metric via ingest payloads.
- `FDB_CLUSTER_FILE` points to your cluster (copy `fdb.cluster` to repo root or set the env var).

## Setup
```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

FoundationDB single-node quick start:
```bash
mkdir -p /tmp/fdb
fdbserver -p 4500 -d /tmp/fdb -C /etc/foundationdb/fdb.cluster
fdbcli --exec "configure new single memory"
export FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
```

## Run/verify
- Demo: `.venv/bin/python demo.py`
- API: `FDB_CLUSTER_FILE=... .venv/bin/uvicorn api:app --reload --host 0.0.0.0 --port 8000`
- Tests: `.venv/bin/python tests.py` (skips if FDB is unavailable)
- Frontend build: `npm install` (once) then `npm run build`

## API quick reference
- Ingest gauge: `POST /ingest/gauge` with `{metric_id?|name?, ts, value, tags?, step?, slots?}`
- Ingest counter: `POST /ingest/counter` with `{metric_id?|name?, ts, raw_value, tags?, step?, slots?}` (stores raw cumulative; use `bucket_rate` in queries)
- Query: `POST /query` with `metric_ids`, `start_ts`, `end_ts`, `sql` (tables: `samples`, `metrics`, `metric_tags`; UDFs: `ts_bucket`, `bucket_rate`, `clamp`, `align_time`, `null_if_outside`, `series_add/sub/mul/div`)
- Series endpoint: `GET /metrics/{metric_id}/series?start_ts&end_ts&bucket`
- Maintenance: `DELETE /metrics/{metric_id}`, `POST /metrics/{metric_id}/retention` with `{step, slots}` (gauges only)
- Lookup: `POST /metrics/lookup`, names: `GET /metrics/names`, tags: `POST /metrics/tag-values`

## Counter handling
- Raw counter samples are stored; rates are derived in queries using `bucket_rate` over bucketed aggregates (see `api.py` series endpoint for a template).
- Retention rewrites are limited to gauges; counters can be rewritten by replaying raw samples if needed.

## Coding notes
- UDFs: see `query_engine.py` (`ts_bucket`, `bucket_rate`, math helpers, clamp/null_if_outside).
- Storage: per-metric `step/slots/type` in `tsdb_fdb.py`; delete/retention helpers available.
