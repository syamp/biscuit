import time

from query_engine import QueryEngine
from tsdb_fdb import get_db, init_tsdb


def main() -> None:
    db = get_db()
    tsdb = init_tsdb(db)
    gauge_metric = 1001
    counter_metric = 1002
    tsdb.ensure_metric(gauge_metric, typ=0)
    tsdb.ensure_metric(counter_metric, typ=1)

    now = int(time.time())
    window_start = now - 300
    for offset in range(6):
        ts = window_start + offset * 30
        tsdb.write_gauge(gauge_metric, ts, 42.0 + offset)
        tsdb.write_counter(counter_metric, ts, 100.0 + offset * 5.0)
    tsdb.write_counter(counter_metric, now, 5.0)  # simulate reset

    engine = QueryEngine(tsdb)
    start_ts = window_start
    end_ts = now
    raw_sql = f"""
SELECT metric_id, ts, value, type
FROM samples
WHERE ts >= {start_ts} AND ts <= {end_ts}
ORDER BY metric_id, ts;
"""
    print("Raw samples:")
    for row in engine.run_sql([gauge_metric, counter_metric], start_ts, end_ts, raw_sql):
        print(row)

    bucket_sql = f"""
SELECT metric_id, ts_bucket(ts, 60) AS bucket, avg(value) AS avg_value
FROM samples
WHERE ts >= {start_ts} AND ts <= {end_ts}
GROUP BY metric_id, bucket
ORDER BY metric_id, bucket;
"""
    print("\nBucketed averages:")
    for row in engine.run_sql([gauge_metric, counter_metric], start_ts, end_ts, bucket_sql):
        print(row)


if __name__ == "__main__":
    main()
