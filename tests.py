import multiprocessing
import random
import time
import unittest

import fdb
from fastapi import HTTPException
import api as api_mod
from query_engine import QueryEngine
from tsdb_fdb import FdbTsdb, get_db, init_tsdb


class TsdbTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fdb.api_version(710)
        try:
            multiprocessing.Semaphore(0).release()
        except PermissionError as exc:  # pragma: no cover - environment lacks semaphores
            raise unittest.SkipTest(f"multiprocessing Semaphore unavailable: {exc}")
        try:
            cls.db = get_db()
        except Exception as exc:  # pragma: no cover - skip when no cluster
            raise unittest.SkipTest(f"FoundationDB not available: {exc}")
        cls.tsdb: FdbTsdb = init_tsdb(cls.db)

    def _unique_metric(self) -> int:
        return random.randint(1, 1 << 30)

    def test_ring_roundtrip(self) -> None:
        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=0)
        base_ts = int(time.time())
        expected = []
        for i in range(5):
            ts = base_ts + i
            value = float(i * 1.5)
            self.tsdb.write_gauge(metric_id, ts, value)
            expected.append((ts, value))
        result = self.tsdb.read_range(metric_id, base_ts, base_ts + 5)
        self.assertEqual(len(result), len(expected))
        for (ts, value, typ), (exp_ts, exp_value) in zip(result, expected):
            self.assertEqual(ts, exp_ts)
            self.assertAlmostEqual(value, exp_value)
            self.assertEqual(typ, 0)

    def test_counter_raw_and_rate_udf(self) -> None:
        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=1)
        base_ts = int(time.time())
        self.tsdb.write_counter(metric_id, base_ts, 100.0)
        self.tsdb.write_counter(metric_id, base_ts + 10, 200.0)
        self.tsdb.write_counter(metric_id, base_ts + 20, 50.0)  # reset
        rows = self.tsdb.read_range(metric_id, base_ts, base_ts + 20)
        # Raw values preserved
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][1], 100.0)
        self.assertEqual(rows[1][1], 200.0)
        self.assertEqual(rows[2][1], 50.0)
        # Rate via SQL + bucket_rate UDF over bucketed counters
        engine = QueryEngine(self.tsdb)
        sql = f"""
WITH bucketed AS (
  SELECT ts_bucket(ts, 10) AS bucket, max(value) AS value
  FROM samples
  WHERE metric_id = {metric_id}
  GROUP BY bucket
),
rates AS (
  SELECT bucket, bucket_rate(value, LAG(value) OVER (ORDER BY bucket), 10) AS rate
  FROM bucketed
)
SELECT bucket, rate FROM rates ORDER BY bucket
"""
        res = engine.run_sql([metric_id], base_ts, base_ts + 20, sql)
        rates = [r["rate"] for r in res]
        self.assertEqual(len(rates), 3)
        self.assertIsNone(rates[0])
        self.assertAlmostEqual(rates[1], 10.0)
        self.assertIsNone(rates[2])

    def test_sql_query(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=0)
        base_ts = (int(time.time()) // 60) * 60
        for i in range(4):
            self.tsdb.write_gauge(metric_id, base_ts + i, float(i + 1))
        sql = f"""
SELECT ts_bucket(ts, 60) AS bucket, avg(value) AS avg_value
FROM samples
WHERE metric_id = {metric_id} AND ts >= {base_ts} AND ts <= {base_ts + 3}
GROUP BY bucket
"""
        rows = engine.run_sql([metric_id], base_ts, base_ts + 3, sql)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["avg_value"], 2.5)

    def test_selector_alias_resolution_and_substitution(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_id = self._unique_metric()
        name = f"selector_metric_{random.randint(1, 1 << 16)}"
        tags = {"role": "web"}
        self.tsdb.ensure_metric(metric_id, typ=0, name=name, tags=tags)
        base_ts = int(time.time())
        values = [1.0, 2.0, 3.0]
        for idx, val in enumerate(values):
            self.tsdb.write_gauge(metric_id, base_ts + idx, val)

        selector = api_mod.SelectorPayload(metric=name, tags=tags, alias="CPU")
        metric_ids, alias_map = api_mod._resolve_metric_ids_from_selectors([selector])
        self.assertEqual(metric_ids, [metric_id])
        self.assertIn("CPU", alias_map)
        sql_template = "SELECT avg(value) AS v FROM samples WHERE metric_id = {{CPU}}"
        sql_resolved = api_mod._replace_alias_placeholders(sql_template, alias_map)
        rows = engine.run_sql(metric_ids, base_ts, base_ts + len(values), sql_resolved)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["v"], sum(values) / len(values))
        with self.assertRaises(HTTPException):
            api_mod._replace_alias_placeholders(sql_template, {"UNKNOWN": [metric_id]})

    def test_udfs_clamp_and_align(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=0)
        base_ts = int(time.time())
        values = [-5.0, 0.5, 5.0, 15.0]
        for idx, val in enumerate(values):
            self.tsdb.write_gauge(metric_id, base_ts + idx, val)
        sql = f"""
SELECT
  ts,
  clamp(value, 0.0, 10.0) AS clamped,
  null_if_outside(value, 0.0, 10.0) AS gated,
  align_time(ts, 60, {base_ts}) AS aligned
FROM samples
WHERE metric_id = {metric_id}
ORDER BY ts
"""
        rows = engine.run_sql([metric_id], base_ts, base_ts + len(values), sql)
        self.assertEqual(len(rows), len(values))
        self.assertEqual([r["clamped"] for r in rows], [0.0, 0.5, 5.0, 10.0])
        gated = [r["gated"] for r in rows]
        self.assertIsNone(gated[0])
        self.assertEqual(gated[1:], [0.5, 5.0, None])
        aligned = [r["aligned"] for r in rows]
        expected_aligned = [((ts - base_ts) // 60) * 60 + base_ts for ts in range(base_ts, base_ts + len(values))]
        self.assertEqual(aligned, expected_aligned)

    def test_udfs_series_math(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_a = self._unique_metric()
        metric_b = self._unique_metric()
        self.tsdb.ensure_metric(metric_a, typ=0)
        self.tsdb.ensure_metric(metric_b, typ=0)
        base_ts = int(time.time())
        samples_a = [1.0, 2.0, 3.0]
        samples_b = [10.0, 20.0, 0.0]
        for idx, val in enumerate(samples_a):
            self.tsdb.write_gauge(metric_a, base_ts + idx, val)
        for idx, val in enumerate(samples_b):
            self.tsdb.write_gauge(metric_b, base_ts + idx, val)
        sql = f"""
WITH pivot AS (
  SELECT
    ts,
    max(CASE WHEN metric_id = {metric_a} THEN value END) AS a,
    max(CASE WHEN metric_id = {metric_b} THEN value END) AS b
  FROM samples
  WHERE metric_id IN ({metric_a}, {metric_b}) AND ts BETWEEN {base_ts} AND {base_ts + 2}
  GROUP BY ts
)
SELECT
  ts,
  series_add(a, b) AS s_add,
  series_sub(b, a) AS s_sub,
  series_mul(a, b) AS s_mul,
  series_div(b, a) AS s_div
FROM pivot
ORDER BY ts
"""
        rows = engine.run_sql([metric_a, metric_b], base_ts, base_ts + 2, sql)
        self.assertEqual(len(rows), 3)
        for idx, row in enumerate(rows):
            a = samples_a[idx]
            b = samples_b[idx]
            self.assertEqual(row["s_add"], a + b)
            self.assertEqual(row["s_sub"], b - a)
            self.assertEqual(row["s_mul"], a * b)
            if a != 0:
                self.assertEqual(row["s_div"], b / a)
            else:
                self.assertIsNone(row["s_div"])

    def test_metrics_table_filtering(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_id = self._unique_metric()
        name = f"disk_usage_gb_{random.randint(1, 1 << 16)}"
        tags = {"role": "mysql", "host": "db01"}
        self.tsdb.ensure_metric(metric_id, typ=0, name=name, tags=tags)
        base_ts = int(time.time())
        values = [5.0, 6.0, 7.0]
        for idx, val in enumerate(values):
            self.tsdb.write_gauge(metric_id, base_ts + idx, val)

        sql = f"""
WITH matched AS (
  SELECT metric_id FROM metric_tags WHERE tag_key = 'role' AND tag_value = 'mysql'
)
SELECT ts_bucket(ts, 60) AS b, avg(value) AS v
FROM samples
WHERE metric_id IN (SELECT metric_id FROM matched) AND ts_bucket(ts, 60) = ts_bucket({base_ts}, 60)
GROUP BY b
ORDER BY b
"""
        rows = engine.run_sql([metric_id], base_ts, base_ts + len(values), sql)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["v"], sum(values) / len(values))

    def test_slot_wrap_overwrite(self) -> None:
        metric_id = self._unique_metric()
        # Use a tiny slot count to force wrap/overwrite behavior.
        original_slots = self.tsdb.default_slots
        try:
            self.tsdb.default_slots = 3
            self.tsdb.ensure_metric(metric_id, typ=0)
            base_ts = int(time.time())
            for i in range(4):  # write 4 samples into 3 slots; first should be overwritten
                self.tsdb.write_gauge(metric_id, base_ts + i, float(i))
            rows = self.tsdb.read_range(metric_id, base_ts, base_ts + 3)
            self.assertEqual(len(rows), 3)
            returned_ts = [r[0] for r in rows]
            self.assertNotIn(base_ts, returned_ts)
            self.assertEqual(returned_ts, [base_ts + 1, base_ts + 2, base_ts + 3])
        finally:
            self.tsdb.default_slots = original_slots

    def test_counter_reset_stores_raw(self) -> None:
        metric_id = self._unique_metric()
        original_slots = self.tsdb.default_slots
        try:
            self.tsdb.default_slots = 4
            self.tsdb.ensure_metric(metric_id, typ=1)
            base_ts = int(time.time())
            self.tsdb.write_counter(metric_id, base_ts, 100.0)
            self.tsdb.write_counter(metric_id, base_ts + 1, 90.0)  # reset
            rows = self.tsdb.read_range(metric_id, base_ts, base_ts + 1)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0][1], 100.0)
            self.assertEqual(rows[1][1], 90.0)
        finally:
            self.tsdb.default_slots = original_slots

    def test_metric_metadata_lookup(self) -> None:
        metric_id = self._unique_metric()
        name = f"test_metric_metadata_{random.randint(1, 1 << 16)}"
        tags = {"env": "dev", "region": "us-east"}
        self.tsdb.ensure_metric(metric_id, typ=0, name=name, tags=tags)
        metrics = self.tsdb.find_metrics(name=name, tags={"env": "dev"})
        self.assertTrue(any(m["metric_id"] == metric_id for m in metrics))
        exact = [m for m in metrics if m["metric_id"] == metric_id][0]
        self.assertEqual(exact.get("name"), name)
        self.assertEqual(exact.get("tags").get("region"), "us-east")

    def test_metric_id_uint32_enforced_and_ts_reconstruction(self) -> None:
        with self.assertRaises(ValueError):
            self.tsdb.ensure_metric(1 << 40, typ=0)

        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=0, step=1, slots=10)
        base_ts = int(time.time())
        values = [10.0, 11.0, 12.0]
        for idx, val in enumerate(values):
            self.tsdb.write_gauge(metric_id, base_ts + idx, val, step=1, slots=10)
        rows = self.tsdb.read_range(metric_id, base_ts, base_ts + len(values))
        self.assertEqual(len(rows), len(values))
        returned_ts = [r[0] for r in rows]
        self.assertEqual(returned_ts, [base_ts + i for i in range(len(values))])

    def test_new_udfs_wow_and_moving(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=0)
        base_ts = int(time.time())
        samples = [10.0, 15.0, 20.0, 30.0, 40.0]
        for idx, val in enumerate(samples):
            self.tsdb.write_gauge(metric_id, base_ts + idx, val)
        sql = f"""
SELECT
  ts,
  diff(value, 1) OVER (PARTITION BY metric_id ORDER BY ts) AS d,
  rolling_mean(value, 3) OVER (PARTITION BY metric_id ORDER BY ts) AS ma,
  diff(value, 2) OVER (PARTITION BY metric_id ORDER BY ts) AS wow_d,
  pct_change(value, 2) OVER (PARTITION BY metric_id ORDER BY ts) AS wow_r
FROM samples
WHERE metric_id = {metric_id}
ORDER BY ts
"""
        rows = engine.run_sql([metric_id], base_ts, base_ts + len(samples), sql)
        self.assertEqual(len(rows), len(samples))
        # delta
        self.assertIsNone(rows[0]["d"])
        self.assertAlmostEqual(rows[1]["d"], 5.0)
        # moving average with window 3
        self.assertAlmostEqual(rows[2]["ma"], sum(samples[:3]) / 3)
        self.assertAlmostEqual(rows[4]["ma"], sum(samples[2:5]) / 3)
        # period diff (lag of 2)
        self.assertIsNone(rows[1]["wow_d"])
        self.assertAlmostEqual(rows[3]["wow_d"], samples[3] - samples[1])
        # period pct change
        self.assertIsNone(rows[1]["wow_r"])
        self.assertAlmostEqual(rows[3]["wow_r"], (samples[3] - samples[1]) / samples[1])

    def test_bucket_rate_udf(self) -> None:
        engine = QueryEngine(self.tsdb)
        metric_id = self._unique_metric()
        self.tsdb.ensure_metric(metric_id, typ=0)
        base_ts = int(time.time())
        values = [1.0, 3.0, 7.0]
        for idx, val in enumerate(values):
            self.tsdb.write_gauge(metric_id, base_ts + idx * 10, val)
        sql = f"""
SELECT
  bucket,
  bucket_rate(value, LAG(value) OVER (ORDER BY bucket), 10) AS r
FROM (
  SELECT ts_bucket(ts, 10) AS bucket, max(value) AS value
  FROM samples
  WHERE metric_id = {metric_id}
  GROUP BY bucket
)
ORDER BY bucket
"""
        rows = engine.run_sql([metric_id], base_ts, base_ts + 30, sql)
        self.assertEqual(len(rows), len(values))
        self.assertIsNone(rows[0]["r"])
        self.assertAlmostEqual(rows[1]["r"], (values[1] - values[0]) / 10.0)
        self.assertAlmostEqual(rows[2]["r"], (values[2] - values[1]) / 10.0)

    def test_descriptor_allocation_without_metric_id(self) -> None:
        name = "test_descriptor_alloc"
        tags = {"env": "qa"}
        metric_id = self.tsdb.ensure_metric_descriptor(
            None, typ=0, name=name, tags=tags, step=2, slots=10
        )
        self.assertIsInstance(metric_id, int)
        again = self.tsdb.ensure_metric_descriptor(
            None, typ=0, name=name, tags=tags, step=2, slots=10
        )
        self.assertEqual(metric_id, again)


class ApiTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import os
        import importlib

        os.environ.setdefault("FDB_CLUSTER_FILE", "/etc/foundationdb/fdb.cluster")
        try:
            cls.db = get_db(os.environ["FDB_CLUSTER_FILE"])
        except Exception as exc:  # pragma: no cover - skip when no cluster
            raise unittest.SkipTest(f"FoundationDB not available: {exc}")

        # Reload the API to ensure it binds to the cluster file set above.
        import api as api_module

        api_module = importlib.reload(api_module)
        from fastapi.testclient import TestClient

        cls.client = TestClient(api_module.app)

    def _unique_metric(self) -> int:
        return random.randint(1, 1 << 30)

    def test_gauge_ingest_and_query(self) -> None:
        metric_id = self._unique_metric()
        base_ts = int(time.time())
        resp = self.client.post(
            "/ingest/gauge",
            json={"metric_id": metric_id, "ts": base_ts, "value": 12.5},
        )
        self.assertEqual(resp.status_code, 200)

        sql = f"SELECT ts, value FROM samples WHERE metric_id = {metric_id}"
        query = {
            "metric_ids": [metric_id],
            "start_ts": base_ts,
            "end_ts": base_ts,
            "sql": sql,
        }
        qr = self.client.post("/query", json=query)
        self.assertEqual(qr.status_code, 200)
        data = qr.json()
        self.assertEqual(data["count"], 1)
        row = data["rows"][0]
        self.assertEqual(row["ts"], base_ts)
        self.assertAlmostEqual(row["value"], 12.5)

    def test_ingest_by_name_and_lookup(self) -> None:
        base_ts = int(time.time())
        name = "api_named_metric"
        tags = {"env": "dev"}
        resp = self.client.post(
            "/ingest/gauge",
            json={"ts": base_ts, "value": 1.0, "name": name, "tags": tags},
        )
        self.assertEqual(resp.status_code, 200)
        lookup = self.client.post("/metrics/lookup", json={"name": name, "tags": tags})
        self.assertEqual(lookup.status_code, 200)
        metrics = lookup.json()["metrics"]
        self.assertTrue(any(m["name"] == name for m in metrics))

    def test_query_validates_window(self) -> None:
        query = {
            "metric_ids": [self._unique_metric()],
            "start_ts": 10,
            "end_ts": 5,
            "sql": "SELECT 1",
        }
        qr = self.client.post("/query", json=query)
        self.assertEqual(qr.status_code, 400)


if __name__ == "__main__":
    unittest.main()
