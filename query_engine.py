from __future__ import annotations

from typing import Any, Dict, List
from collections import deque

import pyarrow as pa
from datafusion import SessionContext, udf, udwf
from datafusion.user_defined import WindowEvaluator

from tsdb_fdb import FdbTsdb


def _parse_int_arg(value: pa.Array | pa.Scalar | int | None, default: int = 1) -> int:
    """Return a single integer value from an Arrow scalar/array with sane defaults."""

    if isinstance(value, pa.Array):
        if len(value) == 0:
            return default
        value = value[0]
    if hasattr(value, "as_py"):
        value = value.as_py()
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed


class DiffWindow(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        series = values[0].to_pylist()
        periods = max(1, _parse_int_arg(values[1], 1))
        out: list[float | None] = []
        for idx, curr in enumerate(series):
            if idx < periods or curr is None:
                out.append(None)
                continue
            prev = series[idx - periods]
            out.append(None if prev is None else curr - prev)
        return pa.array(out, type=pa.float64())


class PeriodDiffWindow(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        series = values[0].to_pylist()
        periods = max(1, _parse_int_arg(values[1], 1))
        out: list[float | None] = []
        for idx, curr in enumerate(series):
            if idx < periods or curr is None:
                out.append(None)
                continue
            prev = series[idx - periods]
            out.append(None if prev is None else curr - prev)
        return pa.array(out, type=pa.float64())


class PctChangeWindow(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        series = values[0].to_pylist()
        periods = max(1, _parse_int_arg(values[1], 1))
        out: list[float | None] = []
        for idx, curr in enumerate(series):
            if idx < periods or curr is None:
                out.append(None)
                continue
            prev = series[idx - periods]
            if prev in (None, 0):
                out.append(None)
                continue
            out.append((curr - prev) / prev)
        return pa.array(out, type=pa.float64())


class RollingMeanWindow(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        series = values[0].to_pylist()
        window = max(1, _parse_int_arg(values[1], 1))
        window_vals: deque[float | None] = deque()
        running_sum = 0.0
        running_count = 0
        out: list[float | None] = []
        for val in series:
            window_vals.append(val)
            if val is not None:
                running_sum += val
                running_count += 1
            if len(window_vals) > window:
                old = window_vals.popleft()
                if old is not None:
                    running_sum -= old
                    running_count -= 1
            if running_count == 0:
                out.append(None)
            else:
                out.append(running_sum / running_count)
        return pa.array(out, type=pa.float64())


class RollingSumWindow(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        series = values[0].to_pylist()
        window = max(1, _parse_int_arg(values[1], 1))
        window_vals: deque[float | None] = deque()
        running_sum = 0.0
        running_count = 0
        out: list[float | None] = []
        for val in series:
            window_vals.append(val)
            if val is not None:
                running_sum += val
                running_count += 1
            if len(window_vals) > window:
                old = window_vals.popleft()
                if old is not None:
                    running_sum -= old
                    running_count -= 1
            out.append(None if running_count == 0 else running_sum)
        return pa.array(out, type=pa.float64())


class CounterRateWindow(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        counters = values[0].to_pylist()
        timestamps = values[1].to_pylist()
        out: list[float | None] = []
        for idx in range(num_rows):
            curr = counters[idx]
            t1 = timestamps[idx]
            if idx == 0:
                out.append(None)
                continue
            prev = counters[idx - 1]
            t0 = timestamps[idx - 1]
            if None in (curr, prev, t1, t0):
                out.append(None)
                continue
            if t1 <= t0 or curr < prev:
                out.append(None)
                continue
            out.append((curr - prev) / (t1 - t0))
        return pa.array(out, type=pa.float64())


class QueryEngine:
    """Simple SQL layer that reads from FdbTsdb and exposes a `samples` table."""

    def __init__(self, tsdb: FdbTsdb):
        self.tsdb = tsdb
        # Keep engine stateless per request to avoid DataFusion table clashes.

    def _register_udfs(self, ctx: SessionContext) -> None:
        def ts_bucket(ts: pa.Array | pa.Scalar | int | None, step: pa.Array | pa.Scalar | int | None):
            def bucket_value(ts_val, step_val):
                if ts_val is None or step_val in (None, 0):
                    return None
                return (ts_val // step_val) * step_val

            def normalize(value, length: int | None = None):
                if isinstance(value, pa.Array):
                    return [elem.as_py() for elem in value]
                if hasattr(value, "as_py"):
                    return value.as_py()
                if length is not None:
                    return [value] * length
                return value

            if isinstance(ts, pa.Array):
                length = len(ts)
                ts_list = normalize(ts)
                step_list = normalize(step, length)
                results = [
                    bucket_value(ts_elem, step_elem)
                    for ts_elem, step_elem in zip(ts_list, step_list)
                ]
                return pa.array(results, type=pa.int64())

            ts_val = normalize(ts)
            step_val = normalize(step)
            return bucket_value(ts_val, step_val)

        bucket_udf = udf(
            ts_bucket,
            [pa.int64(), pa.int64()],
            pa.int64(),
            "immutable",
            "ts_bucket",
        )
        ctx.register_udf(bucket_udf)

        def clamp(value: pa.Array | pa.Scalar | float | None, min_val: pa.Array | pa.Scalar | float | None, max_val: pa.Array | pa.Scalar | float | None):
            def clamp_value(val, lo, hi):
                if val is None or lo is None or hi is None:
                    return None
                return max(lo, min(val, hi))

            def normalize(value, length: int | None = None):
                if isinstance(value, pa.Array):
                    return [elem.as_py() for elem in value]
                if hasattr(value, "as_py"):
                    return value.as_py()
                if length is not None:
                    return [value] * length
                return value

            if isinstance(value, pa.Array):
                length = len(value)
                vals = normalize(value)
                mins = normalize(min_val, length)
                maxs = normalize(max_val, length)
                return pa.array(
                    [clamp_value(v, lo, hi) for v, lo, hi in zip(vals, mins, maxs)],
                    type=pa.float64(),
                )
            val = normalize(value)
            lo = normalize(min_val)
            hi = normalize(max_val)
            return clamp_value(val, lo, hi)

        clamp_udf = udf(
            clamp,
            [pa.float64(), pa.float64(), pa.float64()],
            pa.float64(),
            "immutable",
            "clamp",
        )
        ctx.register_udf(clamp_udf)

        def null_if_outside(value: pa.Array | pa.Scalar | float | None, min_val: pa.Array | pa.Scalar | float | None, max_val: pa.Array | pa.Scalar | float | None):
            def keep_or_null(val, lo, hi):
                if val is None or lo is None or hi is None:
                    return None
                return val if lo <= val <= hi else None

            def normalize(value, length: int | None = None):
                if isinstance(value, pa.Array):
                    return [elem.as_py() for elem in value]
                if hasattr(value, "as_py"):
                    return value.as_py()
                if length is not None:
                    return [value] * length
                return value

            if isinstance(value, pa.Array):
                length = len(value)
                vals = normalize(value)
                mins = normalize(min_val, length)
                maxs = normalize(max_val, length)
                return pa.array(
                    [keep_or_null(v, lo, hi) for v, lo, hi in zip(vals, mins, maxs)],
                    type=pa.float64(),
                )
            val = normalize(value)
            lo = normalize(min_val)
            hi = normalize(max_val)
            return keep_or_null(val, lo, hi)

        outside_udf = udf(
            null_if_outside,
            [pa.float64(), pa.float64(), pa.float64()],
            pa.float64(),
            "immutable",
            "null_if_outside",
        )
        ctx.register_udf(outside_udf)

        def align_time(ts: pa.Array | pa.Scalar | int | None, step: pa.Array | pa.Scalar | int | None, origin: pa.Array | pa.Scalar | int | None = None):
            def align_value(ts_val, step_val, origin_val):
                if ts_val is None or step_val in (None, 0):
                    return None
                base = origin_val or 0
                return ((ts_val - base) // step_val) * step_val + base

            def normalize(value, length: int | None = None):
                if isinstance(value, pa.Array):
                    return [elem.as_py() for elem in value]
                if hasattr(value, "as_py"):
                    return value.as_py()
                if length is not None:
                    return [value] * length
                return value

            if isinstance(ts, pa.Array):
                length = len(ts)
                ts_list = normalize(ts)
                step_list = normalize(step, length)
                origin_list = normalize(origin, length)
                return pa.array(
                    [align_value(t, st, o) for t, st, o in zip(ts_list, step_list, origin_list)],
                    type=pa.int64(),
                )
            ts_val = normalize(ts)
            step_val = normalize(step)
            origin_val = normalize(origin)
            return align_value(ts_val, step_val, origin_val)

        align_udf = udf(
            align_time,
            [pa.int64(), pa.int64(), pa.int64()],
            pa.int64(),
            "immutable",
            "align_time",
        )
        ctx.register_udf(align_udf)

        def series_add(lhs: pa.Array | pa.Scalar | float | None, rhs: pa.Array | pa.Scalar | float | None):
            def add_vals(a, b):
                if a is None or b is None:
                    return None
                return a + b

            if isinstance(lhs, pa.Array):
                return pa.array(
                    [add_vals(a.as_py(), b.as_py() if hasattr(b, "as_py") else b) for a, b in zip(lhs, rhs if isinstance(rhs, pa.Array) else [rhs] * len(lhs))],
                    type=pa.float64(),
                )
            a_val = lhs.as_py() if hasattr(lhs, "as_py") else lhs
            b_val = rhs.as_py() if hasattr(rhs, "as_py") else rhs
            return add_vals(a_val, b_val)

        def series_sub(lhs: pa.Array | pa.Scalar | float | None, rhs: pa.Array | pa.Scalar | float | None):
            def sub_vals(a, b):
                if a is None or b is None:
                    return None
                return a - b

            if isinstance(lhs, pa.Array):
                return pa.array(
                    [sub_vals(a.as_py(), b.as_py() if hasattr(b, "as_py") else b) for a, b in zip(lhs, rhs if isinstance(rhs, pa.Array) else [rhs] * len(lhs))],
                    type=pa.float64(),
                )
            a_val = lhs.as_py() if hasattr(lhs, "as_py") else lhs
            b_val = rhs.as_py() if hasattr(rhs, "as_py") else rhs
            return sub_vals(a_val, b_val)

        def series_mul(lhs: pa.Array | pa.Scalar | float | None, rhs: pa.Array | pa.Scalar | float | None):
            def mul_vals(a, b):
                if a is None or b is None:
                    return None
                return a * b

            if isinstance(lhs, pa.Array):
                return pa.array(
                    [mul_vals(a.as_py(), b.as_py() if hasattr(b, "as_py") else b) for a, b in zip(lhs, rhs if isinstance(rhs, pa.Array) else [rhs] * len(lhs))],
                    type=pa.float64(),
                )
            a_val = lhs.as_py() if hasattr(lhs, "as_py") else lhs
            b_val = rhs.as_py() if hasattr(rhs, "as_py") else rhs
            return mul_vals(a_val, b_val)

        def series_div(lhs: pa.Array | pa.Scalar | float | None, rhs: pa.Array | pa.Scalar | float | None):
            def div_vals(a, b):
                if a is None or b in (None, 0):
                    return None
                return a / b

            if isinstance(lhs, pa.Array):
                return pa.array(
                    [div_vals(a.as_py(), b.as_py() if hasattr(b, "as_py") else b) for a, b in zip(lhs, rhs if isinstance(rhs, pa.Array) else [rhs] * len(lhs))],
                    type=pa.float64(),
                )
            a_val = lhs.as_py() if hasattr(lhs, "as_py") else lhs
            b_val = rhs.as_py() if hasattr(rhs, "as_py") else rhs
            return div_vals(a_val, b_val)

        ctx.register_udf(udf(series_add, [pa.float64(), pa.float64()], pa.float64(), "immutable", "series_add"))
        ctx.register_udf(udf(series_sub, [pa.float64(), pa.float64()], pa.float64(), "immutable", "series_sub"))
        ctx.register_udf(udf(series_mul, [pa.float64(), pa.float64()], pa.float64(), "immutable", "series_mul"))
        ctx.register_udf(udf(series_div, [pa.float64(), pa.float64()], pa.float64(), "immutable", "series_div"))

        def bucket_rate(curr: pa.Array | pa.Scalar | float | None, prev: pa.Array | pa.Scalar | float | None, bucket: pa.Array | pa.Scalar | int | None):
            def to_py(val):
                return val.as_py() if hasattr(val, "as_py") else val

            def rate_value(c, p, b):
                c = to_py(c)
                p = to_py(p)
                b = to_py(b)
                if c is None or p is None or b is None or b <= 0:
                    return None
                delta = c - p
                if delta < 0:
                    return None
                return delta / b

            if isinstance(curr, pa.Array):
                curr_list = [to_py(x) for x in curr]
                prev_list = [to_py(x) for x in (prev if isinstance(prev, pa.Array) else [prev] * len(curr_list))]
                bucket_list = [to_py(x) for x in (bucket if isinstance(bucket, pa.Array) else [bucket] * len(curr_list))]
                return pa.array(
                    [rate_value(c, p, b) for c, p, b in zip(curr_list, prev_list, bucket_list)],
                    type=pa.float64(),
                )
            return rate_value(curr, prev, bucket)

        ctx.register_udf(udf(bucket_rate, [pa.float64(), pa.float64(), pa.int64()], pa.float64(), "immutable", "bucket_rate"))

        # Window functions that need full partition context.
        ctx.register_udwf(
            udwf(
                DiffWindow,
                [pa.float64(), pa.int64()],
                pa.float64(),
                "immutable",
                "diff",
            )
        )

        ctx.register_udwf(
            udwf(
                PeriodDiffWindow,
                [pa.float64(), pa.int64()],
                pa.float64(),
                "immutable",
                "period_diff",
            )
        )

        ctx.register_udwf(
            udwf(
                PctChangeWindow,
                [pa.float64(), pa.int64()],
                pa.float64(),
                "immutable",
                "pct_change",
            )
        )

        ctx.register_udwf(
            udwf(
                RollingMeanWindow,
                [pa.float64(), pa.int64()],
                pa.float64(),
                "immutable",
                "rolling_mean",
            )
        )

        ctx.register_udwf(
            udwf(
                RollingSumWindow,
                [pa.float64(), pa.int64()],
                pa.float64(),
                "immutable",
                "rolling_sum",
            )
        )

        ctx.register_udwf(
            udwf(
                CounterRateWindow,
                [pa.float64(), pa.int64()],
                pa.float64(),
                "immutable",
                "counter_rate",
            )
        )

        def shift_ts(ts: pa.Array | pa.Scalar | int | None, offset: pa.Array | pa.Scalar | int | None):
            def to_py(v):
                return v.as_py() if hasattr(v, "as_py") else v
            if isinstance(ts, pa.Array):
                ts_list = [to_py(v) for v in ts]
                off = offset
                off_list = [to_py(v) for v in (off if isinstance(off, pa.Array) else [off] * len(ts_list))]
                return pa.array([None if t is None or o is None else int(t) + int(o) for t, o in zip(ts_list, off_list)], type=pa.int64())
            t_val = to_py(ts)
            o_val = to_py(offset)
            return None if t_val is None or o_val is None else int(t_val) + int(o_val)

        ctx.register_udf(udf(shift_ts, [pa.int64(), pa.int64()], pa.int64(), "immutable", "shift_ts"))

    def _new_context(self) -> SessionContext:
        ctx = SessionContext()
        self._register_udfs(ctx)
        return ctx

    def run_sql(
        self, metric_ids: List[int], start_ts: int, end_ts: int, sql: str
    ) -> List[Dict[str, Any]]:
        ctx = self._new_context()

        # Register metric metadata tables for discovery-based queries.
        metrics_meta = self.tsdb.list_metrics()
        metrics_cols: Dict[str, list] = {
            "metric_id": [],
            "name": [],
            "type": [],
            "step": [],
            "slots": [],
        }
        tags_rows: Dict[str, list] = {"metric_id": [], "tag_key": [], "tag_value": []}
        for meta in metrics_meta:
            metrics_cols["metric_id"].append(int(meta.get("metric_id", 0)))
            metrics_cols["name"].append(meta.get("name") or "")
            metrics_cols["type"].append(int(meta.get("type", 0)))
            metrics_cols["step"].append(int(meta.get("step", 0)))
            metrics_cols["slots"].append(int(meta.get("slots", 0)))
            for k, v in (meta.get("tags") or {}).items():
                tags_rows["metric_id"].append(int(meta.get("metric_id", 0)))
                tags_rows["tag_key"].append(str(k))
                tags_rows["tag_value"].append(str(v))

        metrics_table = pa.Table.from_arrays(
            [
                pa.array(metrics_cols["metric_id"], type=pa.int64()),
                pa.array(metrics_cols["name"], type=pa.string()),
                pa.array(metrics_cols["type"], type=pa.int8()),
                pa.array(metrics_cols["step"], type=pa.int64()),
                pa.array(metrics_cols["slots"], type=pa.int64()),
            ],
            names=["metric_id", "name", "type", "step", "slots"],
        )
        ctx.register_table("metrics", ctx.from_arrow(metrics_table))

        tags_table = pa.Table.from_arrays(
            [
                pa.array(tags_rows["metric_id"], type=pa.int64()),
                pa.array(tags_rows["tag_key"], type=pa.string()),
                pa.array(tags_rows["tag_value"], type=pa.string()),
            ],
            names=["metric_id", "tag_key", "tag_value"],
        )
        ctx.register_table("metric_tags", ctx.from_arrow(tags_table))

        metric_col: List[int] = []
        ts_col: List[int] = []
        value_col: List[float] = []
        type_col: List[int] = []
        for metric_id in metric_ids:
            for ts, value, typ in self.tsdb.read_range(metric_id, start_ts, end_ts):
                metric_col.append(metric_id)
                ts_col.append(ts)
                value_col.append(value)
                type_col.append(typ)

        table = pa.Table.from_arrays(
            [
                pa.array(metric_col, type=pa.int64()),
                pa.array(ts_col, type=pa.int64()),
                pa.array(value_col, type=pa.float64()),
                pa.array(type_col, type=pa.int8()),
            ],
            names=["metric_id", "ts", "value", "type"],
        )

        samples_df = ctx.from_arrow(table)
        ctx.register_table("samples", samples_df)

        df = ctx.sql(sql)
        batches = df.collect()
        rows: List[Dict[str, Any]] = []
        for batch in batches:
            batch_dict = batch.to_pydict()
            if not batch_dict:
                continue
            length = len(next(iter(batch_dict.values())))
            for idx in range(length):
                rows.append({col: batch_dict[col][idx] for col in batch_dict})
        return rows


# Example queries:
# 1) 60 second averages:
# SELECT ts_bucket(ts, 60) AS t, avg(value) AS v
# FROM samples
# WHERE metric_id = 42
# GROUP BY t
# ORDER BY t;

# 2) Last N minutes of raw samples:
# SELECT ts, value
# FROM samples
# WHERE metric_id = 42
# ORDER BY ts;
