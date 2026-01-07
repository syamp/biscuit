import os
import re
from typing import Any, Dict, List, Optional, Tuple

import fdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, PackageLoader, select_autoescape, FileSystemLoader
from pydantic import BaseModel

from query_engine import QueryEngine
from tsdb_fdb import FdbTsdb, get_db, init_tsdb


class GaugePayload(BaseModel):
    metric_id: Optional[int] = None
    ts: int
    value: float
    name: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    step: Optional[int] = None
    slots: Optional[int] = None


class CounterPayload(BaseModel):
    metric_id: Optional[int] = None
    ts: int
    raw_value: float
    name: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    step: Optional[int] = None
    slots: Optional[int] = None


class RetentionPayload(BaseModel):
    step: int
    slots: int


class SelectorPayload(BaseModel):
    metric: str
    tags: Optional[Dict[str, str]] = None
    alias: Optional[str] = None


class QueryPayload(BaseModel):
    metric_ids: Optional[List[int]] = None
    selectors: Optional[List[SelectorPayload]] = None
    start_ts: int
    end_ts: int
    sql: str


class LookupPayload(BaseModel):
    name: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    limit: Optional[int] = None


class TagLookupPayload(BaseModel):
    name: Optional[str] = None


class DashboardPayload(BaseModel):
    slug: str
    title: str
    definition: Dict[str, Any]


def _init_tsdb() -> FdbTsdb:
    cluster = os.environ.get("FDB_CLUSTER_FILE")
    db = get_db(cluster)
    return init_tsdb(db)


tsdb = _init_tsdb()
engine = QueryEngine(tsdb)
app = FastAPI(title="tsdb-codex")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
templates = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)

# Optional metric labels for built-in collectors (e.g., collect_metrics.py).
METRIC_LABELS = {
    3001: "cpu_percent",
    3002: "load_avg_1m",
    3003: "mem_used_percent",
    3004: "disk_used_percent",
    3010: "disk_read_bytes",
    3011: "disk_write_bytes",
    3020: "net_bytes_sent",
    3021: "net_bytes_recv",
}

_ALIAS_PATTERN = re.compile(r"{{\s*([A-Za-z0-9_]+)\s*}}")


def render_template(name: str, **kwargs: Any) -> str:
    template = templates.get_template(name)
    return template.render(**kwargs)


def _resolve_metric_ids_from_selectors(selectors: List[SelectorPayload]) -> Tuple[List[int], Dict[str, List[int]]]:
    metric_ids: List[int] = []
    alias_map: Dict[str, List[int]] = {}
    for idx, sel in enumerate(selectors):
        if not sel.metric:
            raise HTTPException(status_code=400, detail="selector.metric is required")
        alias = sel.alias or f"S{idx + 1}"
        if alias in alias_map:
            raise HTTPException(status_code=400, detail=f"duplicate selector alias: {alias}")
        matches, hit_limit = tsdb.find_metrics(
            name=sel.metric, tags=sel.tags or {}, limit=500, return_hit_limit=True
        )
        ids = [int(m.get("metric_id", 0)) for m in matches if m.get("metric_id") is not None]
        if not ids:
            raise HTTPException(status_code=400, detail=f"selector '{alias}' did not match any metrics")
        if hit_limit:
            raise HTTPException(status_code=400, detail=f"selector '{alias}' matched too many metrics; narrow tags")
        alias_map[alias] = ids
        metric_ids.extend(ids)
    return sorted(set(metric_ids)), alias_map


def _replace_alias_placeholders(sql: str, alias_map: Dict[str, List[int]]) -> str:
    def replacer(match: re.Match[str]) -> str:
        alias = match.group(1)
        if alias not in alias_map:
            raise HTTPException(status_code=400, detail=f"unknown selector alias in sql: {alias}")
        ids = alias_map[alias]
        if len(ids) != 1:
            raise HTTPException(
                status_code=400,
                detail=f"selector alias '{alias}' must resolve to exactly one metric for SQL placeholder substitution",
            )
        return str(ids[0])

    return _ALIAS_PATTERN.sub(replacer, sql)


@app.post("/ingest/gauge")
def write_gauge(payload: GaugePayload) -> Dict[str, Any]:
    if payload.metric_id is None and not payload.name:
        raise HTTPException(status_code=400, detail="metric_id or name is required")
    try:
        metric_id = tsdb.write_gauge(
            payload.metric_id,
            payload.ts,
            payload.value,
            name=payload.name,
            tags=payload.tags,
            step=payload.step,
            slots=payload.slots,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok", "metric_id": metric_id, "timestamp": payload.ts}


@app.post("/ingest/counter")
def write_counter(payload: CounterPayload) -> Dict[str, Any]:
    if payload.metric_id is None and not payload.name:
        raise HTTPException(status_code=400, detail="metric_id or name is required")
    try:
        metric_id = tsdb.write_counter(
            payload.metric_id,
            payload.ts,
            payload.raw_value,
            name=payload.name,
            tags=payload.tags,
            step=payload.step,
            slots=payload.slots,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "status": "ok",
        "metric_id": metric_id,
        "timestamp": payload.ts,
    }


@app.post("/query")
def query(payload: QueryPayload) -> Dict[str, Any]:
    if payload.start_ts > payload.end_ts:
        raise HTTPException(status_code=400, detail="start_ts must be <= end_ts")

    selectors = payload.selectors or []
    computed_ids: List[int] = []
    alias_map: Dict[str, List[int]] = {}
    if selectors:
        computed_ids, alias_map = _resolve_metric_ids_from_selectors(selectors)

    provided_ids = payload.metric_ids or []
    if computed_ids and provided_ids and set(provided_ids) != set(computed_ids):
        raise HTTPException(
            status_code=400, detail="metric_ids do not match selectors"
        )

    metric_ids = sorted(set(computed_ids or provided_ids))
    if not metric_ids:
        raise HTTPException(
            status_code=400,
            detail="metric_ids or selectors must resolve to at least one metric",
        )

    sql = payload.sql
    if alias_map:
        sql = _replace_alias_placeholders(sql, alias_map)

    try:
        rows = engine.run_sql(metric_ids, payload.start_ts, payload.end_ts, sql)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"rows": rows, "count": len(rows)}


@app.get("/metrics")
def list_metrics() -> Dict[str, Any]:
    metrics = tsdb.list_metrics()
    for metric in metrics:
        if not metric.get("name"):
            metric["name"] = METRIC_LABELS.get(metric["metric_id"], "")
    return {"metrics": metrics}

@app.delete("/metrics/{metric_id}")
def delete_metric(metric_id: int) -> Dict[str, Any]:
    try:
        tsdb.delete_metric(metric_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok", "metric_id": metric_id}

@app.post("/metrics/{metric_id}/retention")
def update_metric_retention(metric_id: int, payload: RetentionPayload) -> Dict[str, Any]:
    try:
        tsdb.rewrite_metric_retention(metric_id, payload.step, payload.slots)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok", "metric_id": metric_id, "step": payload.step, "slots": payload.slots}


@app.post("/metrics/lookup")
def lookup_metrics(payload: LookupPayload) -> Dict[str, Any]:
    limit = payload.limit or 200
    limit = max(1, min(limit, 2000))
    metrics, hit_limit = tsdb.find_metrics(
        name=payload.name, tags=payload.tags, limit=limit, return_hit_limit=True
    )
    return {"metrics": metrics, "hit_limit": hit_limit, "limit": limit}


@app.get("/metrics/names")
def metric_names() -> Dict[str, Any]:
    return {"names": tsdb.list_metric_names()}


@app.post("/metrics/tag-values")
def metric_tag_values(payload: TagLookupPayload) -> Dict[str, Any]:
    return {"tags": tsdb.tag_catalog(name=payload.name)}


@app.get("/dashboards")
def list_dashboards() -> Dict[str, Any]:
    return {"dashboards": tsdb.list_dashboards()}


@app.get("/dashboards/{slug}")
def get_dashboard(slug: str) -> Dict[str, Any]:
    dash = tsdb.get_dashboard(slug)
    if not dash:
        raise HTTPException(status_code=404, detail="dashboard not found")
    return dash


@app.delete("/dashboards/{slug}")
def delete_dashboard(slug: str) -> Dict[str, Any]:
    try:
        tsdb.delete_dashboard(slug)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok", "slug": slug}


@app.post("/dashboards")
def save_dashboard(payload: DashboardPayload) -> Dict[str, Any]:
    try:
        tsdb.save_dashboard(payload.slug, payload.title, payload.definition)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok", "slug": payload.slug}


@app.get("/metrics/{metric_id}/series")
def metric_series(metric_id: int, start_ts: int, end_ts: int, bucket: int = 1) -> Dict[str, Any]:
    if start_ts > end_ts:
        raise HTTPException(status_code=400, detail="start_ts must be <= end_ts")
    if bucket <= 0:
        raise HTTPException(status_code=400, detail="bucket must be positive")
    meta = tsdb.get_meta(metric_id)
    metric_type = meta[2] if meta else 0
    # Use QueryEngine to bucket values; ts_bucket returns bucketed ts.
    if metric_type == 1:
        sql = f"""
WITH bucketed AS (
  SELECT ts_bucket(ts, {bucket}) AS bucket, max(value) AS value
  FROM samples
  WHERE metric_id = {metric_id} AND ts >= {start_ts} AND ts <= {end_ts}
  GROUP BY bucket
),
rates AS (
  SELECT bucket,
         bucket_rate(value, LAG(value) OVER (ORDER BY bucket), {bucket}) AS rate
  FROM bucketed
)
SELECT bucket, rate AS value
FROM rates
ORDER BY bucket
"""
    else:
        sql = f"""
SELECT ts_bucket(ts, {bucket}) AS bucket, avg(value) AS value
FROM samples
WHERE metric_id = {metric_id} AND ts >= {start_ts} AND ts <= {end_ts}
GROUP BY bucket
ORDER BY bucket
"""
    try:
        rows = engine.run_sql([metric_id], start_ts, end_ts, sql)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"rows": rows}


@app.get("/ui", response_class=HTMLResponse)
def ui() -> HTMLResponse:
    html = render_template("dashboard.html")
    return HTMLResponse(html)
