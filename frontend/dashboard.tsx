import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Button,
  Card,
  ConfigProvider,
  Drawer,
  Dropdown,
  Empty,
  Form,
  Input,
  InputNumber,
  Checkbox,
  Layout,
  Menu,
  message,
  DatePicker,
  Segmented,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  theme as antdTheme,
} from "antd";
import {
  BulbOutlined,
  DashboardOutlined,
  LineChartOutlined,
  PlusOutlined,
  ReloadOutlined,
  SaveOutlined,
  CloseOutlined,
  HolderOutlined,
  EditOutlined,
  EyeOutlined,
  BellOutlined,
  SettingOutlined,
  MoreOutlined,
  CopyOutlined,
} from "@ant-design/icons";
import CodeMirror from "@uiw/react-codemirror";
import { sql as cmSql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import dayjs, { Dayjs } from "dayjs";
import type { RangePickerProps } from "antd/es/date-picker";
import uPlot from "uplot";
import { Responsive, WidthProvider } from "react-grid-layout";
import "./style.css";

const { Header, Content, Sider } = Layout;
const { Title, Text } = Typography;

function formatSi(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}G`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(2)}k`;
  if (abs < 1 && abs > 0) return value.toExponential(2);
  return value.toFixed(2);
}

function typeLabel(t?: number): string {
  return t === 1 ? "counter" : "gauge";
}

function formatTimestamp(ts: number, timeZone?: string): string {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone: timeZone || Intl.DateTimeFormat().resolvedOptions().timeZone,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  return dtf.format(new Date(ts * 1000));
}

function createInlineLegendPlugin(
  target: HTMLElement,
  opts: { timeZone?: string; formatValue?: (v: number | null) => string }
): uPlot.Plugin {
  const formatValue = opts.formatValue || ((v: number | null) => (v === null || v === undefined ? "–" : formatSi(Number(v))));

  const render = (u: uPlot) => {
    const idx = u.cursor.idx ?? null;
    const xVals = u.data[0] as (number | null)[];
    target.replaceChildren();

    const timeRow = document.createElement("div");
    timeRow.className = "uplot-inline-legend-time";
    if (idx !== null && typeof xVals[idx] === "number") {
      timeRow.textContent = formatTimestamp(Number(xVals[idx]), opts.timeZone);
    } else {
      timeRow.textContent = "—";
    }
    target.appendChild(timeRow);

    const seriesWrap = document.createElement("div");
    seriesWrap.className = "uplot-inline-legend-rows";

    for (let i = 1; i < u.series.length; i++) {
      const series = u.series[i];
      if (series.show === false) continue;
      const row = document.createElement("div");
      row.className = "uplot-inline-legend-row";
      const swatch = document.createElement("span");
      swatch.className = "uplot-inline-legend-swatch";
      const stroke = series.stroke as any;
      swatch.style.background = typeof stroke === "string" ? stroke : "#3b82f6";
      const label = document.createElement("span");
      label.className = "uplot-inline-legend-label";
      label.textContent = series.label || `series-${i}`;
      const valEl = document.createElement("span");
      valEl.className = "uplot-inline-legend-value";
      const val = (u.data[i] as (number | null)[] | undefined)?.[idx ?? 0] ?? null;
      valEl.textContent = formatValue(val);
      row.appendChild(swatch);
      row.appendChild(label);
      row.appendChild(valEl);
      seriesWrap.appendChild(row);
    }

    target.appendChild(seriesWrap);
  };

  return {
    hooks: {
      setCursor: [render],
      setData: [render],
      setSeries: [render],
      destroy: [() => target.replaceChildren()],
    },
  };
}

import {
  normalizeSelectors,
  selectorTagsToDict,
  matchSelectorsToMetrics,
  buildSqlFromConfig,
  SelectorConfig,
  QueryBuilderConfig,
  MetricMeta,
  sqlSafeAlias,
} from "./query_helpers";
import { parseExpression, exprToSqlPivot, collectRefs } from "./expression_parser";

export interface PanelMetric {
  name?: string;
  metric_id?: number;
  tags?: Record<string, string>;
  type?: number;
  step?: number;
  slots?: number;
}

export interface PanelLayout {
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface PanelDef {
  id: number;
  title: string;
  range: number; // seconds
  bucket: number; // seconds
  metrics: PanelMetric[];
  selectors?: SelectorConfig[];
  layout?: PanelLayout;
  sql?: string;
  query?: QueryBuilderConfig;
}

export interface TagCatalog {
  keys: string[];
  values: Record<string, string[]>;
}

type GridLayoutItem = { i: string; x: number; y: number; w: number; h: number; minW?: number; minH?: number };
type GridLayouts = Record<string, GridLayoutItem[]>;

type LabelOperator = "=" | "!=" | "=~" | "!~";

interface LabelFilter {
  key?: string;
  op: LabelOperator;
  value?: string;
}

export type { SelectorConfig, QueryBuilderConfig, MetricMeta };

interface QuerySeries {
  label: string;
  values: (number | null)[];
  metric?: MetricMeta;
  group?: string;
  alias?: string;
}

interface QueryResult {
  buckets: number[];
  series: QuerySeries[];
  metrics: MetricMeta[];
  sql: string;
}

const ResponsiveGridLayout = WidthProvider(Responsive);

const GRID_COLS = 12;
const GRID_ROW_HEIGHT = 130;
const DEFAULT_PANEL_W = 4;
const DEFAULT_PANEL_H = 3;
type ViewKey = "dashboards" | "explorer" | "alerts" | "settings";
const DEFAULT_VIEW: ViewKey = "dashboards";

function parseInitialLocation(): { view: ViewKey; slug?: string } {
  const params = new URLSearchParams(window.location.search);
  const viewParam = params.get("view");
  const slug = params.get("dash") || undefined;
  return {
    view: (["dashboards", "explorer", "alerts", "settings"] as ViewKey[]).includes(viewParam as ViewKey)
      ? (viewParam as ViewKey)
      : DEFAULT_VIEW,
    slug,
  };
}

async function fetchJSON<T>(url: string, options: RequestInit = {}): Promise<T> {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function parseTagsInput(value: string): Record<string, string> {
  if (!value) return {};
  const tags: Record<string, string> = {};
  value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .forEach((pair) => {
      const [k, v] = pair.split("=");
      if (k && v !== undefined) tags[k.trim()] = v.trim();
    });
  return tags;
}

function selectorsToApiPayload(selectors: SelectorConfig[]) {
  return normalizeSelectors(selectors)
    .filter((s) => s.metric)
    .map((sel, idx) => ({
      metric: sel.metric,
      alias: sel.alias || String.fromCharCode(65 + (idx % 26)),
      tags: selectorTagsToDict(sel),
    }));
}

async function resolveMetricsForSelectors(selectors: SelectorConfig[]): Promise<MetricMeta[]> {
  const apiSelectors = selectorsToApiPayload(selectors);
  const metrics: MetricMeta[] = [];
  for (const sel of apiSelectors) {
    const resp = await fetchJSON<{ metrics: MetricMeta[] }>("/metrics/lookup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: sel.metric, tags: sel.tags, limit: 400 }),
    });
    metrics.push(...(resp.metrics || []));
  }
  return metrics;
}

function defaultLayoutAt(index: number): PanelLayout {
  const slotsPerRow = Math.floor(GRID_COLS / DEFAULT_PANEL_W) || 1;
  return {
    x: (index * DEFAULT_PANEL_W) % GRID_COLS,
    y: Math.floor(index / slotsPerRow) * DEFAULT_PANEL_H,
    w: DEFAULT_PANEL_W,
    h: DEFAULT_PANEL_H,
  };
}

function attachLayouts(panels: PanelDef[]): PanelDef[] {
  return panels.map((p, idx) => ({
    ...p,
    layout: p.layout || defaultLayoutAt(idx),
  }));
}

function clampLayout(layout: PanelLayout, cols: number): PanelLayout {
  const w = Math.max(1, Math.min(layout.w, cols));
  const x = Math.min(Math.max(0, layout.x), cols - w);
  return { ...layout, x, w };
}

function buildLayouts(panels: PanelDef[]): GridLayouts {
  const colsByBreakpoint: Record<string, number> = {
    lg: GRID_COLS,
    md: GRID_COLS,
    sm: 8,
    xs: 6,
    xxs: 1,
  };
  const layouts: GridLayouts = {};
  Object.entries(colsByBreakpoint).forEach(([bp, cols]) => {
    layouts[bp] = panels.map((panel, idx) => {
      const layout = clampLayout(panel.layout || defaultLayoutAt(idx), cols);
      return { i: String(panel.id), ...layout, minW: 1, minH: 2 };
    });
  });
  return layouts;
}

function buildDefaultSql(metricId: number, bucket: number): string {
  return `
SELECT ts_bucket(ts, ${bucket}) AS bucket, avg(value) AS value
FROM samples
WHERE metric_id = ${metricId}
GROUP BY bucket
ORDER BY bucket
  `.trim();
}

function formatPromDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "0s";
  if (seconds % 3600 === 0) return `${Math.floor(seconds / 3600)}h`;
  if (seconds % 60 === 0) return `${Math.floor(seconds / 60)}m`;
  return `${Math.max(1, Math.floor(seconds))}s`;
}

function matchesLabelFilter(tags: Record<string, string> | undefined, filter: LabelFilter): boolean {
  if (!filter.key || filter.value === undefined || filter.value === "") return true;
  const val = tags?.[filter.key];
  switch (filter.op) {
    case "=":
      return val === filter.value;
    case "!=":
      return val !== filter.value;
    case "=~":
      try {
        return !!val && new RegExp(filter.value).test(val);
      } catch {
        return false;
      }
    case "!~":
      try {
        return !val || !new RegExp(filter.value).test(val);
      } catch {
        return true;
      }
    default:
      return true;
  }
}

function formatSeriesLabel(metric: MetricMeta): string {
  const base = metric.name || `metric ${metric.metric_id}`;
  const tags = Object.entries(metric.tags || {});
  if (!tags.length) return base;
  const tagText = tags
    .slice(0, 3)
    .map(([k, v]) => `${k}="${v}"`)
    .join(", ");
  return `${base}{${tagText}${tags.length > 3 ? ", …" : ""}}`;
}

function computeRateSeries(values: (number | null)[], buckets: number[], fn: QueryBuilderConfig["rateFunction"]) {
  if (fn === "none") return values;
  const next = values.map(() => null as number | null);
  for (let i = 1; i < values.length; i++) {
    const prev = values[i - 1];
    const curr = values[i];
    if (prev === null || curr === null) continue;
    const delta = curr - prev;
    const dt = Math.max(1, buckets[i] - buckets[i - 1]);
    if (fn === "increase") {
      next[i] = delta >= 0 ? delta : 0;
    } else {
      const rate = delta / dt;
      next[i] = delta < 0 && fn === "rate" ? 0 : rate;
    }
  }
  return next;
}

function aggregateSeries(
  buckets: number[],
  seriesList: { values: (number | null)[]; metric?: MetricMeta }[],
  aggregator: QueryBuilderConfig["aggregator"],
  groupBy: string[]
): QuerySeries[] {
  if (aggregator === "none") {
    return seriesList.map((s) => ({
      label: s.metric ? formatSeriesLabel(s.metric) : "series",
      values: s.values,
      metric: s.metric,
    }));
  }
  const groups = new Map<
    string,
    {
      label: string;
      values: (number | null)[];
      counts: number[];
    }
  >();

  const makeKey = (metric?: MetricMeta) => {
    if (!groupBy.length || !metric) return "__all__";
    return groupBy.map((k) => metric.tags?.[k] ?? "").join("|");
  };

  const groupLabel = (metric?: MetricMeta) => {
    if (!groupBy.length || !metric) return `${aggregator} (all series)`;
    const pairs = groupBy.map((k) => `${k}=${metric.tags?.[k] ?? "*"}`).join(", ");
    return `${aggregator} by (${groupBy.join(",")}) {${pairs}}`;
  };

  seriesList.forEach((series) => {
    const key = makeKey(series.metric);
    if (!groups.has(key)) {
      groups.set(key, {
        label: groupLabel(series.metric),
        values: Array(buckets.length).fill(null),
        counts: Array(buckets.length).fill(0),
      });
    }
    const bucketValues = groups.get(key)!;
    series.values.forEach((val, idx) => {
      if (val === null || Number.isNaN(val)) return;
      const current = bucketValues.values[idx];
      if (aggregator === "min") {
        bucketValues.values[idx] = current === null ? val : Math.min(current, val);
      } else if (aggregator === "max") {
        bucketValues.values[idx] = current === null ? val : Math.max(current, val);
      } else {
        bucketValues.values[idx] = (bucketValues.values[idx] ?? 0) + val;
      }
      bucketValues.counts[idx] += 1;
    });
  });

  const final: QuerySeries[] = [];
  groups.forEach((bucketValues) => {
    const vals = bucketValues.values.map((val, idx) => {
      if (val === null) return null;
      if (aggregator === "avg" && bucketValues.counts[idx] > 0) {
        return val / bucketValues.counts[idx];
      }
      return val;
    });
    final.push({ label: bucketValues.label, values: vals });
  });
  return final;
}


async function executeSqlBuilderQuery(
  config: QueryBuilderConfig,
  options: {
    startTs?: number;
    endTs?: number;
    bucketOverride?: number;
    metricHints?: PanelMetric[];
    sqlOverride?: string;
    expressionSql?: string;
    expressionRefs?: string[];
  } = {}
): Promise<QueryResult> {
  const now = Math.floor(Date.now() / 1000);
  const effectiveEnd = Math.max(0, options.endTs ?? now);
  const bucket = Math.max(1, Math.floor(options.bucketOverride ?? config.bucket ?? 60));
  const windowPad = config.rateFunction === "none" ? 0 : Math.max(bucket, config.rangeSelector || bucket);
  const effectiveStart = Math.max(0, options.startTs ?? effectiveEnd - (config.range || 3600) - windowPad);

  const rawSelectors = config.selectors || [];
  const selectors = normalizeSelectors(rawSelectors).filter((s) => s.metric);
  const apiSelectors = selectorsToApiPayload(selectors);
  const aliasProvided = new Map<string, boolean>();
  const selectorLabel: Record<string, string> = {};
  selectors.forEach((s, idx) => {
    const raw = rawSelectors[idx];
    aliasProvided.set(s.alias, !!raw?.alias?.trim());
    const tagFilters = (rawSelectors[idx]?.tags || []).filter((t) => t.key && t.value);
    const tagText = tagFilters
      .map((t) => `${t.key}="${t.value}"`)
      .join(", ");
    if (s.metric) {
      selectorLabel[s.alias] = tagText ? `${s.metric}{${tagText}}` : s.metric;
    }
  });
  let metrics: MetricMeta[] = [];

  if (!selectors.length) throw new Error("Add at least one metric selector");

  if ((options.metricHints || []).length) {
    metrics = (options.metricHints || [])
      .filter((m): m is PanelMetric & { metric_id: number } => m.metric_id !== undefined)
      .map((m) => ({
        metric_id: m.metric_id!,
        name: m.name,
        tags: m.tags || {},
        step: m.step ?? 0,
        slots: m.slots ?? 0,
        type: m.type ?? 0,
      }));
  } else {
    if (!selectors.length) throw new Error("Add at least one metric selector");
    metrics = await resolveMetricsForSelectors(selectors);
  }

  if (!metrics.length) {
    throw new Error("No metrics matched this query");
  }

  const selectorMatchMap = matchSelectorsToMetrics(selectors, metrics);
  if (!Object.keys(selectorMatchMap).length) {
    throw new Error("Selectors did not resolve to any metrics");
  }
  const metricIds = Array.from(new Set(metrics.map((m) => m.metric_id)));

  const labelForAlias = (alias: string) => {
    const aliasLower = alias.toLowerCase();
    const selector = selectors.find((s) => s.alias === alias || s.alias.toLowerCase() === aliasLower);
    const ids =
      selectorMatchMap[alias] ||
      selectorMatchMap[alias.toUpperCase()] ||
      selectorMatchMap[aliasLower] ||
      [];
    const meta = metrics.find((m) => ids.includes(m.metric_id));
    const aliasExplicit = aliasProvided.get(alias) === true;
    if (aliasExplicit) return alias;
    if (selectorLabel[alias]) return selectorLabel[alias];
    if (meta) return formatSeriesLabel(meta);
    return selector?.metric || alias;
  };

  let sql: string;
  if (options.sqlOverride && options.sqlOverride.trim().length) {
    sql = options.sqlOverride
      .replace(/<start_ts>/g, String(effectiveStart))
      .replace(/<end_ts>/g, String(effectiveEnd));
  } else {
    sql = buildSqlFromConfig(
      config,
      metricIds,
      selectors,
      effectiveStart,
      effectiveEnd,
      selectorMatchMap,
      false,
      options.expressionSql,
      options.expressionRefs
    );
  }

  const resp = await fetchJSON<{ rows: any[] }>("/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      metric_ids: metricIds,
      selectors: apiSelectors,
      start_ts: effectiveStart,
      end_ts: effectiveEnd,
      sql,
    }),
  });

  const rows = resp.rows || [];
  if (!rows.length) {
    throw new Error("No data matched this query");
  }

  const bucketSet = new Set<number>();
  const perAlias: Record<string, Map<number, number | null>> = {};
  rows.forEach((r) => {
    const b = Number(r.bucket);
    const alias = String(r.alias || r.metric_id || "");
    bucketSet.add(b);
    if (!perAlias[alias]) perAlias[alias] = new Map();
    perAlias[alias].set(b, r.value == null ? null : Number(r.value));
  });

  const buckets = Array.from(bucketSet).sort((a, b) => a - b);
  const baseSeries: QuerySeries[] = Object.entries(perAlias).map(([alias, map]) => {
    const values = buckets.map((b) => map.get(b) ?? null);
    const rateAdjusted = computeRateSeries(values, buckets, config.rateFunction);
    const meta = metrics.find((m) => (selectorMatchMap[alias] || []).includes(m.metric_id));
    const label = labelForAlias(alias);
    return { label, values: rateAdjusted, alias, metric: meta };
  });

  let series = baseSeries;

  return {
    buckets,
    series,
    metrics,
    sql,
  };
}

function makeTimeAxisValues(timeZone?: string, maxLabels = 6) {
  const tz = timeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
  const fmtLong = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hour12: false,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
  const fmtTime = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });
  const fmtDay = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });

  return (_u: uPlot, splits: number[]) => {
    if (!splits.length) return [];
    const step = Math.max(1, Math.ceil(splits.length / maxLabels));
    const firstDay = fmtDay.format(new Date(splits[0] * 1000));
    const lastDay = fmtDay.format(new Date(splits[splits.length - 1] * 1000));
    return splits.map((s, idx) => {
      if (idx % step !== 0) return "";
      const date = new Date(s * 1000);
      return firstDay !== lastDay ? fmtLong.format(date) : fmtTime.format(date);
    });
  };
}

const PanelChart: React.FC<{
  panel: PanelDef;
  themeMode: "dark" | "light";
  height?: number;
  startTs?: number;
  endTs?: number;
  bucketOverride?: number;
  timeZone?: string;
}> = ({ panel, themeMode, height, startTs, endTs, bucketOverride, timeZone }) => {
  const ref = useRef<HTMLDivElement | null>(null);
  const legendRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<uPlot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const axisValues = useMemo(() => makeTimeAxisValues(timeZone), [timeZone]);
  const chartHeight = Math.max(250, height || 250);
  const normalizeSeconds = (val?: number | null) => {
    if (val === undefined || val === null) return undefined;
    let n = Number(val);
    if (Number.isNaN(n)) return undefined;
    // Treat values as seconds; if millisecond precision is passed, floor it to seconds.
    n = n >= 1_000_000_000_000 ? n / 1000 : n;
    return Math.floor(n);
  };
  const normStart = normalizeSeconds(startTs);
  const normEnd = normalizeSeconds(endTs);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const now = Math.floor(Date.now() / 1000);
        const effectiveEnd = Math.max(0, normEnd ?? now);
        const effectiveStart = Math.max(0, normStart ?? effectiveEnd - (panel.range || 3600));
        const bucket = Math.max(1, Math.floor(bucketOverride ?? panel.bucket ?? 60));
        const seriesData: (number | null)[][] = [];
        const palette = ["#34d399", "#60a5fa", "#fbbf24", "#a78bfa", "#38bdf8", "#f472b6"];
        let seriesLabels: string[] = [];

        const resolveMetrics = async (desc: PanelMetric): Promise<MetricMeta[]> => {
          if (desc.metric_id)
            return [
              {
                metric_id: desc.metric_id,
                name: desc.name,
                tags: desc.tags,
                step: desc.step ?? 0,
                slots: desc.slots ?? 0,
                type: desc.type ?? 0,
              },
            ];
          const resp = await fetchJSON<{ metrics: MetricMeta[] }>("/metrics/lookup", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: desc.name || null, tags: desc.tags || {} }),
          });
          return resp.metrics || [];
        };

        if (panel.query) {
          const requestedRange = Math.max(1, effectiveEnd - effectiveStart);
          const builderConfig: QueryBuilderConfig = {
            ...panel.query,
            range: requestedRange || panel.query.range || panel.range || 3600,
            bucket,
            rangeSelector: panel.query.rangeSelector || bucket,
            groupBy: panel.query.groupBy || [],
            selectors: panel.query.selectors || [],
          };
          const result = await executeSqlBuilderQuery(builderConfig, {
            startTs: effectiveStart,
            endTs: effectiveEnd,
            bucketOverride: bucket,
            metricHints: panel.metrics,
          });
          if (!result.buckets.length) throw new Error("No data matched this panel");
          seriesData.push(result.buckets);
          result.series.forEach((s) => seriesData.push(s.values));
          seriesLabels = result.series.map((s) => s.label);
        } else if (panel.sql) {
          const descs = panel.metrics && panel.metrics.length ? panel.metrics : [];
          const selectorCfg = normalizeSelectors(panel.selectors || []).filter((s) => s.metric);
          const apiSelectors = selectorsToApiPayload(selectorCfg);
          const metricList: MetricMeta[] = [];
          for (const desc of descs) {
            const matches = await resolveMetrics(desc);
            metricList.push(...matches);
          }
          if (!metricList.length && selectorCfg.length) {
            const matches = await resolveMetricsForSelectors(selectorCfg);
            metricList.push(...matches);
          }
          const metricIds = metricList
            .map((m) => m.metric_id)
            .filter((mId): mId is number => typeof mId === "number");
          if (!metricIds.length) throw new Error("No metrics provided for SQL panel");
          const resp = await fetchJSON<{ rows: any[] }>("/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              metric_ids: metricIds,
              selectors: apiSelectors,
              start_ts: effectiveStart,
              end_ts: effectiveEnd,
              sql: panel.sql,
            }),
          });
          const rows = resp.rows || [];
          if (!rows.length) throw new Error("No data matched this panel");
          const sample = rows[0];
          const bucketKey = ["bucket", "b", "ts"].find((k) => k in sample) || Object.keys(sample)[0];
          const valueKeys = Object.keys(sample).filter((k) => k !== bucketKey);
          if (!valueKeys.length) throw new Error("No value columns returned for SQL panel");
          const buckets = rows.map((r) => Number(r[bucketKey]));
          seriesData.push(buckets);
          valueKeys.forEach((k) => {
            seriesData.push(rows.map((r) => (r[k] == null ? null : Number(r[k]))));
          });
          seriesLabels = valueKeys;
        } else {
          const seriesDefs = panel.metrics || [];
          const resolved: { desc: PanelMetric; match: MetricMeta; rows: any[] }[] = [];
          for (const desc of seriesDefs) {
            const matches = await resolveMetrics(desc);
            for (const match of matches) {
              const data = await fetchJSON<{ rows: any[] }>(
                `/metrics/${match.metric_id}/series?start_ts=${effectiveStart}&end_ts=${effectiveEnd}&bucket=${bucket}`
              );
              resolved.push({ desc, match, rows: data.rows });
            }
          }
          if (!resolved.length) {
            throw new Error("No data matched this panel");
          }
          const bucketsSet = new Set<number>();
          resolved.forEach((r) => r.rows.forEach((row) => bucketsSet.add(row.bucket)));
          const buckets = Array.from(bucketsSet).sort((a, b) => a - b);
          seriesData.push(buckets.map((b) => Number(b)));
          resolved.forEach((r) => {
            const map = new Map<number, number>(r.rows.map((row) => [row.bucket, row.value]));
            seriesData.push(buckets.map((b) => map.get(b) ?? null));
            seriesLabels.push(r.desc.name || r.match.name || String(r.match.metric_id));
          });
        }
        const axesStroke = themeMode === "dark" ? "#94a3b8" : "#0f172a";
        const gridStroke = themeMode === "dark" ? "#1f2937" : "#cbd5e1";
        const container = ref.current;
        const legendContainer = legendRef.current;
        if (!container) return;
        const plugins: uPlot.Plugin[] = [];
        if (legendContainer) {
          plugins.push(
            createInlineLegendPlugin(legendContainer, {
              timeZone,
              formatValue: (v) => (v === null || v === undefined ? "–" : formatSi(Number(v))),
            })
          );
        }
        const opts: uPlot.Options = {
          width: container.clientWidth || 360,
          height: chartHeight,
          scales: { x: { time: true } },
          axes: [
            { stroke: axesStroke, grid: { show: true, stroke: gridStroke }, values: axisValues },
            { stroke: axesStroke, grid: { show: true, stroke: gridStroke }, values: (_u, vals) => vals.map((v) => formatSi(Number(v))), size: 60 },
          ],
          series: [
            {},
            ...(seriesData.slice(1).map((_, idx) => ({
              label: seriesLabels[idx] || `series-${idx + 1}`,
              stroke: palette[idx % palette.length],
              width: 2,
            })) as any),
          ],
          legend: { show: false },
          plugins,
        };
        chartRef.current?.destroy();
        chartRef.current = new uPlot(opts, seriesData as any, container);
      } catch (err: any) {
        if (!cancelled) setError(err?.message || "Failed to load panel");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, [panel, themeMode, height, startTs, endTs, bucketOverride, timeZone]);

  return (
    <div className="chart-frame">
      {error ? (
        <Empty description={error} image={Empty.PRESENTED_IMAGE_SIMPLE} />
      ) : (
        <>
          {loading && (
            <div style={{ textAlign: "center", padding: "12px 0" }}>
              <Text type="secondary">Loading...</Text>
            </div>
          )}
          <div>
            <div ref={ref} style={{ height: chartHeight, position: "relative" }} />
            <div ref={legendRef} className="chart-legend-inline" />
          </div>
        </>
      )}
    </div>
  );
};

const PanelForm: React.FC<{
  open: boolean;
  onClose: () => void;
  onAdd: (panel: Omit<PanelDef, "id">) => void;
}> = ({ open, onClose, onAdd }) => {
  const [form] = Form.useForm();

  const submit = (values: any) => {
    const tags = parseTagsInput(values.tags || "");
    onAdd({
      title: values.title || "Panel",
      range: values.range || 3600,
      bucket: values.bucket || 60,
      metrics: [{ name: values.metric.trim(), tags }],
    });
    form.resetFields();
    onClose();
  };

  return (
    <Drawer
      title="Add panel"
      placement="right"
      width={420}
      onClose={onClose}
      open={open}
      styles={{ body: { paddingBottom: 24 } }}
      className="drawer-content"
    >
      <Form form={form} layout="vertical" onFinish={submit} initialValues={{ range: 3600, bucket: 60 }}>
        <Form.Item label="Title" name="title">
          <Input placeholder="CPU usage" />
        </Form.Item>
        <Form.Item label="Metric name" name="metric" rules={[{ required: true, message: "Metric name is required" }]}>
          <Input placeholder="cpu_percent" />
        </Form.Item>
        <Form.Item label="Tags (k=v, comma separated)" name="tags">
          <Input placeholder="host=myhost,env=dev" />
        </Form.Item>
        <Space size="large">
          <Form.Item label="Range (seconds)" name="range">
            <InputNumber min={1} step={60} style={{ width: 160 }} />
          </Form.Item>
          <Form.Item label="Bucket (seconds)" name="bucket">
            <InputNumber min={1} step={1} style={{ width: 160 }} />
          </Form.Item>
        </Space>
        <Button type="primary" icon={<PlusOutlined />} htmlType="submit" block>
          Add panel
        </Button>
      </Form>
    </Drawer>
  );
};

const Explorer: React.FC<{
  themeMode: "dark" | "light";
  timeZone: string;
  addMode?: boolean;
  onAddPanel?: (panel: PanelDef) => void;
  dashboardTitle?: string;
}> = ({ themeMode, timeZone, addMode = false, onAddPanel, dashboardTitle }) => {
  const queryDefaults: QueryBuilderConfig = {
    selectors: [{ metric: "", tags: [{ op: "=" }], alias: "", transform: { op: "none" } }],
    range: 3600,
    bucket: 60,
    rangeSelector: 300,
    rateFunction: "none",
    aggregator: "none",
    groupBy: [],
    expression: undefined,
    showBase: true,
  };
  const [builderQuery, setBuilderQuery] = useState<QueryBuilderConfig>(queryDefaults);
  const [builderResult, setBuilderResult] = useState<QueryResult | null>(null);
  const [builderLoading, setBuilderLoading] = useState(false);
  const [builderError, setBuilderError] = useState<string | null>(null);
  const [builderSqlGenerated, setBuilderSqlGenerated] = useState<string>("");
  const [builderSqlText, setBuilderSqlText] = useState<string>("");
  const [formula, setFormula] = useState<string>("");
  const [formulaError, setFormulaError] = useState<string | null>(null);
  const [formulaRefs, setFormulaRefs] = useState<string[]>([]);
  const [newPanelTitle, setNewPanelTitle] = useState<string>("");
  const [metricMetaCache, setMetricMetaCache] = useState<Record<string, MetricMeta>>({});
  const [chartStyle, setChartStyle] = useState<"line" | "area" | "points" | "table">("line");
  const [builderMetricsUsed, setBuilderMetricsUsed] = useState<MetricMeta[]>([]);
  const [metricNames, setMetricNames] = useState<string[]>([]);
  const [tagCatalog, setTagCatalog] = useState<TagCatalog>({ keys: [], values: {} });
  const chartRef = useRef<HTMLDivElement | null>(null);
  const legendRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  const normalizedBuilderSelectors = useMemo(
    () => normalizeSelectors(builderQuery.selectors || []),
    [builderQuery.selectors]
  );

  const formatTs = (bucketVal: number) => formatTimestamp(bucketVal, timeZone);

  const builderTableRows = useMemo(() => {
    if (!builderResult) return [];
    return builderResult.buckets.map((bucket, idx) => {
      const row: any = { bucket };
      builderResult.series.forEach((s, sIdx) => {
        row[`s${sIdx}`] = s.values[idx];
      });
      return row;
    });
  }, [builderResult]);

  const builderColumns = useMemo(() => {
    if (!builderResult) return [];
    return [
      { title: "Time", dataIndex: "bucket", key: "bucket", render: (val: number) => formatTs(val) },
      ...builderResult.series.map((s, idx) => ({
        title: s.label || `series-${idx + 1}`,
        dataIndex: `s${idx}`,
        key: `s${idx}`,
        render: (val: number | null) => (val === null || val === undefined ? "" : Number(val).toFixed(3)),
      })),
    ];
  }, [builderResult]);

  const renderSelectorEditor = (selectors: SelectorConfig[], onChange: (next: SelectorConfig[]) => void) => {
    const updateSelector = (idx: number, nextSel: SelectorConfig) => {
      const next = [...selectors];
      next[idx] = nextSel;
      onChange(next);
    };
    const removeSelector = (idx: number) => {
      if (selectors.length <= 1) return;
      onChange(selectors.filter((_, i) => i !== idx));
    };
    const addSelector = () => {
      onChange([
        ...selectors,
        { metric: "", tags: [{ op: "=" }], alias: "", transform: { op: "none" } },
      ]);
    };
    return (
      <Space direction="vertical" style={{ width: "100%" }} size="small">
        <Text type="secondary">Metric selectors</Text>
        {selectors.map((sel, idx) => (
          <Card size="small" key={idx} bodyStyle={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <Space wrap align="center" style={{ width: "100%", justifyContent: "space-between" }}>
              <Space wrap align="center">
                <Space align="center" wrap style={{ minWidth: 240 }}>
          <Select
            showSearch
            allowClear
            placeholder="metric"
            style={{ minWidth: 220, width: 240 }}
            value={sel.metric}
            options={metricNames.map((n) => ({ label: n, value: n }))}
            onChange={(val) => {
              updateSelector(idx, { ...sel, metric: val || "" });
            }}
          />
                  {sel.metric && metricMetaCache[sel.metric] ? (
                    <Tag color="geekblue" style={{ height: 22, lineHeight: "20px" }}>
                      {typeLabel(metricMetaCache[sel.metric].type)}
                    </Tag>
                  ) : null}
                </Space>
                {(sel.tags || []).map((lf, tIdx) => (
                  <Space key={tIdx} wrap>
                    <Select
                      allowClear
                      placeholder="label"
                      value={lf.key}
                      style={{ minWidth: 120 }}
                      options={tagCatalog.keys.map((k) => ({ label: k, value: k }))}
                      onChange={(val) => {
                        const tags = [...(sel.tags || [])];
                        tags[tIdx] = { ...(tags[tIdx] || { op: "=" as LabelOperator }), key: val || undefined };
                        updateSelector(idx, { ...sel, tags });
                      }}
                    />
                    <Select
                      showSearch
                      allowClear
                      placeholder="value"
                      value={lf.value}
                      style={{ minWidth: 120 }}
                      options={(lf.key ? tagCatalog.values[lf.key] || [] : []).map((v) => ({ label: v, value: v }))}
                      onChange={(val) => {
                        const tags = [...(sel.tags || [])];
                        tags[tIdx] = { ...(tags[tIdx] || { op: "=" as LabelOperator }), key: lf.key, value: val || undefined };
                        updateSelector(idx, { ...sel, tags });
                      }}
                      disabled={!lf.key}
                    />
                  </Space>
                ))}
                <Button
                  type="dashed"
                  size="small"
                  onClick={() => {
                    const tags = [...(sel.tags || [])];
                    tags.push({ op: "=", key: undefined, value: undefined });
                    updateSelector(idx, { ...sel, tags });
                  }}
                >
                  + tag
                </Button>
                <Input
                  style={{ width: 120 }}
                  addonBefore="as"
                  value={sel.alias}
                  placeholder={sel.metric ? `${sel.metric}` : String.fromCharCode(65 + idx)}
                  onChange={(e) => updateSelector(idx, { ...sel, alias: e.target.value || "" })}
                />
                <Select
                  style={{ width: 180 }}
                  value={sel.transform?.op || "none"}
                  onChange={(val) => {
                    const op = val as any;
                    updateSelector(idx, { ...sel, transform: { op } });
                  }}
                  options={[
                    { label: "No transform", value: "none" },
                    { label: "Rate (counter_rate)", value: "rate" },
                  ]}
                />
              </Space>
              <Button icon={<CloseOutlined />} onClick={() => removeSelector(idx)} />
            </Space>
          </Card>
        ))}
        <Button onClick={addSelector}>Add selector</Button>
      </Space>
    );
  };

  useEffect(() => {
    fetchJSON<{ names: string[] }>("/metrics/names")
      .then((data) => setMetricNames(data.names || []))
      .catch(() => setMetricNames([]));
  }, []);


  useEffect(() => {
    loadTagCatalog(builderQuery.selectors[0]?.metric);
  }, [builderQuery.selectors]);

  useEffect(() => {
    const names = builderQuery.selectors.map((s) => s.metric).filter(Boolean);
    names.forEach((name) => {
      if (metricMetaCache[name]) return;
      fetchJSON<{ metrics: MetricMeta[] }>("/metrics/lookup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, limit: 1 }),
      })
        .then((data) => {
          if (data.metrics && data.metrics.length) {
            setMetricMetaCache((prev) => ({ ...prev, [name]: data.metrics[0] }));
          }
        })
        .catch(() => {});
    });
  }, [builderQuery.selectors, metricMetaCache]);

  useEffect(() => {
    const usableSelectors = normalizeSelectors(builderQuery.selectors || []).filter((s) => s.metric);
    if (!usableSelectors.length) {
      setBuilderSqlGenerated("");
      setBuilderSqlText("");
      return;
    }
    const metricIds = builderMetricsUsed.length ? builderMetricsUsed.map((m) => m.metric_id) : [0];
    const now = Math.floor(Date.now() / 1000);
    const previewStart = now - (builderQuery.range || 3600);
    const previewEnd = now;
    const selectorMatchMap = matchSelectorsToMetrics(usableSelectors, builderMetricsUsed);
    let expressionSql: string | undefined;
    let expressionRefs: string[] | undefined;
    if (formula.trim()) {
      try {
        const ast = parseExpression(formula.trim());
        expressionSql = exprToSqlPivot(ast);
        expressionRefs = collectRefs(ast);
        setFormulaError(null);
      } catch (err: any) {
        setFormulaError(err?.message || "Invalid formula");
        expressionSql = undefined;
      }
    }
    const preview = buildSqlFromConfig(
      builderQuery,
      metricIds,
      usableSelectors,
      "<start_ts>",
      "<end_ts>",
      selectorMatchMap,
      true,
      expressionSql,
      expressionRefs
    );
    const withPlaceholders = preview
      .replace(String(previewStart), "<start_ts>")
      .replace(String(previewEnd), "<end_ts>");
    setBuilderSqlGenerated(withPlaceholders);
    setBuilderSqlText(withPlaceholders);
  }, [builderQuery, builderMetricsUsed, formula]);

  useEffect(() => {
    if (chartStyle === "table" || !builderResult || !builderResult.buckets.length) {
      if (plotRef.current) {
        plotRef.current.destroy();
        plotRef.current = null;
      }
      return;
    }
    const axesStroke = themeMode === "dark" ? "#94a3b8" : "#0f172a";
    const gridStroke = themeMode === "dark" ? "#1f2937" : "#cbd5e1";
    const baseSeries =
      chartStyle === "points"
        ? { width: 1, points: { show: true, size: 6 } }
        : chartStyle === "area"
        ? { width: 2, fill: "rgba(16, 185, 129, 0.25)" }
        : { width: 2 };
    const palette = ["#34d399", "#60a5fa", "#fbbf24", "#a78bfa", "#38bdf8", "#f472b6"];
    const dataArr = [builderResult.buckets, ...builderResult.series.map((s) => s.values)] as any;
    const container = chartRef.current;
    const legendContainer = legendRef.current;
    if (!container) return;
    const plugins: uPlot.Plugin[] = [];
    if (legendContainer) {
      plugins.push(
        createInlineLegendPlugin(legendContainer, {
          timeZone,
          formatValue: (v) => (v === null || v === undefined ? "–" : formatSi(Number(v))),
        })
      );
    }
    const opts: uPlot.Options = {
      width: container.clientWidth || 600,
      height: 340,
      pxAlign: 0,
      axes: [
        { stroke: axesStroke, grid: { show: true, stroke: gridStroke }, values: makeTimeAxisValues(timeZone) },
        { stroke: axesStroke, grid: { show: true, stroke: gridStroke }, values: (_u, vals) => vals.map((v) => formatSi(Number(v))), size: 60 },
      ],
      scales: { x: { time: true } },
      series: [
        {},
        ...builderResult.series.map((s, idx) => ({
          label: s.label || `series-${idx + 1}`,
          stroke: palette[idx % palette.length],
          ...baseSeries,
        })),
      ],
      legend: { show: false },
      plugins,
    };
    if (plotRef.current) {
      plotRef.current.destroy();
      plotRef.current = null;
    }
    plotRef.current = new uPlot(opts, dataArr, container);
  }, [builderResult, chartStyle, themeMode, timeZone]);

  const runBuilderQuery = async () => {
    setBuilderLoading(true);
    setBuilderError(null);
    let expressionSql: string | undefined;
    let expressionRefs: string[] = [];
    if (formula.trim()) {
      try {
        const ast = parseExpression(formula.trim());
        expressionSql = exprToSqlPivot(ast);
        expressionRefs = collectRefs(ast);
        setFormulaError(null);
      } catch (err: any) {
        setFormulaError(err?.message || "Invalid formula");
        setBuilderLoading(false);
        return;
      }
    }
    try {
      if (!builderQuery.selectors.length || !builderQuery.selectors.some((s) => s.metric)) {
        throw new Error("Add at least one metric selector");
      }
      const res = await executeSqlBuilderQuery(builderQuery, {
        sqlOverride: builderSqlText,
        expressionSql,
        expressionRefs: expressionRefs.length ? expressionRefs : undefined,
      });
      setBuilderResult(res);
      setBuilderMetricsUsed(res.metrics);
      setBuilderSqlText(res.sql);
    } catch (err: any) {
      setBuilderError(err?.message || "Failed to run query");
      setBuilderResult(null);
      setBuilderMetricsUsed([]);
    } finally {
      setBuilderLoading(false);
    }
  };

  const loadTagCatalog = async (metricName: string | undefined) => {
    if (!metricName) {
      setTagCatalog({ keys: [], values: {} });
      return;
    }
    try {
      const data = await fetchJSON<{ tags: Record<string, string[]> }>("/metrics/tag-values", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: metricName }),
      });
      const keys = Object.keys(data.tags || {}).sort();
      setTagCatalog({ keys, values: data.tags || {} });
    } catch {
      setTagCatalog({ keys: [], values: {} });
    }
  };

  const savePanel = () => {
    if (!addMode || !onAddPanel) return;
    if (!builderMetricsUsed.length) return;
    const newPanel: PanelDef = {
      id: 0,
      title: newPanelTitle.trim() || builderQuery.selectors.map((s) => s.metric || s.alias).filter(Boolean).join(", ") || "Query",
      range: builderQuery.range,
      bucket: builderQuery.bucket,
      metrics: builderMetricsUsed.map((m) => ({
        metric_id: m.metric_id,
        name: m.name,
        tags: m.tags || {},
        type: m.type,
        step: m.step,
        slots: m.slots,
      })),
      query: builderQuery,
      selectors: builderQuery.selectors,
      sql: builderSqlText.trim(),
    };
    setNewPanelTitle("");
    onAddPanel(newPanel);
  };

  return (
    <div className="explorer-grid">
      {addMode && (
      <Card style={{ gridColumn: "1 / -1" }} bodyStyle={{ padding: "8px 12px" }}>
            <Space align="center" wrap style={{ width: "100%", justifyContent: "space-between" }}>
              <Text strong>Adding to dashboard {dashboardTitle || ""}</Text>
              <Space>
                <Input
                  placeholder="Panel title"
                  value={newPanelTitle}
                  onChange={(e) => setNewPanelTitle(e.target.value)}
                  style={{ minWidth: 200 }}
                />
                <Text type="secondary">
                  {builderMetricsUsed.length ? "Save to pin this panel" : "Shape a query to enable save"}
                </Text>
                <Button
                  type="primary"
                  disabled={builderMetricsUsed.length === 0}
                  onClick={savePanel}
                >
                  Save panel
                </Button>
              </Space>
            </Space>
          </Card>
      )}
      <Card
        title={
          <Space align="center" style={{ width: "100%", justifyContent: "space-between" }}>
            <Text strong>Metrics query builder</Text>
            {builderSqlGenerated ? <Tag>SQL</Tag> : null}
          </Space>
        }
        style={{ minHeight: "68vh", width: "100%" }}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Card size="small" bodyStyle={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
            <Segmented
              options={[
                { label: "15m", value: 900 },
                { label: "1h", value: 3600 },
                { label: "6h", value: 21600 },
                { label: "24h", value: 86400 },
              ]}
              value={builderQuery.range}
              onChange={(val) => setBuilderQuery((prev) => ({ ...prev, range: Number(val) }))}
            />
            <Select
              value={builderQuery.bucket}
              style={{ minWidth: 130 }}
              onChange={(val) => setBuilderQuery((prev) => ({ ...prev, bucket: val }))}
              options={[1, 10, 30, 60, 300, 900].map((v) => ({ label: `${v}s bucket`, value: v }))}
            />
            <Select
              style={{ width: 140 }}
              value={chartStyle}
              onChange={(val) => setChartStyle(val as "line" | "area" | "points" | "table")}
              options={[
                { label: "Line", value: "line" },
                { label: "Area", value: "area" },
                { label: "Points", value: "points" },
                { label: "Table", value: "table" },
              ]}
            />
          </Card>

          {renderSelectorEditor(builderQuery.selectors, (next) => {
            setBuilderQuery((prev) => ({ ...prev, selectors: next }));
          })}

          <Card size="small" bodyStyle={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <Space align="center" wrap>
              <Text type="secondary">Expression (formula)</Text>
              <Tag color="geekblue">
                Aliases:{" "}
                {normalizedBuilderSelectors.length
                  ? normalizedBuilderSelectors
                      .map((s) => (s.metric ? `${s.alias} (${s.metric})` : s.alias))
                      .join(", ")
                  : "add selectors"}
              </Tag>
              <Checkbox
                checked={builderQuery.showBase !== false}
                onChange={(e) => setBuilderQuery((prev) => ({ ...prev, showBase: e.target.checked }))}
              >
                Include base series
              </Checkbox>
            </Space>
            <Input.TextArea
              rows={3}
              value={formula}
              onChange={(e) => {
                setFormula(e.target.value);
                setFormulaError(null);
              }}
              placeholder="Example: (A * 5 + B) / 2"
            />
            {formulaError && <Text type="danger">{formulaError}</Text>}
          </Card>

          <Space wrap style={{ width: "100%", justifyContent: "space-between" }}>
            <Space wrap>
              <Select
                value={builderQuery.aggregator}
                style={{ minWidth: 160 }}
                onChange={(val) =>
                  setBuilderQuery((prev) => ({ ...prev, aggregator: val as QueryBuilderConfig["aggregator"] }))
                }
                options={[
                  { label: "No aggregation", value: "none" },
                  { label: "sum()", value: "sum" },
                  { label: "avg()", value: "avg" },
                  { label: "min()", value: "min" },
                  { label: "max()", value: "max" },
                ]}
              />
              <Select
                mode="multiple"
                allowClear
                placeholder="group by"
                style={{ minWidth: 200 }}
                value={builderQuery.groupBy}
                options={tagCatalog.keys.map((k) => ({ label: k, value: k }))}
                onChange={(vals) => setBuilderQuery((prev) => ({ ...prev, groupBy: vals as string[] }))}
              />
            </Space>
            <Space wrap>
              <Button type="primary" onClick={runBuilderQuery} loading={builderLoading}>
                Run query
              </Button>
            </Space>
          </Space>

          <Card size="small" bodyStyle={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <Space align="center" wrap>
              <Text type="secondary">SQL</Text>
              <Tag color="blue">{formatPromDuration(builderQuery.range)} window</Tag>
              <Tag color="geekblue">{builderQuery.bucket}s bucket</Tag>
            </Space>
            <div style={{ border: "1px solid #0f172a33", borderRadius: 6 }}>
              <CodeMirror
                value={builderSqlText}
                height="320px"
                theme={themeMode === "dark" ? oneDark : undefined}
                extensions={[cmSql()]}
                onChange={(val) => setBuilderSqlText(val)}
                basicSetup={{ lineNumbers: true, highlightActiveLine: true, foldGutter: true }}
                style={{
                  fontFamily:
                    "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
                  fontSize: "medium",
                }}
              />
            </div>
            <Text type="secondary">
              Functions: ts_bucket, counter_rate (window), shift_ts(ts, offset), series_add/sub/mul/div
            </Text>
            <Space>
              <Button icon={<CopyOutlined />} disabled={!builderSqlText} onClick={() => navigator.clipboard.writeText(builderSqlText || "").catch(() => {})}>
                Copy
              </Button>
              <Button onClick={() => setBuilderSqlText(builderSqlGenerated)}>Reset to generated</Button>
            </Space>
          </Card>
          {builderError && (
            <Card size="small" bodyStyle={{ color: "#ef4444" }}>
              {builderError}
            </Card>
          )}
        </Space>
      </Card>
      <Space direction="vertical" style={{ width: "100%" }} size="large">
        <Card title="Query results">
          {chartStyle !== "table" ? (
            <div className="chart-frame">
              {builderResult && builderResult.buckets.length ? (
                <div>
                  <div ref={chartRef} style={{ height: 340, position: "relative" }} />
                  <div ref={legendRef} className="chart-legend-inline" />
                </div>
              ) : (
                <Empty description={builderError || "Run a query to see results"} />
              )}
            </div>
          ) : builderTableRows.length ? (
            <Table
              className="metric-table"
              size="small"
              dataSource={builderTableRows.slice(-120)}
              rowKey={(r, idx) => `${r.bucket}-${idx}`}
              columns={builderColumns}
              pagination={{ pageSize: 10 }}
              scroll={{ x: true }}
            />
          ) : (
            <Empty description={builderError || "Run a query to see results"} />
          )}
        </Card>
      </Space>
    </div>
  );
};

const App: React.FC = () => {
  const initialLoc = parseInitialLocation();
  const [view, setView] = useState<ViewKey>(initialLoc.view);
  const [dashboards, setDashboards] = useState<{ slug: string; title: string }[]>([]);
  const [panels, setPanels] = useState<PanelDef[]>([]);
  const [title, setTitle] = useState("");
  const [slug, setSlug] = useState(initialLoc.slug || "");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [themeMode, setThemeMode] = useState<"dark" | "light">("dark");
  const [timeZone, setTimeZone] = useState<string>(Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC");
  const [addingPanel, setAddingPanel] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();
  const { token } = antdTheme.useToken();
  const [editMode, setEditMode] = useState(false);
  const [initialized, setInitialized] = useState(false);
  const [siderCollapsed, setSiderCollapsed] = useState(true);
  const [dashRange, setDashRange] = useState<{ start: number; end: number } | null>(null);
  const [dashBucket, setDashBucket] = useState<number | null>(null);

  useEffect(() => {
    loadDashboards();
  }, []);

  useEffect(() => {
    document.body.dataset.theme = themeMode;
  }, [themeMode]);

  useEffect(() => {
    if (view !== "explorer" && addingPanel) {
      setAddingPanel(false);
    }
  }, [view, addingPanel]);

  useEffect(() => {
    if (initialized) return;
    if (initialLoc.slug) {
      loadDashboard(initialLoc.slug);
    }
    setInitialized(true);
  }, [initialized, initialLoc.slug]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (slug) {
      params.set("dash", slug);
    } else {
      params.delete("dash");
    }
    params.set("view", view);
    const next = `${window.location.pathname}?${params.toString()}${window.location.hash || ""}`;
    window.history.replaceState({}, "", next);
  }, [view, slug]);
  async function loadDashboards() {
    try {
      const data = await fetchJSON<{ dashboards: { slug: string; title: string }[] }>("/dashboards");
      setDashboards(data.dashboards || []);
    } catch (e: any) {
      messageApi.error(e?.message || "Failed to load dashboards");
    }
  }

  function newDashboard() {
    setPanels([]);
    setTitle("");
    setSlug("");
    setEditMode(false);
    setView("dashboards");
  }

  async function saveDashboard() {
    let targetSlug = slug;
    let targetTitle = title;
    if (!targetSlug) {
      const input = window.prompt("Dashboard name (letters/numbers)", title || "Untitled");
      if (!input) return;
      targetTitle = input.trim();
      targetSlug = targetTitle
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .slice(0, 64) || "dashboard";
      setTitle(targetTitle);
      setSlug(targetSlug);
    }

    const definition = { panels: panels.map((p) => ({ ...p })) };
    try {
      await fetchJSON("/dashboards", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ slug: targetSlug, title: targetTitle || "Untitled", definition }),
      });
      messageApi.success(`Saved ${targetSlug}`);
      loadDashboards();
    } catch (e: any) {
      messageApi.error(e?.message || "Failed to save");
    }
  }

  async function loadDashboard(slugVal: string) {
    if (!slugVal) return;
    try {
      const data = await fetchJSON<{ title: string; definition: { panels: PanelDef[] } }>(`/dashboards/${slugVal}`);
      setTitle(data.title || slugVal);
      setSlug(slugVal);
      const def = data.definition || {};
      const mapped = attachLayouts((def.panels || []).map((p, idx) => ({ ...p, id: p.id || idx + 1 })));
      setPanels(mapped);
      setEditMode(false);
      setView("dashboards");
    } catch (e: any) {
      messageApi.error(e?.message || "Failed to load dashboard");
    }
  }

  const nextId = (list: PanelDef[] = panels) => Math.max(0, ...list.map((p) => p.id)) + 1;

  const handleAddPanel = (panel: PanelDef) => {
    setPanels((prev) => {
      const newPanel = { ...panel, id: nextId(prev), layout: panel.layout || defaultLayoutAt(prev.length) };
      return [...prev, newPanel];
    });
    setAddingPanel(false);
    setView("dashboards");
    messageApi.success("Panel added to dashboard");
  };

  const handleDeletePanel = (panelId: number) => {
    const confirmed = window.confirm("Remove this panel from the dashboard?");
    if (!confirmed) return;
    setPanels((prev) => prev.filter((p) => p.id !== panelId));
    messageApi.success("Panel removed");
  };

  const onGridLayoutChange = (nextLayout: GridLayoutItem[]) => {
    if (!editMode) return;
    setPanels((prev) =>
      prev.map((p) => {
        const match = nextLayout.find((l) => l.i === String(p.id));
        if (!match) return p;
        const updatedLayout: PanelLayout = { x: match.x, y: match.y, w: match.w, h: match.h };
        if (
          p.layout &&
          p.layout.x === updatedLayout.x &&
          p.layout.y === updatedLayout.y &&
          p.layout.w === updatedLayout.w &&
          p.layout.h === updatedLayout.h
        ) {
          return p;
        }
        return { ...p, layout: updatedLayout };
      })
    );
  };

  const toggleEditMode = () => {
    setEditMode((prev) => !prev);
  };

  const responsiveLayouts = useMemo(() => buildLayouts(panels), [panels]);

  const beginAddPanel = () => {
    setView("explorer");
    setAddingPanel(true);
    messageApi.info("Use Query builder to shape a panel, then save to add.");
  };

  const presetRanges: { label: string; seconds: number }[] = [
    { label: "15m", seconds: 15 * 60 },
    { label: "1h", seconds: 60 * 60 },
    { label: "6h", seconds: 6 * 60 * 60 },
    { label: "24h", seconds: 24 * 60 * 60 },
  ];

  const handleRangePreset = (seconds: number) => {
    const end = Math.floor(Date.now() / 1000);
    const start = end - seconds;
    setDashRange({ start, end });
    setDashBucket(null);
  };

  const handleDateRange: RangePickerProps["onChange"] = (values) => {
    if (!values || values.length !== 2 || !values[0] || !values[1]) {
      setDashRange(null);
      return;
    }
    const [start, end] = values;
    setDashRange({ start: start.unix(), end: end.unix() });
  };

  const handleBucketChange = (val: number) => {
    setDashBucket(val);
  };

  const handleNav = (target: ViewKey) => {
    setView(target);
    setAddingPanel(false);
    if (target !== "dashboards") {
      setEditMode(false);
    }
  };

  const themeConfig = {
    algorithm: themeMode === "dark" ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
  };

  const ThemeSync: React.FC = () => {
    const { token } = antdTheme.useToken();
    useEffect(() => {
      document.body.style.backgroundColor = token.colorBgBase;
      document.body.style.color = token.colorText;
    }, [token.colorBgBase, token.colorText]);
    return null;
  };

  const HeaderBar: React.FC = () => {
    const { token } = antdTheme.useToken();
    return (
      <Header
        className="app-header"
        style={{
          borderBottom: `1px solid ${token.colorBorder}`,
          background: token.colorBgContainer,
          color: token.colorText,
          padding: "4px 16px 6px",
          height: "auto",
          lineHeight: 1.2,
        }}
      >
        <Space direction="horizontal" align="center" wrap size="middle" style={{ width: "100%", justifyContent: "space-between" }}>
          <Space align="center" wrap size="middle">
            <DashboardOutlined style={{ color: token.colorPrimary }} />
            <Title level={4} style={{ margin: 0, color: token.colorTextHeading }}>
              Biscuit
            </Title>
            <Text type="secondary">{view === "dashboards" ? "Dashboards" : view === "explorer" ? "Metrics" : view === "alerts" ? "Alerts" : "Settings"}</Text>
          </Space>
          <Space align="center" wrap size="small">
            <Select
              style={{ minWidth: 180 }}
              value={timeZone}
              onChange={(val) => setTimeZone(val)}
              options={[
                { label: "Local", value: Intl.DateTimeFormat().resolvedOptions().timeZone },
                { label: "UTC", value: "UTC" },
                { label: "America/Los_Angeles", value: "America/Los_Angeles" },
                { label: "Europe/London", value: "Europe/London" },
                { label: "Asia/Kolkata", value: "Asia/Kolkata" },
              ]}
            />
            <Button
              icon={<BulbOutlined />}
              onClick={() => setThemeMode(themeMode === "dark" ? "light" : "dark")}
            >
              {themeMode === "dark" ? "Light" : "Dark"}
            </Button>
          </Space>
        </Space>
      </Header>
    );
  };

  return (
    <ConfigProvider theme={themeConfig} componentSize="middle">
      {contextHolder}
      <ThemeSync />
      <Layout className="app-shell">
        <HeaderBar />
        <Layout>
          <Sider
            collapsible
            collapsed={siderCollapsed}
            onCollapse={(collapsed) => setSiderCollapsed(collapsed)}
            width={200}
            collapsedWidth={72}
            className="side-nav"
          >
            <div className="side-nav-logo">{siderCollapsed ? "" : "Biscuit"}</div>
            <Menu
              mode="inline"
              selectedKeys={[view]}
              onClick={({ key }) => handleNav(key as ViewKey)}
              items={[
                { key: "dashboards", icon: <DashboardOutlined />, label: "Dashboards" },
                { key: "explorer", icon: <LineChartOutlined />, label: "Metrics" },
                { type: "divider" },
                { key: "alerts", icon: <BellOutlined />, label: "Alerts" },
                { key: "settings", icon: <SettingOutlined />, label: "Settings" },
              ]}
            />
          </Sider>
          <Content style={{ padding: "16px 16px 24px" }}>
            {view === "dashboards" ? (
              <div>
                <Space direction="vertical" style={{ width: "100%" }} size="small">
                  <Space className="view-toolbar" align="center" wrap size="small">
                    <Select
                      placeholder="Load dashboard"
                      value={slug || undefined}
                      onChange={(val) => loadDashboard(val)}
                      allowClear
                      options={dashboards.map((d) => ({ label: `${d.title} (${d.slug})`, value: d.slug }))}
                      style={{ minWidth: 200, maxWidth: 260 }}
                    />
                    <Button icon={<PlusOutlined />} onClick={beginAddPanel}>
                      Add panel
                    </Button>
                    <Button
                      icon={editMode ? <EyeOutlined /> : <EditOutlined />}
                      type={editMode ? "primary" : "default"}
                      onClick={toggleEditMode}
                    >
                      {editMode ? "Done editing" : "Edit layout"}
                    </Button>
                    <Button icon={<ReloadOutlined />} onClick={loadDashboards}>
                      Reload
                    </Button>
                    <Button onClick={newDashboard}>New</Button>
                    <Button icon={<SaveOutlined />} type="primary" onClick={saveDashboard}>
                      Save
                    </Button>
                  </Space>
                  <Space className="view-toolbar" align="center" wrap size="small" style={{ marginTop: 4, marginBottom: 8 }}>
                    <Segmented
                      size="small"
                      options={presetRanges.map((p) => ({ label: p.label, value: p.seconds }))}
                      onChange={(val) => handleRangePreset(Number(val))}
                    />
                    <DatePicker.RangePicker
                      showTime
                      allowClear
                      onChange={handleDateRange}
                      value={
                        dashRange
                          ? ([dayjs.unix(dashRange.start), dayjs.unix(dashRange.end)] as [Dayjs, Dayjs])
                          : null
                      }
                    />
                    <Select
                      allowClear
                      placeholder="Bucket"
                      value={dashBucket || undefined}
                      onChange={handleBucketChange}
                      style={{ width: 140 }}
                      options={[1, 10, 30, 60, 300, 900].map((v) => ({ label: `${v}s bucket`, value: v }))}
                    />
                  </Space>
                </Space>
                {panels.length ? (
                  <ResponsiveGridLayout
                    className="panel-grid"
                    rowHeight={GRID_ROW_HEIGHT}
                    breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 }}
                    cols={{ lg: GRID_COLS, md: GRID_COLS, sm: 8, xs: 6, xxs: 1 }}
                    layouts={responsiveLayouts}
                    margin={[12, 16]}
                    containerPadding={[0, 0]}
                    onLayoutChange={onGridLayoutChange}
                    draggableHandle=".panel-drag-handle"
                    isDraggable={editMode}
                    isResizable={editMode}
                    compactType={editMode ? "vertical" : undefined}
                  >
                    {panels.map((panel, idx) => {
                      const layout = panel.layout || defaultLayoutAt(idx);
                    const chartHeight = Math.max(250, layout.h * GRID_ROW_HEIGHT - 120);
                    const effectiveBucket = dashBucket || panel.bucket || 60;
                    const effectiveRange = dashRange ? dashRange.end - dashRange.start : panel.range;
                    return (
                      <div key={String(panel.id)}>
                        <Card
                          className={`panel-card ${editMode ? "is-editing" : "is-view"}`}
                          title={
                            <Space wrap align="center">
                              <span className="panel-drag-handle" title="Drag to move">
                                <HolderOutlined />
                              </span>
                              {panel.title || "Panel"}
                            </Space>
                          }
                          extra={
                            <Space size="small" wrap align="center">
                              <Tag>{effectiveBucket}s bucket</Tag>
                              <Dropdown
                                menu={{
                                  items: [
                                    {
                                      key: "view-sql",
                                      label: (
                                        <span
                                          onClick={() => {
                                            const text = panel.sql || buildDefaultSql(panel.metrics?.[0]?.metric_id || 0, effectiveBucket);
                                            navigator.clipboard.writeText(text).catch(() => {});
                                            messageApi.info("Panel query copied");
                                          }}
                                        >
                                          Copy query
                                        </span>
                                      ),
                                    },
                                    {
                                      key: "delete",
                                      label: (
                                        <span
                                          style={{ color: token.colorError }}
                                          onClick={() => handleDeletePanel(panel.id)}
                                        >
                                          Delete panel
                                        </span>
                                      ),
                                    },
                                  ],
                                }}
                                trigger={["click"]}
                              >
                                <Button type="text" size="small" icon={<MoreOutlined />} />
                              </Dropdown>
                            </Space>
                          }
                          bodyStyle={{ height: "100%", display: "flex", flexDirection: "column", gap: 8 }}
                        >
                          <Space className="panel-meta-inline" size="small" wrap align="center">
                            <Tag>{effectiveBucket}s bucket</Tag>
                          </Space>
                          <PanelChart
                            panel={{
                              ...panel,
                              range: effectiveRange,
                              bucket: effectiveBucket,
                            }}
                            themeMode={themeMode}
                            height={chartHeight}
                            startTs={dashRange?.start}
                            endTs={dashRange?.end}
                            bucketOverride={dashBucket || undefined}
                            timeZone={timeZone}
                          />
                        </Card>
                      </div>
                    );
                  })}
                  </ResponsiveGridLayout>
                ) : (
                  <div className="panel-grid">
                    <Empty
                      description={
                        <Space direction="vertical">
                          <Text>Add your first panel to this dashboard</Text>
                          <Button type="primary" icon={<PlusOutlined />} onClick={beginAddPanel}>
                            Add panel
                          </Button>
                        </Space>
                      }
                      image={Empty.PRESENTED_IMAGE_SIMPLE}
                    />
                  </div>
                )}
              </div>
            ) : view === "explorer" ? (
              <Explorer
                themeMode={themeMode}
                timeZone={timeZone}
                addMode={addingPanel}
                dashboardTitle={title || slug}
                onAddPanel={handleAddPanel}
              />
            ) : view === "alerts" ? (
              <Card>
                <Empty
                  description={
                    <Space direction="vertical" align="center">
                      <Text strong>Alerts</Text>
                      <Text type="secondary">Hook up alerting rules here.</Text>
                    </Space>
                  }
                />
              </Card>
            ) : (
              <Card>
                <Empty
                  description={
                    <Space direction="vertical" align="center">
                      <Text strong>Settings</Text>
                      <Text type="secondary">Configure dashboard defaults and preferences.</Text>
                    </Space>
                  }
                />
              </Card>
            )}
          </Content>
        </Layout>
      </Layout>
      <PanelForm
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        onAdd={(panel) =>
          setPanels((prev) => [...prev, { id: nextId(prev), ...panel, layout: panel.layout || defaultLayoutAt(prev.length) }])
        }
      />
    </ConfigProvider>
  );
};

const root = createRoot(document.getElementById("root")!);
root.render(<App />);
