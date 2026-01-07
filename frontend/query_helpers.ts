export interface SelectorConfig {
  metric: string;
  tags: { op: "=" | "!=" | "=~" | "!~"; key?: string; value?: string }[];
  alias: string;
  transform?: {
    op: "none" | "rate";
  };
}

export interface QueryBuilderConfig {
  selectors: SelectorConfig[];
  range: number;
  bucket: number;
  rangeSelector: number;
  rateFunction: "none" | "rate" | "increase" | "irate";
  aggregator: "none" | "sum" | "avg" | "min" | "max";
  groupBy: string[];
  expression?: string;
  showBase?: boolean;
}

export interface MetricMeta {
  metric_id: number;
  name?: string;
  tags?: Record<string, string>;
  step: number;
  slots: number;
  type: number;
}

export type SelectorMatchMap = Record<string, number[]>;

export function sqlSafeAlias(alias: string, fallback: string = "a"): string {
  const base = alias && alias.trim().length ? alias.trim() : fallback;
  let safe = base.replace(/[^A-Za-z0-9_]/g, "_");
  if (!/^[A-Za-z]/.test(safe)) {
    safe = `a_${safe}`;
  }
  if (!safe.length) {
    safe = fallback || "a";
  }
  return safe;
}

export function normalizeSelectors(selectors: SelectorConfig[]): SelectorConfig[] {
  return (selectors || []).map((sel, idx) => ({
    ...sel,
    metric: sel.metric || "",
    alias: sel.alias && sel.alias.trim().length ? sqlSafeAlias(sel.alias) : String.fromCharCode(65 + (idx % 26)),
    tags: sel.tags && sel.tags.length ? sel.tags : [{ op: "=" }],
    transform: sel.transform || { op: "none" },
  }));
}

export function selectorTagsToDict(sel: SelectorConfig): Record<string, string> {
  return (sel.tags || [])
    .filter((t) => t.op === "=" && t.key && t.value !== undefined && t.value !== "")
    .reduce<Record<string, string>>((acc, t) => {
      acc[t.key!] = String(t.value);
      return acc;
    }, {});
}

export function matchSelectorsToMetrics(selectors: SelectorConfig[], metrics: MetricMeta[]): SelectorMatchMap {
  const normalized = normalizeSelectors(selectors).filter((s) => s.metric);
  const map: SelectorMatchMap = {};
  normalized.forEach((sel, idx) => {
    const alias = sel.alias || String.fromCharCode(65 + idx);
    const filterTags = selectorTagsToDict(sel);
    const ids = metrics
      .filter((m) => m.name === sel.metric)
      .filter((m) => Object.entries(filterTags).every(([k, v]) => (m.tags || {})[k] === v))
      .map((m) => m.metric_id);
    if (ids.length) {
      map[alias] = ids;
    }
  });
  return map;
}

export function buildSqlFromConfig(
  config: QueryBuilderConfig,
  metricIds: number[],
  selectors: SelectorConfig[] | undefined,
  startTs: number | string,
  endTs: number | string,
  selectorMatchMap?: SelectorMatchMap,
  usePlaceholders: boolean = false,
  expressionSql?: string,
  expressionRefs?: string[]
): string {
  const bucket = Math.max(1, config.bucket || 60);
  const agg = config.aggregator && config.aggregator !== "none" ? config.aggregator : "avg";
  const normalized = normalizeSelectors(selectors && selectors.length ? selectors : []);
  const usableSelectors = normalized.filter((s) => s.metric);
  if (!usableSelectors.length) {
    throw new Error("At least one selector is required to build SQL");
  }

  const selectorRows: string[] = [];
  const rateAliases: string[] = [];
  if (usePlaceholders) {
    usableSelectors.forEach((sel, idx) => {
      const alias = sel.alias || String.fromCharCode(65 + idx);
      selectorRows.push(`SELECT '${alias}' AS alias, {{${alias}}} AS metric_id`);
    });
  } else if (selectorMatchMap && Object.keys(selectorMatchMap).length) {
    Object.entries(selectorMatchMap).forEach(([alias, ids]) => {
      ids.forEach((id) => selectorRows.push(`SELECT '${alias}' AS alias, ${id} AS metric_id`));
    });
  }
  if (!selectorRows.length) {
    throw new Error("Selectors did not resolve to any metrics");
  }

  const selectorCtes = [`selector_map AS (\n  ${selectorRows.join("\n  UNION ALL\n  ")}\n)`];

  const valueExpr = (sel: SelectorConfig) => {
    const base = `${agg}(s.value)`;
    const t = sel.transform || { op: "none" };
    if (t.op === "rate") {
      rateAliases.push(sel.alias || "A");
      return base;
    }
    return base;
  };

  const baseUnion = usableSelectors.map((sel) => {
    const alias = sel.alias || "A";
    return `SELECT ts_bucket(s.ts, ${bucket}) AS bucket, '${alias}' AS alias, ${valueExpr(sel)} AS value
  FROM samples s
  JOIN selector_map sel ON s.metric_id = sel.metric_id
  WHERE s.ts >= ${startTs} AND s.ts <= ${endTs} AND sel.alias = '${alias}'
  GROUP BY bucket`;
  });

  const baseCte = `base_agg AS (
  ${baseUnion.join("\n  UNION ALL\n  ")}
)`;
  const ctes = [...selectorCtes, baseCte];
  let sourceCte = "base_agg";
  if (rateAliases.length) {
    const uniqRates = Array.from(new Set(rateAliases)).map((a) => `'${a}'`).join(", ");
    ctes.push(`base_with_rate AS (
  SELECT bucket, alias,
    CASE
      WHEN alias IN (${uniqRates}) THEN counter_rate(value, bucket) OVER (PARTITION BY alias ORDER BY bucket)
      ELSE value
    END AS value
  FROM base_agg
)`);
    sourceCte = "base_with_rate";
  }

  const aliases =
    expressionRefs && expressionRefs.length
      ? expressionRefs
      : Array.from(new Set(Object.keys(selectorMatchMap || {})));
  const hasExpr = Boolean(expressionSql) && aliases.length > 0;

  if (hasExpr && expressionSql) {
    const baseAlias = aliases[0];
    const aliasMap: Record<string, string> = {};
    aliases.forEach((name, idx) => {
      aliasMap[name] = `r${idx}`;
    });
    const renderedExpr = aliases.reduce((sql, name) => {
      const aliasRef = `${aliasMap[name]}.value`;
      return sql.replace(new RegExp(`\\b${name}\\b`, "g"), aliasRef);
    }, expressionSql);

    const joins = aliases
      .map((name, idx) => {
        const refAlias = aliasMap[name];
        if (idx === 0) {
          return `${sourceCte} ${refAlias}`;
        }
        const baseRef = aliasMap[baseAlias];
        return `JOIN ${sourceCte} ${refAlias} ON ${refAlias}.bucket = ${baseRef}.bucket AND ${refAlias}.alias = '${name}'`;
      })
      .join("\n  ");

    const whereFirst = `WHERE ${aliasMap[baseAlias]}.alias = '${baseAlias}'`;

    ctes.push(`expr AS (
  SELECT ${aliasMap[baseAlias]}.bucket AS bucket, 'expr' AS alias, ${renderedExpr} AS value
  FROM ${joins}
  ${whereFirst}
)`);
  }

  const baseSelect = `SELECT bucket, alias, value FROM ${sourceCte}`;

  const mainSelect = hasExpr
    ? config.showBase === false
      ? `SELECT bucket, alias, value FROM expr`
      : `${baseSelect}\nUNION ALL\nSELECT bucket, alias, value FROM expr`
    : baseSelect;

  const withParts = [...ctes];

  return `
WITH
${withParts.join(",\n")}
${mainSelect}
ORDER BY bucket, alias
  `.trim();
}
