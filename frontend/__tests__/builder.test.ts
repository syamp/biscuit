import assert from "assert";
import {
  buildSqlFromConfig,
  matchSelectorsToMetrics,
  normalizeSelectors,
  QueryBuilderConfig,
  SelectorConfig,
  MetricMeta,
  sqlSafeAlias,
} from "../query_helpers";

const baseConfig: QueryBuilderConfig = {
  selectors: [{ metric: "cpu_percent", alias: "A", tags: [{ op: "=" }], transform: { op: "none" } }],
  range: 3600,
  bucket: 60,
  rangeSelector: 300,
  rateFunction: "none",
  aggregator: "none",
  groupBy: [],
  expression: undefined,
  showBase: true,
};

const metricFixtures: MetricMeta[] = [
  { metric_id: 42, name: "cpu_percent", tags: { host: "demo" }, step: 60, slots: 1000, type: 0 },
];

function buildSelectors(): SelectorConfig[] {
  return normalizeSelectors([{ metric: "cpu_percent", alias: "A", tags: [{ op: "=", key: "host", value: "demo" }] }]);
}

(() => {
  const selectors = buildSelectors();
  const matchMap = matchSelectorsToMetrics(selectors, metricFixtures);
  const sql = buildSqlFromConfig(
    baseConfig,
    [42],
    selectors,
    "<start_ts>",
    "<end_ts>",
    matchMap,
    true
  );
  assert(sql.includes("selector_map"), "selector_map CTE should exist");
  assert(sql.includes("{{A}}"), "alias placeholder should be present");
  assert(!sql.includes("metrics m"), "should not reference metrics table");
})();

(() => {
  const selectors = buildSelectors();
  const matchMap = matchSelectorsToMetrics(selectors, metricFixtures);
  const sql = buildSqlFromConfig(baseConfig, [42], selectors, 100, 200, matchMap, false);
  assert(sql.includes("42"), "resolved metric id should be substituted");
  assert(sql.includes("bucket"), "bucket column should be present");
  assert(sql.includes("alias = 'A'"), "alias filter should be retained");
})();

(() => {
  const selectors = normalizeSelectors([
    { metric: "cpu_percent", alias: "A", tags: [{ op: "=", key: "host", value: "demo" }], transform: { op: "rate" } },
  ]);
  const matchMap = matchSelectorsToMetrics(selectors, metricFixtures);
  const sql = buildSqlFromConfig(baseConfig, [42], selectors, 100, 200, matchMap, false);
  assert(sql.includes("base_with_rate"), "rate path should wrap base_agg with counter_rate");
  assert(sql.includes("counter_rate(value, bucket) OVER (PARTITION BY alias ORDER BY bucket)"), "windowed counter_rate should be used");
  assert(sql.includes("alias = 'A'"), "alias filter should survive rate wrapping");
})();

(() => {
  // normalization should not overwrite blank aliases; aliases are A/B/C internally but metrics use names for labels
  const selectors: SelectorConfig[] = normalizeSelectors([
    { metric: "cpu_percent", alias: "", tags: [{ op: "=" }] },
    { metric: "mem_used", alias: "", tags: [{ op: "=" }] },
  ]);
  assert.equal(selectors[0].alias, "A", "first selector gets default alias A");
  assert.equal(selectors[1].alias, "B", "second selector gets default alias B");
})();

(() => {
  // sqlSafeAlias should strip unsafe characters and prefix if starting with number
  assert.equal(sqlSafeAlias("1bad"), "a_1bad");
  assert.equal(sqlSafeAlias("cpu.percent"), "cpu_percent");
  assert.equal(sqlSafeAlias("good_alias"), "good_alias");
})();

console.log("builder tests passed");
