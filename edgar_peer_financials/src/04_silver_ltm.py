# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver — LTM metrics per company per reported quarter
# MAGIC
# MAGIC 1. **Resolve** a value per `(cik, metric, candidate, period_end, months)` from
# MAGIC    `silver_financial_facts` for every candidate in `config/concept_map.json`
# MAGIC    (per-ticker overrides, then defaults; `A+B` entries are summed). Within a
# MAGIC    concept the latest-filed figure wins (restatements), instance before companyfacts.
# MAGIC    The LTM is then built **per candidate** and the highest-priority candidate
# MAGIC    with a complete LTM is used — one number never mixes two tags.
# MAGIC 2. **Spine** — one row per original 10-K/10-Q `report_date` ("as of"), with the
# MAGIC    fiscal quarter derived from the company's fiscal-year-end month
# MAGIC    (companyfacts' `fy`/`fp` are unreliable).
# MAGIC 3. **LTM flows** — filers report cash flows (and often P&L) year-to-date, and
# MAGIC    there is no discrete Q4 filing, so
# MAGIC    `LTM = YTD(as_of) + FY(prior) − YTD(as_of − 1y)`; at fiscal Q4, `LTM = FY`.
# MAGIC 4. **Instants** — balance-sheet value at `as_of` and one year earlier (for averages).
# MAGIC 5. **Adj. EBITDA LTM** — sum of the last four quarterly values from
# MAGIC    `silver_adj_ebitda` (Q4 derived from the full-year figure when only that is
# MAGIC    printed), or the full-year figure at fiscal Q4.
# MAGIC
# MAGIC Writes `silver_ltm_metrics` (wide) and `silver_ltm_lineage` (which concept /
# MAGIC filing fed every number — the first stop when reconciliation shows a miss).

# COMMAND ----------

# DBTITLE 1,Setup & concept map
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "engineering_playground_dev")
dbutils.widgets.text("silver_schema", "silver_edgar")
dbutils.widgets.text("concept_map", "../config/concept_map.json")
dbutils.widgets.text("date_tolerance_days", "10")
CATALOG = dbutils.widgets.get("catalog")
S = lambda n: f"{CATALOG}.{dbutils.widgets.get('silver_schema')}.{n}"
TOL = int(dbutils.widgets.get("date_tolerance_days"))

with open(dbutils.widgets.get("concept_map")) as fh:
    CMAP = json.load(fh)
METRICS = CMAP["metrics"]
FLOW = [m for m, d in METRICS.items() if d["kind"] == "flow"]
INSTANT = [m for m, d in METRICS.items() if d["kind"] == "instant"]

companies = spark.table(S("silver_companies"))
ticker_cik = {r["ticker"]: r["cik"] for r in companies.select("ticker", "cik").collect()}


def _split(c):
    """'us-gaap:X[axis=member]' -> ('us-gaap:X', 'axis=member'); no brackets -> dims ''."""
    if c.endswith("]") and "[" in c:
        concept, dims = c[:-1].split("[", 1)
        return concept, ";".join(sorted(dims.split(";")))
    return c, ""


# One row per term of each candidate. A candidate is one map entry; 'A+B' sums
# its terms (e.g. CAT's debt split across segment dimensions). An override of
# [] switches the metric off for that ticker (e.g. GEV's finance leases are
# already inside its debt tag).
cand_rows = []
for ticker, cik in ticker_cik.items():
    overrides = CMAP.get("overrides", {}).get(ticker, {})
    for metric, d in METRICS.items():
        if metric in overrides and not overrides[metric]:
            continue
        ordered = [(p, c) for p, c in enumerate(overrides.get(metric, []))]
        ordered += [(100 + p, c) for p, c in enumerate(d["concepts"])]
        for prio, entry in ordered:
            terms = entry.split("+")
            for t in terms:
                concept, dims = _split(t.strip())
                cand_rows.append((cik, metric, d["kind"], d["unit"], prio, entry, len(terms), concept, dims))
candidates = spark.createDataFrame(
    cand_rows, ["cik", "metric", "kind", "unit_kind", "priority", "candidate", "n_terms", "concept", "dims"])

# COMMAND ----------

# DBTITLE 1,1 · Resolve one value per (cik, metric, candidate, period_end, months)
# Every candidate is kept (not just the top one): step 3 builds the LTM per
# candidate and picks the highest-priority candidate whose LTM is complete, so
# one LTM never mixes two different tags across its YTD / FY / prior-YTD parts.
facts = spark.table(S("silver_financial_facts"))
unit_ok = (
    ((F.col("unit_kind") == "USD") & (F.col("unit") == "USD"))
    | ((F.col("unit_kind") == "USD/shares") & F.col("unit").startswith("USD/"))
    | ((F.col("unit_kind") == "shares") & ~F.col("unit").startswith("USD"))
)
shape_ok = (
    ((F.col("kind") == "flow") & F.col("months").isin(3, 6, 9, 12))
    | ((F.col("kind") == "instant") & F.col("period_start").isNull())
)
per_term = (
    facts.join(candidates, ["cik", "concept", "dims"])
    .filter(unit_ok & shape_ok)
    .withColumn("months", F.coalesce("months", F.lit(0)))
    # latest-filed wins (restatements), the filing's own instance before companyfacts
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("cik", "metric", "priority", "concept", "dims", "period_end", "months").orderBy(
            F.col("filed").desc(), F.when(F.col("source") == "instance", 0).otherwise(1))))
    .filter("rn = 1")
)
resolved = (
    per_term.groupBy("cik", "metric", "kind", "priority", "candidate", "n_terms", "period_end", "months")
    .agg(F.sum("value").alias("value"), F.count("*").alias("n_found"),
         F.max("accession").alias("accession"), F.max("filed").alias("filed"),
         F.concat_ws(",", F.collect_set("source")).alias("source"))
    .filter(F.col("n_found") == F.col("n_terms"))          # a sum needs every term
    .drop("n_found", "n_terms")
)
resolved.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(S("silver_resolved_facts"))
resolved = spark.table(S("silver_resolved_facts"))
print("resolved facts:", resolved.count())

# COMMAND ----------

# DBTITLE 1,2 · Spine — one row per reported quarter
spine = (
    spark.table(S("silver_filings"))
    .filter(F.col("form").isin("10-K", "10-Q") & F.col("report_date").isNotNull())
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("cik", "report_date").orderBy("filing_date")))   # original, not a re-file
    .filter("rn = 1")
    .join(companies.select("cik", "ticker", "bloomberg_ticker", "name", "fye_month"), "cik")
    .select("cik", "ticker", "bloomberg_ticker", "name",
            F.col("report_date").alias("as_of_date"), "form", "accession", "filing_date", "fye_month")
    .withColumn("fiscal_quarter",
                (F.pmod(F.month("as_of_date") - F.col("fye_month") - 1, F.lit(12)) / 3).cast("int") + 1)
    .withColumn("fiscal_year",
                F.year("as_of_date") + F.when(F.month("as_of_date") > F.col("fye_month"), 1).otherwise(0))
    .withColumn("ytd_months", F.col("fiscal_quarter") * 3)
    .withColumn("fiscal_period", F.format_string("%d Q%d", "fiscal_year", "fiscal_quarter"))
    .drop("fye_month")
)
spine.createOrReplaceTempView("spine")
resolved.createOrReplaceTempView("resolved")

# COMMAND ----------

# DBTITLE 1,3 · LTM flows + 4 · instants (long, with lineage)
spark.createDataFrame([(m,) for m in FLOW], ["metric"]).createOrReplaceTempView("flow_metrics")
spark.createDataFrame([(m,) for m in INSTANT], ["metric"]).createOrReplaceTempView("instant_metrics")

# part -> (metric set, months to match, target period_end, extra condition)
#   cur = YTD at as_of | fy = prior full year | py = YTD a year earlier |
#   q3 = discrete quarter at as_of | bs = instant at as_of | bs_py = instant a year earlier
PARTS = {
    "cur":   ("flow_metrics",    "s.ytd_months", "s.as_of_date",                             "TRUE"),
    "fy":    ("flow_metrics",    "12",           "add_months(s.as_of_date, -s.ytd_months)",  "s.fiscal_quarter < 4"),
    "py":    ("flow_metrics",    "s.ytd_months", "add_months(s.as_of_date, -12)",            "s.fiscal_quarter < 4"),
    "q3":    ("flow_metrics",    "3",            "s.as_of_date",                             "TRUE"),
    "bs":    ("instant_metrics", "0",            "s.as_of_date",                             "TRUE"),
    "bs_py": ("instant_metrics", "0",            "add_months(s.as_of_date, -12)",            "TRUE"),
}
cand_sql = "\nUNION ALL\n".join(f"""
  SELECT s.cik, s.as_of_date, m.metric, r.priority, r.candidate, '{part}' AS part, r.value, r.source,
         r.accession, r.period_end, r.months, abs(datediff(r.period_end, {target})) AS dd
  FROM spine s CROSS JOIN {metric_view} m
  JOIN resolved r ON r.cik = s.cik AND r.metric = m.metric AND r.months = {months}
   AND abs(datediff(r.period_end, {target})) <= {TOL}
  WHERE {cond}""" for part, (metric_view, months, target, cond) in PARTS.items())

components = spark.sql(f"""
SELECT cik, as_of_date, metric, priority, candidate, part, value, source, accession, period_end, months
FROM (SELECT *, row_number() OVER (PARTITION BY cik, as_of_date, metric, priority, part ORDER BY dd) AS rn
      FROM ({cand_sql}))
WHERE rn = 1
""")

# Last resort when no single candidate is complete (e.g. CMI moved revenue
# from Revenues to RevenueFromContract… mid-year): the best candidate per part,
# as candidate 'mixed' at priority 999.
mixed = (
    components.withColumn("rn", F.row_number().over(
        Window.partitionBy("cik", "as_of_date", "metric", "part").orderBy("priority")))
    .filter("rn = 1").drop("rn")
    .withColumn("priority", F.lit(999)).withColumn("candidate", F.lit("mixed"))
)
components = components.unionByName(mixed)

ltm_value = (
    F.when(F.col("fiscal_quarter") == 4, F.col("cur"))
     .otherwise(F.col("cur") + F.col("fy") - F.col("py"))
)
SHARE_METRICS = [m for m, d in METRICS.items() if d["unit"] == "shares"]
per_candidate = (
    components.groupBy("cik", "as_of_date", "metric", "priority", "candidate")
    .pivot("part", ["cur", "fy", "py", "q3", "bs", "bs_py"]).agg(F.first("value"))
    .join(spine.select("cik", "as_of_date", "fiscal_quarter", "ytd_months"), ["cik", "as_of_date"])
    .select(
        "cik", "as_of_date", "metric", "priority", "candidate",
        # share count: weighted average over the LTM window (share-months / 12),
        # the latest quarter's count if the YTD parts are missing
        F.when(F.col("metric").isin(SHARE_METRICS),
               F.coalesce(
                   F.when(F.col("fiscal_quarter") == 4, F.col("cur"))
                    .otherwise((F.col("cur") * F.col("ytd_months") + F.col("fy") * 12
                                - F.col("py") * F.col("ytd_months")) / 12),
                   F.col("q3")))
         .when(F.col("metric").isin(FLOW), ltm_value)
         .otherwise(F.col("bs")).alias("value"),
        F.when(F.col("metric").isin(INSTANT), F.col("bs_py")).alias("value_py"),
    )
)


def _first_complete(col):
    """Per (cik, as_of, metric): the highest-priority candidate with a non-null `col`."""
    w = Window.partitionBy("cik", "as_of_date", "metric").orderBy("priority")
    return (per_candidate.filter(F.col(col).isNotNull())
            .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
            .select("cik", "as_of_date", "metric", "priority", "candidate", col))


chosen = _first_complete("value")
chosen_py = _first_complete("value_py").select("cik", "as_of_date", "metric", "value_py")
values = chosen.join(chosen_py, ["cik", "as_of_date", "metric"], "full")

# lineage: the parts behind every chosen value
(components.join(chosen.select("cik", "as_of_date", "metric", "priority"),
                 ["cik", "as_of_date", "metric", "priority"])
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(S("silver_ltm_lineage")))

# COMMAND ----------

# DBTITLE 1,5 · Adjusted EBITDA LTM from the earnings releases
adj = spark.table(S("silver_adj_ebitda")).filter("found AND coalesce(is_adjusted, false)")
w_latest = lambda *k: Window.partitionBy(*k).orderBy(F.col("filing_date").desc())
q_vals = (
    adj.filter(F.col("quarter_value_usd").isNotNull() & F.col("quarter_end").isNotNull())
    .withColumn("rn", F.row_number().over(w_latest("cik", "quarter_end"))).filter("rn = 1")
    .select("cik", F.col("quarter_end").alias("period_end"), F.col("quarter_value_usd").alias("value"), "label")
)
fy_vals = (
    adj.filter(F.col("fiscal_year_value_usd").isNotNull() & F.col("fiscal_year_end").isNotNull())
    .withColumn("rn", F.row_number().over(w_latest("cik", "fiscal_year_end"))).filter("rn = 1")
    .select("cik", F.col("fiscal_year_end").alias("period_end"), F.col("fiscal_year_value_usd").alias("fy_value"), "label")
)
q_vals.createOrReplaceTempView("adj_q")
fy_vals.createOrReplaceTempView("adj_fy")
adj_ltm = spark.sql(f"""
WITH q4_derived AS (   -- Q4 = FY − (Q1+Q2+Q3) when the Q4 release prints only the full year
  SELECT f.cik, f.period_end, f.fy_value - sum(q.value) AS value, first(f.label) AS label
  FROM adj_fy f JOIN adj_q q ON q.cik = f.cik
   AND q.period_end BETWEEN date_sub(add_months(f.period_end, -9), {TOL}) AND date_sub(f.period_end, 60)
  WHERE NOT EXISTS (SELECT 1 FROM adj_q x WHERE x.cik = f.cik AND abs(datediff(x.period_end, f.period_end)) <= {TOL})
  GROUP BY f.cik, f.period_end, f.fy_value HAVING count(*) = 3
),
quarters AS (SELECT * FROM adj_q UNION ALL SELECT * FROM q4_derived),
by_quarters AS (
  SELECT s.cik, s.as_of_date, sum(q.value) AS ltm, count(*) AS n, first(q.label) AS label
  FROM spine s JOIN quarters q ON q.cik = s.cik
   AND q.period_end BETWEEN date_sub(add_months(s.as_of_date, -9), {TOL}) AND date_add(s.as_of_date, {TOL})
  GROUP BY s.cik, s.as_of_date
)
SELECT s.cik, s.as_of_date,
       coalesce(f.fy_value, CASE WHEN b.n = 4 THEN b.ltm END) AS adj_ebitda_ltm,
       coalesce(f.label, b.label) AS adj_ebitda_label
FROM spine s
LEFT JOIN adj_fy f ON f.cik = s.cik AND s.fiscal_quarter = 4 AND abs(datediff(f.period_end, s.as_of_date)) <= {TOL}
LEFT JOIN by_quarters b ON b.cik = s.cik AND b.as_of_date = s.as_of_date
""")

# COMMAND ----------

# DBTITLE 1,Wide silver_ltm_metrics
wide = (
    values.groupBy("cik", "as_of_date")
    .pivot("metric", FLOW + INSTANT).agg(F.first("value"))
    .join(values.filter(F.col("metric").isin(INSTANT)).groupBy("cik", "as_of_date")
          .pivot("metric", INSTANT).agg(F.first("value_py"))
          .select("cik", "as_of_date", *[F.col(m).alias(f"{m}_py") for m in INSTANT]),
          ["cik", "as_of_date"], "left")
)
ltm = (
    spine.join(wide, ["cik", "as_of_date"], "left")
    .join(adj_ltm, ["cik", "as_of_date"], "left")
    .orderBy("ticker", "as_of_date")
)
ltm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(S("silver_ltm_metrics"))
n = spark.table(S("silver_ltm_metrics")).count()
print("silver_ltm_metrics:", n)
display(spark.table(S("silver_ltm_metrics")).filter("ticker = 'CAT'").orderBy(F.col("as_of_date").desc()).limit(4))

dbutils.notebook.exit(json.dumps({"status": "ok", "rows": n}))
