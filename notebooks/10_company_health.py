# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 10 · Company health score & investor report  (AI, grounded on the ratios)
# MAGIC
# MAGIC For each company: feed the last ~8 periods of `gold_financial_ratios` +
# MAGIC `gold_company_financials` + recent `gold_filing_intelligence` briefings to
# MAGIC `ai_query`, and get back the Investor Health Score (0-100 per dimension) plus
# MAGIC the structured investor report from `docs/ANALYST_SPEC.md` §20-22.
# MAGIC
# MAGIC The LLM analyzes **numbers we computed** — it is told not to invent figures.
# MAGIC Dimensions needing un-ingested data (governance, valuation, sector) are
# MAGIC omitted, not faked. Lands `gold_company_health` (one row per cik).

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("periods", "8")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LLM = dbutils.widgets.get("llm_endpoint")
PERIODS = int(dbutils.widgets.get("periods"))
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Build one grounded prompt per company (driver-side, 5 companies)
RATIO_FIELDS = [
    "fiscal_year", "fiscal_period", "revenue", "revenue_growth_yoy", "gross_margin",
    "operating_margin", "net_margin", "net_income", "eps_diluted", "operating_cash_flow",
    "fcf", "fcf_margin", "fcf_conversion", "cash_and_equivalents", "long_term_debt",
    "net_debt", "debt_to_equity", "return_on_equity", "roic_approx", "capex_intensity",
    "diluted_shares_approx",
    "operating_margin_trend", "fcf_margin_trend", "roic_approx_trend", "net_debt_trend",
    "diluted_shares_approx_trend", "revenue_trend",
]

ratios = spark.table(T("gold_financial_ratios"))
w = Window.partitionBy("cik").orderBy(F.col("period_end").desc())
recent = (
    ratios.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= PERIODS)
    .orderBy("cik", "period_end")
)
companies = [r.asDict() for r in spark.table(T("silver_companies")).select("cik", "ticker", "name", "sic_description").collect()]
rows = [r.asDict() for r in recent.collect()]

try:
    intel = {
        (r["cik"]): r["blurbs"]
        for r in spark.sql(f"""
            SELECT cik, concat_ws('\n', collect_list(concat(form,' ',cast(filing_date as string),': ',executive_summary))) AS blurbs
            FROM {T('gold_filing_intelligence')} GROUP BY cik
        """).collect()
    }
except Exception:
    intel = {}


def fmt_num(v):
    if v is None:
        return "n/a"
    if abs(v) >= 1e9:
        return f"{v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v:.4f}" if abs(v) < 100 else f"{v:.2f}"


PROMPT = (
    "You are a senior equity research analyst. Analyze the company below using ONLY "
    "the figures provided (they were computed from its SEC filings — do not invent "
    "any number; if something is missing say so). Separate reported facts, "
    "calculated metrics, management statements, and your interpretation. Fill every "
    "field of the required structure:\n"
    "- scores.*: integers 0-100.  overall_score: integer 0-100.\n"
    '- overall_label: "Strong" | "Healthy" | "Mixed" | "Weak" | "Distressed".\n'
    '- direction: "Improving" | "Stable" | "Deteriorating".\n'
    "- what_changed: 3-5 short strings, each a development + why it matters.\n"
    "- numbers_that_matter: one plain-text block, 'metric: latest vs prior-yr (trend)' per line.\n"
    "- cash_check / debt_check / shareholder_check: 2-4 sentences each; note any missing data.\n"
    "- accounting_check: 1-3 short findings, each prefixed GREEN / YELLOW / RED.\n"
    "- management_says: management's claims, then whether the numbers support them.\n"
    "- risks: 3-5 short measurable risk strings.\n"
    "- bull_case / base_case / bear_case: one short paragraph each.\n"
    "- watch_next: 3-5 strings, each a specific metric + threshold.\n"
    "- bottom_line: 120-220 words, plain language.\n"
    "- primary_strength / primary_risk / key_metric_next_quarter: one sentence each.\n\n"
)

HEALTH_SCHEMA = StructType([
    StructField("scores", StructType([StructField(k, IntegerType()) for k in [
        "growth_quality", "profitability", "cash_generation", "balance_sheet",
        "capital_allocation", "capital_efficiency", "financial_health"]])),
    StructField("overall_score", IntegerType()),
    StructField("overall_label", StringType()),
    StructField("direction", StringType()),
    StructField("what_changed", ArrayType(StringType())),
    StructField("numbers_that_matter", StringType()),
    StructField("cash_check", StringType()),
    StructField("debt_check", StringType()),
    StructField("shareholder_check", StringType()),
    StructField("accounting_check", StringType()),
    StructField("management_says", StringType()),
    StructField("risks", ArrayType(StringType())),
    StructField("bull_case", StringType()),
    StructField("base_case", StringType()),
    StructField("bear_case", StringType()),
    StructField("watch_next", ArrayType(StringType())),
    StructField("bottom_line", StringType()),
    StructField("primary_strength", StringType()),
    StructField("primary_risk", StringType()),
    StructField("key_metric_next_quarter", StringType()),
])

prompt_rows = []
for co in companies:
    cik = co["cik"]
    lines = []
    for r in [x for x in rows if x["cik"] == cik]:
        lines.append(" | ".join(f"{k}={fmt_num(r[k]) if isinstance(r[k], (int, float)) else r[k]}" for k in RATIO_FIELDS))
    if not lines:
        continue
    body = (
        f"COMPANY: {co['ticker']} — {co['name']} ({co.get('sic_description') or 'n/a'})\n\n"
        f"PER-PERIOD RATIOS (oldest first; margins are fractions, growth is fraction, "
        f"*_trend is up/down/stable vs. same period prior year):\n" + "\n".join(lines) +
        "\n\nRECENT AI FILING BRIEFINGS:\n" + (intel.get(cik, "(none)"))
    )
    prompt_rows.append((cik, co["ticker"], co["name"], PROMPT + body))

pdf = spark.createDataFrame(prompt_rows, ["cik", "ticker", "name", "prompt"])
print("companies to score:", pdf.count())

# COMMAND ----------

# DBTITLE 1,ai_query (json_object) -> parse -> gold_company_health
# responseFormat 'json_object' guarantees a single valid JSON object (no markdown
# fences, escaped newlines). Then from_json with the flat schema — arrays where
# arrays belong so a list value doesn't null the row.
scored = pdf.withColumn(
    "raw", F.expr(f"ai_query('{LLM}', prompt, responseFormat => 'json_object')")
)

parsed = (
    scored.withColumn("p", F.from_json("raw", HEALTH_SCHEMA))
    .select(
        "cik", "ticker", "name",
        F.col("p.overall_score").alias("overall_score"),
        F.col("p.overall_label").alias("overall_label"),
        F.col("p.direction").alias("direction"),
        F.col("p.scores").alias("scores"),
        F.col("p.what_changed").alias("what_changed"),
        F.col("p.numbers_that_matter").alias("numbers_that_matter"),
        F.col("p.cash_check").alias("cash_check"),
        F.col("p.debt_check").alias("debt_check"),
        F.col("p.shareholder_check").alias("shareholder_check"),
        F.col("p.accounting_check").alias("accounting_check"),
        F.col("p.management_says").alias("management_says"),
        F.col("p.risks").alias("risks"),
        F.col("p.bull_case").alias("bull_case"),
        F.col("p.base_case").alias("base_case"),
        F.col("p.bear_case").alias("bear_case"),
        F.col("p.watch_next").alias("watch_next"),
        F.col("p.bottom_line").alias("bottom_line"),
        F.col("p.primary_strength").alias("primary_strength"),
        F.col("p.primary_risk").alias("primary_risk"),
        F.col("p.key_metric_next_quarter").alias("key_metric_next_quarter"),
        F.lit(LLM).alias("model"),
        F.current_timestamp().alias("generated_at"),
        F.col("raw"),
    )
)

parsed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_company_health")
)
out = spark.table(T("gold_company_health"))
print("gold_company_health:", out.count())
display(out.select("ticker", "overall_score", "overall_label", "direction", "primary_strength", "primary_risk"))

n_bad = out.filter(F.col("overall_score").isNull()).count()
dbutils.notebook.exit(json.dumps({"status": "ok", "rows": out.count(), "unparsed": n_bad}))
