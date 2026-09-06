# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 10 · Company health score & investor report  (AI, grounded on everything)
# MAGIC
# MAGIC For each company, feed `ai_query` the last ~8 periods of
# MAGIC `gold_financial_ratios` (now the full §3-10 set incl. the FCF bridge,
# MAGIC leverage, working capital, payout) + `gold_valuation` + `gold_governance`
# MAGIC + `gold_insider_activity` + recent `gold_filing_intelligence` briefings,
# MAGIC and get back the 11-dimension Investor Health Score plus the structured
# MAGIC investor report from `docs/ANALYST_SPEC.md` §20-22.
# MAGIC
# MAGIC The LLM analyzes **numbers we computed** — told not to invent figures, and
# MAGIC to return `null` for a dimension whose input block is absent (nothing
# MAGIC faked). Lands `gold_company_health` (one row per cik).

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
    "fiscal_year", "fiscal_period", "revenue", "revenue_growth_yoy",
    "gross_margin", "operating_margin", "net_margin", "net_income", "eps_diluted",
    "operating_cash_flow", "fcf", "fcf_margin", "fcf_conversion", "cfo_conversion",
    "capex_intensity", "capex_to_da",
    # FCF bridge (§5)
    "bridge_da", "bridge_sbc", "wc_change_total",
    # balance sheet (§6)
    "cash_and_equivalents", "total_debt", "net_debt_full", "current_ratio",
    "debt_to_ebitda", "net_debt_to_ebitda", "interest_coverage", "goodwill_pct_assets",
    # capital allocation (§7) + dilution (§8)
    "dividends_paid_abs", "buybacks_abs", "dividend_payout", "fcf_payout",
    "buyback_pct_fcf", "sbc_pct_revenue", "sbc_vs_buybacks", "diluted_shares",
    # returns (§9) + working capital (§10)
    "return_on_equity", "return_on_assets", "effective_tax_rate", "roic",
    "dso", "dio", "dpo", "ccc",
    # trend flags
    "revenue_trend", "operating_margin_trend", "fcf_margin_trend", "roic_trend",
    "net_debt_full_trend", "diluted_shares_trend", "current_ratio_trend",
    "net_debt_to_ebitda_trend", "interest_coverage_trend", "ccc_trend",
    "fcf_payout_trend", "sbc_pct_revenue_trend",
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


def _side_table(name, cols):
    """{cik: 'k=v | k=v'} from a gold table if it exists, else {}."""
    try:
        return {
            r["cik"]: " | ".join(f"{c}={r[c]}" for c in cols if r[c] is not None)
            for r in spark.table(T(name)).select("cik", *cols).collect()
        }
    except Exception:
        return {}


valn = _side_table("gold_valuation", ["market_cap", "pe", "ev_ebitda", "ev_revenue",
                                      "price_to_fcf", "fcf_yield", "price_to_book",
                                      "dividend_yield", "shareholder_yield"])
govn = _side_table("gold_governance", ["incentivized_to_optimize", "pay_is_equity_heavy",
                                       "say_on_pay_support_pct", "board_independent_pct",
                                       "incentive_risk", "related_party_transactions"])
insd = _side_table("gold_insider_activity", ["signal", "buy_value_180d", "sell_value_180d",
                                             "net_value_180d", "n_buyers_180d", "n_sellers_180d"])


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
    "calculated metrics, management statements, and your interpretation.\n\n"
    "Respond with ONLY a single JSON object — no markdown fences, no text before or "
    "after. Every string value must be ONE line (no literal newline characters). "
    "Keys:\n"
    "- scores: object of integers 0-100: growth_quality, profitability, cash_generation, "
    "balance_sheet, capital_allocation, capital_efficiency, management_governance, "
    "accounting_quality, valuation, sector_specific, financial_health. Return null for "
    "any dimension whose input block is absent below (no GOVERNANCE block -> "
    "management_governance null; no VALUATION block -> valuation null; sector_specific "
    "null unless the filings clearly support a sector KPI read).\n"
    "- overall_score: integer 0-100 — the composite MUST NOT hide a flagged risk "
    "(an 82 with a liquidity problem still shows the liquidity risk prominently)\n"
    '- overall_label: "Strong" | "Healthy" | "Mixed" | "Weak" | "Distressed"\n'
    '- direction: "Improving" | "Stable" | "Deteriorating"\n'
    "- what_changed: array of 3-5 strings (development + why it matters)\n"
    "- numbers_that_matter: array of strings, one per line 'metric: latest vs prior-yr (trend)'\n"
    "- cash_check, debt_check, shareholder_check: string, 2-4 sentences; note missing data\n"
    "- accounting_check: string, 1-3 findings each prefixed GREEN/YELLOW/RED\n"
    "- management_says: string — management's claims, then whether the numbers support them\n"
    "- risks: array of 3-5 measurable risk strings\n"
    "- bull_case, base_case, bear_case: string, one short paragraph each\n"
    "- watch_next: array of 3-5 strings, each a specific metric + threshold\n"
    "- bottom_line: string, 120-220 words, plain language\n"
    "- primary_strength, primary_risk, key_metric_next_quarter: string, one sentence each\n\n"
    "Assess COMPANY QUALITY only. Do NOT give a Buy/Sell/Hold recommendation, a "
    "price target, or an expected return — no 'X% upside', no 'good investment'. "
    "Keep company health separate from stock valuation.\n\n"
)

HEALTH_SCHEMA = StructType([
    StructField("scores", StructType([StructField(k, IntegerType()) for k in [
        "growth_quality", "profitability", "cash_generation", "balance_sheet",
        "capital_allocation", "capital_efficiency", "management_governance",
        "accounting_quality", "valuation", "sector_specific", "financial_health"]])),
    StructField("overall_score", IntegerType()),
    StructField("overall_label", StringType()),
    StructField("direction", StringType()),
    StructField("what_changed", ArrayType(StringType())),
    StructField("numbers_that_matter", ArrayType(StringType())),
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

from collections import defaultdict
_by_cik = defaultdict(list)
for x in rows:
    _by_cik[x["cik"]].append(x)


def _fmt(v):
    return fmt_num(v) if isinstance(v, (int, float)) else v


prompt_rows = []
for co in companies:
    cik = co["cik"]
    lines = [" | ".join(f"{k}={_fmt(r.get(k))}" for k in RATIO_FIELDS) for r in _by_cik.get(cik, [])]
    if not lines:
        continue
    body = (
        f"COMPANY: {co['ticker']} — {co['name']} ({co.get('sic_description') or 'n/a'})\n\n"
        f"PER-PERIOD RATIOS (oldest first; margins/growth are fractions; *_trend is "
        f"up/down/stable vs. the same period a year earlier):\n" + "\n".join(lines)
    )
    if cik in valn:
        body += f"\n\nVALUATION (yfinance price + XBRL): {valn[cik]}"
    if cik in govn:
        body += f"\n\nGOVERNANCE (latest proxy): {govn[cik]}"
    if cik in insd:
        body += f"\n\nINSIDER ACTIVITY (180d, open market): {insd[cik]}"
    body += "\n\nRECENT AI FILING BRIEFINGS:\n" + intel.get(cik, "(none)")
    prompt_rows.append((cik, co["ticker"], co["name"], PROMPT + body))

pdf = spark.createDataFrame(prompt_rows, ["cik", "ticker", "name", "prompt"])
print("companies to score:", pdf.count())

# COMMAND ----------

# DBTITLE 1,ai_query -> parse -> gold_company_health
# Plain ai_query (this build's responseFormat only accepts a DDL and returns a
# string anyway). Prompt asks for a single JSON object with no literal newlines;
# regex-extract the braces (handles any stray fences) then from_json. Array-typed
# list fields so a JSON array doesn't null the row.
scored = pdf.withColumn("raw", F.expr(f"ai_query('{LLM}', prompt)"))

parsed = (
    scored.withColumn("json_str", F.regexp_extract("raw", r"\{[\s\S]*\}", 0))
    .withColumn("p", F.from_json("json_str", HEALTH_SCHEMA))
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
