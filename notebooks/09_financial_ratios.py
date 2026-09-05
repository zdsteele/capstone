# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 09 · Financial ratios & trend engine
# MAGIC
# MAGIC Derives the ratios from `docs/ANALYST_SPEC.md` sections 3-9 & 19 that our
# MAGIC companyfacts subset supports, per (cik, fiscal_year, fiscal_period), with a
# MAGIC `*_trend` classification (↑ improving / → stable / ↓ deteriorating) vs. the
# MAGIC same period a year earlier. Lands `gold_financial_ratios`.
# MAGIC
# MAGIC **Approximations** (flagged in column comments): diluted shares ≈
# MAGIC net_income / eps_diluted; ROIC uses a 21% assumed tax rate and a rough
# MAGIC invested-capital proxy; no D&A / interest-expense / dividend / buyback /
# MAGIC working-capital line items in the pilot data.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window

TAX = 0.21  # assumed effective rate for NOPAT (spec §9)


def safe_div(a, b):
    return F.when((F.col(b).isNotNull()) & (F.col(b) != 0), F.col(a) / F.col(b))

# COMMAND ----------

# DBTITLE 1,Discrete quarterly cash flow (filers report OCF/capex cumulatively)
# companyfacts gives OCF/capex as YTD (3/6/9/12-mo). gold_company_financials kept
# only the 3-mo value, so Q2/Q3 come out null. Rebuild discrete quarters from
# silver_financial_facts: Q2 = H1 - Q1, Q3 = 9M - H1, FY = annual.
_CF = {
    "ocf": ["NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
}
cf_map = spark.createDataFrame(
    [(m, c, p) for m, cs in _CF.items() for p, c in enumerate(cs)],
    ["metric", "concept", "prio"],
)
_cf_wide = (
    spark.table(T("silver_financial_facts")).join(cf_map, "concept")
    .filter(F.col("period_type").isin("quarter", "half", "ytd9", "annual"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("cik", "fiscal_year", "metric", "period_type")
        .orderBy(F.col("prio").asc(), F.col("filed").desc(), F.col("period_end").desc())))
    .filter(F.col("rn") == 1)
    .groupBy("cik", "fiscal_year", "metric")
    .pivot("period_type", ["quarter", "half", "ytd9", "annual"]).agg(F.first("value"))
)
_discrete = _cf_wide.select(
    "cik", "fiscal_year", "metric",
    F.explode(F.array(
        F.struct(F.lit("Q1").alias("fp"), F.col("quarter").alias("val")),
        F.struct(F.lit("Q2").alias("fp"), (F.col("half") - F.col("quarter")).alias("val")),
        F.struct(F.lit("Q3").alias("fp"), (F.col("ytd9") - F.col("half")).alias("val")),
        F.struct(F.lit("FY").alias("fp"), F.col("annual").alias("val")),
    )).alias("e"),
).select("cik", "fiscal_year", F.col("e.fp").alias("fiscal_period"), "metric", F.col("e.val").alias("val"))

disc_ocf = _discrete.filter(F.col("metric") == "ocf").select(
    "cik", "fiscal_year", "fiscal_period", F.col("val").alias("ocf_discrete"))
disc_capex = _discrete.filter(F.col("metric") == "capex").select(
    "cik", "fiscal_year", "fiscal_period", F.col("val").alias("capex_discrete"))

# COMMAND ----------

# DBTITLE 1,Per-period ratios
g = (
    spark.table(T("gold_company_financials"))
    .join(disc_ocf, ["cik", "fiscal_year", "fiscal_period"], "left")
    .join(disc_capex, ["cik", "fiscal_year", "fiscal_period"], "left")
    .withColumn("operating_cash_flow", F.coalesce("ocf_discrete", "operating_cash_flow"))
    .withColumn("capital_expenditures", F.coalesce("capex_discrete", "capital_expenditures"))
    # annualization factor: quarterly flow ratios x4 so ROIC/ROE compare to FY
    .withColumn("_ann", F.when(F.col("fiscal_period") == "FY", F.lit(1.0)).otherwise(F.lit(4.0)))
)

r = (
    g.withColumn("fcf", F.col("operating_cash_flow") - F.col("capital_expenditures"))
    .withColumn("gross_margin", safe_div("gross_profit", "revenue"))
    .withColumn("operating_margin", safe_div("operating_income", "revenue"))
    .withColumn("net_margin", safe_div("net_income", "revenue"))
    .withColumn("rd_pct_revenue", safe_div("research_development", "revenue"))
    .withColumn("sga_pct_revenue", safe_div("sga_expense", "revenue"))
    .withColumn("fcf_margin", safe_div("fcf", "revenue"))
    .withColumn("fcf_conversion", safe_div("fcf", "net_income"))
    .withColumn("cfo_conversion", safe_div("operating_cash_flow", "net_income"))
    .withColumn("capex_intensity", safe_div("capital_expenditures", "revenue"))
    .withColumn("net_debt", F.col("long_term_debt") - F.col("cash_and_equivalents"))
    .withColumn("debt_to_equity", safe_div("total_liabilities", "stockholders_equity"))
    .withColumn("equity_ratio", safe_div("stockholders_equity", "total_assets"))
    # ROE / ROA / ROIC annualized (quarterly earnings x4) so trends & the FY row
    # are on the same basis
    .withColumn(
        "return_on_equity",
        F.when((F.col("stockholders_equity").isNotNull()) & (F.col("stockholders_equity") != 0),
               F.col("net_income") * F.col("_ann") / F.col("stockholders_equity")),
    )
    .withColumn(
        "return_on_assets",
        F.when((F.col("total_assets").isNotNull()) & (F.col("total_assets") != 0),
               F.col("net_income") * F.col("_ann") / F.col("total_assets")),
    )
    .withColumn(
        "diluted_shares_approx",
        F.when(F.col("eps_diluted").isNotNull() & (F.col("eps_diluted") != 0),
               F.col("net_income") / F.col("eps_diluted")),
    )
    .withColumn(
        "invested_capital_approx",
        F.col("total_liabilities") + F.col("stockholders_equity") - F.col("cash_and_equivalents"),
    )
    .withColumn(
        "roic_approx",
        F.when(F.col("invested_capital_approx") > 0,
               (F.col("operating_income") * F.col("_ann") * (1 - TAX)) / F.col("invested_capital_approx")),
    )
    .withColumn("revenue_per_share", safe_div("revenue", "diluted_shares_approx"))
    .withColumn("fcf_per_share", safe_div("fcf", "diluted_shares_approx"))
    .withColumn("ni_per_share", safe_div("net_income", "diluted_shares_approx"))
)

# COMMAND ----------

# DBTITLE 1,YoY growth + trend classification
w_yoy = Window.partitionBy("cik", "fiscal_period").orderBy("fiscal_year")

TREND_COLS = [
    "revenue", "gross_margin", "operating_margin", "net_margin", "fcf", "fcf_margin",
    "roic_approx", "net_debt", "diluted_shares_approx", "return_on_equity",
    "capex_intensity", "fcf_per_share",
]

for c in TREND_COLS:
    r = r.withColumn(f"{c}_prior_yr", F.lag(c).over(w_yoy))

r = r.withColumn(
    "revenue_growth_yoy",
    F.when(
        (F.col("revenue_prior_yr").isNotNull()) & (F.col("revenue_prior_yr") != 0),
        F.col("revenue") / F.col("revenue_prior_yr") - 1,
    ),
)


def trend(col):
    prev = F.col(f"{col}_prior_yr")
    delta = (F.col(col) - prev) / F.abs(prev)
    return (
        F.when(prev.isNull() | (prev == 0), F.lit("n/a"))
        .when(delta > 0.02, F.lit("up"))
        .when(delta < -0.02, F.lit("down"))
        .otherwise(F.lit("stable"))
    )


for c in TREND_COLS:
    r = r.withColumn(f"{c}_trend", trend(c))

# COMMAND ----------

# DBTITLE 1,Write gold_financial_ratios
cols = [
    "cik", "ticker", "name", "fiscal_year", "fiscal_period", "fiscal_quarter", "period_end",
    # levels
    "revenue", "gross_profit", "operating_income", "net_income", "eps_diluted",
    "operating_cash_flow", "capital_expenditures", "fcf",
    "cash_and_equivalents", "long_term_debt", "net_debt",
    "total_assets", "total_liabilities", "stockholders_equity",
    "diluted_shares_approx", "invested_capital_approx",
    # ratios
    "revenue_growth_yoy", "gross_margin", "operating_margin", "net_margin",
    "rd_pct_revenue", "sga_pct_revenue",
    "fcf_margin", "fcf_conversion", "cfo_conversion", "capex_intensity",
    "debt_to_equity", "equity_ratio", "return_on_equity", "return_on_assets", "roic_approx",
    "revenue_per_share", "fcf_per_share", "ni_per_share",
]
cols += [f"{c}_trend" for c in TREND_COLS]

out = r.select(*cols)
out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_financial_ratios")
)
n = out.count()
print("gold_financial_ratios:", n)
display(
    out.filter(F.col("ticker") == "AAPL")
    .select("fiscal_year", "fiscal_period", "revenue_growth_yoy", "operating_margin",
            "operating_margin_trend", "fcf_margin", "fcf_margin_trend", "roic_approx")
    .orderBy(F.col("period_end").desc()).limit(6)
)

dbutils.notebook.exit(json.dumps({"status": "ok", "rows": n}))
