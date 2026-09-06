# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 11 · Valuation  (analyst spec §18)
# MAGIC
# MAGIC The one section that needs market data. Combines the yfinance price bars
# MAGIC (`bronze_market_bars`) with the XBRL-derived financials
# MAGIC (`gold_financial_ratios`) to compute the standard multiples:
# MAGIC
# MAGIC - Market cap = latest close × diluted shares
# MAGIC - Enterprise value = market cap + net debt
# MAGIC - P/E, EV/EBIT, EV/Revenue, Price/FCF, FCF yield, Price/Book
# MAGIC
# MAGIC Flows use the latest fiscal-year row; if a company has no `FY` row we
# MAGIC annualise from the last four quarters. One row per company →
# MAGIC `gold_valuation`, read by the agent's `get_valuation` tool and the
# MAGIC Dashboard. **Company quality (nb 10) stays separate from this** — a low
# MAGIC multiple is not a "cheap" verdict.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window

# COMMAND ----------

# DBTITLE 1,Inputs — latest price + latest financials
px = (
    spark.table(T("bronze_market_bars"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("ticker").orderBy(F.col("bar_date").desc())))
    .filter("rn = 1")
    .select("ticker",
            F.coalesce("adj_close", "close").alias("price"),
            F.col("bar_date").alias("price_date"))
)

r = spark.table(T("gold_financial_ratios"))

# balance-sheet items + share count: the most recent row of any period
latest = (
    r.withColumn("rn", F.row_number().over(
        Window.partitionBy("cik").orderBy(F.col("period_end").desc())))
    .filter("rn = 1")
    .select("cik", "ticker", "name", F.col("period_end").alias("as_of"),
            "net_debt", "cash_and_equivalents", "stockholders_equity",
            "diluted_shares_approx")
)

# flow items: latest FY row
fy = (
    r.filter(F.col("fiscal_period") == "FY")
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("cik").orderBy(F.col("period_end").desc())))
    .filter("rn = 1")
    .select("cik",
            F.col("revenue").alias("rev_fy"),
            F.col("net_income").alias("ni_fy"),
            F.col("operating_income").alias("ebit_fy"),
            F.col("fcf").alias("fcf_fy"))
)

# fallback: annualise the last 4 quarters
q4 = (
    r.filter(F.col("fiscal_period").startswith("Q"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("cik").orderBy(F.col("period_end").desc())))
    .filter("rn <= 4")
    .groupBy("cik")
    .agg(F.count("*").alias("nq"),
         F.sum("revenue").alias("rev_q"),
         F.sum("net_income").alias("ni_q"),
         F.sum("operating_income").alias("ebit_q"),
         F.sum("fcf").alias("fcf_q"))
    .withColumn("scale", F.lit(4.0) / F.col("nq"))   # annualise if < 4 quarters
    .select("cik",
            (F.col("rev_q") * F.col("scale")).alias("rev_q"),
            (F.col("ni_q") * F.col("scale")).alias("ni_q"),
            (F.col("ebit_q") * F.col("scale")).alias("ebit_q"),
            (F.col("fcf_q") * F.col("scale")).alias("fcf_q"))
)

# COMMAND ----------

# DBTITLE 1,Compute multiples -> gold_valuation
base = (
    latest.join(px, "ticker", "left")
    .join(fy, "cik", "left")
    .join(q4, "cik", "left")
    .withColumn("revenue_ttm", F.coalesce("rev_fy", "rev_q"))
    .withColumn("net_income_ttm", F.coalesce("ni_fy", "ni_q"))
    .withColumn("ebit_ttm", F.coalesce("ebit_fy", "ebit_q"))
    .withColumn("fcf_ttm", F.coalesce("fcf_fy", "fcf_q"))
    .withColumn("market_cap", F.col("price") * F.col("diluted_shares_approx"))
    .withColumn("enterprise_value", F.col("market_cap") + F.coalesce("net_debt", F.lit(0.0)))
)


def _ratio(num, den):
    d = F.col(den)
    return F.when(d.isNotNull() & (d != 0) & (d > 0), F.col(num) / d).otherwise(F.lit(None))


valuation = (
    base.withColumn("pe", _ratio("market_cap", "net_income_ttm"))
    .withColumn("ev_ebit", _ratio("enterprise_value", "ebit_ttm"))
    .withColumn("ev_revenue", _ratio("enterprise_value", "revenue_ttm"))
    .withColumn("price_to_fcf", _ratio("market_cap", "fcf_ttm"))
    .withColumn(
        "fcf_yield",
        F.when(F.col("market_cap") > 0, F.col("fcf_ttm") / F.col("market_cap")).otherwise(None),
    )
    .withColumn("price_to_book", _ratio("market_cap", "stockholders_equity"))
    .select(
        "cik", "ticker", "name", "as_of", "price", "price_date",
        "diluted_shares_approx", "market_cap", "net_debt", "enterprise_value",
        "revenue_ttm", "net_income_ttm", "ebit_ttm", "fcf_ttm",
        "pe", "ev_ebit", "ev_revenue", "price_to_fcf", "fcf_yield", "price_to_book",
        F.current_timestamp().alias("generated_at"),
    )
)

valuation.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("gold_valuation"))

n = spark.table(T("gold_valuation")).count()
n_priced = spark.table(T("gold_valuation")).filter(F.col("market_cap").isNotNull()).count()
print(f"gold_valuation: {n} rows, {n_priced} with a market cap")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "ok", "rows": n, "priced": n_priced}))
