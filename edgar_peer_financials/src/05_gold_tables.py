# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold — the three Bloomberg peer tables
# MAGIC
# MAGIC One row per company per reported quarter (LTM basis), plus a `*_latest` view
# MAGIC holding each company's most recent quarter — the Bloomberg screen layout.
# MAGIC Currency amounts are **USD millions**; margins / growth / returns are
# MAGIC **percent** (13.4 = 13.4%); leverage multiples are **x**.
# MAGIC
# MAGIC | Table | Bloomberg screen |
# MAGIC |---|---|
# MAGIC | `gold_peer_financials` | Financials (Revenue … Net Debt) |
# MAGIC | `gold_peer_profitability` | Profitability (growth, margins, ROIC/ROA/ROE) |
# MAGIC | `gold_peer_leverage` | Balance sheet (Net Debt/EBITDA … EBITDA/Interest) |
# MAGIC
# MAGIC Definitions (matched against the Bloomberg sample — see notebook 06):
# MAGIC - **EBIT** = operating income (fallback: pretax income + interest expense).
# MAGIC - **EBITDA** (margin, leverage) = EBIT + D&A, *reported*. Bloomberg's leverage
# MAGIC   multiples divide by this, not by Adj. EBITDA (AMRC: 1894.9 / 6.98x ≈ 271.6
# MAGIC   = 13.42% × revenue).
# MAGIC - **Adj. EBITDA** = company-reported LTM from the 8-K releases (nb 03);
# MAGIC   falls back to reported EBITDA, see `adj_ebitda_source`.
# MAGIC - **Total Debt** = short-term borrowings + commercial paper + other debt
# MAGIC   (e.g. convertibles) + current & non-current debt + finance-lease +
# MAGIC   operating-lease liabilities. **Net Debt** = Total Debt − cash − short-term investments.
# MAGIC - **Cash flow per share** = LTM OCF / LTM-average basic shares.
# MAGIC - **Growth** = LTM vs the LTM four quarters earlier; NULL ("NM") when the
# MAGIC   base is ≤ 0 or missing. **ROA/ROE/ROIC** use averages of the current and
# MAGIC   year-ago balance sheet; ROIC = (NI + interest × (1 − tax rate)) /
# MAGIC   avg(equity + total debt).
# MAGIC - **1Y TSR / Stock Return** need market prices — NULL for now.

# COMMAND ----------

import json

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "engineering_playground_dev")
dbutils.widgets.text("silver_schema", "silver_edgar")
dbutils.widgets.text("gold_schema", "gold_edgar")
dbutils.widgets.text("default_tax_rate", "0.21")
CATALOG = dbutils.widgets.get("catalog")
S = lambda n: f"{CATALOG}.{dbutils.widgets.get('silver_schema')}.{n}"
G = lambda n: f"{CATALOG}.{dbutils.widgets.get('gold_schema')}.{n}"
TAX = float(dbutils.widgets.get("default_tax_rate"))
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{dbutils.widgets.get('gold_schema')}")

MM = 1e6
z = lambda c: F.coalesce(F.col(c), F.lit(0.0))


def div(a, b):
    """a / b, NULL when b is 0/NULL (serverless ANSI mode raises on a raw / 0)."""
    return F.try_divide(a, b)


def pct(a, b):
    return div(a, b) * 100


def growth(cur, prev):
    """LTM-over-LTM growth in percent; NULL ('NM') on a non-positive base."""
    return F.when(F.col(prev) > 0, (F.col(cur) / F.col(prev) - 1) * 100)


def total_debt(sfx=""):
    c = lambda m: f"{m}{sfx}"
    debt = (z(c("short_term_borrowings")) + z(c("commercial_paper")) + z(c("other_debt"))
            + F.when(F.col(c("debt_current")).isNull() & F.col(c("debt_noncurrent")).isNull(),
                     z(c("debt_total_fallback")))
               .otherwise(z(c("debt_current")) + z(c("debt_noncurrent"))))
    fin_lease = F.coalesce(F.col(c("finance_lease")),
                           z(c("finance_lease_current")) + z(c("finance_lease_noncurrent")))
    op_lease = F.coalesce(F.col(c("operating_lease")),
                          z(c("operating_lease_current")) + z(c("operating_lease_noncurrent")))
    return debt + fin_lease + op_lease

# COMMAND ----------

# DBTITLE 1,Derived LTM measures
m = (
    spark.table(S("silver_ltm_metrics"))
    .withColumn("interest", F.abs("interest_expense"))
    .withColumn("ebit", F.coalesce("operating_income", F.col("pretax_income") + F.col("interest")))
    .withColumn("ebitda", F.col("ebit") + F.col("depreciation_amortization"))
    .withColumn("adj_ebitda_source",
                F.when(F.col("adj_ebitda_ltm").isNotNull(), F.lit("company_release"))
                 .when(F.col("ebitda").isNotNull(), F.lit("reported_fallback")))
    .withColumn("adj_ebitda", F.coalesce("adj_ebitda_ltm", "ebitda"))
    .withColumn("capex", -F.abs("capital_expenditures"))
    .withColumn("fcf", F.col("operating_cash_flow") + F.col("capex"))
    .withColumn("total_debt", total_debt())
    .withColumn("total_debt_py", total_debt("_py"))
    .withColumn("net_debt", F.col("total_debt") - z("cash") - z("short_term_investments"))
    # LTM-average basic shares: tracks Bloomberg's implied count, where Up-C annual
    # diluted counts (exchangeable units) would not
    .withColumn("cf_per_share", div(F.col("operating_cash_flow"),
                                    F.coalesce("basic_shares", "diluted_shares")))
    .withColumn("tax_rate",
                F.when(F.col("pretax_income") > 0,
                       F.greatest(F.lit(0.0), F.least(F.lit(0.5), F.col("income_tax") / F.col("pretax_income"))))
                 .otherwise(F.lit(TAX)))
    .withColumn("avg_assets", (F.col("total_assets") + F.col("total_assets_py")) / 2)
    .withColumn("avg_equity", (F.col("total_equity") + F.col("total_equity_py")) / 2)
    .withColumn("avg_invested_capital",
                (F.col("total_equity") + F.col("total_debt") + F.col("total_equity_py") + F.col("total_debt_py")) / 2)
)

# year-ago LTM for growth rates
# (renamed key: prev derives from m, and a same-lineage self-join can't tell m.cik from prev.cik)
prev = m.select(
    F.col("cik").alias("prev_cik"), F.col("as_of_date").alias("prev_as_of"),
    F.col("revenue").alias("revenue_prev"), F.col("ebitda").alias("ebitda_prev"),
    F.col("net_income").alias("net_income_prev"))
m = (
    m.join(prev, (F.col("cik") == F.col("prev_cik"))
           & (F.abs(F.datediff("prev_as_of", F.add_months("as_of_date", -12))) <= 10), "left")
    .drop("prev_cik", "prev_as_of")
)

keys = ["cik", "ticker", "bloomberg_ticker", F.col("name").alias("company_name"),
        "fiscal_period", "fiscal_year", "fiscal_quarter", "as_of_date", "form", "accession", "filing_date"]

# COMMAND ----------

# DBTITLE 1,gold_peer_financials
financials = m.select(
    *keys,
    (F.col("revenue") / MM).alias("revenue"),
    (F.col("adj_ebitda") / MM).alias("adj_ebitda"),
    "adj_ebitda_source", F.col("adj_ebitda_label"),
    (F.col("ebit") / MM).alias("ebit"),
    (F.col("ebitda") / MM).alias("ebitda_reported"),
    pct(F.col("ebitda"), F.col("revenue")).alias("ebitda_margin"),
    (F.col("net_income") / MM).alias("net_income"),
    F.col("eps_diluted"),
    (F.col("capex") / MM).alias("capex"),
    (F.col("operating_cash_flow") / MM).alias("operating_cash_flow"),
    (F.col("fcf") / MM).alias("free_cash_flow"),
    (F.col("total_debt") / MM).alias("total_debt"),
    F.lit(None).cast("double").alias("tsr_1y"),
    F.col("cf_per_share").alias("cash_flow_per_share"),
    (F.col("net_debt") / MM).alias("net_debt"),
)

# COMMAND ----------

# DBTITLE 1,gold_peer_profitability
profitability = m.select(
    *keys,
    growth("revenue", "revenue_prev").alias("sales_growth_pct"),
    growth("ebitda", "ebitda_prev").alias("ebitda_growth_pct"),
    pct(F.col("ebitda"), F.col("revenue")).alias("ebitda_margin_pct"),
    pct(F.col("ebit"), F.col("revenue")).alias("operating_margin_pct"),
    growth("net_income", "net_income_prev").alias("net_income_growth_pct"),
    pct(F.col("net_income"), F.col("revenue")).alias("net_margin_pct"),
    F.when(F.col("avg_invested_capital") > 0,
           pct(F.col("net_income") + z("interest") * (1 - F.col("tax_rate")), F.col("avg_invested_capital")))
     .alias("roic_pct"),
    pct(F.col("net_income"), F.col("avg_assets")).alias("roa_pct"),
    F.when(F.col("avg_equity") > 0, pct(F.col("net_income"), F.col("avg_equity"))).alias("roe_pct"),
    F.lit(None).cast("double").alias("stock_return_pct"),
)

# COMMAND ----------

# DBTITLE 1,gold_peer_leverage
leverage = m.select(
    *keys,
    F.when(F.col("ebitda") > 0, F.col("net_debt") / F.col("ebitda")).alias("net_debt_to_ebitda_x"),
    F.when(F.col("ebitda") > 0, F.col("total_debt") / F.col("ebitda")).alias("total_debt_to_ebitda_x"),
    pct(F.col("net_debt"), F.col("total_equity")).alias("net_debt_to_equity_pct"),
    pct(F.col("total_debt"), F.col("total_assets")).alias("total_debt_to_total_assets_pct"),
    F.when(F.col("interest") > 0, F.col("ebitda") / F.col("interest")).alias("ebitda_to_interest_x"),
)

# COMMAND ----------

# DBTITLE 1,Write tables + latest views
out = {}
for name, df in [("gold_peer_financials", financials),
                 ("gold_peer_profitability", profitability),
                 ("gold_peer_leverage", leverage)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(G(name))
    spark.sql(f"""
        CREATE OR REPLACE VIEW {G(name + '_latest')} AS
        SELECT * EXCEPT (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY cik ORDER BY as_of_date DESC) AS rn FROM {G(name)})
        WHERE rn = 1""")
    out[name] = spark.table(G(name)).count()
print(out)
display(spark.table(G("gold_peer_financials_latest")).orderBy("ticker"))

dbutils.notebook.exit(json.dumps({"status": "ok", "rows": out}))
