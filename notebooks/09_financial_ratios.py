# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 09 · Financial ratios & trend engine
# MAGIC
# MAGIC Derives the ratios from `docs/ANALYST_SPEC.md` sections 3-10 & 19 per
# MAGIC (cik, fiscal_year, fiscal_period), each with a `*_trend` classification
# MAGIC (↑ improving / → stable / ↓ deteriorating) vs. the same period a year
# MAGIC earlier. Lands `gold_financial_ratios`.
# MAGIC
# MAGIC Now covers: income-statement margins & growth (§3), cash flow + the FCF
# MAGIC **bridge** (§4-5), balance-sheet health — current ratio, debt/EBITDA,
# MAGIC interest coverage, goodwill % (§6), capital allocation — dividend/FCF
# MAGIC payout, buyback intensity (§7), share count & SBC (§8), real ROIC with an
# MAGIC effective tax rate (§9), and working capital — DSO/DIO/DPO/CCC (§10).
# MAGIC Diluted shares now come from the `shares_diluted` XBRL concept (falls back
# MAGIC to net_income / eps_diluted).

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

# DBTITLE 1,Extended ratios — §5-10 (needs the expanded concept set from nb 04)
_z = lambda c: F.coalesce(F.col(c), F.lit(0.0))   # treat a missing line item as 0

r = (
    r
    # real diluted share count (XBRL concept) — prefer over the eps-derived approx
    .withColumn("diluted_shares",
                F.coalesce(F.col("shares_diluted"), F.col("diluted_shares_approx")))
    .withColumn("basic_shares", F.col("shares_basic"))
    # §6 total debt / leverage
    .withColumn("total_debt", _z("short_term_debt") + _z("long_term_debt") + _z("lease_liabilities"))
    .withColumn("net_debt_full", F.col("total_debt") - _z("cash_and_equivalents") - _z("marketable_securities"))
    .withColumn("ebitda", (_z("operating_income") + _z("depreciation_amortization")) * F.col("_ann"))
    .withColumn("current_ratio", safe_div("current_assets", "current_liabilities"))
    .withColumn("debt_to_ebitda", F.when(F.col("ebitda") > 0, F.col("total_debt") / F.col("ebitda")))
    .withColumn("net_debt_to_ebitda", F.when(F.col("ebitda") > 0, F.col("net_debt_full") / F.col("ebitda")))
    .withColumn("interest_coverage",
                F.when(F.abs(_z("interest_expense")) > 0,
                       (F.col("operating_income") * F.col("_ann")) / F.abs(F.col("interest_expense"))))
    .withColumn("goodwill_pct_assets", safe_div("goodwill", "total_assets"))
    .withColumn("capex_to_da", F.when(F.abs(_z("depreciation_amortization")) > 0,
                                      F.abs(F.col("capital_expenditures")) / F.abs(F.col("depreciation_amortization"))))
    # §5 FCF bridge (NI + D&A + SBC ± working-capital changes -> OCF; -capex -> FCF)
    .withColumn("bridge_net_income", F.col("net_income"))
    .withColumn("bridge_da", _z("depreciation_amortization"))
    .withColumn("bridge_sbc", _z("stock_based_compensation"))
    .withColumn("bridge_wc_receivables", -_z("chg_accounts_receivable"))
    .withColumn("bridge_wc_inventory", -_z("chg_inventory"))
    .withColumn("bridge_wc_payables", _z("chg_accounts_payable"))
    .withColumn("bridge_wc_deferred_rev", _z("chg_deferred_revenue"))
    .withColumn("bridge_capex", -F.abs(_z("capital_expenditures")))
    .withColumn("wc_change_total",
                -_z("chg_accounts_receivable") - _z("chg_inventory")
                + _z("chg_accounts_payable") + _z("chg_deferred_revenue"))
    # §7 capital allocation
    .withColumn("dividends_paid_abs", F.abs(_z("dividends_paid")))
    .withColumn("buybacks_abs", F.abs(_z("stock_repurchased")))
    .withColumn("acquisitions_abs", F.abs(_z("acquisitions")))
    .withColumn("dividend_payout", safe_div("dividends_paid_abs", "net_income"))
    .withColumn("fcf_payout",
                F.when(F.col("fcf") > 0, (F.col("dividends_paid_abs") + F.col("buybacks_abs")) / F.col("fcf")))
    .withColumn("buyback_pct_fcf", F.when(F.col("fcf") > 0, F.col("buybacks_abs") / F.col("fcf")))
    .withColumn("shareholder_cash_return", F.col("dividends_paid_abs") + F.col("buybacks_abs"))
    # §8 dilution
    .withColumn("sbc_pct_revenue", safe_div("stock_based_compensation", "revenue"))
    .withColumn("sbc_vs_buybacks",
                F.when(F.col("buybacks_abs") > 0, _z("stock_based_compensation") / F.col("buybacks_abs")))
    # §9 real ROIC with an effective tax rate
    .withColumn("effective_tax_rate",
                F.when((F.col("pretax_income").isNotNull()) & (F.col("pretax_income") > 0),
                       F.greatest(F.least(F.col("income_tax_expense") / F.col("pretax_income"), F.lit(0.5)), F.lit(0.0)))
                .otherwise(F.lit(TAX)))
    .withColumn("nopat", F.col("operating_income") * F.col("_ann") * (1 - F.col("effective_tax_rate")))
    .withColumn("roic",
                F.when(F.col("invested_capital_approx") > 0, F.col("nopat") / F.col("invested_capital_approx")))
    # §10 working capital
    .withColumn("dso", F.when(F.col("revenue") > 0, _z("accounts_receivable") / (F.col("revenue") * F.col("_ann")) * 365.0))
    .withColumn("dio", F.when(F.abs(_z("cost_of_revenue")) > 0, _z("inventory") / (F.abs(F.col("cost_of_revenue")) * F.col("_ann")) * 365.0))
    .withColumn("dpo", F.when(F.abs(_z("cost_of_revenue")) > 0, _z("accounts_payable") / (F.abs(F.col("cost_of_revenue")) * F.col("_ann")) * 365.0))
    .withColumn("ccc", F.col("dso") + F.col("dio") - F.col("dpo"))
    # per-share on the real share count
    .withColumn("fcf_per_share", safe_div("fcf", "diluted_shares"))
    .withColumn("revenue_per_share", safe_div("revenue", "diluted_shares"))
    .withColumn("ni_per_share", safe_div("net_income", "diluted_shares"))
)

# COMMAND ----------

# DBTITLE 1,YoY growth + trend classification
w_yoy = Window.partitionBy("cik", "fiscal_period").orderBy("fiscal_year")

TREND_COLS = [
    "revenue", "gross_margin", "operating_margin", "net_margin", "fcf", "fcf_margin",
    "roic", "roic_approx", "net_debt_full", "diluted_shares", "return_on_equity",
    "capex_intensity", "fcf_per_share", "current_ratio", "net_debt_to_ebitda",
    "interest_coverage", "ccc", "dividend_payout", "fcf_payout", "sbc_pct_revenue",
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
    "revenue", "cost_of_revenue", "gross_profit", "operating_income", "interest_expense",
    "pretax_income", "income_tax_expense", "net_income", "eps_diluted",
    "operating_cash_flow", "capital_expenditures", "fcf", "ebitda",
    "depreciation_amortization", "stock_based_compensation",
    "cash_and_equivalents", "marketable_securities", "short_term_debt", "long_term_debt",
    "total_debt", "net_debt", "net_debt_full",
    "current_assets", "current_liabilities", "accounts_receivable", "inventory",
    "accounts_payable", "goodwill", "intangible_assets", "lease_liabilities",
    "total_assets", "total_liabilities", "stockholders_equity",
    "diluted_shares", "basic_shares", "diluted_shares_approx", "invested_capital_approx",
    "dividends_paid_abs", "buybacks_abs", "acquisitions_abs", "shareholder_cash_return",
    # FCF bridge (§5)
    "bridge_net_income", "bridge_da", "bridge_sbc", "bridge_wc_receivables",
    "bridge_wc_inventory", "bridge_wc_payables", "bridge_wc_deferred_rev",
    "wc_change_total", "bridge_capex",
    # ratios
    "revenue_growth_yoy", "gross_margin", "operating_margin", "net_margin",
    "rd_pct_revenue", "sga_pct_revenue",
    "fcf_margin", "fcf_conversion", "cfo_conversion", "capex_intensity", "capex_to_da",
    "debt_to_equity", "equity_ratio", "current_ratio",
    "debt_to_ebitda", "net_debt_to_ebitda", "interest_coverage", "goodwill_pct_assets",
    "return_on_equity", "return_on_assets", "effective_tax_rate", "nopat",
    "roic", "roic_approx",
    "dividend_payout", "fcf_payout", "buyback_pct_fcf", "sbc_pct_revenue", "sbc_vs_buybacks",
    "dso", "dio", "dpo", "ccc",
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
