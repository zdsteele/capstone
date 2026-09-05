# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 · Gold — application-ready financial marts
# MAGIC
# MAGIC Maps raw us-gaap concepts to friendly metric names (priority-ordered
# MAGIC fallbacks per metric), picks one value per company/period/metric, then
# MAGIC builds the 7 gold marts the app + agent read. Referential check against
# MAGIC `silver_companies` before write (proposal's promotion rule).

# COMMAND ----------

# DBTITLE 1,Setup & concept map
import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zachy_zacharysteele8")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window

# metric -> ordered candidate us-gaap concepts (index = priority, lower is better)
CONCEPT_MAP = {
    "revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax"],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "gross_profit": ["GrossProfit"],
    "operating_expenses": ["OperatingExpenses", "CostsAndExpenses"],
    "research_development": ["ResearchAndDevelopmentExpense"],
    "sga_expense": ["SellingGeneralAndAdministrativeExpense", "GeneralAndAdministrativeExpense"],
    "operating_income": ["OperatingIncomeLoss"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "eps_basic": ["EarningsPerShareBasic"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
    "stockholders_equity": ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
    "cash_and_equivalents": ["CashAndCashEquivalentsAtCarryingValue"],
    "long_term_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "operating_cash_flow": ["NetCashProvidedByUsedInOperatingActivities", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
    "investing_cash_flow": ["NetCashProvidedByUsedInInvestingActivities"],
    "financing_cash_flow": ["NetCashProvidedByUsedInFinancingActivities"],
    "capital_expenditures": ["PaymentsToAcquirePropertyPlantAndEquipment"],
}
STATEMENT = {
    **{m: "income" for m in ["revenue", "cost_of_revenue", "gross_profit", "operating_expenses", "research_development", "sga_expense", "operating_income", "net_income", "eps_basic", "eps_diluted"]},
    **{m: "balance" for m in ["total_assets", "total_liabilities", "stockholders_equity", "cash_and_equivalents", "long_term_debt"]},
    **{m: "cash_flow" for m in ["operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "capital_expenditures"]},
}

map_rows = [
    (metric, concept, prio, STATEMENT[metric])
    for metric, concepts in CONCEPT_MAP.items()
    for prio, concept in enumerate(concepts)
]
metric_map = spark.createDataFrame(map_rows, ["metric", "concept", "priority", "statement"])

# COMMAND ----------

# DBTITLE 1,Resolve one value per (cik, period, metric)
facts = spark.table(T("silver_financial_facts"))
companies = spark.table(T("silver_companies")).select("cik", "ticker", "name")

resolved = (
    facts.join(metric_map, "concept")
    .filter(F.col("fiscal_period").isNotNull())
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("cik", "fiscal_year", "fiscal_period", "metric").orderBy(
                F.col("priority").asc(), F.col("filed").desc()
            )
        ),
    )
    .filter(F.col("rn") == 1)
    .join(companies, "cik", "left_semi")  # referential check
    .select("cik", "fiscal_year", "fiscal_period", "fiscal_quarter", "form",
            "period_start", "period_end", "metric", "statement", "value")
)
# Materialize once as a Delta table instead of .cache() — serverless compute
# rejects PERSIST/CACHE. Downstream marts read from this.
(resolved.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(T("silver_resolved_metrics")))
resolved = spark.table(T("silver_resolved_metrics"))
print("resolved metric-values:", resolved.count())

# COMMAND ----------

# DBTITLE 1,gold_company_financials  (wide)
wide = (
    resolved.groupBy("cik", "fiscal_year", "fiscal_period", "fiscal_quarter", "form", "period_end")
    .pivot("metric", list(CONCEPT_MAP.keys()))
    .agg(F.first("value"))
    .join(companies, "cik")
)
wide.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_company_financials")
)
print("gold_company_financials:", wide.count())

# COMMAND ----------

# DBTITLE 1,gold_revenue_history  (with YoY / QoQ)
rev = (
    wide.select("cik", "name", "ticker", "fiscal_year", "fiscal_period", "period_end", "revenue")
    .filter(F.col("revenue").isNotNull())
)
w_time = Window.partitionBy("cik").orderBy("period_end")
w_year = Window.partitionBy("cik", "fiscal_period").orderBy("fiscal_year")
rev_hist = (
    rev.withColumn("prev_q_revenue", F.lag("revenue").over(w_time))
    .withColumn("prev_y_revenue", F.lag("revenue").over(w_year))
    .withColumn("qoq_pct", F.round((F.col("revenue") - F.col("prev_q_revenue")) / F.col("prev_q_revenue") * 100, 2))
    .withColumn("yoy_pct", F.round((F.col("revenue") - F.col("prev_y_revenue")) / F.col("prev_y_revenue") * 100, 2))
)
rev_hist.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_revenue_history")
)
print("gold_revenue_history:", rev_hist.count())

# COMMAND ----------

# DBTITLE 1,gold_{income_statement,balance_sheet,cash_flow}_metrics  (long)
for stmt, table in [
    ("income", "gold_income_statement_metrics"),
    ("balance", "gold_balance_sheet_metrics"),
    ("cash_flow", "gold_cash_flow_metrics"),
]:
    df = (
        resolved.filter(F.col("statement") == stmt)
        .join(companies, "cik")
        .select("cik", "name", "ticker", "fiscal_year", "fiscal_period", "fiscal_quarter",
                "period_start", "period_end", "form", "metric", "value")
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(table))
    print(f"{table}:", df.count())

# COMMAND ----------

# DBTITLE 1,gold_filing_activity
activity = (
    spark.table(T("silver_filings"))
    .join(companies, "cik")
    .groupBy("cik", "name", "ticker", "form", "fiscal_year")
    .agg(F.count("*").alias("filing_count"), F.max("filing_date").alias("latest_filing_date"))
)
activity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_filing_activity")
)
print("gold_filing_activity:", activity.count())

# COMMAND ----------

# DBTITLE 1,gold_company_comparisons  (latest value per metric, cross-company)
w_latest = Window.partitionBy("cik", "metric").orderBy(F.col("period_end").desc())
comparisons = (
    resolved.withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .join(companies, "cik")
    .select("cik", "name", "ticker", "metric", "statement",
            F.col("value").alias("latest_value"),
            F.col("period_end").alias("latest_period_end"),
            "fiscal_year", "fiscal_period")
)
comparisons.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_company_comparisons")
)
print("gold_company_comparisons:", comparisons.count())

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "ok",
            "gold_company_financials": wide.count(),
            "gold_revenue_history": rev_hist.count(),
            "gold_filing_activity": activity.count(),
            "gold_company_comparisons": comparisons.count(),
        }
    )
)
