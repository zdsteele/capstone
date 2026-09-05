# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 08 · AI filing intelligence  (Spark job with AI in the middle)
# MAGIC
# MAGIC For every 10-K / 10-Q, feed the narrative sections (MD&A, risk factors,
# MAGIC business) to `ai_query('databricks-meta-llama-3-3-70b-instruct', ...)` and
# MAGIC extract a structured briefing:
# MAGIC
# MAGIC - `executive_summary` — 2-3 sentence plain-English recap
# MAGIC - `revenue_commentary` — what drove revenue
# MAGIC - `risk_themes` — 3-5 short labels
# MAGIC - `management_tone` — optimistic / confident / balanced / cautious / negative
# MAGIC - `notable_items` — one-time items, guidance changes
# MAGIC
# MAGIC Lands `gold_filing_intelligence`, surfaced in the Filing Explorer and via the
# MAGIC agent's `get_filing_intelligence` tool. Idempotent (`MERGE` on accession).

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("max_chars", "40000")     # narrative budget per filing
dbutils.widgets.text("forms", "10-K,10-Q")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LLM = dbutils.widgets.get("llm_endpoint")
MAX_CHARS = int(dbutils.widgets.get("max_chars"))
FORMS = [f.strip() for f in dbutils.widgets.get("forms").split(",")]
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Gather narrative sections per filing
# Keep the analytically dense Items: 1 (business), 1A (risk), 2 & 7 (MD&A).
KEEP = ("Item 1", "Item 1A", "Item 2", "Item 7", "Item 7A")

filings = (
    spark.table(T("silver_filings"))
    .filter(F.col("form").isin(FORMS))
    .select("accession", "cik", "form", "filing_date", "report_date")
)
companies = spark.table(T("silver_companies")).select("cik", "ticker", "name")

secs = (
    spark.table(T("silver_filing_sections"))
    .filter(F.col("section").isin(list(KEEP)))
    .groupBy("accession")
    .agg(F.concat_ws("\n\n", F.collect_list(F.concat_ws(": ", "section", "text"))).alias("narrative"))
)

base = (
    filings.join(secs, "accession", "left")
    .join(companies, "cik")
    .withColumn("narrative", F.substring(F.coalesce("narrative", F.lit("")), 1, MAX_CHARS))
    .filter(F.length("narrative") > 500)
)
print("filings to summarize:", base.count())

# COMMAND ----------

# DBTITLE 1,ai_query — one structured briefing per filing
PROMPT_PREFIX = (
    "You are an equity research analyst. Read the SEC-filing excerpts below and "
    "respond with ONLY a compact JSON object (no markdown, no prose) with keys:\n"
    '  "executive_summary": 2-3 sentences, plain English, what happened this period.\n'
    '  "revenue_commentary": 1-2 sentences on what drove revenue.\n'
    '  "risk_themes": array of 3-5 short risk-theme labels (2-4 words each).\n'
    '  "management_tone": exactly one of "optimistic","confident","balanced","cautious","negative".\n'
    '  "notable_items": 1 sentence on any one-time items / guidance changes / unusual events, else "".\n\n'
)

scored = base.withColumn(
    "prompt",
    F.concat(
        F.lit(PROMPT_PREFIX),
        F.lit("FILING: "), F.col("ticker"), F.lit(" "), F.col("form"),
        F.lit(" filed "), F.col("filing_date").cast("string"),
        F.lit("\n\nEXCERPTS:\n"), F.col("narrative"),
    ),
).withColumn("raw", F.expr(f"ai_query('{LLM}', prompt)"))

# COMMAND ----------

# DBTITLE 1,Parse JSON -> gold_filing_intelligence
schema = StructType([
    StructField("executive_summary", StringType()),
    StructField("revenue_commentary", StringType()),
    StructField("risk_themes", ArrayType(StringType())),
    StructField("management_tone", StringType()),
    StructField("notable_items", StringType()),
])

parsed = (
    scored.withColumn("json_str", F.regexp_extract("raw", r"\{[\s\S]*\}", 0))
    .withColumn("p", F.from_json("json_str", schema))
    .select(
        "accession", "cik", "ticker", "name", "form", "filing_date", "report_date",
        F.col("p.executive_summary").alias("executive_summary"),
        F.col("p.revenue_commentary").alias("revenue_commentary"),
        F.col("p.risk_themes").alias("risk_themes"),
        F.lower(F.col("p.management_tone")).alias("management_tone"),
        F.col("p.notable_items").alias("notable_items"),
        F.lit(LLM).alias("model"),
        F.current_timestamp().alias("generated_at"),
        F.col("raw"),
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {T('gold_filing_intelligence')} (
        accession STRING, cik STRING, ticker STRING, name STRING, form STRING,
        filing_date DATE, report_date DATE,
        executive_summary STRING, revenue_commentary STRING, risk_themes ARRAY<STRING>,
        management_tone STRING, notable_items STRING,
        model STRING, generated_at TIMESTAMP, raw STRING
    ) USING DELTA
    """
)
parsed.createOrReplaceTempView("_new_intel")
spark.sql(
    f"""
    MERGE INTO {T('gold_filing_intelligence')} t
    USING _new_intel s ON t.accession = s.accession
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

out = spark.table(T("gold_filing_intelligence"))
print("gold_filing_intelligence:", out.count())
display(
    out.select("ticker", "form", "filing_date", "management_tone", "executive_summary")
    .orderBy(F.col("filing_date").desc())
    .limit(10)
)

# COMMAND ----------

n_bad = out.filter(F.col("executive_summary").isNull()).count()
dbutils.notebook.exit(
    json.dumps({"status": "ok", "rows": out.count(), "unparsed": n_bad})
)
