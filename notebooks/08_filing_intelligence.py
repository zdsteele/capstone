# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 08 · AI filing intelligence  (Spark job with AI in the middle)
# MAGIC
# MAGIC Three `ai_query` passes, each `MERGE`-idempotent:
# MAGIC
# MAGIC 1. **`gold_filing_intelligence`** — one briefing per 10-K / 10-Q
# MAGIC    (executive_summary, revenue_commentary, risk_themes, management_tone,
# MAGIC    notable_items) from Items 1 / 1A / 2 / 7 / 7A.
# MAGIC 2. **`gold_8k_events`** — one row per 8-K: event_type, event_summary,
# MAGIC    materiality, key_figures. 8-Ks announce material events between the
# MAGIC    periodic reports; this is the "8-K agent" (analyst spec §2 timeline).
# MAGIC 3. **`gold_business_profile`** — one row per company from the latest 10-K
# MAGIC    Item 1: segments, geographies, customers/concentration, competitors,
# MAGIC    revenue model, sector, cyclicality, capital intensity (analyst spec §2).

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

# COMMAND ----------

# DBTITLE 1,gold_8k_events — one row per 8-K (the "8-K agent")
_8K = (
    spark.table(T("silver_filings")).filter(F.col("form") == "8-K")
    .select("accession", "cik", "filing_date", "report_date")
    .join(companies, "cik")
)
_8k_secs = (
    spark.table(T("silver_filing_sections"))
    .groupBy("accession")
    .agg(F.substring(F.concat_ws("\n\n", F.collect_list(F.concat_ws(": ", "section", "text"))), 1, 20000).alias("body"))
)
_8k_ex = (
    spark.table(T("silver_exhibits"))
    .filter(F.col("doc_type").rlike("(?i)ex-?99"))
    .groupBy("accession").agg(F.concat_ws(", ", F.collect_list("description")).alias("press_releases"))
)
_8k_base = (
    _8K.join(_8k_secs, "accession", "left").join(_8k_ex, "accession", "left")
    .withColumn("body", F.coalesce("body", F.lit("")))
    .filter(F.length("body") > 120)
)

_8K_PROMPT = (
    "You classify SEC Form 8-K filings. Respond with ONLY one compact JSON object, "
    "single-line string values, keys:\n"
    '  "event_type": one of "earnings","m&a","executive_change","financing","guidance",'
    '"restructuring","legal","dividend_buyback","product_operations","impairment","other".\n'
    '  "event_summary": 2-3 plain-English sentences on what was announced.\n'
    '  "materiality": one of "high","medium","low".\n'
    '  "key_figures": array of 0-5 short strings with the dollar amounts / percentages / dates mentioned.\n\n'
)
_8k_scored = _8k_base.withColumn(
    "prompt",
    F.concat(F.lit(_8K_PROMPT), F.lit("8-K: "), F.col("ticker"),
             F.lit(" filed "), F.col("filing_date").cast("string"),
             F.lit("\nEXHIBITS: "), F.coalesce("press_releases", F.lit("none")),
             F.lit("\n\nTEXT:\n"), F.col("body")),
).withColumn("raw", F.expr(f"ai_query('{LLM}', prompt)"))

_8k_schema = StructType([
    StructField("event_type", StringType()),
    StructField("event_summary", StringType()),
    StructField("materiality", StringType()),
    StructField("key_figures", ArrayType(StringType())),
])
_8k_parsed = (
    _8k_scored.withColumn("json_str", F.regexp_extract("raw", r"\{[\s\S]*\}", 0))
    .withColumn("p", F.from_json("json_str", _8k_schema))
    .select("accession", "cik", "ticker", "name", "filing_date", "report_date",
            F.lower("p.event_type").alias("event_type"),
            F.col("p.event_summary").alias("event_summary"),
            F.lower("p.materiality").alias("materiality"),
            F.col("p.key_figures").alias("key_figures"),
            F.lit(LLM).alias("model"), F.current_timestamp().alias("generated_at"))
)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T('gold_8k_events')} (
        accession STRING, cik STRING, ticker STRING, name STRING,
        filing_date DATE, report_date DATE,
        event_type STRING, event_summary STRING, materiality STRING,
        key_figures ARRAY<STRING>, model STRING, generated_at TIMESTAMP
    ) USING DELTA
""")
_8k_parsed.createOrReplaceTempView("_new_8k")
spark.sql(f"""
    MERGE INTO {T('gold_8k_events')} t USING _new_8k s ON t.accession = s.accession
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
""")
n_8k = spark.table(T("gold_8k_events")).count()
print("gold_8k_events:", n_8k)

# COMMAND ----------

# DBTITLE 1,gold_business_profile — one row per company from the latest 10-K Item 1
from pyspark.sql import Window as _W

_latest_10k = (
    spark.table(T("silver_filings")).filter(F.col("form") == "10-K")
    .withColumn("rn", F.row_number().over(_W.partitionBy("cik").orderBy(F.col("filing_date").desc())))
    .filter("rn = 1").select("accession", "cik", "filing_date")
)
_biz_secs = (
    spark.table(T("silver_filing_sections"))
    .filter(F.col("section").isin("Item 1", "Item 1A", "Item 7"))
    .groupBy("accession")
    .agg(F.substring(F.concat_ws("\n\n", F.collect_list("text")), 1, 30000).alias("body"))
)
_biz_base = (
    _latest_10k.join(_biz_secs, "accession").join(companies, "cik")
    .filter(F.length("body") > 500)
)
_BIZ_PROMPT = (
    "You are an equity analyst profiling a company from its 10-K. Respond with ONLY "
    "one compact JSON object, single-line string values, keys:\n"
    '  "primary_business": 1 sentence.\n'
    '  "revenue_model": one of "recurring","transactional","mixed".\n'
    '  "segments": array of reportable segment names.\n'
    '  "geographies": array of key regions/countries.\n'
    '  "key_customers": array (named customers or channels), else [].\n'
    '  "customer_concentration": 1 sentence, or "not disclosed".\n'
    '  "competitors": array of named competitors.\n'
    '  "sector": one of "Technology","Semiconductors","SaaS","Banking","Insurance",'
    '"Energy","Industrial","Consumer/Retail","Healthcare","Utilities","Telecom","REIT","Other".\n'
    '  "cyclicality": one of "low","moderate","high".\n'
    '  "capital_intensity": one of "low","moderate","high".\n'
    '  "regulatory_exposure": 1 sentence.\n'
    '  "economic_drivers": array of 2-5 short phrases.\n\n'
)
_biz_scored = _biz_base.withColumn(
    "prompt", F.concat(F.lit(_BIZ_PROMPT), F.lit("COMPANY: "), F.col("ticker"), F.lit(" "),
                       F.col("name"), F.lit("\n\n10-K BUSINESS SECTION:\n"), F.col("body")),
).withColumn("raw", F.expr(f"ai_query('{LLM}', prompt)"))

_biz_schema = StructType([
    StructField("primary_business", StringType()),
    StructField("revenue_model", StringType()),
    StructField("segments", ArrayType(StringType())),
    StructField("geographies", ArrayType(StringType())),
    StructField("key_customers", ArrayType(StringType())),
    StructField("customer_concentration", StringType()),
    StructField("competitors", ArrayType(StringType())),
    StructField("sector", StringType()),
    StructField("cyclicality", StringType()),
    StructField("capital_intensity", StringType()),
    StructField("regulatory_exposure", StringType()),
    StructField("economic_drivers", ArrayType(StringType())),
])
_biz_parsed = (
    _biz_scored.withColumn("json_str", F.regexp_extract("raw", r"\{[\s\S]*\}", 0))
    .withColumn("p", F.from_json("json_str", _biz_schema))
    .select("cik", "ticker", "name", "accession", F.col("filing_date").alias("source_10k_date"),
            *[F.col(f"p.{f.name}").alias(f.name) for f in _biz_schema.fields],
            F.lit(LLM).alias("model"), F.current_timestamp().alias("generated_at"))
)
_biz_parsed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_business_profile")
)
n_biz = spark.table(T("gold_business_profile")).count()
print("gold_business_profile:", n_biz)

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps({"status": "ok", "intelligence_rows": out.count(), "unparsed": n_bad,
                "gold_8k_events": n_8k, "gold_business_profile": n_biz})
)
