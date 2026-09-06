# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 12 · Filing-language changes  (analyst spec §16)
# MAGIC
# MAGIC For every 10-K / 10-Q, pair it with the company's **previous same-form**
# MAGIC filing and ask `ai_query` what *materially* changed in the risk factors
# MAGIC (Item 1A) and MD&A (Item 7) — new risks, removed risks, tone shift,
# MAGIC changes around demand / pricing / liquidity / litigation / AI / going
# MAGIC concern. Boilerplate re-wordings are ignored. Lands
# MAGIC `gold_filing_language_changes` (one row per filing that has a predecessor),
# MAGIC `MERGE`-idempotent on accession. Read by the agent's
# MAGIC `get_filing_changes` tool.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("section_chars", "14000")   # per section, per side

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LLM = dbutils.widgets.get("llm_endpoint")
SC = int(dbutils.widgets.get("section_chars"))
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Pair each filing with its previous same-form filing
filings = (
    spark.table(T("silver_filings"))
    .filter(F.col("form").isin("10-K", "10-Q"))
    .select("accession", "cik", "form", "filing_date", "report_date")
    .withColumn("prev_accession",
                F.lag("accession").over(Window.partitionBy("cik", "form").orderBy("filing_date")))
    .withColumn("prev_filing_date",
                F.lag("filing_date").over(Window.partitionBy("cik", "form").orderBy("filing_date")))
    .filter(F.col("prev_accession").isNotNull())
)
companies = spark.table(T("silver_companies")).select("cik", "ticker", "name")

# section text: Item 1A + Item 7, per accession
sec = (
    spark.table(T("silver_filing_sections"))
    .filter(F.col("section").isin("Item 1A", "Item 7"))
    .groupBy("accession")
    .pivot("section", ["Item 1A", "Item 7"])
    .agg(F.first("text"))
    .withColumnRenamed("Item 1A", "risk_txt")
    .withColumnRenamed("Item 7", "mda_txt")
)

paired = (
    filings.join(companies, "cik")
    .join(sec.withColumnRenamed("accession", "accession"), "accession", "left")
    .join(sec.select(F.col("accession").alias("prev_accession"),
                     F.col("risk_txt").alias("prev_risk_txt"),
                     F.col("mda_txt").alias("prev_mda_txt")),
          "prev_accession", "left")
    .filter((F.length(F.coalesce("risk_txt", F.lit(""))) > 400) |
            (F.length(F.coalesce("mda_txt", F.lit(""))) > 400))
)
print("filing pairs to diff:", paired.count())

# COMMAND ----------

# DBTITLE 1,ai_query — what materially changed
PROMPT = (
    "You compare two consecutive SEC filings from the same company and report "
    "what MATERIALLY changed. Ignore boilerplate re-wordings. Respond with ONLY "
    "one compact JSON object, single-line string values, keys:\n"
    '  "change_summary": 2-4 plain-English sentences on the substantive changes.\n'
    '  "new_risks": array of short labels for risk factors added since the prior filing.\n'
    '  "removed_risks": array of short labels for risk factors dropped.\n'
    '  "escalated_topics": array from {demand,competition,pricing,customers,liquidity,'
    'debt,supply,regulation,litigation,AI,cybersecurity,restructuring,geography,'
    'going_concern} that got materially more prominent/negative.\n'
    '  "tone_shift": one of "more_positive","unchanged","more_cautious","more_negative".\n'
    '  "materiality": one of "high","medium","low".\n\n'
)
scored = paired.withColumn(
    "prompt",
    F.concat(
        F.lit(PROMPT),
        F.lit("COMPANY: "), F.col("ticker"), F.lit("  "), F.col("form"),
        F.lit("\nPRIOR filing "), F.col("prev_filing_date").cast("string"),
        F.lit("  ->  CURRENT filing "), F.col("filing_date").cast("string"),
        F.lit("\n\n--- PRIOR RISK FACTORS ---\n"), F.substring(F.coalesce("prev_risk_txt", F.lit("")), 1, SC),
        F.lit("\n\n--- CURRENT RISK FACTORS ---\n"), F.substring(F.coalesce("risk_txt", F.lit("")), 1, SC),
        F.lit("\n\n--- PRIOR MD&A ---\n"), F.substring(F.coalesce("prev_mda_txt", F.lit("")), 1, SC),
        F.lit("\n\n--- CURRENT MD&A ---\n"), F.substring(F.coalesce("mda_txt", F.lit("")), 1, SC),
    ),
).withColumn("raw", F.expr(f"ai_query('{LLM}', prompt)"))

schema = StructType([
    StructField("change_summary", StringType()),
    StructField("new_risks", ArrayType(StringType())),
    StructField("removed_risks", ArrayType(StringType())),
    StructField("escalated_topics", ArrayType(StringType())),
    StructField("tone_shift", StringType()),
    StructField("materiality", StringType()),
])
parsed = (
    scored.withColumn("json_str", F.regexp_extract("raw", r"\{[\s\S]*\}", 0))
    .withColumn("p", F.from_json("json_str", schema))
    .select(
        "accession", "prev_accession", "cik", "ticker", "name", "form",
        "filing_date", "prev_filing_date",
        F.col("p.change_summary").alias("change_summary"),
        F.col("p.new_risks").alias("new_risks"),
        F.col("p.removed_risks").alias("removed_risks"),
        F.col("p.escalated_topics").alias("escalated_topics"),
        F.lower("p.tone_shift").alias("tone_shift"),
        F.lower("p.materiality").alias("materiality"),
        F.lit(LLM).alias("model"), F.current_timestamp().alias("generated_at"),
    )
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T('gold_filing_language_changes')} (
        accession STRING, prev_accession STRING, cik STRING, ticker STRING, name STRING,
        form STRING, filing_date DATE, prev_filing_date DATE,
        change_summary STRING, new_risks ARRAY<STRING>, removed_risks ARRAY<STRING>,
        escalated_topics ARRAY<STRING>, tone_shift STRING, materiality STRING,
        model STRING, generated_at TIMESTAMP
    ) USING DELTA
""")
parsed.createOrReplaceTempView("_new_diffs")
spark.sql(f"""
    MERGE INTO {T('gold_filing_language_changes')} t USING _new_diffs s
      ON t.accession = s.accession
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
""")
n = spark.table(T("gold_filing_language_changes")).count()
n_bad = spark.table(T("gold_filing_language_changes")).filter(F.col("change_summary").isNull()).count()
print(f"gold_filing_language_changes: {n} rows, {n_bad} unparsed")

dbutils.notebook.exit(json.dumps({"status": "ok", "rows": n, "unparsed": n_bad}))
