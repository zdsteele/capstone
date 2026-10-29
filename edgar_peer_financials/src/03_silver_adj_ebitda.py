# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — Adjusted EBITDA from earnings releases  (LLM, incremental)
# MAGIC
# MAGIC Adj. EBITDA is non-GAAP: it is not tagged in 10-K/10-Q XBRL, only printed in
# MAGIC the EX-99.1 press release attached to each Item 2.02 8-K. This notebook sends
# MAGIC each release to `ai_query` and extracts the consolidated, company-defined
# MAGIC Adjusted EBITDA for the quarter (and the full year on Q4 releases).
# MAGIC
# MAGIC - `MERGE` on `accession` — re-runs only send **new** releases to the model.
# MAGIC - Prompt asks for the number *as printed* plus its scale, so the model never
# MAGIC   does arithmetic; scaling to USD happens here.
# MAGIC - `found = false` (e.g. CAT/CMI publish no Adj. EBITDA) is a valid answer —
# MAGIC   notebook 04 falls back to reported EBITDA and flags the source.
# MAGIC
# MAGIC Writes `silver_adj_ebitda`.

# COMMAND ----------

import json

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "engineering_playground_dev")
dbutils.widgets.text("bronze_schema", "bronze_edgar")
dbutils.widgets.text("silver_schema", "silver_edgar")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-5-5")
# Long releases carry the reconciliation table near the end; 150k chars ≈ 40k tokens.
dbutils.widgets.text("max_release_chars", "150000")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])

CATALOG = dbutils.widgets.get("catalog")
B = lambda n: f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}.{n}"
S = lambda n: f"{CATALOG}.{dbutils.widgets.get('silver_schema')}.{n}"
ENDPOINT = dbutils.widgets.get("llm_endpoint")
MAX_CHARS = int(dbutils.widgets.get("max_release_chars"))
FULL = dbutils.widgets.get("mode") == "full"
TARGET = S("silver_adj_ebitda")

# COMMAND ----------

# DBTITLE 1,Prompt
PROMPT = """You extract one non-GAAP metric from a public company's quarterly earnings press release.

Find the company's consolidated ADJUSTED EBITDA for the most recent reported quarter. It may be labelled
"Adjusted EBITDA", "Adj. EBITDA", "Consolidated Adjusted EBITDA", "EBITDA, as adjusted" or a similar
company-defined adjusted EBITDA. Rules:
- Consolidated total only, never a segment figure.
- Actual results only, never guidance / outlook / forecast ranges.
- If the release has no adjusted EBITDA but reports plain EBITDA, return that with "is_adjusted": false.
- If neither exists, return "found": false and nulls.
- Copy numbers exactly as printed: no thousands separators, a leading minus for values shown in
  parentheses. Do not convert units; report the scale the table states.

Reply with ONE JSON object on a single line and nothing else:
{"found": true, "label": "<label as printed>", "is_adjusted": true, "quarter_end": "YYYY-MM-DD", "quarter_value": 123.4, "fiscal_year_end": "YYYY-MM-DD or null", "fiscal_year_value": 456.7, "scale": "units|thousands|millions|billions"}

"quarter_value" is the three-months-ended figure. "fiscal_year_value" is only for a release that also
reports the full fiscal year ending on that quarter (fourth-quarter releases); otherwise null.

PRESS RELEASE:
"""

_RESP_SCHEMA = (
    "found BOOLEAN, label STRING, is_adjusted BOOLEAN, quarter_end STRING, quarter_value DOUBLE, "
    "fiscal_year_end STRING, fiscal_year_value DOUBLE, scale STRING"
)

# COMMAND ----------

# DBTITLE 1,Releases not yet extracted
releases = spark.table(B("bronze_earnings_releases")).filter(F.col("char_len") > 0)
if spark.catalog.tableExists(TARGET) and not FULL:
    releases = releases.join(spark.table(TARGET).select("accession"), "accession", "left_anti")
n_todo = releases.count()
print(f"{n_todo} release(s) to extract with {ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,ai_query → parse → MERGE
if n_todo:
    releases.createOrReplaceTempView("_todo_releases")
    # The prompt goes in as a named parameter, so it never needs SQL quoting.
    raw = spark.sql(f"""
        SELECT cik, accession, to_date(filing_date) AS filing_date,
               ai_query('{ENDPOINT}',
                        concat(:prompt, substr(text, 1, {MAX_CHARS})),
                        modelParameters => named_struct('max_tokens', 400),
                        failOnError => false) AS resp
        FROM _todo_releases
    """, args={"prompt": PROMPT})

    scale = (F.when(F.lower("p.scale") == "thousands", F.lit(1e3))
             .when(F.lower("p.scale") == "millions", F.lit(1e6))
             .when(F.lower("p.scale") == "billions", F.lit(1e9))
             .otherwise(F.lit(1.0)))
    parsed = (
        raw.withColumn("raw_response", F.col("resp.result"))
        .withColumn("error", F.col("resp.errorMessage"))
        .withColumn("p", F.from_json(F.regexp_extract("raw_response", r"(?s)\{.*\}", 0), _RESP_SCHEMA))
        .select(
            "cik", "accession", "filing_date",
            F.coalesce(F.col("p.found"), F.lit(False)).alias("found"),
            F.col("p.label").alias("label"),
            F.col("p.is_adjusted").alias("is_adjusted"),
            F.to_date("p.quarter_end").alias("quarter_end"),
            (F.col("p.quarter_value") * scale).alias("quarter_value_usd"),
            F.to_date(F.expr("nullif(p.fiscal_year_end, 'null')")).alias("fiscal_year_end"),
            (F.col("p.fiscal_year_value") * scale).alias("fiscal_year_value_usd"),
            F.col("p.scale").alias("scale"),
            "raw_response", "error",
            F.lit(ENDPOINT).alias("model"),
            F.current_timestamp().alias("extracted_at"),
        )
    )
    # Materialize once so the LLM is called exactly one time per release.
    parsed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(S("_stg_adj_ebitda"))
    stg = spark.table(S("_stg_adj_ebitda"))
    if not spark.catalog.tableExists(TARGET):
        stg.write.format("delta").saveAsTable(TARGET)
    else:
        stg.createOrReplaceTempView("_stg")
        spark.sql(f"""MERGE INTO {TARGET} t USING _stg s ON t.accession = s.accession
                      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    spark.sql(f"DROP TABLE IF EXISTS {S('_stg_adj_ebitda')}")

# COMMAND ----------

# DBTITLE 1,Summary
summary = (spark.table(TARGET).groupBy("found", "is_adjusted").count()
           .orderBy("found", "is_adjusted").collect())
print([r.asDict() for r in summary])
display(spark.table(TARGET).filter("error IS NOT NULL OR (found AND quarter_value_usd IS NULL)")
        .select("cik", "accession", "label", "error", "raw_response").limit(20))

dbutils.notebook.exit(json.dumps({"status": "ok", "extracted": n_todo}))
