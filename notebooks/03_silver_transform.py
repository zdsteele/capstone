# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 · Silver — clean & normalize
# MAGIC
# MAGIC | Table | Built from | Notes |
# MAGIC |---|---|---|
# MAGIC | `silver_companies` | `bronze_company_submissions` | typed dimension, CIK/ticker/name |
# MAGIC | `silver_filings` | `bronze_filings` | dedup on accession, typed dates |
# MAGIC | `silver_financial_facts` | `bronze_xbrl_facts` (companyfacts JSON) | exploded to one row per observation; partitioned by fiscal year/quarter, ZORDER cik |
# MAGIC | `silver_financial_periods` | `silver_financial_facts` | distinct reporting periods |
# MAGIC | `silver_filing_sections` | `bronze_filing_text` (primary_html) | HTML → 10-K/10-Q Item sections |
# MAGIC | `silver_exhibits` | `bronze_filing_text` (submission_txt) | SGML `<DOCUMENT>` manifest |
# MAGIC | `silver_filing_text_chunks` | `silver_filing_sections` | 400/60 chunks, CDF enabled (feeds Vector Search) |

# COMMAND ----------

# DBTITLE 1,Setup
import json
import os
import sys

_repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from lib.edgar_parse import (
    flatten_company_facts,
    parse_filing_html,
    parse_submission_documents,
    chunk_text,
)

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zachy_zacharysteele8")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,silver_companies
subs = spark.table(T("bronze_company_submissions"))
silver_companies = (
    subs.select(
        F.lpad(F.col("cik"), 10, "0").alias("cik"),
        F.coalesce(F.col("ticker"), F.split(F.col("tickers"), ",").getItem(0)).alias("ticker"),
        F.col("entity_name").alias("name"),
        F.col("sic"),
        F.col("sic_description"),
        F.col("fiscal_year_end"),
        F.col("exchanges"),
        F.col("ingested_at"),
    )
    .dropDuplicates(["cik"])
)
silver_companies.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_companies"))
print("silver_companies:", silver_companies.count())

# COMMAND ----------

# DBTITLE 1,silver_filings
filings = spark.table(T("bronze_filings"))
silver_filings = (
    filings.withColumn("cik", F.lpad(F.col("cik"), 10, "0"))
    .withColumn("filing_date", F.to_date("filing_date"))
    .withColumn("report_date", F.to_date("report_date"))
    .withColumn("fiscal_year", F.year("report_date"))
    .withColumn("fiscal_quarter", F.quarter("report_date"))
    .withColumn(
        "sec_url",
        F.concat(
            F.lit("https://www.sec.gov/Archives/edgar/data/"),
            F.expr("cast(cast(cik as bigint) as string)"),
            F.lit("/"),
            F.regexp_replace("accession", "-", ""),
            F.lit("/"),
            F.col("primary_document"),
        ),
    )
    .dropDuplicates(["accession"])
)
# quality gate: keys present
silver_filings = silver_filings.filter(
    F.col("cik").isNotNull() & F.col("accession").isNotNull()
)
silver_filings.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_filings"))
print("silver_filings:", silver_filings.count())

# COMMAND ----------

# DBTITLE 1,silver_financial_facts  (explode companyfacts JSON)
# Driver-side flatten: a handful of large JSON blobs (one per CIK). At >100 CIKs,
# swap this loop for a mapInPandas over bronze_xbrl_facts.
facts_src = spark.table(T("bronze_xbrl_facts")).select("cik", "companyfacts_json").collect()

all_rows = []
for r in facts_src:
    try:
        payload = json.loads(r["companyfacts_json"])
    except Exception as exc:
        print("  bad companyfacts json for", r["cik"], exc)
        continue
    all_rows.extend(flatten_company_facts(payload, r["cik"]))
print("raw fact rows:", len(all_rows))

facts_df = spark.createDataFrame(all_rows)

known_filings = spark.table(T("silver_filings")).select("cik", "accession").dropDuplicates()

silver_financial_facts = (
    facts_df.withColumn("period_start", F.to_date("period_start"))
    .withColumn("period_end", F.to_date("period_end"))
    .withColumn(
        "fiscal_quarter",
        F.when(F.col("fiscal_period") == "FY", F.lit(4))
        .when(F.col("fiscal_period").startswith("Q"), F.regexp_extract("fiscal_period", r"Q(\d)", 1).cast("int"))
        .otherwise(F.quarter("period_end")),
    )
    # period_type — companyfacts reports the same concept at several durations for
    # the same fiscal_period (3-month, 6/9-month YTD, 12-month). Classify by the
    # start..end span so Gold can pick the right one (a "Q3" number should be the
    # 3-month value, not the 9-month YTD).
    .withColumn("duration_days", F.datediff("period_end", "period_start"))
    .withColumn(
        "period_type",
        F.when(F.col("period_start").isNull(), F.lit("instant"))       # balance-sheet
        .when(F.col("duration_days") <= 110, F.lit("quarter"))         # ~90-92
        .when(F.col("duration_days") <= 200, F.lit("half"))            # ~180
        .when(F.col("duration_days") <= 300, F.lit("ytd9"))            # ~270
        .otherwise(F.lit("annual")),                                   # ~365
    )
    # quality gates: non-null keys + numeric present + referential to a known filing
    .filter(
        F.col("cik").isNotNull()
        & F.col("period_end").isNotNull()
        & F.col("value").isNotNull()
        & F.col("accession").isNotNull()
    )
    .join(known_filings, ["cik", "accession"], "left_semi")
    .dropDuplicates(["cik", "accession", "taxonomy", "concept", "unit", "period_start", "period_end"])
)

(
    silver_financial_facts.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("fiscal_year", "fiscal_quarter")
    .saveAsTable(T("silver_financial_facts"))
)
try:
    spark.sql(f"OPTIMIZE {T('silver_financial_facts')} ZORDER BY (cik, concept)")
except Exception as exc:
    print("  OPTIMIZE skipped:", exc)
print("silver_financial_facts:", spark.table(T("silver_financial_facts")).count())

# COMMAND ----------

# DBTITLE 1,silver_financial_periods
silver_periods = (
    spark.table(T("silver_financial_facts"))
    .select("cik", "fiscal_year", "fiscal_period", "fiscal_quarter", "period_start", "period_end", "form", "accession")
    .dropDuplicates(["cik", "accession", "period_start", "period_end"])
)
silver_periods.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_financial_periods"))
print("silver_financial_periods:", silver_periods.count())

# COMMAND ----------

# DBTITLE 1,silver_filing_sections  (HTML -> Item sections)
html_rows = (
    spark.table(T("bronze_filing_text"))
    .filter(F.col("doc_kind") == "primary_html")
    .select("cik", "accession", "document", "text")
    .collect()
)

section_rows = []
for r in html_rows:
    for i, sec in enumerate(parse_filing_html(r["text"])):
        section_rows.append(
            {
                "cik": str(r["cik"]).zfill(10),
                "accession": r["accession"],
                "document": r["document"],
                "section_index": i,
                "section": sec["section"],
                "heading": sec["heading"],
                "text": sec["text"],
                "char_len": len(sec["text"]),
            }
        )
print("section rows:", len(section_rows))

silver_sections = spark.createDataFrame(section_rows) if section_rows else spark.createDataFrame(
    [], "cik string, accession string, document string, section_index int, section string, heading string, text string, char_len long"
)
silver_sections.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_filing_sections"))
print("silver_filing_sections:", silver_sections.count())

# COMMAND ----------

# DBTITLE 1,silver_exhibits  (submission .txt SGML manifest)
sub_rows = (
    spark.table(T("bronze_filing_text"))
    .filter(F.col("doc_kind") == "submission_txt")
    .select("cik", "accession", "text")
    .collect()
)
exhibit_rows = []
for r in sub_rows:
    for d in parse_submission_documents(r["text"]):
        exhibit_rows.append(
            {
                "cik": str(r["cik"]).zfill(10),
                "accession": r["accession"],
                "sequence": d["sequence"],
                "doc_type": d["doc_type"],
                "filename": d["filename"],
                "description": d["description"],
            }
        )
print("exhibit rows:", len(exhibit_rows))
silver_exhibits = spark.createDataFrame(exhibit_rows) if exhibit_rows else spark.createDataFrame(
    [], "cik string, accession string, sequence string, doc_type string, filename string, description string"
)
silver_exhibits.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_exhibits"))
print("silver_exhibits:", silver_exhibits.count())

# COMMAND ----------

# DBTITLE 1,silver_filing_text_chunks  (CDF enabled -> feeds Vector Search)
chunk_rows = []
for r in section_rows:
    for j, ch in enumerate(chunk_text(r["text"], 400, 60)):
        chunk_rows.append(
            {
                "chunk_id": f"{r['accession']}::{r['section_index']}::{j}",
                "cik": r["cik"],
                "accession": r["accession"],
                "section": r["section"],
                "heading": r["heading"],
                "chunk_index": j,
                "chunk_text": ch,
            }
        )
print("chunk rows:", len(chunk_rows))

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {T('silver_filing_text_chunks')} (
        chunk_id STRING, cik STRING, accession STRING, section STRING,
        heading STRING, chunk_index INT, chunk_text STRING
    ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """
)
if chunk_rows:
    (
        spark.createDataFrame(chunk_rows)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(T("silver_filing_text_chunks"))
    )
print("silver_filing_text_chunks:", spark.table(T("silver_filing_text_chunks")).count())

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "ok",
            "silver_companies": spark.table(T("silver_companies")).count(),
            "silver_filings": spark.table(T("silver_filings")).count(),
            "silver_financial_facts": spark.table(T("silver_financial_facts")).count(),
            "silver_filing_sections": spark.table(T("silver_filing_sections")).count(),
            "silver_filing_text_chunks": spark.table(T("silver_filing_text_chunks")).count(),
        }
    )
)
