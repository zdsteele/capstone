# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 · Silver — clean & normalize  (distributed — scales to the full universe)
# MAGIC
# MAGIC | Table | Built from | Notes |
# MAGIC |---|---|---|
# MAGIC | `silver_companies` | `bronze_company_submissions` | typed dimension, CIK/ticker/name |
# MAGIC | `silver_filings` | `bronze_filings` | dedup on accession, typed dates |
# MAGIC | `silver_financial_facts` | `bronze_xbrl_facts` (companyfacts JSON) | **`mapInPandas`** explode → one row per observation; partitioned by fiscal year/quarter |
# MAGIC | `silver_financial_periods` | `silver_financial_facts` | distinct reporting periods |
# MAGIC | `silver_filing_sections` | `bronze_filing_text` (primary_html) | **`mapInPandas`** HTML → Item sections |
# MAGIC | `silver_exhibits` | `bronze_filing_text` (submission_txt) | **`mapInPandas`** SGML `<DOCUMENT>` manifest |
# MAGIC | `silver_filing_text_chunks` | `silver_filing_sections` | **`mapInPandas`** 1100/150 chunks, CDF enabled |
# MAGIC
# MAGIC The parse helpers in `lib/edgar_parse.py` are pure-Python (only `re`) — they
# MAGIC run inside the `mapInPandas` UDFs on executors via `addPyFile`, so nothing
# MAGIC is `collect()`-ed to the driver. This is what lets it handle ~500 companies
# MAGIC × years of filings without OOMing.

# COMMAND ----------

# DBTITLE 1,Setup
import json
import os
import sys

_repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# ship the pure-Python parser to executors for the mapInPandas UDFs
_edgar_parse_path = os.path.join(_repo_root, "lib", "edgar_parse.py")
spark.sparkContext.addPyFile(_edgar_parse_path)

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("shuffle_partitions", "200")   # bump for the full universe
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

spark.conf.set("spark.sql.shuffle.partitions", dbutils.widgets.get("shuffle_partitions"))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType,
)


def _import_edgar_parse():
    """Import the parser whether it's on the path as `lib.edgar_parse` (driver)
    or as a top-level `edgar_parse` module (executor, via addPyFile)."""
    try:
        from lib import edgar_parse as ep
    except Exception:
        import edgar_parse as ep
    return ep

# COMMAND ----------

# DBTITLE 1,silver_companies
subs = spark.table(T("bronze_company_submissions"))
silver_companies = subs.select(
    F.lpad(F.col("cik"), 10, "0").alias("cik"),
    F.coalesce(F.col("ticker"), F.split(F.col("tickers"), ",").getItem(0)).alias("ticker"),
    F.col("entity_name").alias("name"),
    F.col("sic"),
    F.col("sic_description"),
    F.col("fiscal_year_end"),
    F.col("exchanges"),
    F.col("ingested_at"),
).dropDuplicates(["cik"])
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
            F.expr("cast(cast(cik as bigint) as string)"), F.lit("/"),
            F.regexp_replace("accession", "-", ""), F.lit("/"),
            F.col("primary_document"),
        ),
    )
    .dropDuplicates(["accession"])
    .filter(F.col("cik").isNotNull() & F.col("accession").isNotNull())
)
silver_filings.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_filings"))
print("silver_filings:", silver_filings.count())

# COMMAND ----------

# DBTITLE 1,silver_financial_facts  (mapInPandas explode of companyfacts JSON)
_FACTS_SCHEMA = StructType([
    StructField("cik", StringType()),
    StructField("entity_name", StringType()),
    StructField("taxonomy", StringType()),
    StructField("concept", StringType()),
    StructField("label", StringType()),
    StructField("unit", StringType()),
    StructField("period_start", StringType()),
    StructField("period_end", StringType()),
    StructField("value", DoubleType()),
    StructField("accession", StringType()),
    StructField("fiscal_year", LongType()),
    StructField("fiscal_period", StringType()),
    StructField("form", StringType()),
    StructField("frame", StringType()),
    StructField("filed", StringType()),
])
_FACT_COLS = [f.name for f in _FACTS_SCHEMA.fields]


def _flatten_facts(iterator):
    import json as _json
    import pandas as pd
    ep = _import_edgar_parse()
    for pdf in iterator:
        rows = []
        for _, r in pdf.iterrows():
            try:
                payload = _json.loads(r["companyfacts_json"])
            except Exception:
                continue
            rows.extend(ep.flatten_company_facts(payload, r["cik"]))
        yield pd.DataFrame(rows, columns=_FACT_COLS) if rows else pd.DataFrame(columns=_FACT_COLS)


n_ciks = spark.table(T("bronze_xbrl_facts")).count()
facts_raw = (
    spark.table(T("bronze_xbrl_facts"))
    .select("cik", "companyfacts_json")
    .repartition(max(8, min(256, n_ciks)))          # one big JSON blob per CIK — spread them
    .mapInPandas(_flatten_facts, schema=_FACTS_SCHEMA)
)

known_filings = spark.table(T("silver_filings")).select("cik", "accession").dropDuplicates()

silver_financial_facts = (
    facts_raw.withColumn("period_start", F.to_date("period_start"))
    .withColumn("period_end", F.to_date("period_end"))
    .withColumn(
        "fiscal_quarter",
        F.when(F.col("fiscal_period") == "FY", F.lit(4))
        .when(F.col("fiscal_period").startswith("Q"), F.regexp_extract("fiscal_period", r"Q(\d)", 1).cast("int"))
        .otherwise(F.quarter("period_end")),
    )
    # companyfacts `fy` is unreliable — keep it, but fall back to calendar year so
    # the partition column is never null at scale.
    .withColumn("fiscal_year", F.coalesce(F.col("fiscal_year").cast("int"), F.year("period_end")))
    .withColumn("duration_days", F.datediff("period_end", "period_start"))
    .withColumn(
        "period_type",
        F.when(F.col("period_start").isNull(), F.lit("instant"))
        .when(F.col("duration_days") <= 110, F.lit("quarter"))
        .when(F.col("duration_days") <= 200, F.lit("half"))
        .when(F.col("duration_days") <= 300, F.lit("ytd9"))
        .otherwise(F.lit("annual")),
    )
    .filter(
        F.col("cik").isNotNull() & F.col("period_end").isNotNull()
        & F.col("value").isNotNull() & F.col("accession").isNotNull()
    )
    .join(known_filings, ["cik", "accession"], "left_semi")
    .dropDuplicates(["cik", "accession", "taxonomy", "concept", "unit", "period_start", "period_end"])
)

(
    silver_financial_facts.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
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

# DBTITLE 1,silver_filing_sections  (mapInPandas HTML -> Item sections)
_SEC_SCHEMA = StructType([
    StructField("cik", StringType()),
    StructField("accession", StringType()),
    StructField("document", StringType()),
    StructField("section_index", IntegerType()),
    StructField("section", StringType()),
    StructField("heading", StringType()),
    StructField("text", StringType()),
    StructField("char_len", LongType()),
])
_SEC_COLS = [f.name for f in _SEC_SCHEMA.fields]


def _parse_sections(iterator):
    import pandas as pd
    ep = _import_edgar_parse()
    for pdf in iterator:
        out = []
        for _, r in pdf.iterrows():
            try:
                secs = ep.parse_filing_html(r["text"] or "")
            except Exception:
                secs = []
            for i, s in enumerate(secs):
                out.append({
                    "cik": str(r["cik"]).zfill(10),
                    "accession": r["accession"],
                    "document": r["document"],
                    "section_index": i,
                    "section": s["section"],
                    "heading": s["heading"],
                    "text": s["text"],
                    "char_len": len(s["text"] or ""),
                })
        yield pd.DataFrame(out, columns=_SEC_COLS) if out else pd.DataFrame(columns=_SEC_COLS)


html_src = (
    spark.table(T("bronze_filing_text"))
    .filter(F.col("doc_kind") == "primary_html")
    .select("cik", "accession", "document", "text")
)
silver_sections = html_src.repartition(200).mapInPandas(_parse_sections, schema=_SEC_SCHEMA)
silver_sections.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_filing_sections"))
print("silver_filing_sections:", spark.table(T("silver_filing_sections")).count())

# COMMAND ----------

# DBTITLE 1,silver_exhibits  (mapInPandas submission .txt SGML manifest)
_EXH_SCHEMA = StructType([
    StructField("cik", StringType()),
    StructField("accession", StringType()),
    StructField("sequence", StringType()),
    StructField("doc_type", StringType()),
    StructField("filename", StringType()),
    StructField("description", StringType()),
])
_EXH_COLS = [f.name for f in _EXH_SCHEMA.fields]


def _parse_exhibits(iterator):
    import pandas as pd
    ep = _import_edgar_parse()
    for pdf in iterator:
        out = []
        for _, r in pdf.iterrows():
            try:
                docs = ep.parse_submission_documents(r["text"] or "")
            except Exception:
                docs = []
            for d in docs:
                out.append({
                    "cik": str(r["cik"]).zfill(10),
                    "accession": r["accession"],
                    "sequence": d["sequence"], "doc_type": d["doc_type"],
                    "filename": d["filename"], "description": d["description"],
                })
        yield pd.DataFrame(out, columns=_EXH_COLS) if out else pd.DataFrame(columns=_EXH_COLS)


sub_src = (
    spark.table(T("bronze_filing_text"))
    .filter(F.col("doc_kind") == "submission_txt")
    .select("cik", "accession", "text")
)
silver_exhibits = sub_src.repartition(200).mapInPandas(_parse_exhibits, schema=_EXH_SCHEMA)
silver_exhibits.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_exhibits"))
print("silver_exhibits:", spark.table(T("silver_exhibits")).count())

# COMMAND ----------

# DBTITLE 1,silver_filing_text_chunks  (mapInPandas 1100/150 chunks; CDF enabled)
# ~1100/150 chars (~170/25 tokens): SEC prose is dense and long-sentenced — 400
# fragments a single risk/MD&A point. Larger chunks + the section-level
# `parent_text` join in nb 05 = parent-child retrieval.
_CHUNK_SCHEMA = StructType([
    StructField("chunk_id", StringType()),
    StructField("cik", StringType()),
    StructField("accession", StringType()),
    StructField("section", StringType()),
    StructField("section_index", IntegerType()),
    StructField("heading", StringType()),
    StructField("chunk_index", IntegerType()),
    StructField("chunk_text", StringType()),
])
_CHUNK_COLS = [f.name for f in _CHUNK_SCHEMA.fields]


def _chunk_sections(iterator):
    import pandas as pd
    ep = _import_edgar_parse()
    for pdf in iterator:
        out = []
        for _, r in pdf.iterrows():
            for j, ch in enumerate(ep.chunk_text(r["text"] or "", 1100, 150)):
                out.append({
                    "chunk_id": f"{r['accession']}::{r['section_index']}::{j}",
                    "cik": r["cik"], "accession": r["accession"],
                    "section": r["section"], "section_index": int(r["section_index"]),
                    "heading": r["heading"], "chunk_index": j, "chunk_text": ch,
                })
        yield pd.DataFrame(out, columns=_CHUNK_COLS) if out else pd.DataFrame(columns=_CHUNK_COLS)


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {T('silver_filing_text_chunks')} (
        chunk_id STRING, cik STRING, accession STRING, section STRING,
        section_index INT, heading STRING, chunk_index INT, chunk_text STRING
    ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """
)
chunks = (
    spark.table(T("silver_filing_sections"))
    .filter(F.length("text") > 0)
    .select("cik", "accession", "section", "section_index", "heading", "text")
    .repartition(200)
    .mapInPandas(_chunk_sections, schema=_CHUNK_SCHEMA)
)
chunks.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("silver_filing_text_chunks"))
print("silver_filing_text_chunks:", spark.table(T("silver_filing_text_chunks")).count())

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps({
        "status": "ok",
        "silver_companies": spark.table(T("silver_companies")).count(),
        "silver_filings": spark.table(T("silver_filings")).count(),
        "silver_financial_facts": spark.table(T("silver_financial_facts")).count(),
        "silver_filing_sections": spark.table(T("silver_filing_sections")).count(),
        "silver_filing_text_chunks": spark.table(T("silver_filing_text_chunks")).count(),
    })
)
