# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 16 · Beneficial ownership — Schedules 13D / 13G  (analyst spec §12a)  —  standalone
# MAGIC
# MAGIC Fetches **Schedule 13D / 13D-A / 13G / 13G-A** for every tracked CIK
# MAGIC (rate-limited, incremental `MERGE` — same shape as notebook 13's Forms
# MAGIC 3/4/5 fetch), parses each filing's cover page, and lands:
# MAGIC
# MAGIC - `bronze_beneficial_ownership_filings` — raw filing text per accession
# MAGIC - `silver_beneficial_ownership_stakes` — one row per reporting-person stake
# MAGIC - `gold_beneficial_ownership` — latest stake per (company, holder), with a
# MAGIC   trend column and an activist (13D) vs passive (13G) flag
# MAGIC
# MAGIC 13D/13G are filed **by a holder about an issuer** — same CIK-driven fetch
# MAGIC pattern as Forms 3/4/5 and DEF 14A. (Form 13F is the opposite direction —
# MAGIC filed by a manager about many issuers — see notebook 17's separate
# MAGIC manager-CIK-driven axis.)
# MAGIC
# MAGIC Does **not** touch the daily pipeline — run it on its own. `mode=full`
# MAGIC re-fetches; `incremental` skips accessions already in
# MAGIC `bronze_beneficial_ownership_filings`.

# COMMAND ----------

import json
import os
import sys
import datetime as dt

_repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from lib.sec_client import SecClient, cik10, iter_recent_filings
from lib import edgar_parse

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("ciks_config", "../config/ciks_full.json")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("max_new_per_cik", "30")
dbutils.widgets.text("batch_size", "25")
dbutils.widgets.text("sec_user_agent",
                     "EDGAR Intelligence Platform - Zach Steele zacharysteele8@gmail.com")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODE = dbutils.widgets.get("mode")
MAX_NEW = int(dbutils.widgets.get("max_new_per_cik"))
BATCH = int(dbutils.widgets.get("batch_size"))
UA = dbutils.widgets.get("sec_user_agent")
FULL = MODE == "full"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
# Pre-Dec-2024 filings use "SC 13D/G"; EDGAR's structured Schedule 13D/G since
# then uses "SCHEDULE 13D/G" (XML primary_doc.xml). Both must be ingested.
FORMS = {
    "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A",
    "SCHEDULE 13D", "SCHEDULE 13D/A", "SCHEDULE 13G", "SCHEDULE 13G/A",
}

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# filer_cik is always None today (the cover-page parser can't resolve an
# individual reporting person's own CIK from filing text) — an entirely-null
# column defeats spark.createDataFrame's type inference
# (CANNOT_DETERMINE_TYPE), so this needs an explicit schema rather than
# relying on inference the way notebook 13's bronze/silver rows do.
_STAKE_SCHEMA = StructType([
    StructField("stake_id", StringType()), StructField("accession", StringType()),
    StructField("cik", StringType()), StructField("ticker", StringType()),
    StructField("form", StringType()), StructField("filing_date", StringType()),
    StructField("event_date", StringType()), StructField("filer_name", StringType()),
    StructField("filer_cik", StringType()), StructField("cusip", StringType()),
    StructField("pct_of_class", DoubleType()),
    StructField("shares_beneficially_owned", DoubleType()),
    StructField("sole_voting_power", DoubleType()),
    StructField("shared_voting_power", DoubleType()),
    StructField("sole_dispositive_power", DoubleType()),
    StructField("shared_dispositive_power", DoubleType()),
    StructField("is_activist", BooleanType()), StructField("parse_method", StringType()),
    StructField("ingested_at", StringType()),
])
_BRONZE_SCHEMA = StructType([
    StructField("accession", StringType()), StructField("cik", StringType()),
    StructField("ticker", StringType()), StructField("form", StringType()),
    StructField("filing_date", StringType()), StructField("primary_document", StringType()),
    StructField("raw_text", StringType()), StructField("ingested_at", StringType()),
])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

with open(dbutils.widgets.get("ciks_config")) as fh:
    COMPANIES = json.load(fh)["companies"]

seen = set()
if (spark.catalog.tableExists(T("bronze_beneficial_ownership_filings"))
        and spark.catalog.tableExists(T("silver_beneficial_ownership_stakes"))):
    seen = {r["accession"] for r in
            spark.table(T("bronze_beneficial_ownership_filings")).select("accession").collect()}
print(f"{len(COMPANIES)} companies, {len(seen):,} beneficial-ownership filings already stored")

# COMMAND ----------

# DBTITLE 1,Fetch Schedules 13D/13G, parse cover pages (batch-flushed)
client = SecClient(user_agent=UA, requests_per_second=5.0)
raw_rows, stake_rows = [], []
merged = {"bronze_beneficial_ownership_filings": 0, "silver_beneficial_ownership_stakes": 0}
_i, skipped = 0, 0


def _flush():
    global raw_rows, stake_rows
    if raw_rows:
        df = spark.createDataFrame(raw_rows, schema=_BRONZE_SCHEMA)
        fq = T("bronze_beneficial_ownership_filings")
        if not spark.catalog.tableExists(fq):
            df.write.format("delta").saveAsTable(fq)
        else:
            df.createOrReplaceTempView("_stg_benef_bronze")
            spark.sql(f"""MERGE INTO {fq} t USING _stg_benef_bronze s ON t.accession = s.accession
                          WHEN MATCHED THEN UPDATE SET *
                          WHEN NOT MATCHED THEN INSERT *""")
        merged["bronze_beneficial_ownership_filings"] += len(raw_rows)
    if stake_rows:
        df = spark.createDataFrame(stake_rows, schema=_STAKE_SCHEMA)
        fq = T("silver_beneficial_ownership_stakes")
        if not spark.catalog.tableExists(fq):
            df.write.format("delta").saveAsTable(fq)
        else:
            df.createOrReplaceTempView("_stg_benef_stake")
            spark.sql(f"""MERGE INTO {fq} t USING _stg_benef_stake s ON t.stake_id = s.stake_id
                          WHEN NOT MATCHED THEN INSERT *""")
        merged["silver_beneficial_ownership_stakes"] += len(stake_rows)
    raw_rows, stake_rows = [], []


for _i, co in enumerate(COMPANIES, 1):
    c10 = cik10(co["cik"])
    ticker = co.get("ticker")
    try:
        subs = client.submissions(c10)
    except Exception as exc:
        print(f"  {ticker}: submissions failed — {exc}")
        continue
    new_here = 0
    for f in iter_recent_filings(subs):
        form = (f.get("form") or "").strip()
        if form not in FORMS:
            continue
        accession = f.get("accessionNumber")
        if accession in seen and not FULL:
            skipped += 1
            continue
        if new_here >= MAX_NEW:
            break
        primary = f.get("primaryDocument") or ""
        is_schedule_xml = form.startswith("SCHEDULE")
        if is_schedule_xml:
            primary = "primary_doc.xml"
        if not primary:
            continue
        new_here += 1
        try:
            raw_bytes = client.filing_document(c10, accession, primary)
            doc = raw_bytes.decode("utf-8", "replace")
        except Exception:
            continue
        raw_rows.append({
            "accession": accession, "cik": c10, "ticker": ticker, "form": form,
            "filing_date": f.get("filingDate"), "primary_document": primary,
            "raw_text": doc[:2_000_000],
            "ingested_at": dt.datetime.utcnow().isoformat(),
        })
        if is_schedule_xml:
            people = edgar_parse.parse_13dg_schedule_xml(doc) or []
        else:
            single = edgar_parse.parse_13dg_cover(doc, is_xml=primary.lower().endswith(".xml"))
            people = [single] if single else []
        for i, parsed in enumerate(people):
            stake_rows.append({
                "stake_id": f"{accession}::{i}",
                "accession": accession, "cik": c10, "ticker": ticker, "form": form,
                "filing_date": f.get("filingDate"), "event_date": f.get("filingDate"),
                "filer_name": parsed["filer_name"], "filer_cik": None,
                "cusip": parsed["cusip"], "pct_of_class": parsed["pct_of_class"],
                "shares_beneficially_owned": parsed["shares_beneficially_owned"],
                "sole_voting_power": parsed["sole_voting_power"],
                "shared_voting_power": parsed["shared_voting_power"],
                "sole_dispositive_power": parsed["sole_dispositive_power"],
                "shared_dispositive_power": parsed["shared_dispositive_power"],
                "is_activist": "13D" in form,
                "parse_method": parsed["parse_method"],
                "ingested_at": dt.datetime.utcnow().isoformat(),
            })
    if _i % BATCH == 0:
        _flush()
        print(f"  {_i}/{len(COMPANIES)}  (+{merged['silver_beneficial_ownership_stakes']} stakes, "
              f"{skipped} skipped)")

_flush()
print("fetched:", merged, "skipped:", skipped)

# COMMAND ----------

# DBTITLE 1,gold_beneficial_ownership — latest stake per (company, holder)
if not spark.catalog.tableExists(T("silver_beneficial_ownership_stakes")):
    _empty = spark.createDataFrame([], schema=(
        "cik string, ticker string, name string, filer_name string, filer_cik string, "
        "is_activist boolean, pct_of_class double, pct_change_vs_prior double, "
        "shares_beneficially_owned double, filing_date string, form string, "
        "accession string, generated_at timestamp"))
    _empty.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        T("gold_beneficial_ownership"))
    print("no silver_beneficial_ownership_stakes — wrote empty gold_beneficial_ownership")
    dbutils.notebook.exit(json.dumps(
        {"status": "no_stakes", "raw": merged, "gold_beneficial_ownership": 0,
         "skipped": skipped}))

stakes = spark.table(T("silver_beneficial_ownership_stakes")).withColumn(
    "filing_date", F.to_date("filing_date")
).withColumn(
    "filer_name_normalized", F.upper(F.trim(F.coalesce("filer_name", F.lit("UNKNOWN"))))
)

w_trend = Window.partitionBy("cik", "filer_name_normalized").orderBy("filing_date")
with_trend = stakes.withColumn(
    "pct_change_vs_prior", F.col("pct_of_class") - F.lag("pct_of_class").over(w_trend)
)

w_latest = Window.partitionBy("cik", "filer_name_normalized").orderBy(F.col("filing_date").desc())
latest = (
    with_trend.withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

companies_df = spark.table(T("silver_companies")).select("cik", "name")
gold = (
    latest.join(companies_df, "cik", "left")
    .withColumn("generated_at", F.current_timestamp())
    .select("cik", "ticker", "name", "filer_name", "filer_cik", "is_activist",
            "pct_of_class", "pct_change_vs_prior", "shares_beneficially_owned",
            "filing_date", "form", "accession", "generated_at")
)
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_beneficial_ownership")
)
n = spark.table(T("gold_beneficial_ownership")).count()
print("gold_beneficial_ownership:", n)

dbutils.notebook.exit(json.dumps(
    {"status": "ok", "raw": merged, "gold_beneficial_ownership": n, "skipped": skipped}))
