# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 17 · Institutional ownership — Form 13F  (analyst spec §12b)  —  standalone
# MAGIC
# MAGIC Fetches each seed manager's quarterly **Form 13F-HR** information table
# MAGIC (rate-limited, incremental `MERGE`), resolves each holding's CUSIP against
# MAGIC the tracked universe via `silver_cusip_mapping` (built by notebook 15), and
# MAGIC lands:
# MAGIC
# MAGIC - `bronze_13f_filings` — raw infotable XML per (manager, accession)
# MAGIC - `silver_13f_holdings` — one row per holding line
# MAGIC - `gold_institutional_ownership` — one row per (manager, company, quarter)
# MAGIC   with a quarter-over-quarter new/increased/decreased/exited status
# MAGIC
# MAGIC **This is a different ingestion axis from every other notebook in this
# MAGIC repo**: Form 13F is filed *by an institutional manager*, listing holdings
# MAGIC across many issuers — it cannot be driven off the tracked-company CIK list
# MAGIC (`config/ciks_full.json`) the way Forms 3/4/5, DEF 14A, and 13D/13G all are.
# MAGIC Instead it iterates `config/13f_managers.json`, a small curated list of
# MAGIC large institutional managers.
# MAGIC
# MAGIC **Run notebook 15 first** (or at least once) so `silver_cusip_mapping`
# MAGIC exists — if it doesn't, this still runs and writes holdings with `cik =
# MAGIC NULL` (visible-but-unmatched) rather than failing, matching the defensive-
# MAGIC empty-table convention used elsewhere in this pipeline.
# MAGIC
# MAGIC Does **not** touch the daily pipeline — run it on its own. `mode=full`
# MAGIC re-fetches; `incremental` skips accessions already in `bronze_13f_filings`.

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
dbutils.widgets.text("managers_config", "../config/13f_managers.json")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("max_new_per_manager", "8")   # ~2 yrs of quarterly 13F-HR
dbutils.widgets.text("max_xml_chars", "8000000")   # large managers' infotables run several MB
dbutils.widgets.text("batch_size", "5")
dbutils.widgets.text("sec_user_agent",
                     "EDGAR Intelligence Platform - Zach Steele zacharysteele8@gmail.com")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODE = dbutils.widgets.get("mode")
MAX_NEW = int(dbutils.widgets.get("max_new_per_manager"))
MAX_XML_CHARS = int(dbutils.widgets.get("max_xml_chars"))
BATCH = int(dbutils.widgets.get("batch_size"))
UA = dbutils.widgets.get("sec_user_agent")
FULL = MODE == "full"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Explicit schemas, not inference: `put_call` is null for virtually every
# equity holding (only options carry it) — an entirely-null column in a batch
# defeats spark.createDataFrame's type inference (CANNOT_DETERMINE_TYPE),
# confirmed live while building notebook 16's equivalent (filer_cik there).
_BRONZE_SCHEMA = StructType([
    StructField("accession", StringType()), StructField("manager_cik", StringType()),
    StructField("manager_name", StringType()), StructField("form", StringType()),
    StructField("filing_date", StringType()), StructField("period_of_report", StringType()),
    StructField("primary_document", StringType()),
    StructField("infotable_document", StringType()), StructField("infotable_xml", StringType()),
    StructField("ingested_at", StringType()),
])
_HOLDING_SCHEMA = StructType([
    StructField("holding_id", StringType()), StructField("accession", StringType()),
    StructField("manager_cik", StringType()), StructField("manager_name", StringType()),
    StructField("form", StringType()), StructField("filing_date", StringType()),
    StructField("period_of_report", StringType()), StructField("cusip", StringType()),
    StructField("issuer_name_raw", StringType()), StructField("title_of_class", StringType()),
    StructField("value_usd", DoubleType()), StructField("shares_or_prn_amt", DoubleType()),
    StructField("shares_type", StringType()), StructField("investment_discretion", StringType()),
    StructField("voting_sole", DoubleType()), StructField("voting_shared", DoubleType()),
    StructField("voting_none", DoubleType()), StructField("put_call", StringType()),
])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

with open(dbutils.widgets.get("managers_config")) as fh:
    MANAGERS = json.load(fh)["managers"]

seen = set()
if (spark.catalog.tableExists(T("bronze_13f_filings"))
        and spark.catalog.tableExists(T("silver_13f_holdings"))):
    seen = {r["accession"] for r in
            spark.table(T("bronze_13f_filings")).select("accession").collect()}
print(f"{len(MANAGERS)} managers, {len(seen):,} 13F filings already stored")

# COMMAND ----------

# DBTITLE 1,Fetch 13F-HR information tables, parse holdings (batch-flushed)
client = SecClient(user_agent=UA, requests_per_second=5.0)
raw_rows, holding_rows = [], []
merged = {"bronze_13f_filings": 0, "silver_13f_holdings": 0}
_i, skipped = 0, 0


def _flush():
    global raw_rows, holding_rows
    if raw_rows:
        df = spark.createDataFrame(raw_rows, schema=_BRONZE_SCHEMA)
        fq = T("bronze_13f_filings")
        if not spark.catalog.tableExists(fq):
            df.write.format("delta").saveAsTable(fq)
        else:
            df.createOrReplaceTempView("_stg_13f_bronze")
            spark.sql(f"""MERGE INTO {fq} t USING _stg_13f_bronze s ON t.accession = s.accession
                          WHEN MATCHED THEN UPDATE SET *
                          WHEN NOT MATCHED THEN INSERT *""")
        merged["bronze_13f_filings"] += len(raw_rows)
    if holding_rows:
        df = spark.createDataFrame(holding_rows, schema=_HOLDING_SCHEMA)
        fq = T("silver_13f_holdings")
        if not spark.catalog.tableExists(fq):
            df.write.format("delta").saveAsTable(fq)
        else:
            df.createOrReplaceTempView("_stg_13f_holdings")
            spark.sql(f"""MERGE INTO {fq} t USING _stg_13f_holdings s ON t.holding_id = s.holding_id
                          WHEN NOT MATCHED THEN INSERT *""")
        merged["silver_13f_holdings"] += len(holding_rows)
    raw_rows, holding_rows = [], []


for _i, m in enumerate(MANAGERS, 1):
    mcik = cik10(m["manager_cik"])
    mname = m["manager_name"]
    try:
        subs = client.submissions(mcik)
    except Exception as exc:
        print(f"  {mname}: submissions failed — {exc}")
        continue
    hr_filings = [f for f in iter_recent_filings(subs) if f.get("form") == "13F-HR"]
    new_here = 0
    for f in hr_filings:
        accession = f["accessionNumber"]
        if accession in seen and not FULL:
            skipped += 1
            continue
        if new_here >= MAX_NEW:
            break
        try:
            txt = client.submission_txt(mcik, accession).decode("utf-8", "replace")
            docs = edgar_parse.parse_submission_documents(txt)
            info_doc = next(
                (d for d in docs if "INFORMATION TABLE" in (d.get("doc_type") or "").upper()),
                None,
            )
            if not info_doc or not info_doc.get("filename"):
                skipped += 1
                continue
            xml = client.filing_document(mcik, accession, info_doc["filename"]).decode(
                "utf-8", "replace")
        except Exception as exc:
            print(f"  {mname} {accession}: fetch failed — {exc}")
            continue
        new_here += 1
        # Parse from the untruncated string first — a large manager's infotable
        # can run several MB and must not be corrupted by the storage cap below.
        rows = edgar_parse.parse_13f_infotable(xml)
        raw_rows.append({
            "accession": accession, "manager_cik": mcik, "manager_name": mname,
            "form": f.get("form"), "filing_date": f.get("filingDate"),
            "period_of_report": f.get("reportDate"),
            "primary_document": f.get("primaryDocument"),
            "infotable_document": info_doc["filename"],
            "infotable_xml": xml[:MAX_XML_CHARS],
            "ingested_at": dt.datetime.utcnow().isoformat(),
        })
        if not rows:
            continue
        for k, r in enumerate(rows):
            holding_rows.append({
                "holding_id": f"{accession}::{r.get('cusip')}::{k}",
                "accession": accession, "manager_cik": mcik, "manager_name": mname,
                "form": f.get("form"), "filing_date": f.get("filingDate"),
                "period_of_report": f.get("reportDate"),
                "cusip": r.get("cusip"), "issuer_name_raw": r.get("name_of_issuer"),
                "title_of_class": r.get("title_of_class"),
                # NOT thousands -> see lib/edgar_parse.parse_13f_infotable's
                # docstring: the modern XML infotable's `value` is already
                # whole USD, confirmed against real data (x1000 produced a
                # position bigger than the issuer's entire market cap).
                "value_usd": r.get("value"),
                "shares_or_prn_amt": r.get("shares_or_prn_amt"),
                "shares_type": r.get("shares_type"),
                "investment_discretion": r.get("investment_discretion"),
                "voting_sole": r.get("voting_sole"), "voting_shared": r.get("voting_shared"),
                "voting_none": r.get("voting_none"), "put_call": r.get("put_call"),
            })
    if _i % BATCH == 0:
        _flush()
        print(f"  {_i}/{len(MANAGERS)}  (+{merged['silver_13f_holdings']} holdings, "
              f"{skipped} skipped)")

_flush()
print("fetched:", merged, "skipped:", skipped)

# COMMAND ----------

# DBTITLE 1,gold_institutional_ownership — QoQ position changes per (manager, company)
_GOLD_SCHEMA = (
    "manager_cik string, manager_name string, cik string, ticker string, name string, "
    "period_of_report string, value_usd double, shares double, prior_value_usd double, "
    "prior_shares double, prior_period_of_report string, value_change_usd double, "
    "pct_change double, status string, voting_sole double, voting_shared double, "
    "voting_none double, generated_at timestamp"
)

if not spark.catalog.tableExists(T("silver_13f_holdings")):
    spark.createDataFrame([], schema=_GOLD_SCHEMA).write.format("delta").mode(
        "overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_institutional_ownership"))
    print("no silver_13f_holdings — wrote empty gold_institutional_ownership")
    dbutils.notebook.exit(json.dumps(
        {"status": "no_holdings", "raw": merged, "gold_institutional_ownership": 0,
         "skipped": skipped}))

holdings = spark.table(T("silver_13f_holdings"))

# A single filing can carry multiple infoTable rows for the same (manager, cusip)
# — confirmed against real data (Berkshire's AAPL position splits across several
# rows by sub-account/"otherManager"). Sum to one row per (manager, cusip, period)
# before any ranking, or dense_rank below would be ranking fragments, not positions.
per_position = (
    holdings.groupBy("manager_cik", "manager_name", "cusip", "period_of_report")
    .agg(
        F.first("issuer_name_raw", ignorenulls=True).alias("issuer_name_raw"),
        F.sum("value_usd").alias("value_usd"),
        F.sum("shares_or_prn_amt").alias("shares"),
        F.sum("voting_sole").alias("voting_sole"),
        F.sum("voting_shared").alias("voting_shared"),
        F.sum("voting_none").alias("voting_none"),
    )
)

# dense_rank (not row_number): every row in the latest period must get rn=1,
# and there are many cusip rows per period within one manager partition.
w = Window.partitionBy("manager_cik").orderBy(F.col("period_of_report").desc())
ranked = per_position.withColumn("rn", F.dense_rank().over(w))
curr = ranked.filter(F.col("rn") == 1).drop("rn")
# `manager_name` travels with both curr and prior (it was a groupBy key in
# per_position) specifically so an exited position — present only in `prior`
# after the full outer join below — still carries a real manager name, not NULL.
prior = (
    ranked.filter(F.col("rn") == 2).drop("rn")
    .select(
        "manager_cik", "cusip", "manager_name",
        F.col("value_usd").alias("prior_value_usd"),
        F.col("shares").alias("prior_shares"),
        F.col("period_of_report").alias("prior_period_of_report"),
    )
    .withColumnRenamed("manager_name", "prior_manager_name")
)

# Full outer join (not left/lag) is what surfaces exits: a position present last
# period but absent this period has no row in `curr` to lag into. `on` as a
# column-name list merges manager_cik/cusip into single unambiguous columns in
# the output — no need to coalesce them again afterward.
joined = curr.join(prior, on=["manager_cik", "cusip"], how="full_outer")

with_status = (
    joined
    .withColumn("manager_name", F.coalesce(F.col("manager_name"), F.col("prior_manager_name")))
    .withColumn(
        "value_change_usd",
        F.col("value_usd") - F.coalesce("prior_value_usd", F.lit(0.0)),
    )
    .withColumn(
        "pct_change",
        F.when(F.col("prior_value_usd").isNotNull() & (F.col("prior_value_usd") != 0),
               (F.col("value_usd") - F.col("prior_value_usd")) / F.col("prior_value_usd")),
    )
    .withColumn(
        "status",
        F.when(F.col("value_usd").isNull(), F.lit("exited"))
        .when(F.col("prior_value_usd").isNull(), F.lit("new"))
        .when(F.col("value_usd") > F.col("prior_value_usd") * 1.10, F.lit("increased"))
        .when(F.col("value_usd") < F.col("prior_value_usd") * 0.90, F.lit("decreased"))
        .otherwise(F.lit("unchanged")),
    )
    .withColumn(
        "period_of_report",
        F.coalesce(F.col("period_of_report"), F.col("prior_period_of_report")),
    )
)

mapping = (
    spark.table(T("silver_cusip_mapping")).filter("confident")
    .select("cusip", "cik", "ticker")
    if spark.catalog.tableExists(T("silver_cusip_mapping"))
    else spark.createDataFrame([], schema="cusip string, cik string, ticker string")
)
companies_df = spark.table(T("silver_companies")).select(
    F.col("cik").alias("_cik"), F.col("name").alias("_name"))

gold = (
    with_status
    .join(mapping, "cusip", "left")
    .join(companies_df, F.col("cik") == F.col("_cik"), "left")
    .withColumn("name", F.col("_name"))
    .withColumn("generated_at", F.current_timestamp())
    .select("manager_cik", "manager_name", "cik", "ticker", "name", "period_of_report",
            "value_usd", "shares", "prior_value_usd", "prior_shares",
            "prior_period_of_report", "value_change_usd", "pct_change", "status",
            "voting_sole", "voting_shared", "voting_none", "generated_at")
)

gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_institutional_ownership")
)
n = spark.table(T("gold_institutional_ownership")).count()
n_matched = spark.table(T("gold_institutional_ownership")).filter("cik IS NOT NULL").count()
print(f"gold_institutional_ownership: {n} rows, {n_matched} with a resolved CIK "
      f"({n_matched / n:.0%})" if n else "gold_institutional_ownership: 0 rows")

dbutils.notebook.exit(json.dumps(
    {"status": "ok", "raw": merged, "gold_institutional_ownership": n,
     "matched": n_matched, "skipped": skipped}))
