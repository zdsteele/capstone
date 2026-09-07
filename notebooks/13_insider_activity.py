# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 13 · Insider activity  (analyst spec §11)  —  standalone
# MAGIC
# MAGIC Fetches **Forms 3 / 4 / 5** for every CIK (rate-limited, incremental
# MAGIC `MERGE`), parses the `ownershipDocument` XML, and lands:
# MAGIC
# MAGIC - `bronze_ownership_filings` — raw XML per filing (keep the source)
# MAGIC - `silver_insider_transactions` — one row per reported transaction
# MAGIC   (insider, role, date, code — P/S/A/M/F/G — shares, price, value,
# MAGIC   acquired/disposed, holdings after)
# MAGIC - `gold_insider_activity` — one row per company: 180-day open-market
# MAGIC   buy $ vs sell $, distinct buyers vs sellers, cluster-buying flag,
# MAGIC   largest open-market purchase, a plain-English signal
# MAGIC
# MAGIC Does **not** touch the daily pipeline — run it on its own. `mode=full`
# MAGIC re-fetches; `incremental` skips accessions already in
# MAGIC `bronze_ownership_filings`.

# COMMAND ----------

import json
import os
import sys
import datetime as dt

_repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from lib.sec_client import SecClient, cik10, accession_nodash, iter_recent_filings
from lib import edgar_parse

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("ciks_config", "../config/ciks_full.json")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("max_new_per_cik", "60")   # ~1-2 yrs of Form 4s
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
FORMS = {"3", "4", "5"}

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

with open(dbutils.widgets.get("ciks_config")) as fh:
    COMPANIES = json.load(fh)["companies"]

seen = set()
# Only trust the incremental skip-set when BOTH tables exist. If bronze exists
# but silver doesn't, a prior run stored filings it couldn't parse (the XSL-path
# bug) — clear `seen` so those accessions get re-fetched with the fixed path.
if (spark.catalog.tableExists(T("bronze_ownership_filings"))
        and spark.catalog.tableExists(T("silver_insider_transactions"))):
    seen = {r["accession"] for r in
            spark.table(T("bronze_ownership_filings")).select("accession").collect()}
print(f"{len(COMPANIES)} companies, {len(seen):,} ownership filings already stored")

# COMMAND ----------

# DBTITLE 1,Fetch Forms 3/4/5, parse ownership XML (batch-flushed)
client = SecClient(user_agent=UA, requests_per_second=5.0)
raw_rows, txn_rows = [], []
merged = {"bronze_ownership_filings": 0, "silver_insider_transactions": 0}
_i, skipped = 0, 0


def _flush():
    global raw_rows, txn_rows
    if raw_rows:
        df = spark.createDataFrame(raw_rows)
        fq = T("bronze_ownership_filings")
        if not spark.catalog.tableExists(fq):
            df.write.format("delta").saveAsTable(fq)
        else:
            df.createOrReplaceTempView("_stg_own")
            spark.sql(f"""MERGE INTO {fq} t USING _stg_own s ON t.accession = s.accession
                          WHEN MATCHED THEN UPDATE SET *
                          WHEN NOT MATCHED THEN INSERT *""")
        merged["bronze_ownership_filings"] += len(raw_rows)
    if txn_rows:
        df = spark.createDataFrame(txn_rows)
        fq = T("silver_insider_transactions")
        if not spark.catalog.tableExists(fq):
            df.write.format("delta").saveAsTable(fq)
        else:
            df.createOrReplaceTempView("_stg_txn")
            spark.sql(f"""MERGE INTO {fq} t USING _stg_txn s ON t.txn_id = s.txn_id
                          WHEN NOT MATCHED THEN INSERT *""")
        merged["silver_insider_transactions"] += len(txn_rows)
    raw_rows, txn_rows = [], []


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
        # The submissions feed usually points primaryDocument at the XSL-rendered
        # copy (e.g. "xslF345X05/wf-form4_123.xml") — fetching that path returns an
        # HTML page, not the ownershipDocument XML, so parse_form4 gets nothing.
        # The raw submitted XML sits in the accession root under the same basename.
        if "/" in primary:
            primary = primary.rsplit("/", 1)[-1]
        if not primary.lower().endswith(".xml"):
            continue
        new_here += 1
        try:
            xml = client.filing_document(c10, accession, primary).decode("utf-8", "replace")
        except Exception:
            continue
        raw_rows.append({"accession": accession, "cik": c10, "form": form,
                         "filing_date": f.get("filingDate"), "primary_document": primary,
                         "xml": xml[:2_000_000],
                         "ingested_at": dt.datetime.utcnow().isoformat()})
        parsed = edgar_parse.parse_form4(xml)
        if not parsed:
            continue
        for k, txn in enumerate(parsed["transactions"]):
            if txn.get("derivative"):
                continue
            txn_rows.append({
                "txn_id": f"{accession}::{k}",
                "accession": accession, "cik": c10, "ticker": ticker,
                "form": form, "filing_date": f.get("filingDate"),
                "owner_name": parsed["owner_name"], "owner_cik": parsed["owner_cik"],
                "is_director": parsed["is_director"], "is_officer": parsed["is_officer"],
                "is_ten_pct_owner": parsed["is_ten_pct_owner"],
                "officer_title": parsed["officer_title"],
                "txn_date": txn["date"], "code": txn["code"], "code_label": txn["code_label"],
                "open_market": txn["open_market"], "acquired_disposed": txn["acquired_disposed"],
                "shares": txn["shares"], "price": txn["price"], "value": txn["value"],
                "shares_owned_after": txn["shares_owned_after"], "security": txn["security"],
            })
    if _i % BATCH == 0:
        _flush()
        print(f"  {_i}/{len(COMPANIES)}  (+{merged['silver_insider_transactions']} txns, {skipped} skipped)")

_flush()
print("fetched:", merged, "skipped:", skipped)

# COMMAND ----------

# DBTITLE 1,gold_insider_activity — 180-day open-market rollup per company
if not spark.catalog.tableExists(T("silver_insider_transactions")):
    # No Forms 3/4/5 parsed this run (e.g. first run hit only stale filings, or
    # every fetch failed). Write an empty gold table so downstream joins in
    # company_health still resolve, and exit clean instead of failing the task.
    _empty = spark.createDataFrame([], schema=(
        "cik string, ticker string, buy_value_180d double, sell_value_180d double, "
        "n_buyers_180d long, n_sellers_180d long, n_open_market_buys_180d long, "
        "n_open_market_sells_180d long, largest_buy_180d double, latest_txn_date string, "
        "net_value_180d double, signal string, name string, generated_at timestamp"))
    _empty.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        T("gold_insider_activity"))
    print("no silver_insider_transactions — wrote empty gold_insider_activity")
    dbutils.notebook.exit(json.dumps(
        {"status": "no_transactions", "raw": merged, "gold_insider_activity": 0,
         "skipped": skipped}))

txns = spark.table(T("silver_insider_transactions")).withColumn(
    "filing_date", F.to_date("filing_date")
).filter(F.col("filing_date") >= F.date_sub(F.current_date(), 180))

om = txns.filter(F.col("open_market"))
agg = (
    om.groupBy("cik", "ticker")
    .agg(
        F.sum(F.when(F.col("code") == "P", F.col("value"))).alias("buy_value_180d"),
        F.sum(F.when(F.col("code") == "S", F.col("value"))).alias("sell_value_180d"),
        F.countDistinct(F.when(F.col("code") == "P", F.col("owner_name"))).alias("n_buyers_180d"),
        F.countDistinct(F.when(F.col("code") == "S", F.col("owner_name"))).alias("n_sellers_180d"),
        F.count(F.when(F.col("code") == "P", True)).alias("n_open_market_buys_180d"),
        F.count(F.when(F.col("code") == "S", True)).alias("n_open_market_sells_180d"),
        F.max(F.when(F.col("code") == "P", F.col("value"))).alias("largest_buy_180d"),
        F.max("txn_date").alias("latest_txn_date"),
    )
    .withColumn("buy_value_180d", F.coalesce("buy_value_180d", F.lit(0.0)))
    .withColumn("sell_value_180d", F.coalesce("sell_value_180d", F.lit(0.0)))
    .withColumn("net_value_180d", F.col("buy_value_180d") - F.col("sell_value_180d"))
    .withColumn(
        "signal",
        F.when((F.col("n_buyers_180d") >= 2) & (F.col("net_value_180d") > 1_000_000),
               F.lit("cluster buying — multiple insiders bought with personal capital"))
        .when(F.col("net_value_180d") > 0, F.lit("net insider buying"))
        .when((F.col("sell_value_180d") > 5_000_000) & (F.col("buy_value_180d") == 0),
              F.lit("net insider selling (no open-market buys)"))
        .otherwise(F.lit("routine — no clear open-market signal")),
    )
)
companies = spark.table(T("silver_companies")).select("cik", "name")
gold = agg.join(companies, "cik", "left").withColumn("generated_at", F.current_timestamp())
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_insider_activity")
)
n = spark.table(T("gold_insider_activity")).count()
print("gold_insider_activity:", n)

dbutils.notebook.exit(json.dumps(
    {"status": "ok", "raw": merged, "gold_insider_activity": n, "skipped": skipped}))
