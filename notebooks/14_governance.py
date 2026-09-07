# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 14 · Management & governance  (analyst spec §13)  —  standalone
# MAGIC
# MAGIC Fetches each company's **latest DEF 14A** (proxy), then an `ai_query`
# MAGIC extraction of the compensation-discussion section:
# MAGIC
# MAGIC - CEO / CFO total compensation, equity-heavy or not
# MAGIC - the performance metrics the incentive plan pays on (Revenue / EPS /
# MAGIC   EBITDA / FCF / ROIC / TSR / margin / …) and what management is thereby
# MAGIC   incentivized to optimise
# MAGIC - say-on-pay support, board size & independence, ownership guidelines
# MAGIC - related-party transactions, and any incentive-misalignment risk
# MAGIC   (excess growth / acquisitions / leverage / dilution / short-term EPS)
# MAGIC
# MAGIC Lands `bronze_proxy_filings` (raw text) + `gold_governance` (one row per
# MAGIC company). Standalone — not in the daily pipeline. `MERGE` on cik.

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
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("max_chars", "45000")
dbutils.widgets.text("batch_size", "25")
dbutils.widgets.text("sec_user_agent",
                     "EDGAR Intelligence Platform - Zach Steele zacharysteele8@gmail.com")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODE = dbutils.widgets.get("mode")
LLM = dbutils.widgets.get("llm_endpoint")
MAX_CHARS = int(dbutils.widgets.get("max_chars"))
BATCH = int(dbutils.widgets.get("batch_size"))
UA = dbutils.widgets.get("sec_user_agent")
FULL = MODE == "full"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType, IntegerType

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
with open(dbutils.widgets.get("ciks_config")) as fh:
    COMPANIES = json.load(fh)["companies"]

seen = set()
if spark.catalog.tableExists(T("bronze_proxy_filings")):
    seen = {r["cik"] for r in spark.table(T("bronze_proxy_filings")).select("cik").collect()}
print(f"{len(COMPANIES)} companies, {len(seen)} proxies already stored")

# COMMAND ----------

# DBTITLE 1,Fetch the latest DEF 14A per company (comp section, batch-flushed)
client = SecClient(user_agent=UA, requests_per_second=5.0)
rows = []
n_written, skipped, _i = 0, 0, 0


def _flush():
    global rows, n_written
    if not rows:
        return
    df = spark.createDataFrame(rows)
    fq = T("bronze_proxy_filings")
    if not spark.catalog.tableExists(fq):
        df.write.format("delta").saveAsTable(fq)
    else:
        df.createOrReplaceTempView("_stg_proxy")
        spark.sql(f"""MERGE INTO {fq} t USING _stg_proxy s ON t.cik = s.cik
                      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    n_written += len(rows)
    rows = []


def _comp_window(text: str) -> str:
    low = text.lower()
    for anchor in ("compensation discussion and analysis", "executive compensation",
                   "compensation of named executive"):
        idx = low.find(anchor)
        if idx > 0:
            return text[idx: idx + MAX_CHARS]
    return text[:MAX_CHARS]


for _i, co in enumerate(COMPANIES, 1):
    c10 = cik10(co["cik"])
    if c10 in seen and not FULL:
        skipped += 1
        continue
    try:
        subs = client.submissions(c10)
    except Exception as exc:
        print(f"  {co.get('ticker')}: submissions failed — {exc}")
        continue
    latest = None
    for f in iter_recent_filings(subs):
        if (f.get("form") or "").strip() in ("DEF 14A", "DEFA14A"):
            latest = f
            break
    if not latest or not (latest.get("primaryDocument") or "").lower().endswith((".htm", ".html")):
        continue
    try:
        html = client.filing_document(c10, latest["accessionNumber"],
                                      latest["primaryDocument"]).decode("utf-8", "replace")
    except Exception:
        continue
    text = edgar_parse._html_to_text(html)
    rows.append({
        "cik": c10, "ticker": co.get("ticker"), "name": co.get("name"),
        "accession": latest["accessionNumber"], "filing_date": latest.get("filingDate"),
        "comp_text": _comp_window(text)[:MAX_CHARS],
        "ingested_at": dt.datetime.utcnow().isoformat(),
    })
    if _i % BATCH == 0:
        _flush()
        print(f"  {_i}/{len(COMPANIES)}  (+{n_written} proxies, {skipped} skipped)")

_flush()
print(f"proxies stored: {n_written}, skipped: {skipped}")

# COMMAND ----------

# DBTITLE 1,ai_query -> gold_governance
PROMPT = (
    "You are a governance analyst reading a DEF 14A proxy. Respond with ONLY one "
    "compact JSON object, single-line string values, keys:\n"
    '  "ceo_name": string.  "ceo_total_comp_usd": number (most recent year) or null.\n'
    '  "cfo_total_comp_usd": number or null.\n'
    '  "pay_is_equity_heavy": one of "yes","no","mixed".\n'
    '  "performance_metrics": array from {revenue,EPS,operating_income,EBITDA,FCF,'
    'margin,ROIC,ROE,TSR,cash_flow,other} that the incentive plan actually pays on.\n'
    '  "incentivized_to_optimize": 1-3 words — what the pay plan rewards most.\n'
    '  "say_on_pay_support_pct": number (last vote) or null.\n'
    '  "board_size": integer or null.  "board_independent_pct": number or null.\n'
    '  "ownership_guidelines": 1 sentence, or "not disclosed".\n'
    '  "related_party_transactions": 1 sentence, or "none disclosed".\n'
    '  "incentive_risk": 1 sentence flagging any misalignment (excess growth, '
    'acquisitions, leverage, dilution, short-term EPS management), or "none noted".\n\n'
)
if spark.catalog.tableExists(T("bronze_proxy_filings")):
    scored = spark.table(T("bronze_proxy_filings")).withColumn(
        "prompt", F.concat(F.lit(PROMPT), F.lit("COMPANY: "), F.col("ticker"),
                           F.lit("\n\nPROXY COMP SECTION:\n"), F.col("comp_text")),
    ).withColumn("raw", F.expr(f"ai_query('{LLM}', prompt)"))

    schema = StructType([
        StructField("ceo_name", StringType()),
        StructField("ceo_total_comp_usd", DoubleType()),
        StructField("cfo_total_comp_usd", DoubleType()),
        StructField("pay_is_equity_heavy", StringType()),
        StructField("performance_metrics", ArrayType(StringType())),
        StructField("incentivized_to_optimize", StringType()),
        StructField("say_on_pay_support_pct", DoubleType()),
        StructField("board_size", IntegerType()),
        StructField("board_independent_pct", DoubleType()),
        StructField("ownership_guidelines", StringType()),
        StructField("related_party_transactions", StringType()),
        StructField("incentive_risk", StringType()),
    ])
    parsed = (
        scored.withColumn("json_str", F.regexp_extract("raw", r"\{[\s\S]*\}", 0))
        .withColumn("p", F.from_json("json_str", schema))
        .select("cik", "ticker", "name", "accession",
                F.col("filing_date").alias("proxy_date"),
                *[F.col(f"p.{f.name}").alias(f.name) for f in schema.fields],
                F.lit(LLM).alias("model"), F.current_timestamp().alias("generated_at"))
    )
    parsed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        T("gold_governance")
    )
    n = spark.table(T("gold_governance")).count()
    n_bad = spark.table(T("gold_governance")).filter(F.col("ceo_name").isNull()).count()
    print(f"gold_governance: {n} rows, {n_bad} unparsed")
else:
    n, n_bad = 0, 0
    print("no proxies fetched")

dbutils.notebook.exit(json.dumps({"status": "ok", "rows": n, "unparsed": n_bad}))
