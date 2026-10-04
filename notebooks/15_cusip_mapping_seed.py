# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 15 · CUSIP -> CIK mapping seed  (analyst spec §12, supports notebook 17)  —  standalone
# MAGIC
# MAGIC Form 13F holdings are keyed by **CUSIP + free-text issuer name** — there is
# MAGIC no ticker/CIK field, and the free SEC JSON APIs this project otherwise uses
# MAGIC don't expose a CUSIP -> CIK mapping. This notebook builds one, specific to
# MAGIC the tracked universe, by pulling a handful of large institutional managers'
# MAGIC *most recent* 13F information tables (a seed corpus — large multi-strategy /
# MAGIC index managers between them hold nearly every S&P 500 constituent) and
# MAGIC fuzzy-matching each distinct `nameOfIssuer` against `silver_companies.name`.
# MAGIC
# MAGIC Writes **`silver_cusip_mapping`** (`cusip, issuer_name_raw,
# MAGIC issuer_name_normalized, cik, ticker, match_score, match_method, confident,
# MAGIC first_seen_accession, updated_at`), `MERGE`d on `cusip` so a CUSIP that's
# MAGIC already confidently resolved is never re-litigated by a later fuzzy pass —
# MAGIC only new/unmatched CUSIPs get re-evaluated.
# MAGIC
# MAGIC **Run this manually / periodically** (not on the daily `pipeline_daily_refresh`
# MAGIC schedule) — the mapping only needs rebuilding when a genuinely new issuer
# MAGIC shows up in a seed manager's 13F, which is rare week-to-week. Re-run whenever
# MAGIC `config/13f_managers.json` or `config/cusip_overrides.json` changes, and
# MAGIC before the first run of notebook 17 against a new manager list.

# COMMAND ----------

import difflib
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
dbutils.widgets.text("overrides_config", "../config/cusip_overrides.json")
dbutils.widgets.text("match_threshold", "0.82")
dbutils.widgets.text("sec_user_agent",
                     "EDGAR Intelligence Platform - Zach Steele zacharysteele8@gmail.com")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
THRESHOLD = float(dbutils.widgets.get("match_threshold"))
UA = dbutils.widgets.get("sec_user_agent")
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# Explicit schema, not inference: `cik`/`ticker` are null for any CUSIP that
# matched nothing at all — if a given run's batch happens to have zero
# matches, an entirely-null column defeats spark.createDataFrame's type
# inference (CANNOT_DETERMINE_TYPE, confirmed live while building the
# notebook 16/17 equivalents).
_MAPPING_SCHEMA = StructType([
    StructField("cusip", StringType()), StructField("issuer_name_raw", StringType()),
    StructField("issuer_name_normalized", StringType()), StructField("cik", StringType()),
    StructField("ticker", StringType()), StructField("match_score", DoubleType()),
    StructField("match_method", StringType()), StructField("confident", BooleanType()),
    StructField("first_seen_accession", StringType()), StructField("updated_at", StringType()),
])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

with open(dbutils.widgets.get("managers_config")) as fh:
    MANAGERS = json.load(fh)["managers"]

try:
    with open(dbutils.widgets.get("overrides_config")) as fh:
        OVERRIDES = json.load(fh)
except FileNotFoundError:
    OVERRIDES = []

print(f"{len(MANAGERS)} seed managers, {len(OVERRIDES)} manual overrides")

# COMMAND ----------

# DBTITLE 1,Fetch each manager's most recent 13F information table
client = SecClient(user_agent=UA, requests_per_second=5.0)
# {cusip: {"issuer_name_raw": str, "first_seen_accession": str}}
distinct_cusips: dict[str, dict] = {}

for m in MANAGERS:
    mcik = cik10(m["manager_cik"])
    try:
        subs = client.submissions(mcik)
    except Exception as exc:
        print(f"  {m['manager_name']}: submissions failed — {exc}")
        continue
    hr_filings = [f for f in iter_recent_filings(subs) if f.get("form") == "13F-HR"]
    if not hr_filings:
        print(f"  {m['manager_name']}: no 13F-HR filings found")
        continue
    accession = hr_filings[0]["accessionNumber"]  # most recent only — seed corpus, not a series
    try:
        txt = client.submission_txt(mcik, accession).decode("utf-8", "replace")
        docs = edgar_parse.parse_submission_documents(txt)
        info_doc = next(
            (d for d in docs if "INFORMATION TABLE" in (d.get("doc_type") or "").upper()),
            None,
        )
        if not info_doc or not info_doc.get("filename"):
            print(f"  {m['manager_name']}: no INFORMATION TABLE document in manifest")
            continue
        xml = client.filing_document(mcik, accession, info_doc["filename"]).decode(
            "utf-8", "replace")
        rows = edgar_parse.parse_13f_infotable(xml)
    except Exception as exc:
        print(f"  {m['manager_name']}: fetch/parse failed — {exc}")
        continue
    if not rows:
        print(f"  {m['manager_name']}: infotable unparseable")
        continue
    new_here = 0
    for r in rows:
        cusip = (r.get("cusip") or "").strip()
        if not cusip or cusip in distinct_cusips:
            continue
        distinct_cusips[cusip] = {
            "issuer_name_raw": r.get("name_of_issuer"),
            "first_seen_accession": accession,
        }
        new_here += 1
    print(f"  {m['manager_name']}: {len(rows)} holdings, {new_here} new distinct CUSIPs "
          f"(running total {len(distinct_cusips)})")

print(f"\n{len(distinct_cusips)} distinct CUSIPs collected from {len(MANAGERS)} managers")

# COMMAND ----------

# DBTITLE 1,Fuzzy-match issuer names against the tracked company universe
companies = spark.table(T("silver_companies")).select("cik", "ticker", "name").collect()
# normalized name -> (cik, ticker, name) ; skip blanks and collisions (first wins, rare)
norm_to_company: dict[str, tuple] = {}
for c in companies:
    norm = edgar_parse.normalize_company_name(c["name"])
    if norm and norm not in norm_to_company:
        norm_to_company[norm] = (c["cik"], c["ticker"], c["name"])
choices = list(norm_to_company.keys())
print(f"{len(choices)} normalized company names to match against")

# Already-confident CUSIPs stay pinned — only re-evaluate new/low-confidence ones.
already_confident: dict[str, dict] = {}
if spark.catalog.tableExists(T("silver_cusip_mapping")):
    for r in (spark.table(T("silver_cusip_mapping"))
              .filter("confident = true AND match_method != 'manual_override'")
              .collect()):
        already_confident[r["cusip"]] = {
            "cik": r["cik"], "ticker": r["ticker"], "match_score": r["match_score"],
            "issuer_name_normalized": r["issuer_name_normalized"],
        }

mapping_rows = []
now = dt.datetime.utcnow().isoformat()
for cusip, info in distinct_cusips.items():
    raw_name = info["issuer_name_raw"]
    norm_name = edgar_parse.normalize_company_name(raw_name)
    if cusip in already_confident:
        prior = already_confident[cusip]
        mapping_rows.append({
            "cusip": cusip, "issuer_name_raw": raw_name,
            "issuer_name_normalized": norm_name, "cik": prior["cik"], "ticker": prior["ticker"],
            "match_score": prior["match_score"], "match_method": "exact_cusip_cache",
            "confident": True, "first_seen_accession": info["first_seen_accession"],
            "updated_at": now,
        })
        continue
    best_matches = difflib.get_close_matches(norm_name, choices, n=1, cutoff=THRESHOLD)
    if best_matches:
        best = best_matches[0]
        score = difflib.SequenceMatcher(None, norm_name, best).ratio()
        cik, ticker, _ = norm_to_company[best]
    else:
        # still record the best-effort score even below threshold, for review
        close = difflib.get_close_matches(norm_name, choices, n=1, cutoff=0.0)
        if close:
            score = difflib.SequenceMatcher(None, norm_name, close[0]).ratio()
            cik, ticker, _ = norm_to_company[close[0]]
        else:
            score, cik, ticker = 0.0, None, None
    mapping_rows.append({
        "cusip": cusip, "issuer_name_raw": raw_name, "issuer_name_normalized": norm_name,
        "cik": cik, "ticker": ticker, "match_score": float(score), "match_method": "name_fuzzy",
        "confident": score >= THRESHOLD, "first_seen_accession": info["first_seen_accession"],
        "updated_at": now,
    })

n_confident = sum(1 for r in mapping_rows if r["confident"])
print(f"{n_confident}/{len(mapping_rows)} CUSIPs matched with confidence >= {THRESHOLD}")

# COMMAND ----------

# DBTITLE 1,Apply manual overrides (always win)
override_by_cusip = {o["cusip"]: o for o in OVERRIDES}
cik_to_ticker = {c["cik"]: c["ticker"] for c in companies}
for row in mapping_rows:
    ov = override_by_cusip.get(row["cusip"])
    if ov:
        row["cik"] = ov["cik"]
        row["ticker"] = cik_to_ticker.get(ov["cik"])
        row["match_score"] = 1.0
        row["match_method"] = "manual_override"
        row["confident"] = True
print(f"{len(override_by_cusip)} overrides applied")

# COMMAND ----------

# DBTITLE 1,Write silver_cusip_mapping (MERGE on cusip)
if mapping_rows:
    df = spark.createDataFrame(mapping_rows, schema=_MAPPING_SCHEMA)
    fq = T("silver_cusip_mapping")
    if not spark.catalog.tableExists(fq):
        df.write.format("delta").saveAsTable(fq)
    else:
        df.createOrReplaceTempView("_stg_cusip")
        spark.sql(f"""MERGE INTO {fq} t USING _stg_cusip s ON t.cusip = s.cusip
                      WHEN MATCHED THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")
    print(f"wrote/merged {len(mapping_rows)} rows into {fq}")
else:
    print("no mapping rows produced — nothing written")

n_total = (spark.table(T("silver_cusip_mapping")).count()
           if spark.catalog.tableExists(T("silver_cusip_mapping")) else 0)
n_conf_total = (spark.table(T("silver_cusip_mapping")).filter("confident").count()
                if spark.catalog.tableExists(T("silver_cusip_mapping")) else 0)
print(f"silver_cusip_mapping total: {n_total} rows, {n_conf_total} confident")

dbutils.notebook.exit(json.dumps({
    "status": "ok", "managers": len(MANAGERS), "distinct_cusips": len(distinct_cusips),
    "confident": n_confident, "total_mapping_rows": n_total,
}))
