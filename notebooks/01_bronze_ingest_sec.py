# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze — ingest SEC EDGAR
# MAGIC
# MAGIC Pulls, per CIK in `config/ciks.json`:
# MAGIC - the Submissions API payload (filing history + metadata)
# MAGIC - the XBRL companyfacts payload (pre-extracted financial facts)
# MAGIC - each referenced primary filing document (HTML) + the full submission `.txt`
# MAGIC
# MAGIC Raw filing bytes land in the `bronze_edgar_raw` **Volume**; metadata / JSON /
# MAGIC extracted text land in `bronze_*` Delta tables. All fetches go through
# MAGIC `lib/sec_client.SecClient` (token bucket ≤ 8 req/s, descriptive User-Agent,
# MAGIC 429/5xx retry). Fetch is **driver-sequential** so the 10 req/s SEC cap is
# MAGIC honored globally without a distributed rate limiter.

# COMMAND ----------

# DBTITLE 1,Widgets & config
import json
import os
import sys
import datetime as dt

# Make `lib/` importable when run from a Databricks Git folder.
_repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from lib.sec_client import SecClient, cik10, accession_nodash, iter_recent_filings

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele")
dbutils.widgets.text("ciks_config", "../config/ciks.json")
dbutils.widgets.text("max_filings_per_cik", "40")
dbutils.widgets.text(
    "sec_user_agent",
    "EDGAR Intelligence Platform - Zach Steele zacharysteele8@gmail.com",
)

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MAX_FILINGS = int(dbutils.widgets.get("max_filings_per_cik"))
USER_AGENT = dbutils.widgets.get("sec_user_agent")

with open(dbutils.widgets.get("ciks_config")) as fh:
    CFG = json.load(fh)
COMPANIES = CFG["companies"]
FORMS = set(CFG.get("forms", ["10-K", "10-Q", "8-K"]))

VOLUME = f"{CATALOG}.{SCHEMA}.bronze_edgar_raw"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/bronze_edgar_raw"
INGESTED_AT = dt.datetime.utcnow().isoformat()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
print(f"catalog={CATALOG} schema={SCHEMA} companies={len(COMPANIES)} forms={sorted(FORMS)}")

# COMMAND ----------

# DBTITLE 1,Fetch (driver-sequential, rate-limited)
client = SecClient(user_agent=USER_AGENT, requests_per_second=8.0)

submission_rows, filing_rows, document_rows, facts_rows, text_rows = [], [], [], [], []

for co in COMPANIES:
    cik_raw, ticker, name = co["cik"], co.get("ticker"), co.get("name")
    c10 = cik10(cik_raw)
    print(f"\n=== {ticker or ''} CIK {c10} ({name}) ===")

    subs = client.submissions(c10)
    submission_rows.append(
        {
            "cik": c10,
            "entity_name": subs.get("name"),
            "ticker": ticker,
            "sic": subs.get("sic"),
            "sic_description": subs.get("sicDescription"),
            "fiscal_year_end": subs.get("fiscalYearEnd"),
            "exchanges": ",".join(subs.get("exchanges", []) or []),
            "tickers": ",".join(subs.get("tickers", []) or []),
            "raw_json": json.dumps(subs)[:15_000_000],
            "ingested_at": INGESTED_AT,
        }
    )

    # companyfacts — the pre-extracted XBRL financial facts
    try:
        facts = client.company_facts(c10)
        facts_rows.append(
            {
                "cik": c10,
                "entity_name": facts.get("entityName"),
                "companyfacts_json": json.dumps(facts),
                "ingested_at": INGESTED_AT,
            }
        )
        n_units = sum(
            len(u)
            for t in (facts.get("facts") or {}).values()
            for b in t.values()
            for u in (b.get("units") or {}).values()
        )
        print(f"  companyfacts observations ~ {n_units:,}")
    except Exception as exc:  # some CIKs have no companyfacts
        print(f"  companyfacts unavailable: {exc}")

    # filings — filter to the forms we care about, newest first, capped
    kept = 0
    for f in iter_recent_filings(subs):
        if f.get("form") not in FORMS:
            continue
        if kept >= MAX_FILINGS:
            break
        kept += 1
        accession = f.get("accessionNumber")
        primary = f.get("primaryDocument") or ""
        accn_nd = accession_nodash(accession)
        filing_rows.append(
            {
                "cik": c10,
                "accession": accession,
                "form": f.get("form"),
                "filing_date": f.get("filingDate"),
                "report_date": f.get("reportDate") or None,
                "primary_document": primary,
                "primary_doc_description": f.get("primaryDocDescription"),
                "is_xbrl": int(f.get("isXBRL") or 0),
                "size": int(f.get("size") or 0),
                "ingested_at": INGESTED_AT,
            }
        )

        # primary HTML document -> Volume + text
        if primary:
            try:
                raw = client.filing_document(c10, accession, primary)
                ext = primary.rsplit(".", 1)[-1].lower() if "." in primary else "bin"
                vol_dir = f"{VOLUME_ROOT}/{c10}/{accn_nd}"
                os.makedirs(vol_dir, exist_ok=True)
                vol_path = f"{vol_dir}/{primary}"
                with open(vol_path, "wb") as out:
                    out.write(raw)
                document_rows.append(
                    {
                        "cik": c10,
                        "accession": accession,
                        "document": primary,
                        "source_format": ext,
                        "volume_path": vol_path,
                        "byte_size": len(raw),
                        "ingested_at": INGESTED_AT,
                    }
                )
                if ext in ("htm", "html", "txt"):
                    body = raw.decode("utf-8", errors="replace")
                    text_rows.append(
                        {
                            "cik": c10,
                            "accession": accession,
                            "doc_kind": "primary_html",
                            "document": primary,
                            "text": body,
                            "char_len": len(body),
                            "ingested_at": INGESTED_AT,
                        }
                    )
            except Exception as exc:
                print(f"  [{accession}] primary doc failed: {exc}")

        # full submission text file (Variety: mixed structured + unstructured)
        try:
            sub_txt = client.submission_txt(c10, accession)
            vol_dir = f"{VOLUME_ROOT}/{c10}/{accn_nd}"
            os.makedirs(vol_dir, exist_ok=True)
            with open(f"{vol_dir}/{accn_nd}.txt", "wb") as out:
                out.write(sub_txt)
            body = sub_txt.decode("utf-8", errors="replace")
            text_rows.append(
                {
                    "cik": c10,
                    "accession": accession,
                    "doc_kind": "submission_txt",
                    "document": f"{accn_nd}.txt",
                    "text": body[:8_000_000],
                    "char_len": len(body),
                    "ingested_at": INGESTED_AT,
                }
            )
        except Exception as exc:
            print(f"  [{accession}] submission .txt failed: {exc}")

    print(f"  kept {kept} filings")

print(
    f"\nfetched: {len(submission_rows)} submissions, {len(facts_rows)} companyfacts, "
    f"{len(filing_rows)} filings, {len(document_rows)} docs, {len(text_rows)} text blobs"
)

# COMMAND ----------

# DBTITLE 1,Write Bronze Delta tables
def _write(rows, table):
    if not rows:
        print(f"  (skip {table} — 0 rows)")
        return 0
    (
        spark.createDataFrame(rows)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{table}")
    )
    print(f"  wrote {len(rows):>6} -> {table}")
    return len(rows)

counts = {
    "bronze_company_submissions": _write(submission_rows, "bronze_company_submissions"),
    "bronze_filings": _write(filing_rows, "bronze_filings"),
    "bronze_filing_documents": _write(document_rows, "bronze_filing_documents"),
    "bronze_xbrl_facts": _write(facts_rows, "bronze_xbrl_facts"),
    "bronze_filing_text": _write(text_rows, "bronze_filing_text"),
}

# COMMAND ----------

# DBTITLE 1,Exit
dbutils.notebook.exit(json.dumps({"status": "ok", "counts": counts, "ingested_at": INGESTED_AT}))
