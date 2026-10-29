# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingest SEC EDGAR for the peer set  (incremental)
# MAGIC
# MAGIC Per CIK in `config/peers.json`:
# MAGIC - Submissions API payload (filing index + fiscal-year end) and the XBRL
# MAGIC   companyfacts payload — both refreshed every run.
# MAGIC - Every **10-K / 10-Q** (and /A) since `since_date`: primary HTML + full
# MAGIC   submission `.txt`, plus the XBRL instance (`*_htm.xml`) extracted from the
# MAGIC   `.txt` — the instance carries company extension concepts and dimensioned
# MAGIC   facts that companyfacts drops.
# MAGIC - Every **8-K with Item 2.02** (results of operations) since `since_date`:
# MAGIC   the `.txt` plus the EX-99.1 earnings release (Adj. EBITDA source).
# MAGIC
# MAGIC Raw bytes land in the `raw_edgar` **Volume** under `/{cik}/{accession}/`;
# MAGIC metadata + release text land in `bronze_*` Delta tables via `MERGE`.
# MAGIC Fetch is driver-sequential through `lib/sec_client.SecClient` (token bucket,
# MAGIC descriptive User-Agent, 429/5xx retry) so SEC's 10 req/s cap holds globally.
# MAGIC
# MAGIC | Table | Key | On match |
# MAGIC |---|---|---|
# MAGIC | `bronze_submissions` | `cik` | refresh |
# MAGIC | `bronze_companyfacts` | `cik` | refresh |
# MAGIC | `bronze_filings` | `cik, accession` | keep original |
# MAGIC | `bronze_filing_documents` | `cik, accession, document` | keep original |
# MAGIC | `bronze_earnings_releases` | `cik, accession` | keep original |
# MAGIC
# MAGIC `mode=incremental` skips an accession already in `bronze_filings` whose
# MAGIC `.txt` is in the Volume; `mode=full` re-downloads in-window filings.

# COMMAND ----------

# DBTITLE 1,Widgets & config
import datetime as dt
import json
import os
import sys

# Make `lib/` importable when run from a bundle / Git folder (cwd = src/).
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from lib.sec_client import SecClient, cik10, accession_nodash, iter_recent_filings
from lib.edgar_parse import (
    extract_submission_document, extract_xbrl_instance, html_to_text,
    parse_submission_documents,
)

dbutils.widgets.text("catalog", "engineering_playground_dev")
dbutils.widgets.text("bronze_schema", "bronze_edgar")
dbutils.widgets.text("peers_config", "../config/peers.json")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
# Earliest filing date to download. LTM growth needs 2 years of history and the
# averages another year, so 2021 leaves a few years of trend on top.
dbutils.widgets.text("since_date", "2021-01-01")
dbutils.widgets.text("sec_user_agent", "EDGAR Peer Financials zacharysteele8@gmail.com")
dbutils.widgets.text("sec_requests_per_second", "5")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("bronze_schema")
MODE = dbutils.widgets.get("mode")
FULL = MODE == "full"
SINCE = dbutils.widgets.get("since_date")
USER_AGENT = dbutils.widgets.get("sec_user_agent")
SEC_RPS = float(dbutils.widgets.get("sec_requests_per_second"))

with open(dbutils.widgets.get("peers_config")) as fh:
    COMPANIES = json.load(fh)["companies"]

PERIODIC_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
EARNINGS_ITEM = "2.02"

T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw_edgar"
INGESTED_AT = dt.datetime.utcnow().isoformat()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_edgar")
print(f"{CATALOG}.{SCHEMA} mode={MODE} since={SINCE} companies={len(COMPANIES)}")

# COMMAND ----------

# DBTITLE 1,What's already ingested
if spark.catalog.tableExists(T("bronze_filings")):
    seen_accessions = {r["accession"] for r in spark.table(T("bronze_filings")).select("accession").collect()}
else:
    seen_accessions = set()
print(f"{len(seen_accessions):,} filings already in bronze_filings"
      f"{' — ignored (mode=full)' if FULL else ''}")

# COMMAND ----------

# DBTITLE 1,Helpers
client = SecClient(user_agent=USER_AGENT, requests_per_second=SEC_RPS)

submission_rows, facts_rows, filing_rows, document_rows, release_rows = [], [], [], [], []
merged = {}


def _upsert(rows, table, keys, refresh_on_match):
    """MERGE `rows` into `table` on `keys`. refresh_on_match=True overwrites the
    stored row (re-pulled payloads); False keeps the first scraping (immutable
    filing data). Creates the table on first run."""
    if not rows:
        return 0
    fq = T(table)
    df = spark.createDataFrame(rows)
    if not spark.catalog.tableExists(fq):
        df.write.format("delta").saveAsTable(fq)
        return len(rows)
    view = f"_stg_{table}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    matched = "WHEN MATCHED THEN UPDATE SET *\n" if refresh_on_match else ""
    spark.sql(f"MERGE INTO {fq} t USING {view} s ON {on}\n{matched}WHEN NOT MATCHED THEN INSERT *")
    return len(rows)


def _flush():
    """Write everything buffered so far — called after every company, so a
    failure only loses the company in flight; the next run resumes."""
    global submission_rows, facts_rows, filing_rows, document_rows, release_rows
    plan = [
        (submission_rows, "bronze_submissions", ["cik"], True),
        (facts_rows, "bronze_companyfacts", ["cik"], True),
        (filing_rows, "bronze_filings", ["cik", "accession"], False),
        (document_rows, "bronze_filing_documents", ["cik", "accession", "document"], False),
        (release_rows, "bronze_earnings_releases", ["cik", "accession"], False),
    ]
    for rows, table, keys, refresh in plan:
        merged[table] = merged.get(table, 0) + _upsert(rows, table, keys, refresh)
    submission_rows, facts_rows, filing_rows, document_rows, release_rows = [], [], [], [], []


def _write(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as out:
        out.write(data)


def _doc_row(cik, accession, document, doc_type, path, size):
    return {
        "cik": cik, "accession": accession, "document": document, "doc_type": doc_type,
        "volume_path": path, "byte_size": size, "ingested_at": INGESTED_AT,
    }


def iter_filings(subs: dict):
    """Every filing in the submissions payload: `filings.recent` (latest ~1000)
    plus the older paged files when `since_date` reaches back past them."""
    yield from iter_recent_filings(subs)
    for page in subs.get("filings", {}).get("files", []) or []:
        if (page.get("filingTo") or "") < SINCE:
            continue
        older = client.get_json(f"https://data.sec.gov/submissions/{page['name']}")
        yield from iter_recent_filings({"filings": {"recent": older}})

# COMMAND ----------

# DBTITLE 1,Fetch (driver-sequential, rate-limited) — flushes after each company
skipped = 0
for i, co in enumerate(COMPANIES, 1):
    c10, ticker = cik10(co["cik"]), co["ticker"]
    print(f"=== {i}/{len(COMPANIES)} {ticker} CIK {c10} ===")
    try:
        subs = client.submissions(c10)
    except Exception as exc:
        print(f"  submissions failed, skipping: {exc}")
        continue

    subs_bytes = json.dumps(subs).encode()
    subs_path = f"{VOLUME_ROOT}/{c10}/submissions.json"
    _write(subs_path, subs_bytes)
    submission_rows.append({
        "cik": c10, "ticker": ticker, "entity_name": subs.get("name"),
        "fiscal_year_end": subs.get("fiscalYearEnd"), "sic": subs.get("sic"),
        "sic_description": subs.get("sicDescription"),
        "tickers": ",".join(subs.get("tickers") or []),
        "volume_path": subs_path, "raw_json": subs_bytes.decode(), "ingested_at": INGESTED_AT,
    })

    try:
        facts = client.company_facts(c10)
        facts_bytes = json.dumps(facts).encode()
        facts_path = f"{VOLUME_ROOT}/{c10}/companyfacts.json"
        _write(facts_path, facts_bytes)
        facts_rows.append({
            "cik": c10, "entity_name": facts.get("entityName"), "volume_path": facts_path,
            "companyfacts_json": facts_bytes.decode(), "ingested_at": INGESTED_AT,
        })
    except Exception as exc:
        print(f"  companyfacts unavailable: {exc}")

    n_new = 0
    for f in iter_filings(subs):
        form, filed = f.get("form"), f.get("filingDate") or ""
        items = f.get("items") or ""
        is_periodic = form in PERIODIC_FORMS
        is_earnings = form == "8-K" and EARNINGS_ITEM in items.split(",")
        if filed < SINCE or not (is_periodic or is_earnings):
            continue

        accession = f["accessionNumber"]
        accn_nd = accession_nodash(accession)
        vol_dir = f"{VOLUME_ROOT}/{c10}/{accn_nd}"
        txt_path = f"{vol_dir}/{accn_nd}.txt"
        if accession in seen_accessions and os.path.exists(txt_path) and not FULL:
            skipped += 1
            continue

        filing_rows.append({
            "cik": c10, "accession": accession, "form": form, "items": items,
            "filing_date": filed, "report_date": f.get("reportDate") or "",
            "primary_document": f.get("primaryDocument") or "",
            "is_xbrl": int(f.get("isXBRL") or 0),
            "is_inline_xbrl": int(f.get("isInlineXBRL") or 0),
            "ingested_at": INGESTED_AT,
        })
        n_new += 1

        try:
            raw_txt = client.submission_txt(c10, accession)
        except Exception as exc:
            print(f"  [{accession}] submission .txt failed: {exc}")
            continue
        _write(txt_path, raw_txt)
        document_rows.append(_doc_row(c10, accession, f"{accn_nd}.txt", "SUBMISSION_TXT", txt_path, len(raw_txt)))
        sub_txt = raw_txt.decode("utf-8", errors="replace")

        if is_periodic:
            primary = f.get("primaryDocument")
            if primary:
                try:
                    raw = client.filing_document(c10, accession, primary)
                    _write(f"{vol_dir}/{primary}", raw)
                    document_rows.append(_doc_row(c10, accession, primary, form, f"{vol_dir}/{primary}", len(raw)))
                except Exception as exc:
                    print(f"  [{accession}] primary doc failed: {exc}")
            inst = extract_xbrl_instance(sub_txt)
            if inst:
                fname, body = inst
                data = body.encode("utf-8")
                _write(f"{vol_dir}/{fname}", data)
                document_rows.append(_doc_row(c10, accession, fname, "XBRL_INSTANCE", f"{vol_dir}/{fname}", len(data)))
            else:
                print(f"  [{accession}] {form}: no XBRL instance in submission")

        if is_earnings:
            # EX-99.1 is the press release; fall back to the first EX-99.x.
            manifest = parse_submission_documents(sub_txt)
            ex99 = [d for d in manifest if (d["doc_type"] or "").upper().startswith("EX-99")]
            pick = next((d for d in ex99 if d["doc_type"].upper() in ("EX-99.1", "EX-99.01")), ex99[0] if ex99 else None)
            doc = extract_submission_document(sub_txt, filename_suffix=pick["filename"]) if pick and pick["filename"] else None
            if doc:
                fname, body = doc
                data = body.encode("utf-8")
                _write(f"{vol_dir}/{fname}", data)
                document_rows.append(_doc_row(c10, accession, fname, pick["doc_type"], f"{vol_dir}/{fname}", len(data)))
                text = html_to_text(body)
                release_rows.append({
                    "cik": c10, "accession": accession, "filing_date": filed,
                    "report_date": f.get("reportDate") or "", "document": fname,
                    "doc_type": pick["doc_type"], "text": text, "char_len": len(text),
                    "ingested_at": INGESTED_AT,
                })
            else:
                print(f"  [{accession}] 8-K 2.02: no EX-99 exhibit")

    print(f"  {n_new} new filing(s)")
    _flush()

print(f"\ndone: {skipped} already-ingested skipped, rows merged this run: {merged}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "ok", "mode": MODE, "merged": merged, "skipped": skipped}))
