# AGENT.md — EDGAR Intelligence Platform

Guide for engineers and AI coding agents working in this repo. Capstone for the
DataExpert.io × Databricks bootcamp.

## What it is

Turns SEC EDGAR into a searchable financial-data platform:

- **Spark medallion pipeline** over EDGAR filings + market data (bronze → silver → gold)
- **AI layer**: per-filing briefings, computed financial ratios + trend engine, and
  an LLM-generated **Investor Health Score** + report (see `docs/ANALYST_SPEC.md`)
- **Vector Search** (hybrid) over filing text
- **Lakebase (Postgres)** operational store for users / watchlists / saved
  filings / research notes / agent activity
- **Reverse Change Data Feed**: Lakebase → Delta history tables → a triggered
  analytics job (see `docs/ARCHITECTURE.md`)
- **Flask app** (4 screens) with an in-process tool-calling **agent**

## Repository layout

```
config/ciks.json          pilot company list (scale here for the graded run)
lib/lakebase.py            Postgres helper — run_query / run_write (edgar schema)
lib/warehouse.py           Gold/Silver Delta reads via the SQL warehouse
lib/sec_client.py          rate-limited SEC EDGAR client (token bucket, retry)
lib/edgar_parse.py         companyfacts -> rows, HTML -> sections, SGML manifest, chunker
sql/00_create_schema.sql   CREATE SCHEMA edgar  (on the zdsteele-capstone Lakebase)
sql/10_operational_tables.sql   8 tables, each REPLICA IDENTITY FULL
sql/20_seed.sql            demo user + pilot companies + a watchlist
notebooks/                 01 bronze SEC · 02 market · 03 silver · 04 gold ·
                           05 vector search · 06 analytics CDF · 07 UC tags ·
                           08 filing intelligence · 09 ratios · 10 company health
agent/prompt.py            analyst system prompt
agent/tools.py             12 tools (retrieval + write), each logged to agent_actions
agent/graph.py             LangGraph ReAct loop (ChatDatabricks), run_agent()
app.py                     Flask: 4 screens + JSON API
templates/ static/         Jinja + vanilla-JS frontend
app.yaml  databricks.yml   Databricks App + Asset Bundle (analytics job) config
docs/ARCHITECTURE.md       Lakebase <-> Lakehouse data-flow + job triggers
docs/ANALYST_SPEC.md       the target analyst spec + what's built vs. scoped-future
docs/SETUP.md              first-time infra runbook
```

## Namespaces (this workspace)

| Layer | Location |
|---|---|
| Delta medallion (bronze/silver/gold, `lb_*_history`) | `bootcamp_students.zdsteele_capstone` (Unity Catalog) |
| Raw filing bytes | Volume `bootcamp_students.zdsteele_capstone.bronze_edgar_raw` |
| Operational Postgres | schema `edgar` on the dedicated Lakebase instance `zdsteele-capstone` |
| SQL warehouse (Delta reads) | `Serverless Starter Warehouse`, id `b15d3d6f837ba428` |
| LLM | serving endpoint `databricks-meta-llama-3-3-70b-instruct` (also `ai_query` in notebooks) |
| Vector Search | index `…zdsteele_capstone.filing_text_index` on endpoint `zachy_vs` |

## Run locally

```bash
cd capstone
python -m venv venv && . venv/bin/activate      # Windows: .\venv\Scripts\Activate.ps1
pip install -r requirements.txt                  # if pip's resolver stalls: add --use-deprecated=legacy-resolver
cp .env.example .env                             # then fill it in (see below)
python app.py                                    # http://localhost:8000
```

`.env` (all consumed at process start via `python-dotenv`):

| var | what |
|---|---|
| `LAKEBASE_URL` | `postgresql://<role>:<password>@<host>/databricks_postgres?sslmode=require` — use a **native-password** Lakebase role (`edgar_app`), not the 1-hour OAuth token |
| `LAKEBASE_SCHEMA` | `edgar` |
| `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | service-principal M2M auth for the warehouse + LLM (PATs are disabled in this org) |
| `DATABRICKS_WAREHOUSE_ID` | `b15d3d6f837ba428` |
| `UC_CATALOG` / `UC_SCHEMA` | `bootcamp_students` / `zdsteele_capstone` |
| `LLM_ENDPOINT` | `databricks-meta-llama-3-3-70b-instruct` |
| `VS_ENDPOINT` / `VS_INDEX` | `zachy_vs` / `bootcamp_students.zdsteele_capstone.filing_text_index` (blank both → keyword search) |
| `FLASK_SECRET_KEY` | any string (session signing) |

The deployed Databricks App gets Lakebase creds + the warehouse binding from
`databricks.yml`; `.env` is local-dev only.

## Pipeline (run order, Databricks notebooks)

`01_bronze_ingest_sec` → `02_bronze_ingest_market` → `03_silver_transform` →
`04_gold_marts` → `08_filing_intelligence` → `09_financial_ratios` →
`10_company_health` → `05_vector_search_index` (optional) →
`06_analytics_cdf` (after there's app/agent activity) → `07_tag_capstone_tables` (optional).

Widgets default to `catalog=bootcamp_students`, `schema=zdsteele_capstone`.

## Conventions & gotchas

- **Commits**: imperative subject, `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>`.
  Working on `main` (solo capstone repo). Line endings normalize to CRLF on Windows checkout — harmless.
- **`databricks-langchain==0.16.1` is pinned** — newer releases pull
  `openai-agents` / `mcp` and blow up pip's resolver. `ChatOpenAI` against the
  serving endpoint does NOT parse Llama's tool-call format (`<function=…>` text),
  so `agent/graph.py` uses `ChatDatabricks`.
- **`ai_query` on this workspace** does not support `responseFormat` (`'json_object'`
  rejected; a DDL string returns plain text). Notebooks 08/10 use plain
  `ai_query` + a strict "one JSON object, single-line strings" prompt +
  `regexp_extract` + `from_json` with `ARRAY<STRING>` fields.
- **`databricks-sql-connector` without pyarrow** returns `ARRAY`/`STRUCT`/`MAP`
  columns as JSON strings — `lib/warehouse.py:_clean` parses them back, and also
  maps `NaN`/`Inf`→`None`, `Decimal`→`float`, dates→isoformat, so `jsonify` is safe.
- **Serverless compute** rejects `.cache()`/`PERSIST` — notebook 04 materializes a
  Delta table instead.
- **companyfacts quirks**: `fiscal_year` is unreliable (stamps the filing FY on
  comparative-period facts) — disambiguate by `period_end`. The same concept is
  reported at multiple durations per period; `silver_financial_facts.period_type`
  (`instant`/`quarter`/`half`/`ytd9`/`annual`) fixes quarterly-vs-YTD confusion.
  Operating cash flow / capex are filed cumulatively — notebook 09 rebuilds
  discrete quarters (Q2 = H1−Q1, Q3 = 9M−H1).
- **Frontend**: `static/app.js` loads in `<head>` so page scripts (in any block)
  can use `getJSON`/`renderMarkdown`. Wrap every data table in `<div class="tbl">`
  (scroll container + sticky header).
- **Verifying against the live workspace**: read-only diagnostics via
  `databricks-sql-connector` + `databricks.sdk.core.Config` (M2M) against the
  warehouse — see the pattern in `docs/SETUP.md`.

## The one manual UI step

Reverse CDC: in the `zdsteele-capstone` Lakebase project → **Lakebase CDF** →
Start, source schema `edgar` → target `bootcamp_students.zdsteele_capstone`. This
creates the 8 `lb_<table>_history` Delta tables. Everything else is
paste-and-run. Details + status in `docs/ARCHITECTURE.md`.
