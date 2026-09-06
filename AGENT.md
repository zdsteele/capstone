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
config/ciks.json          pilot company list (6 — fast dev runs)
config/ciks_full.json     scaled universe (~470 S&P 500) — `python config/build_universe.py`
config/build_universe.py  resolves an S&P 500 ticker list to CIKs via SEC's map
lib/lakebase.py            Postgres helper — run_query / run_write (edgar schema)
lib/warehouse.py           Gold/Silver Delta reads via the SQL warehouse
lib/sec_client.py          rate-limited SEC EDGAR client (token bucket, retry)
lib/edgar_parse.py         companyfacts -> rows, HTML -> sections, SGML manifest, chunker
sql/00_create_schema.sql   CREATE SCHEMA edgar  (on the zdsteele-capstone Lakebase)
sql/10_operational_tables.sql   8 tables, each REPLICA IDENTITY FULL
sql/20_seed.sql            demo user + pilot companies + a watchlist
notebooks/                 01 bronze SEC · 02 market · 03 silver · 04 gold ·
                           05 vector search · 06 analytics CDF · 07 UC tags ·
                           08 filing intelligence + 8-K + business profile · 09 ratios ·
                           10 company health · 11 valuation · 12 filing-language diff ·
                           13 ownership forms · 14 governance
agent/prompt.py            analyst system prompt
agent/tools.py             21 tools (retrieval + 5 writes), each logged to agent_actions
agent/graph.py             LangGraph ReAct loop (ChatDatabricks), run_agent()
app.py                     Flask: 4 screens + JSON API
templates/ static/         Jinja + vanilla-JS frontend
app.yaml  databricks.yml   Databricks App + Asset Bundle (analytics job) config
docs/ARCHITECTURE.md       Lakebase <-> Lakehouse data-flow + job triggers
docs/PIPELINE.md           plain-language: every notebook, the 5 bronze tables, CIK/forms/XBRL
docs/ANALYST_SPEC.md       the target analyst spec + what's built vs. scoped-future
docs/RAG_REVIEW.md         chunking / embedding / retrieval choices vs the modules
docs/SCALING.md            running the full ~470-company universe (Volume V)
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

The `NN_` prefixes are a human build order — no notebook triggers the next.
Dependency order: `01 → 03 → {04, 05, 08, 12} → 09 → 11 → 10`; `02` alongside
`01` (best-effort); `07` whenever; `06` is event-triggered (see Jobs). `13-14`
(ownership/governance) run standalone. Full graph + cadence table in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

- **01 / 02 are incremental** — `mode` widget (`incremental` default | `full`).
  Every bronze write is a Delta `MERGE`, never an overwrite: a scraping is never
  lost if a CIK leaves `config/ciks.json` or ages out of the submissions window.
  `mode=full` re-fetches in-window filings to repair missing Volume bytes.
- **08 / 10 / 12** `MERGE` on accession/cik, so re-runs only send *new*
  filings to `ai_query`.
- Widgets default to `catalog=bootcamp_students`, `schema=zdsteele_capstone`.

## Jobs (`databricks.yml`)

| Job | Kind | Runs |
|---|---|---|
| `pipeline_daily_refresh` | scheduled, 06:30 America/New_York | chain 01→12 (skips 06, 07, 13, 14); **live only under `-t prod`** — dev mode auto-pauses schedules |
| `analytics_cdf_on_change` | `trigger.table_update` on the 6 `lb_*_history` tables (60 s debounce) | `06_analytics_cdf.py` |

`databricks bundle deploy -t dev` for local iteration (schedule paused);
`databricks bundle deploy -t prod` to arm the daily job. The bundle also deploys
the `edgar-intelligence` App.

Run a job from the CLI: `databricks bundle run pipeline_daily_refresh -t dev`
(or `analytics_cdf_on_change`). Add `--no-wait` to fire and return.

**Lakebase binding.** This workspace runs *autoscaling* Lakebase (Postgres
projects/branches), not classic Database Instances — the app resource is a
`postgres:` block (`branch` + `database` resource-name paths), added only under
`targets.prod` in `databricks.yml`. Binding it needs `CAN_MANAGE` on the
`zdsteele-capstone` Postgres project, so `-t prod` must be deployed by the
project owner (or a principal granted CAN_MANAGE). `-t dev` deploys the app
without the binding; it then reads Lakebase via `LAKEBASE_URL` (the `edgar_app`
role). Resource paths: `databricks postgres list-branches projects/zdsteele-capstone`.

## Conventions & gotchas

- **Commits**: imperative subject, `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>`.
  Working on `main` (solo capstone repo). Line endings normalize to CRLF on Windows checkout — harmless.
- **`databricks-langchain==0.4.0` is pinned** — 0.16.x needs `langchain>=1.0` +
  `databricks-mcp` + `databricks-openai` and cannot resolve alongside the
  langchain 0.3 line; 0.5+ pulls `databricks-connect`. `ChatOpenAI` against the
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
