# EDGAR Intelligence Platform

SEC filing search, analytics, and an AI research assistant — built on Databricks.
Capstone for the DataExpert.io × Databricks bootcamp.

Turns SEC EDGAR from a document repository into a searchable financial data
platform: a Spark medallion pipeline over EDGAR + market data, a Lakebase
(Postgres) operational store, an action-taking tool-calling AI agent, a
reverse-Change-Data-Feed analytics pipeline, and a deployed Databricks App.

## Architecture

```
SEC EDGAR (Submissions API, XBRL companyfacts, filing archives, full .txt)
yfinance daily bars
        │  rate-limited PySpark ingestion (lib/sec_client.py)
        ▼
Bronze Delta  bootcamp_students.zdsteele_capstone.bronze_*   + Volume bronze_edgar_raw
        ▼  parse XBRL facts, HTML → sections, chunk text, entity-resolve
Silver Delta  silver_companies / silver_filings / silver_financial_facts / silver_filing_sections / silver_filing_text_chunks
        ▼  GAAP concepts → metrics, YoY/QoQ
Gold Delta    gold_company_financials / gold_revenue_history / gold_*_metrics / gold_filing_activity / gold_company_comparisons
        │  read via SQL warehouse (lib/warehouse.py)  — no Synced Tables to hand-create
        ▼
Flask app + in-process LangGraph agent (retrieval tools)
        │  agent write tools + app writes INSERT into Lakebase
edgar.{saved_filings, watchlist_companies, saved_research, agent_conversations, agent_actions}
        │  Lakebase Change Data Feed (reverse Synced Tables — the one UI step)
        ▼
Delta  lb_*_history  →  06_analytics_cdf.py (watermark)  →  gold_usage_events + marts  →  read back via warehouse  →  dashboard
```

## Required components → where they live

| Requirement | Implementation |
|---|---|
| Spark data pipeline | `notebooks/01_bronze_ingest_sec.py` … `04_gold_marts.py` |
| Third-party API | SEC EDGAR APIs (`lib/sec_client.py`) + yfinance (`02_bronze_ingest_market.py`) |
| Lakebase data model | `sql/10_operational_tables.sql` — 8 tables, `REPLICA IDENTITY FULL` |
| Action-taking AI agent | `agent/` — 7 retrieval + 5 write tools, LangGraph loop, in-process in `app.py` |
| Analytics pipeline (CDF) | `notebooks/06_analytics_cdf.py` + `databricks.yml` `trigger.table_update` job |
| Frontend | `app.py` + `templates/` — Company Search / Filing Explorer / Financial Dashboard / AI Research Assistant |
| Deployed app | Databricks App via `app.yaml` |
| 2 of 3 Vs | Variety (JSON/HTML/XBRL/text) + Volume (`silver_financial_facts` > 1M at ≥100 CIKs) |

## Layout

```
config/ciks.json           pilot CIK list (scale here for the graded run)
lib/lakebase.py            Lakebase (Postgres) writer — run_query / run_write
lib/warehouse.py           Gold/Silver Delta reader — SQL warehouse b15d3d6f837ba428
lib/sec_client.py          rate-limited SEC EDGAR client
lib/edgar_parse.py         HTML → sections, XBRL companyfacts → rows, chunking
sql/                       schema + operational tables + seed
notebooks/                 01 bronze → 02 market → 03 silver → 04 gold → (05 vector search) → 06 analytics CDF
agent/                     prompt.py / tools.py / graph.py
app.py, templates/, static/  Flask 4-screen frontend
app.yaml, databricks.yml   deploy config
```

## Setup (summary — full runbook in `docs/SETUP.md`)

1. Run `sql/00`–`20` in the Lakebase **SQL Editor** (tables `edgar.*`).
2. Run notebooks `01`→`04` (widgets default to `bootcamp_students` / `zdsteele_capstone`).
   `05` is optional (keyword search by default).
3. Set up **reverse** CDC only: the 6 `edgar.*` tables →
   `bootcamp_students.zdsteele_capstone.lb_*_history`. Forward reads go through the
   warehouse — nothing to create.
4. `cp .env.example .env` (paste the Lakebase Connect string + OAuth token),
   `pip install -r requirements.txt`, `python app.py`.
5. `databricks bundle deploy && databricks bundle run edgar_intelligence`.

See `CONTEXT.md` (local) for the full working notes and task log.
