# Architecture — how Lakebase relates to the Lakehouse

Two stores, one loop.

- **Lakehouse (Delta / Unity Catalog)** — `bootcamp_students.zdsteele_capstone`.
  Analytical: the medallion pipeline output (bronze/silver/gold) and the AI
  tables. Big, append-heavy, batch-built by Spark.
- **Lakebase (Postgres)** — schema `edgar` on the dedicated `zdsteele-capstone`
  instance. Operational / transactional: `users`, `watchlists`,
  `watchlist_companies`, `saved_filings`, `saved_research`,
  `agent_conversations`, `agent_actions`. Small, row-level, written live by the
  app + agent.

## Data flow

```mermaid
flowchart TB
  subgraph SRC[External]
    SEC[SEC EDGAR APIs + archives]
    MKT[yfinance]
  end

  subgraph LH[Lakehouse — bootcamp_students.zdsteele_capstone  Delta]
    BRZ[bronze_*  + Volume bronze_edgar_raw]
    SLV[silver_*  incl. silver_financial_facts, *_chunks]
    GLD[gold_*  financials / ratios / filing_intelligence / company_health]
    LBH[lb_*_history  ← CDC landing]
    USE[gold_usage_events + gold_agent_tool_stats / gold_agent_confidence / gold_usage_funnel]
    VS[(Vector Search index<br/>filing_text_index @ zachy_vs)]
  end

  subgraph APP[Databricks App  — Flask + in-process agent]
    API[JSON API / 4 screens]
    AGT[LangGraph tool-calling agent]
  end

  subgraph LB[Lakebase — zdsteele-capstone / schema edgar  Postgres]
    OPS[users · watchlists · watchlist_companies ·<br/>saved_filings · saved_research ·<br/>agent_conversations · agent_actions]
  end

  SEC & MKT -->|nb 01-02  rate-limited PySpark| BRZ
  BRZ -->|nb 03| SLV
  SLV -->|nb 04| GLD
  SLV -->|nb 08 ai_query| GLD
  GLD -->|nb 09 ratios / nb 10 ai_query| GLD
  SLV -->|nb 05  DELTA_SYNC + gte-large-en| VS

  GLD -->|SQL warehouse b15d3d6f837ba428<br/>lib/warehouse.py — READ ONLY| API
  VS  -->|hybrid query| AGT
  API --> AGT

  AGT -->|lib/lakebase.py  run_write  (the ONLY writes)| OPS
  API -->|login upsert| OPS

  OPS ==>|Lakebase CDF  (logical replication)<br/>REPLICA IDENTITY FULL| LBH
  LBH -->|nb 06  watermark job — TRIGGERED| USE
  USE -->|SQL warehouse| API
```

## Answers to the review questions

**How is Lakebase related to the Lakehouse?**
The app **reads** analytics straight from Delta through the SQL warehouse
(`lib/warehouse.py`). The app + agent **write** operational rows to Lakebase
(`lib/lakebase.py`). Those Lakebase writes then **replicate into Delta** as
`lb_*_history` tables (Lakebase Change Data Feed). So the two stores are joined
by exactly one automated bridge: **Lakebase → Delta**, one direction.

**Does anything get synced *into* Lakebase from the Lakehouse?**
No — by design. The app reads Gold directly from Delta through the SQL warehouse
(`lib/warehouse.py`), so there is no need to mirror analytics into Postgres.
Databricks Synced Tables (Delta → Lakebase) are **not a capstone requirement**
— nothing in the Required Components list mentions them — and adding them would
mean a second stale-able copy of every Gold table plus per-environment setup in
the workspace UI. Keeping reads pointed at Delta gives the same result with one
data direction and one source of truth.

**What gets synced back (Lakebase → Lakehouse)?**
The 8 `edgar` tables, each with `REPLICA IDENTITY FULL`, land as change-history
Delta tables carrying `_pg_change_type` / `_pg_lsn` / `_pg_xid` / `_timestamp` /
`_sort_by` plus the row image:

| Postgres (`edgar`) | Delta (`…zdsteele_capstone`) |
|---|---|
| `agent_actions` | `lb_agent_actions_history` |
| `agent_conversations` | `lb_agent_conversations_history` |
| `saved_filings` | `lb_saved_filings_history` |
| `saved_research` | `lb_saved_research_history` |
| `watchlists` | `lb_watchlists_history` |
| `watchlist_companies` | `lb_watchlist_companies_history` |
| `companies` | `lb_companies_history` |
| `users` | `lb_users_history` |

**Do any jobs get triggered?**
`databricks.yml` defines **two** jobs (and nothing else is automated):

1. **`analytics_cdf_on_change`** — *event-triggered, not scheduled*. A
   `trigger.table_update` on the six analytics-relevant `lb_*_history` tables
   (`ANY_UPDATED`, 60 s debounce). Runs `notebooks/06_analytics_cdf.py`, which
   does an incremental watermark read of the new CDF rows
   (`_sort_by > last_processed AND _pg_change_type NOT IN ('update_preimage','delete')`),
   appends normalized rows to append-only `gold_usage_events`, and rebuilds the
   usage marts: `gold_agent_tool_stats` (per-tool call counts + success rate),
   `gold_agent_confidence` (answer-confidence distribution),
   `gold_watchlist_activity`, `gold_filing_view_stats`, `gold_metric_view_stats`,
   `gold_usage_funnel`. The Dashboard → **Platform activity** tab reads those.
2. **`pipeline_daily_refresh`** — *scheduled*, 06:30 America/New_York. An
   8-task job that keeps the platform current: incremental SEC ingest (01) gates
   silver (03) → gold marts (04), vector search (05), filing intelligence (08)
   in parallel → ratios (09) → health score (10). Market ingest (02) runs
   alongside as best-effort enrichment and does **not** gate silver (yfinance is
   flaky; silver joins the last good bars). Runs live only under `-t prod` (DAB
   development mode auto-pauses schedules). See the run-order section below.

The medallion notebooks are otherwise run **by hand** — the numbering (01…10) is
a build order for a human, not an automation chain; no notebook fires the next.

**Has it been set up?**

| Piece | Status |
|---|---|
| `edgar` schema + 8 tables, `REPLICA IDENTITY FULL` | ✅ created on `zdsteele-capstone` |
| Lakebase CDF `edgar` → `bootcamp_students.zdsteele_capstone` | ✅ enabled; all 8 `lb_*_history` present, seeded rows flowed, metadata columns confirmed |
| Warehouse reads (app/agent → Delta) | ✅ working locally against the live workspace |
| `analytics_cdf_on_change` job | ✅ defined in `databricks.yml`; deploys with `databricks bundle deploy`. Needs app/agent activity before its marts have content |
| Notebook 06 output marts | ⏳ populate after the agent has been used (writes `agent_actions` → CDF → job) |
| `pipeline_daily_refresh` job | ✅ defined in `databricks.yml`; runs live under `-t prod`. 01 / 02 are incremental (MERGE, never overwrite) |

## Notebooks, jobs, and what runs when

The `NN_` prefixes are a **human build order**, not a pipeline. Nothing chains
automatically except inside the two Asset-Bundle jobs.

### One-time / periodic build (you run these, in order)

```
config/ciks.json                         yfinance
      │                                     │
      ▼                                     ▼
┌──────────────┐                   ┌──────────────────┐
│ 01 bronze    │                   │ 02 bronze market │   independent —
│    SEC       │  (incremental)    │   (incremental)  │   either order
└──────┬───────┘                   └────────┬─────────┘
       │ bronze_*                           │ bronze_market_bars
       └─────────────────┬──────────────────┘
                         ▼
                 ┌───────────────┐
                 │ 03 silver     │  silver_companies / _filings /
                 │    transform  │  _financial_facts / _filing_sections /
                 └──┬────┬────┬──┘  _filing_text_chunks
                    │    │    │
        ┌───────────┘    │    └──────────────┐
        ▼                ▼                    ▼
  ┌───────────┐   ┌──────────────┐   ┌──────────────────┐
  │ 04 gold   │   │ 05 vector    │   │ 08 filing        │  ai_query →
  │    marts  │   │    search    │   │    intelligence  │  gold_filing_intelligence
  └─────┬─────┘   └──────────────┘   └────────┬─────────┘
        │ gold_company_financials             │
        ▼                                     │
  ┌──────────────┐                            │
  │ 09 financial │                            │
  │    ratios    │                            │
  └──────┬───────┘                            │
         │ gold_financial_ratios              │
         └──────────────┬─────────────────────┘
                        ▼
                ┌────────────────┐
                │ 10 company     │  ai_query → gold_company_health
                │    health      │
                └────────────────┘

  ┌────────────────┐
  │ 07 tag tables  │  UC tags/comments — run last, any time, idempotent
  └────────────────┘
```

Strict dependency order: `01, 02 → 03 → {04, 05, 08} → 09 → 10`; `07` whenever.
Every notebook is idempotent (`MERGE` / `CREATE OR REPLACE` / partition
overwrite) — safe to re-run; you re-run only to add companies or refresh data.

### Live system (runs on its own after deploy)

```
Databricks App (Flask + agent)   ← long-running service, never "finishes"
      │  agent writes a row
      ▼
Lakebase Postgres   edgar.agent_actions / saved_filings / watchlists / …
      │
      │  Lakebase Change Data Feed   (automatic, continuous — not a job)
      ▼
lb_*_history  Delta tables
      │
      │  table_update TRIGGER   ANY_UPDATED · wait 60s · max once / 60s
      ▼
JOB  analytics_cdf_on_change  ──runs──▶  06_analytics_cdf.py
      │                                   (watermark: each run reads only new rows)
      ▼
gold_usage_events  (+ gold_agent_tool_stats, gold_usage_funnel, …)
      │
      ▼
Dashboard → "Platform activity" tab
```

### Cadence

| # | Notebook | Needs | Started by | How often |
|---|---|---|---|---|
| 01 | bronze SEC | `config/ciks.json` | you / `pipeline_daily_refresh` | daily incremental; `mode=full` to backfill |
| 02 | bronze market | — | you / `pipeline_daily_refresh` | daily incremental (5-day pull, MERGE) |
| 03 | silver transform | 01, 02 | you / `pipeline_daily_refresh` | after 01/02 |
| 04 | gold marts | 03 | you / `pipeline_daily_refresh` | after 03 |
| 05 | vector search index | 03 | you / `pipeline_daily_refresh` | after 03 (index then self-syncs; managed by Vector Search, not the bundle) |
| 08 | filing intelligence | 03 | you / `pipeline_daily_refresh` | after 03 — `MERGE`, only new filings hit `ai_query` |
| 09 | financial ratios | 04 | you / `pipeline_daily_refresh` | after 04 |
| 10 | company health | 08, 09 | you / `pipeline_daily_refresh` | after 08/09 |
| 07 | tag tables | any tables | you | once / after adding tables — cosmetic |
| 06 | analytics CDF | `lb_*_history` + agent activity | **`analytics_cdf_on_change` trigger** | every burst of agent writes, 60 s debounce — the only continuously-recurring notebook |

- **Scheduled jobs:** one — `pipeline_daily_refresh` (prod target only).
- **Triggered jobs:** one — `analytics_cdf_on_change` (on `lb_*_history` change).
- **Everything else:** manual notebook runs, done once for a scope and re-run
  only to widen scope or force a refresh.
- **Not jobs:** the Databricks App and the Lakebase CDF stream are always-on
  services once configured.

### Incremental ingest (notebooks 01 & 02)

Both bronze ingests **MERGE, never overwrite** — a scraping is never lost, even
if a CIK later leaves `config/ciks.json` or ages out of the submissions window.

| Table | MERGE key | On match |
|---|---|---|
| `bronze_company_submissions` | `cik` | refresh (filing list changes daily) |
| `bronze_xbrl_facts` | `cik` | refresh (new facts each filing) |
| `bronze_filings` | `cik, accession` | keep original |
| `bronze_filing_documents` | `cik, accession, document` | keep original |
| `bronze_filing_text` | `cik, accession, doc_kind` | keep original |
| `bronze_market_bars` | `cik, bar_date` | refresh (yfinance revises recent closes) |

`01` in `mode=incremental` skips the HTTP fetch for any accession already in
`bronze_filings` whose Volume files exist; `mode=full` re-fetches every in-window
filing to repair missing raw bytes. `02` pulls a 10-year history on the first run
and a 5-day window thereafter.

## Meets the capstone's "2 of 3 Vs"

- **Variety** — JSON (submissions, companyfacts), HTML (filing docs), SGML
  (`submission .txt` → `silver_exhibits`), XBRL, plain text, parsed sections,
  46k text chunks.
- **Volume** — `silver_financial_facts` at 5 pilot CIKs ≈ 25k rows; the CIK list
  is a one-line config and clears **> 1,000,000 rows at ≥ ~100 companies** (full
  S&P 500 ≈ 16 M). Verify with
  `SELECT COUNT(*) FROM bootcamp_students.zdsteele_capstone.silver_financial_facts`.
