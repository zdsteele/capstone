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
No. There are no Synced Tables / reverse-ETL into Postgres. The app reads Gold
directly from Delta via the warehouse — nothing is mirrored back to Lakebase.
(An earlier plan used Synced Tables for this; it was dropped to avoid the manual
UI work and keep one clear data direction.)

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
Yes — `databricks.yml` defines job **`analytics_cdf_on_change`**: a
`trigger.table_update` on the six analytics-relevant `lb_*_history` tables
(`ANY_UPDATED`, 60 s debounce). It runs `notebooks/06_analytics_cdf.py`, which
does an incremental watermark read of the new CDF rows
(`_sort_by > last_processed AND _pg_change_type NOT IN ('update_preimage','delete')`),
appends normalized rows to append-only `gold_usage_events`, and rebuilds the
usage marts: `gold_agent_tool_stats` (per-tool call counts + success rate),
`gold_agent_confidence` (answer-confidence distribution),
`gold_watchlist_activity`, `gold_filing_view_stats`, `gold_metric_view_stats`,
`gold_usage_funnel`. The Dashboard → **Platform activity** tab reads those.

**Has it been set up?**

| Piece | Status |
|---|---|
| `edgar` schema + 8 tables, `REPLICA IDENTITY FULL` | ✅ created on `zdsteele-capstone` |
| Lakebase CDF `edgar` → `bootcamp_students.zdsteele_capstone` | ✅ enabled; all 8 `lb_*_history` present, seeded rows flowed, metadata columns confirmed |
| Warehouse reads (app/agent → Delta) | ✅ working locally against the live workspace |
| `analytics_cdf_on_change` job | ✅ defined in `databricks.yml`; runs on `databricks bundle deploy`. Needs app/agent activity before its marts have content |
| Notebook 06 output marts | ⏳ populate after the agent has been used (writes `agent_actions` → CDF → job) |

## Meets the capstone's "2 of 3 Vs"

- **Variety** — JSON (submissions, companyfacts), HTML (filing docs), SGML
  (`submission .txt` → `silver_exhibits`), XBRL, plain text, parsed sections,
  46k text chunks.
- **Volume** — `silver_financial_facts` at 5 pilot CIKs ≈ 25k rows; the CIK list
  is a one-line config and clears **> 1,000,000 rows at ≥ ~100 companies** (full
  S&P 500 ≈ 16 M). Verify with
  `SELECT COUNT(*) FROM bootcamp_students.zdsteele_capstone.silver_financial_facts`.
