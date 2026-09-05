# Setup & run guide

Do these in order. The only real UI step is **§3** (reverse CDC) — everything
else is paste-and-run.

Known values for this workspace:

| Thing | Value |
|---|---|
| Lakebase instance | `summer-bootcamp-2026-v2` (branch `production`, db `databricks_postgres`, role `users`) |
| Lakebase host | `ep-patient-sun-d1ycq936.database.us-west-2.cloud.databricks.com` |
| Lakebase Postgres schema | `bootcamp_students` (shared — per the TA). Every capstone table is prefixed `edgar_zdsteele_` |
| SQL warehouse | `Serverless Starter Warehouse`, id `b15d3d6f837ba428` |
| Unity Catalog schema | `bootcamp_students.zdsteele` (Delta medallion tables) |
| Vector Search | not used (keyword search); `zachy_vs` exists if you want it later |
| LLM endpoint | `databricks-meta-llama-3-3-70b-instruct` |

---

## 1. Lakebase tables — Lakebase **SQL Editor**

In the Lakebase project UI (`summer-bootcamp-2026-v2` → branch `production` →
**SQL Editor** in the left nav), paste and run, in order:

1. `sql/00_create_schema.sql` — `CREATE SCHEMA IF NOT EXISTS bootcamp_students`
   (it already exists — harmless)
2. `sql/10_operational_tables.sql` — 8 tables named `edgar_zdsteele_*`, each
   `REPLICA IDENTITY FULL`
3. `sql/20_seed.sql` — demo user + pilot companies + a default watchlist

All 8 tables live in the shared `bootcamp_students` schema, prefixed
`edgar_zdsteele_` so they don't collide with other students'.

## 2. Pipeline notebooks — Unity Catalog `bootcamp_students.zdsteele`

Import the repo as a **Git folder** in Databricks, then run `notebooks/` in
order on a serverless cluster. Widgets already default to
`catalog=bootcamp_students`, `schema=zdsteele`.

| Run | Notebook | Output |
|---|---|---|
| ✅ | `01_bronze_ingest_sec.py` | `bronze_*` + the `bronze_edgar_raw` Volume |
| ✅ | `02_bronze_ingest_market.py` | `bronze_market_bars` (installs `yfinance` via `%pip`) |
| ✅ | `03_silver_transform.py` | `silver_*` |
| ✅ | `04_gold_marts.py` | `gold_*` financial marts |
| ⤵️ skip | `05_vector_search_index.py` | only if you decide to add hybrid search later |

Each notebook prints row counts and `dbutils.notebook.exit(...)` with a summary.

## 3. Reverse CDC — Lakebase → Delta  **(the one UI step)**

**Goal:** stream these 6 writable Postgres tables into Delta history tables so
the analytics job can read them.

| Source (Lakebase `bootcamp_students`) | Target (Unity Catalog `bootcamp_students.zdsteele`) |
|---|---|
| `edgar_zdsteele_watchlists` | `lb_edgar_zdsteele_watchlists_history` |
| `edgar_zdsteele_watchlist_companies` | `lb_edgar_zdsteele_watchlist_companies_history` |
| `edgar_zdsteele_saved_filings` | `lb_edgar_zdsteele_saved_filings_history` |
| `edgar_zdsteele_saved_research` | `lb_edgar_zdsteele_saved_research_history` |
| `edgar_zdsteele_agent_conversations` | `lb_edgar_zdsteele_agent_conversations_history` |
| `edgar_zdsteele_agent_actions` | `lb_edgar_zdsteele_agent_actions_history` |

Rule: target name = `lb_` + source table name + `_history`. Snapshot or
continuous mode both fine.

`REPLICA IDENTITY FULL` is already set (step 1), so the source side is ready.

Where to click depends on your workspace's Lakebase version — it's either:
- **Catalog Explorer** → your Lakebase instance → **Synced tables** / **Create →
  Synced table**, or
- the Lakebase project → **Tables** / a **Sync to Delta** action.

> **Send me a screenshot of that screen and I'll write the exact clicks + field
> values for all 6.** The target table names above are what the rest of the code
> expects — don't rename them.

The history tables will carry `_pg_change_type` / `_sort_by` / `_timestamp`
alongside the source columns (the ltap-cdc-day-2 contract). If your version names
them differently, tell me and I'll adjust `notebooks/06_analytics_cdf.py`.

## 4. Run the app locally

```
cd capstone
cp .env.example .env
```

Edit `.env`: in the Lakebase UI click **Connect** → **Copy snippet** for the URL
and **Copy OAuth token** for the password; paste both into `LAKEBASE_URL`
(token is good for ~1 hour). Make sure `databricks auth login` /
`DATABRICKS_CONFIG_PROFILE` is set so the warehouse reads work.

```
pip install -r requirements.txt
python app.py            # http://localhost:8000
```

Sign in with any lowercase username → creates a row in `bootcamp_students.edgar_zdsteele_users`.
Smoke test:
- **Search** → type `Alphabet` → click the result → filings + metrics load
- **Filing** → open a 10-Q → sections / exhibits / XBRL facts
- **Dashboard** → enter `AAPL` → revenue bars + tables
- **Research Assistant** → "Summarize Alphabet's latest 10-Q revenue vs operating
  expenses", then "Save that filing and add a research note comparing Google
  Cloud to Microsoft" → check `bootcamp_students.edgar_zdsteele_saved_filings` /
  `bootcamp_students.edgar_zdsteele_saved_research` / `bootcamp_students.edgar_zdsteele_agent_actions`

## 5. Deploy

```
databricks bundle validate
databricks bundle deploy        # creates the app + the analytics_cdf job
databricks bundle run edgar_intelligence
```

The `analytics_cdf_on_change` job fires `06_analytics_cdf.py` whenever an
`lb_*_history` table changes (60s debounce). Its outputs
(`gold_agent_tool_stats`, `gold_usage_funnel`, …) land in Delta and the
dashboard's **Platform activity** tab reads them through the warehouse — no extra
sync needed.

## 6. Scale for the graded run

Edit `config/ciks.json` to ≥ 100 companies, re-run notebooks `01`→`06`, then:

```sql
SELECT COUNT(*) FROM bootcamp_students.zdsteele.silver_financial_facts;  -- want > 1,000,000
```

Variety is already satisfied (JSON submissions + HTML filings + XBRL + full
`.txt` in `bronze_edgar_raw`).
