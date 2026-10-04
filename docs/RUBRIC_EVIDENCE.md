# Capstone rubric — evidence map

Maps each Databricks AI Capstone rubric category to the code, live tables, and
screenshots behind it. Items marked **Pending** are in the repo but a runtime
step has not finished, so they are not credited until that step is shown.

Snapshot: 2026-10-04. Live app: https://edgar-research-assistant.onrender.com
(Render, Docker, deploys from `zdsteele/capstone` `main`, commit `4952a87`).

## 1. Spark data pipeline (15)

- Distributed transforms: `notebooks/03_silver_transform.py` parses companyfacts
  JSON, filing HTML, exhibits, and chunks with `mapInPandas` on executors.
- Validation and error handling: rows failing key or value checks go to
  `quarantine_financial_facts` and `quarantine_filings` with a `reject_reason`
  (`null_cik`, `null_period_end`, `null_value`, `null_accession`). Duplicates are
  removed on natural keys. Explicit schemas are used wherever a column can be all
  null in a batch (`CANNOT_DETERMINE_TYPE` was hit and fixed in notebooks 15–17).
- Run metrics: each silver run appends `dq_silver_run_metrics`
  (`rows_out`, `rows_quarantined`, `duplicates_removed`).
- Re-runnable: silver tables are overwrite-idempotent; bronze ingest uses `MERGE`
  and skips accessions already stored.
- Live volume: `silver_financial_facts` 12,508,859 rows; `silver_filing_sections`
  303,668; `bronze_market_bars` 1,182,105.
- **Pending:** the quarantine and metrics tables appear after the current refresh
  run finishes.
- **Known issue:** some XBRL facts carry the placeholder `period_end = 2199-12-31`.
  A plausibility rule is not yet in silver.

## 2. Third-party API integration (10)

- SEC EDGAR: `lib/sec_client.py` — token-bucket rate limit (5 req/s configured,
  10 req/s is SEC's cap), exponential backoff with jitter, `Retry-After` honoured,
  descriptive User-Agent from `SEC_USER_AGENT`. Retry behaviour is covered by
  `tests/test_libs.py::test_sec_client_retries_on_429_then_succeeds`.
- Malformed responses: parsers return `None` on unparseable input and callers
  skip those filings (`lib/edgar_parse.py`). Library tests: 12 passing.
- yfinance market data: `notebooks/02_bronze_ingest_market.py` — bounded retries
  with jittered backoff (`_history_with_retry`), OHLCV schema check, null-close rows
  dropped. Code only; the retry path has not been exercised against a failing
  upstream.
- Live downstream use: market bars feed the valuation and health pages; SEC facts
  feed ratios, health score, and agent tools.
- Secrets: no credentials in the repo (scanned tracked files; only placeholders
  appear in `.env.example` and `docs/SETUP.md`). Runtime secrets are in the Render
  environment group.

## 3. Lakebase data model (15)

- DDL: `sql/10_operational_tables.sql` — 8 tables in schema `edgar`: primary keys,
  foreign keys, `UNIQUE` constraints, `created_at` timestamps, `REPLICA IDENTITY FULL`
  for CDF.
- Constraints: `sql/11_constraints_and_indexes.sql`, **applied** to project
  `zdsteele-capstone`, branch `production`. Verified: 10 `ck_*` constraints
  present in `pg_constraint` (CIK format, username non-empty, agent status/kind/
  confidence enumerations, non-negative counts and latency). Indexes on
  `agent_actions` and `agent_conversations` created in the same run.
- Live use: the Render app and the agent write to Lakebase; those writes land in
  Delta through CDF (`lb_*_history` tables; 1,267 usage events in `gold_usage_events`).
- Trade-off recorded: `updated_at` is not added to replicated tables, because a
  column change would desync the CDF history feed.
- **Unverified:** the Render app's watchlist write path has not been exercised
  end-to-end in this snapshot.

## 4. Action-taking agent (20)

- Retrieval (6): Lakebase reads; Delta gold and silver through the SQL warehouse;
  Vector Search over filing chunks (`filing_text_index`); SEC- and yfinance-derived
  tables.
- Write tools (8): `save_filing`, `save_company_to_watchlist`,
  `create_research_note`, `update_research_note`, `remove_from_watchlist`.
  Each validates input against silver tables (for example, a filing must exist
  in `silver_filings`), uses `ON CONFLICT` dedupe where relevant, and scopes
  update and delete to the caller's `user_id`.
- Confirmation safeguard: `remove_from_watchlist` is two-step. Without
  `confirmed=True` it writes nothing and asks the user to confirm. The prompt
  forbids passing `confirmed=True` without an explicit user yes. The browser
  watchlist toggle also asks for confirmation before removing.
- Quality: `agent/eval/cases.jsonl`, 33 cases. Latest run 33/33 passing
  (`--no-judge`, average 21.1 s/case). The confirmation case was checked on tool
  selection only.
- Telemetry: every tool call and final answer is logged to `agent_actions` with
  status, latency, and self-reported confidence.

## 5. Analytics pipeline (10)

- Reverse CDF: `notebooks/06_analytics_cdf.py` reads the `lb_*_history` tables with
  a per-source watermark (`analytics_watermarks`), appends `gold_usage_events`
  incrementally, and rebuilds the usage marts.
- Marts: `gold_agent_tool_stats` (calls, success rate, average latency per tool),
  `gold_agent_confidence`, `gold_usage_funnel`, `gold_watchlist_activity`,
  `gold_filing_view_stats`, `gold_metric_view_stats`.
- Monitoring: `gold_pipeline_monitor` — per-source freshness, events in the last
  24 hours, and measured CDF lag (average and p95 seconds from Lakebase write to
  Delta ingest). Measured, not a velocity claim.
- **Pending:** `gold_pipeline_monitor` appears after the current refresh run.

## 6. Frontend and core workflow (10)

- Flask app (`app.py`) with templates for dashboard, workspace, assistant (chat
  with tool activity), filing viewer, and search.
- Write actions surfaced in the UI: watchlist toggle, saved filings, research notes.
- Confirmation: browser confirm before removing from the watchlist.
- Live check: login and `/api/health/<cik>` return data from the warehouse on the
  deployed app (Apple: 2 institutional holders, 3 beneficial-ownership stakes).
- **Unverified:** loading, empty, and error states are not captured in screenshots.

## 7. Deployed application (5)

- Render web service `edgar-research-assistant`, Docker, Free plan. Deploy
  `4952a87` is **Live**; `/healthz` returns HTTP 200; the root redirects to login.
  Screenshot: `screenshots/render_deploy_live.png`.
- Render env group holds the Databricks, Lakebase, and Flask variables. Values are
  not included here.
- Databricks App `edgar-intelligence` is also defined in `databricks.yml` but is
  stopped and is not the graded deployment.
- Risk to note: the Free plan has 512 MB of memory. The `render.yaml` comment says
  the mlflow and langchain stack needs about 2 GB, so the app may run out of memory
  under load.

## 8. Big data — two of three Vs (15)

- **Volume (met):** 12.5M rows in `silver_financial_facts`, distributed explode,
  partitioned by `fiscal_year` and `fiscal_quarter`. Also 303,668 filing sections
  and 1.18M market bars.
- **Variety (met):** narrative filing text is chunked, context-enriched, embedded
  with `databricks-gte-large-en`, indexed in Vector Search, and surfaced by the
  `search_filing_text` agent tool and the filing-intelligence layer.
- **Velocity (not claimed):** the pipeline is a daily batch; the last full run took
  about 4 hours. No sub-minute processing is claimed.
- Data coverage and gaps are documented in the status log below.

## Screenshots

- `screenshots/databricks_jobs_list.png` — the dev daily refresh and analytics CDF jobs
- `screenshots/lakebase_sql_editor_constraints.png` — constraint migration running
- `screenshots/lakebase_constraints_verified.png` — the 10 `ck_*` constraints in `pg_constraint`
- `screenshots/render_deploy_building.png` and `screenshots/render_deploy_live.png` — auto-deploy from `main`

## Status log

| Item | State |
|---|---|
| Lakebase constraints and indexes (`sql/11`) | Applied and verified (10 constraints) |
| Render deploy `4952a87` | Live; `/healthz` 200 |
| Dev daily refresh (rebuilds silver with quarantine and metrics) | Running; output tables pending |
| 13D/13G form names fixed (`SCHEDULE 13D/G` XML) | Code deployed to dev; backfill pending |
| 13F for all 19 managers | Pending in the current run (only Berkshire and Vanguard loaded so far) |
| Eval suite | 33/33 (`--no-judge`) |
| Library tests | 12/12 |
| Watchlist write from the Render app | Not yet exercised end-to-end |
| Core filings completeness vs SEC (since 2026-06-01) | 79 of 2,304 missing at last check, mostly from late September; rechecked after the run |
