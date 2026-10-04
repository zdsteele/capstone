# Capstone rubric — evidence map

Maps each Databricks AI Capstone rubric category to the code, live tables, and
status behind it. "Pending" means the work is in the repo but a runtime step is
not yet done; those items should not be credited until the step is shown.

## 1. Spark data pipeline (15)

- Distributed transforms: `notebooks/03_silver_transform.py` parses companyfacts
  JSON, filing HTML, exhibits, and chunks with `mapInPandas` on executors.
- Quality handling: rows that fail key/value checks are written to
  `quarantine_financial_facts` and `quarantine_filings` with a `reject_reason`
  (`null_cik`, `null_period_end`, `null_value`, `null_accession`); duplicates are
  removed on natural keys.
- Run metrics: each silver run appends `dq_silver_run_metrics`
  (`rows_out`, `rows_quarantined`, `duplicates_removed`) per table.
- Re-runnable: silver tables are overwrite-idempotent; bronze ingest is `MERGE`.
- Pending: the quarantine and metrics tables are produced by the next silver
  run (blocked from a one-off run in this session, see the status log below).

## 2. Third-party API integration (10)

- SEC EDGAR: `lib/sec_client.py` — token-bucket rate limit (10 req/s cap),
  exponential backoff with jitter, `Retry-After` honoured, descriptive User-Agent
  from `SEC_USER_AGENT`, retry tests in `tests/test_libs.py::test_sec_client_retries_on_429_then_succeeds`.
- yfinance market data: `notebooks/02_bronze_ingest_market.py` — bounded retries
  with jittered backoff (`_history_with_retry`), response-schema validation
  (required OHLCV columns), and null-close rows dropped before write.
- Malformed responses: the parsers return `None` on unparseable input and the
  callers skip those filings (`lib/edgar_parse.py`), covered by the library tests.
- Live downstream use: 1,179,735 market bars; SEC facts feed the health score,
  ratios, valuation, and agent tools.
- Secrets: no keys in the repo; `.env` is gitignored; deploy uses app resources
  and a Render environment group.

## 3. Lakebase data model (15)

- DDL: `sql/10_operational_tables.sql` — 8 tables in schema `edgar` with primary
  keys, foreign keys, `UNIQUE` constraints, `created_at` timestamps, and
  `REPLICA IDENTITY FULL` for CDF.
- Constraints and indexes: `sql/11_constraints_and_indexes.sql` — CHECK constraints
  on CIK format, agent status, tool kind, confidence, and non-negative counts;
  indexes on agent actions by user/time and by tool/status.
  **Pending:** this file must be run once as the table owner. The app role is not
  the owner and the migration was refused for that reason.
- Live use: writes from the agent and UI land in Lakebase and flow to Delta through
  CDF (`lb_*_history` tables; 1,267 usage events in `gold_usage_events`).
- Known trade-off: `updated_at` is not added to replicated tables, because a column
  change would desync the CDF history feed.

## 4. Action-taking agent (20)

- Retrieval (6): Lakebase reads, Delta gold/silver via the SQL warehouse, Vector
  Search over filing chunks (`filing_text_index`), and SEC/yfinance-derived tables.
- Write tools (8): `save_filing`, `save_company_to_watchlist`, `create_research_note`,
  `update_research_note`, `remove_from_watchlist`. Each validates its input against
  silver tables, uses `ON CONFLICT` dedupe, and scopes update/delete to the caller's
  `user_id`.
- Confirmation safeguard: `remove_from_watchlist` is two-step. Without
  `confirmed=True` it writes nothing and asks the user to confirm. The prompt forbids
  passing `confirmed=True` without an explicit user yes. The UI's watchlist toggle
  also asks the browser to confirm before removing.
- Quality: 33-case eval suite in `agent/eval/cases.jsonl`; 33/33 pass on the latest
  run with the confirmation change (see the status log).
- Agent telemetry: every tool call and final answer is logged to `agent_actions`
  with status, latency, and self-reported confidence.

## 5. Analytics pipeline (10)

- Reverse CDF: `notebooks/06_analytics_cdf.py` reads `lb_*_history` tables with
  a per-source watermark (`analytics_watermarks`), appends `gold_usage_events`
  incrementally, and rebuilds the usage marts.
- Marts: `gold_agent_tool_stats` (call count, success rate, average latency per
  tool), `gold_agent_confidence`, `gold_usage_funnel`, `gold_watchlist_activity`,
  `gold_filing_view_stats`, `gold_metric_view_stats`.
- Monitoring: `gold_pipeline_monitor` (per-source freshness, events in the last 24h,
  and measured CDF lag: average and p95 seconds from Lakebase write to Delta
  ingest). This is measured, not a velocity claim.

## 6. Frontend and core workflow (10)

- Flask app (`app.py`) with templates for dashboard, workspace, assistant (chat with
  streamed tool activity), filing viewer, and search.
- Write actions reflected in the UI: watchlist toggle, saved filings, research notes.
- Confirmation: browser confirm before removing from the watchlist.
- Pending: screenshots of loading, empty, and error states should be captured for
  the submission.

## 7. Deployed application (5)

- Bundle defines the Databricks App `edgar-intelligence` with the SQL warehouse
  resource, and a `prod` target that binds the Lakebase Postgres branch.
- Render Blueprint (`render.yaml`, Dockerfile) is also present as an alternative.
- **Status: not deployed as of this writing.** `databricks apps get` reports compute
  STOPPED and app status UNAVAILABLE. The production deploy was refused by the
  auto-mode permission check and has not been retried. Until it is deployed and a
  URL is recorded here, this category should be scored as unverified.

## 8. Big data — two of three Vs (15)

- **Volume (met):** `silver_financial_facts` has 12,508,859 rows (>1M), built with
  distributed explode and partitioned by `fiscal_year`/`fiscal_quarter`.
  `silver_filing_sections` 303,668 rows; `bronze_market_bars` 1,179,735 rows.
- **Variety (met):** filing narrative text is chunked, context-enriched, embedded
  (`databricks-gte-large-en`), indexed in Vector Search, and surfaced by the
  `search_filing_text` agent tool and the filing-intelligence layer.
- **Velocity (not claimed):** the pipeline is a daily batch. No sub-minute
  processing is claimed or measured.

## Status log

| Item | State |
|---|---|
| Lakebase constraints migration (`sql/11`) | Written; needs one run as the table owner |
| Silver rebuild for quarantine and DQ metrics | Blocked from running in this session; dev notebooks are uploaded |
| Production bundle deploy and app start | Blocked by permission check; not retried |
| Full eval after confirmation change | 33/33 pass (`--no-judge`, avg 21.1s/case). The new `write_remove_needs_confirm` case was checked on tool selection only; its "asks to confirm" rubric needs a judged run |
