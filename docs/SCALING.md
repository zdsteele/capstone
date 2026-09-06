# Scaling the pipeline — the full company universe

The pilot runs 6 companies from `config/ciks.json`. The scaled run uses
`config/ciks_full.json` (~470 S&P 500 companies + the pilot) and clears the
capstone's **Volume V (>1M rows)** with large margin.

## Why not "every company / every form"

- EDGAR has **~800k filers** (mostly funds, shells, defunct). SEC caps automated
  access at **10 req/s** — a single request each is ~22 hours; with filing
  documents it's weeks. Most have no usable XBRL.
- **Ownership / registration forms** (3/4/5, 13D/G/F, S-1, 424B) are tens of
  millions of filings that the section parser, XBRL flattener, and AI briefing
  can't process. Ingesting them adds cost and zero analytical value here.

The scaled set is **~470 large operating companies**, forms **10-K / 10-Q / 8-K
+ 20-F / 40-F / 6-K** (foreign-filer equivalents). That's the standard target
the proposal itself names.

## The key decoupling: Volume ≠ filing-document count

`silver_financial_facts` is the row-count anchor, and it comes from **XBRL
companyfacts** — one cheap API call per CIK that returns the company's *entire*
reporting history (10-15 years). It does **not** depend on how many filing
documents you download.

So `max_new_filings_per_cik` is set to **12** for the scaled run (≈ 3 years of
10-K/10-Q/8-K, enough for the sections / chunks / AI-briefing layers), and you
still get **millions of fact rows**:

```
~470 CIKs × ~40 reporting periods × ~800 us-gaap concepts ≈ 15M rows
```

Verify after the run:
```sql
SELECT COUNT(*) FROM bootcamp_students.zdsteele_capstone.silver_financial_facts;
```

## What was hardened for scale

| Notebook | Change |
|---|---|
| **01** `bronze_sec` | `max_new_filings_per_cik` default 40 → **12**; progress log every 25 CIKs; a failed `submissions` call skips that CIK instead of aborting the run. companyfacts still fetched for every CIK. |
| **02** `bronze_market` | already distributed (`mapInPandas` over tickers); MERGE on `(cik, bar_date)`. Best-effort — does not gate silver. |
| **03** `silver` | **rewritten distributed.** The three driver-side `collect()` → Python-loop → `createDataFrame` blocks (companyfacts explode, HTML→sections, submission→exhibits, section→chunks) are now `mapInPandas` UDFs running `lib/edgar_parse.py` on executors (shipped via `addPyFile`). Nothing is pulled to the driver — this is what stops the OOM at ~470 companies. `shuffle_partitions` widget (400 for the full run). |
| **04** `gold_marts` | already Spark-native; materializes `silver_resolved_metrics` instead of `.cache()` (serverless). |
| **05** `vector_search` | DELTA_SYNC index handles millions of chunks; first sync is long (watch the VS pipeline). `TRIGGERED` pipeline. |
| **08** `filing_intelligence` | `ai_query` over every 10-K/10-Q, MERGE on accession so re-runs only hit **new** filings. First scaled run ≈ a few thousand `ai_query` calls, parallelized across the cluster. |
| **09** `financial_ratios` | Spark-native window/aggregation — scales as-is. |
| **10** `company_health` | one `ai_query` per company (~470 calls). `collect()`s are small (per-company summary rows). |

## Running it

1. **Generate the universe** (once, or to refresh):
   ```bash
   python config/build_universe.py          # writes config/ciks_full.json
   ```
2. **Deploy** so the notebooks + config reach the workspace:
   ```bash
   databricks bundle deploy -t dev
   ```
3. **First full ingest** — run `01` with `ciks_config=../config/ciks_full.json`,
   `mode=full` the first time (or `incremental` — it's empty so everything is
   new). Budget **2-4 hours** for `bronze_sec` (rate-limited HTTP for ~470
   companies × up to 12 docs + companyfacts). This is a one-time cost.
   ```bash
   databricks bundle run pipeline_daily_refresh -t dev
   ```
   The bundle's `ciks_config` variable already points `01`/`02` at the full file
   and the task timeouts are raised (3h for bronze_sec, 2h for silver).
4. **After that**, the scheduled `pipeline_daily_refresh` keeps it current —
   incremental, only new filings, ~15-30 min per run.
5. **Verify Volume**: the `COUNT(*)` query above should be well over 1,000,000.

## Cost note

The AI notebooks (08, 10) call `ai_query` (Llama 3.3 70B FMAPI). The first full
run is a few thousand calls; after that they're incremental (`MERGE` on
accession / one row per company). If cost is a concern, run 08/10 weekly rather
than in every daily job — they don't need to be as fresh as the financials.
