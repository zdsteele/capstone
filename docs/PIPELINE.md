# Pipeline reference — every notebook, every bronze table, the concepts

A plain-language walkthrough of how the data platform is built, what each
notebook does and why, which parts are AI, and how the app connects.

See also: [ARCHITECTURE.md](ARCHITECTURE.md) (Lakebase ↔ Lakehouse loop + diagrams),
[ANALYST_SPEC.md](ANALYST_SPEC.md) (the analyst target the AI layer aims at).

---

## 1. There are two jobs, not one

The `NN_` prefixes on the notebooks are a **build order for a human** — no
notebook triggers the next. Automation happens only inside two Asset-Bundle jobs:

| Job | Notebooks | Trigger | What it does |
|---|---|---|---|
| `pipeline_daily_refresh` | 01 → 02 → 03 → 04 → 05 → 08 → 09 → 10 | schedule (06:30 America/New_York) or manual | pull new SEC + market data, rebuild silver/gold + the AI layer |
| `analytics_cdf_on_change` | 06 only | fires when a `lb_*_history` table changes | turn app/agent activity into usage analytics |

Notebook **07** is in neither — run it by hand whenever you want to (re)tag
tables in Unity Catalog.

**The daily pipeline never reads an app table.** The only place app data
re-enters the lakehouse is job #2 (notebook 06). Details in §7.

---

## 2. Concepts you need first

### CIK — Central Index Key

The SEC's permanent ID for a *filer* (a company or a person). Ten digits,
zero-padded. **Tickers and company names change; the CIK never does.**

| Company | Ticker | CIK |
|---|---|---|
| Apple | AAPL | `0000320193` |
| Microsoft | MSFT | `0000789019` |
| Amazon | AMZN | `0001018724` |
| Alphabet | GOOGL | `0001652044` |
| Tesla | TSLA | `0001318605` |

These five are the pilot set in [`config/ciks.json`](../config/ciks.json). Every
SEC API call is keyed by the 10-digit CIK (`data.sec.gov/submissions/CIK0000320193.json`).
Scaling the platform = adding CIKs to that file and re-running.

### Accession number

The SEC's ID for **one filing** — e.g. `0000320193-24-000081`
(`<filer>-<year>-<sequence>`). Unique across all of EDGAR. It's the natural key
for `bronze_filings`, `silver_filings`, and every AI table keyed to a filing.

### SEC form types

A "form" is the *kind* of document filed. The pipeline ingests three (set in
`config/ciks.json` → `forms`); the rest are listed so you know what's out of
scope.

| Form | What it is | Ingested? | Why / why not |
|---|---|---|---|
| **10-K** | **Annual report** — full audited financials, business description, risk factors, MD&A | ✅ | the backbone: yearly financials + the narrative the AI layer reads |
| **10-Q** | **Quarterly report** — unaudited financials + MD&A, lighter than a 10-K | ✅ | quarterly trend data + fresher narrative |
| **8-K** | **"Current report"** — a material event between quarters (earnings release, exec change, acquisition, big contract) | ✅ | filing-activity signal; event timeline |
| DEF 14A | **Proxy statement** — exec pay, board, shareholder-vote items | ❌ | needed for the analyst spec's governance section (§13) — not ingested yet |
| Forms 3 / 4 / 5 | **Insider transactions** — an officer/director/10%-owner buying or selling stock | ❌ | analyst spec §11; not ingested |
| Form 144 | Notice of a planned insider sale | ❌ | §11; not ingested |
| 13D / 13G | A >5% shareholder disclosing a stake (13D = activist, 13G = passive) | ❌ | §12; not ingested |
| 13F | A big institution's quarterly holdings list | ❌ | §12; not ingested |
| S-1 | **IPO registration** — first-time public offering | ❌ | our companies are long public |
| 424B | Prospectus (final offering terms) | ❌ | out of scope |
| 20-F / 6-K | Annual / interim reports for **foreign** filers (instead of 10-K/10-Q) | ❌ | pilot is all US filers |

### XBRL / "company facts"

**XBRL** is a machine-readable tagging standard for financial statements. Every
number in a 10-K/10-Q is tagged with a standard concept name
(`RevenueFromContractWithCustomerExcludingAssessedTax`, `NetIncomeLoss`, …), a
period, and a unit. The SEC aggregates all of a company's XBRL facts into one
**"company facts"** JSON document per CIK
(`data.sec.gov/api/xbrl/companyfacts/CIK##########.json`). That single document
holds *every financial number the company has ever reported in structured form* —
which is why `bronze_xbrl_facts` has one fat row per company and
`silver_financial_facts` explodes to ~46,000 rows.

### Medallion layers

- **Bronze** = raw, exactly as the source gave it. Never edited. One table per
  source *shape*.
- **Silver** = cleaned, typed, deduplicated, one concept per table.
- **Gold** = application-ready: friendly names, computed metrics, one row per
  the grain the app/agent needs.

---

## 3. Why *five* bronze tables

Because SEC hands you **five different shapes of data** for each company, from
different endpoints, at different grains. The medallion rule is: land each shape
raw and verbatim in its own table, transform later. One combined table would mix
grains (per-company vs per-filing vs per-document) and you couldn't reprocess one
part without the others.

| Table | Grain (one row per…) | Source | What's in it | Used by |
|---|---|---|---|---|
| `bronze_company_submissions` | **company** (CIK) | Submissions API | company profile — name, SIC industry code, fiscal-year-end, exchanges, all tickers — plus the **entire filing history** as `raw_json` | nb 03 → `silver_companies`, `silver_filings` |
| `bronze_xbrl_facts` | **company** (CIK) | Company Facts API | one column, `companyfacts_json`: every XBRL-tagged financial number the company ever reported | nb 03 → `silver_financial_facts` (explodes to ~46k rows) |
| `bronze_filings` | **filing** (accession) | Submissions API (`filings.recent`) | the flat filing index — `form`, `filing_date`, `report_date`, `primary_document`, `is_xbrl`, `size` | nb 03 → `silver_filings`; nb 01 itself reads it to know what's already ingested (incremental) |
| `bronze_filing_documents` | **downloaded file** | EDGAR archives | manifest of the actual files pulled — `document` name, `source_format` (htm/html/txt), `volume_path` (pointer to the raw bytes in the Volume), `byte_size` | provenance / debugging; `silver_exhibits` context |
| `bronze_filing_text` | **text blob** (accession + `doc_kind`) | EDGAR archives | the decoded text of each filing: `doc_kind` = `primary_html` (the main 10-K/10-Q document) or `submission_txt` (the full submission bundle) | nb 03 → `silver_filing_sections`, `silver_exhibits`, then `silver_filing_text_chunks` |

Plus the **Volume** `bronze_edgar_raw` (not a table — a governed file store):
the untouched raw bytes of every filing document, keyed `/{cik}/{accession}/…`.
So you can always reproduce or re-parse a filing without re-hitting SEC.

Every bronze table also carries `ingested_at`. Notebook 01 writes them with
`MERGE` (never overwrite), so a scraping is never lost even if a company later
leaves `config/ciks.json`.

---

## 4. `pipeline_daily_refresh` — notebook by notebook

### 01 · `01_bronze_ingest_sec.py` — Bronze: SEC ingest
- **Reads:** `config/ciks.json` + SEC EDGAR APIs (submissions JSON, XBRL company
  facts JSON, filing HTML, full submission `.txt`).
- **Does:** driver-sequential, rate-limited (≤ 8 req/s) HTTP. **Incremental** —
  skips any accession already in `bronze_filings` whose raw files exist. Every
  write is a Delta `MERGE`.
- **Writes:** the five `bronze_*` tables above + raw bytes into the Volume.
- **AI?** No.
- **App connection?** None — the app never reads bronze.
- **Why needed:** the raw landing zone. Everything downstream derives from it;
  keeping originals means you can reprocess without re-hitting SEC.

### 02 · `02_bronze_ingest_market.py` — Bronze: market data
- **Reads:** yfinance (one call per ticker).
- **Does:** daily OHLCV price bars. Incremental `MERGE` on `(cik, bar_date)` —
  10-year history on the first run, a 5-day window after.
- **Writes:** `bronze_market_bars`.
- **AI?** No.
- **App connection?** None.
- **Why needed:** satisfies the "third-party API" capstone requirement and is
  staged for price enrichment. *Notebooks 03/04 don't join it yet* — which is
  why this task is best-effort and doesn't block the rest of the job.

### 03 · `03_silver_transform.py` — Silver: clean & structure
- **Reads:** the `bronze_*` tables.
- **Does:** pure parsing / Spark, no AI:
  - `bronze_company_submissions` → `silver_companies` (typed company dimension)
  - `bronze_filings` → `silver_filings` (one row per accession, deduped, typed dates)
  - **explodes** `bronze_xbrl_facts` → `silver_financial_facts` (one row per
    reported number), with a `period_type` column
    (`instant` / `quarter` / `half` / `ytd9` / `annual`) so quarterly vs
    year-to-date figures never get mixed up
  - `bronze_filing_text` (HTML) → `silver_filing_sections` (10-K/10-Q Items 1, 1A, 7, 7A, 8…)
  - submission `.txt` → `silver_exhibits`
  - `silver_filing_sections` → `silver_filing_text_chunks` (~400-char chunks,
    Change-Data-Feed enabled — feeds Vector Search)
- **AI?** No.
- **App connection?** **Yes** — Filing Explorer reads `silver_filings` +
  `silver_filing_sections`; Company Search reads `silver_companies`.
- **Why needed:** turns messy raw formats into typed, queryable tables — the
  foundation for both the gold marts and the AI layer.

### 04 · `04_gold_marts.py` — Gold: financial marts
- **Reads:** `silver_financial_facts` + `silver_filings` + `silver_companies`.
- **Does:** maps raw us-gaap concept names to friendly metric names (priority
  fallbacks per metric), picks one value per company/period/metric, computes
  YoY / QoQ deltas. Deterministic arithmetic, not AI.
- **Writes:** `gold_company_financials` (revenue, margins, EPS, assets, cash
  flow per period), `gold_revenue_history`, `gold_company_comparisons`.
- **AI?** No.
- **App connection?** **Yes, heavily** — Dashboard "Financials" tab and Company
  Search headline metrics.
- **Why needed:** silver has several candidate values per number; this resolves
  them to the clean one-row-per-period table the app and notebook 09 need.

### 05 · `05_vector_search_index.py` — Vector Search index
- **Reads:** `silver_filing_text_chunks`.
- **Does:** builds `silver_filing_chunks_enriched` — each chunk prefixed with its
  company / form / period / section so the embedding has context. Creates /
  refreshes a `DELTA_SYNC` Vector Search index (`filing_text_index` on the
  `zachy_vs` endpoint).
- **AI?** **Partly** — uses an *embedding* model (`databricks-gte-large-en`) to
  turn text into vectors. Not a chat model, but it is ML.
- **App connection?** **Yes, via the agent** — the `search_filing_text` tool runs
  hybrid (vector + keyword) queries against this index; falls back to `ILIKE`
  keyword search if the index is absent.
- **Why needed:** lets the agent find passages across tens of thousands of chunks
  by meaning, not exact words.

### 08 · `08_filing_intelligence.py` — AI filing briefings  ← **AI**
- **Reads:** `silver_filing_sections` (+ filings/companies), 10-K and 10-Q only.
- **Does:** concatenates the narrative sections (business, risk factors, MD&A)
  and calls **`ai_query('databricks-meta-llama-3-3-70b-instruct', prompt)`** —
  Llama 3.3 70B, inside Spark SQL. Parses the JSON it returns.
- **Writes:** `gold_filing_intelligence` — `executive_summary`,
  `revenue_commentary`, `risk_themes[]`, `management_tone`, `notable_items`.
  `MERGE` on accession, so re-runs only send *new* filings to the model.
- **AI?** **Yes** — this is the "Spark job with AI in the middle" the bootcamp asks for.
- **App connection?** **Yes** — Filing Explorer's "AI briefing" card, the Company
  Search briefing, and the agent's `get_filing_intelligence` tool.
- **Why needed:** turns a 100-page filing into a paragraph a person can read.

### 09 · `09_financial_ratios.py` — Ratios & trend engine
- **Reads:** `gold_company_financials` + `silver_financial_facts`.
- **Does:** computes analyst ratios — gross/operating/net margin, revenue & EPS
  growth, FCF, FCF conversion, ROIC≈, net debt, interest coverage — per
  `(cik, fiscal_year, fiscal_period)`. Rebuilds operating cash flow / capex into
  **discrete quarters** (Q2 = H1−Q1, Q3 = 9M−H1) and annualizes quarterly return
  ratios. Adds a `*_trend` label (up / stable / down) vs the same period a year
  earlier.
- **Writes:** `gold_financial_ratios`.
- **AI?** No — pure formulas. The "calculate what the business actually did" layer.
- **App connection?** **Yes** — Dashboard "Company health" tab (ratio-trend
  table); the agent's `get_financial_ratios` tool.
- **Why needed:** gives notebook 10 and the agent computed numbers to reason
  about instead of raw GAAP line items.

### 10 · `10_company_health.py` — Health score & report  ← **AI**
- **Reads:** `gold_financial_ratios` + `gold_company_financials` +
  `gold_filing_intelligence`.
- **Does:** per company, builds a prompt from the **computed ratios** + the AI
  blurbs, calls **`ai_query` (Llama 3.3 70B)**, and gets back the Investor Health
  Score (0–100 per dimension), overall score / label / direction, and the
  structured report (`what_changed`, `cash_check`, `debt_check`, `risks`,
  `bull/base/bear_case`, `watch_next`, `bottom_line`). The prompt forbids
  inventing numbers and forbids Buy/Sell calls.
- **Writes:** `gold_company_health` — one row per company.
- **AI?** **Yes** — grounded on notebook 09's numbers.
- **App connection?** **Yes** — Dashboard "Company health" tab (the big score +
  report); the agent's `get_company_health` tool.
- **Why needed:** the headline "is this company healthy, improving or
  deteriorating, what to watch" answer — the product's whole point.

---

## 5. `analytics_cdf_on_change` — notebook 06

### 06 · `06_analytics_cdf.py` — Reverse-CDC usage analytics
- **Triggered by:** the job's `table_update` trigger firing when any
  `lb_*_history` table changes (60-second debounce). **Not** part of the daily
  pipeline.
- **Reads:** the reverse-CDC Delta tables — `lb_agent_actions_history`,
  `lb_saved_filings_history`, `lb_saved_research_history`,
  `lb_watchlist_companies_history`, `lb_watchlists_history`,
  `lb_agent_conversations_history`. These are Delta mirrors of the Postgres
  `edgar.*` tables, produced automatically by **Lakebase Change Data Feed**.
- **Does:** watermark read (only rows newer than last time, skip
  pre-images/deletes), appends to append-only `gold_usage_events`, rebuilds
  `gold_agent_tool_stats` (per-tool call counts + success rate),
  `gold_agent_confidence`, `gold_watchlist_activity`, `gold_filing_view_stats`,
  `gold_metric_view_stats`, `gold_usage_funnel`. Advances `analytics_watermarks`.
- **AI?** No — aggregation.
- **App connection?** **Yes** — Dashboard "Platform activity" tab.
- **Why needed:** the "analytics pipeline" capstone requirement — analytics
  *about app usage*, built from Change Data Feed.

---

## 6. Notebook 07 (run by hand)

### 07 · `07_tag_capstone_tables.py`
Applies Unity Catalog tags/comments (`project=edgar_capstone`) to the
`bronze_/silver_/gold_/lb_` tables so they're easy to find in Catalog Explorer.
No AI, no app connection, in neither job. Idempotent — re-run any time.

---

## 7. What the app writes, and does any of it feed the job?

### The app is the only writer to Postgres (`lib/lakebase.py` → schema `edgar`)

| Trigger in the app | Writes to |
|---|---|
| `/login` | `edgar.users` (upsert) |
| every agent turn | `edgar.agent_conversations` (new conversation) |
| every agent tool call | `edgar.agent_actions` (PENDING → SUCCESS/ERROR, with `tool_kind`, `confidence`) |
| agent tool `save_filing` | `edgar.saved_filings` |
| agent tool `save_company_to_watchlist` | `edgar.watchlists` + `edgar.watchlist_companies` |
| agent tools `create/update_research_note` | `edgar.saved_research` |
| agent tool `remove_from_watchlist` | delete from `edgar.watchlist_companies` |

8 Postgres tables: `users`, `companies`, `watchlists`, `watchlist_companies`,
`saved_filings`, `saved_research`, `agent_conversations`, `agent_actions`.

Everything else the app does is **read-only against Delta** through the SQL
warehouse (`lib/warehouse.py`) — it reads the outputs of notebooks 03, 04, 08,
09, 10, and 06.

### Do app tables feed a job?

- **The daily pipeline (01–05, 08–10): no.** It only knows SEC + market data. It
  never reads `edgar.*` or `lb_*`.
- **Notebook 06 (`analytics_cdf_on_change`): yes — indirectly.** It reads the
  `lb_*_history` Delta mirrors, which Lakebase Change Data Feed produces from the
  Postgres `edgar.*` tables. This is the full round trip:

```
App agent writes  →  edgar.agent_actions, edgar.saved_filings, …   (Postgres, live)
                          │  Lakebase Change Data Feed (automatic)
                          ▼
                  lb_agent_actions_history, …                       (Delta mirror)
                          │  table_update trigger → analytics_cdf_on_change
                          ▼
                  notebook 06 aggregates
                          ▼
                  gold_usage_events, gold_agent_tool_stats, …       (Delta)
                          │  SQL warehouse read
                          ▼
                  Dashboard → "Platform activity" tab
```

So the app's operational writes make a full round trip back into the lakehouse
and onto a screen — via notebook 06, never via the daily pipeline.
