# Capstone Project Proposal

## EDGAR Intelligence Platform
### SEC Filing Search, Analytics, and AI Research Assistant

Prepared by: Zach

---

## Problem and Target Users

SEC EDGAR is the authoritative source for U.S. public company filings, but it is hard to actually use. The data is scattered across raw HTML documents, XBRL files, and archive directories, with no easy way to search across companies, compare financial metrics over time, or ask plain-language questions about a filing. Analysts end up manually opening filings one at a time and copying numbers into spreadsheets.

**Target users:**

- Equity research analysts who need to compare financials across companies and quarters
- Retail investors doing fundamental research on individual companies
- Compliance and corporate-research teams who monitor filing activity

**Primary workflow (end-to-end user journey):** a user searches for a company, browses its filing history, opens a specific filing to view extracted financial statements and sections, compares metrics across periods or against peers on a dashboard, then asks the AI agent to summarize the filing and save findings to a watchlist or research note. The platform turns EDGAR from a document repository into a searchable financial data platform. It is a mini financial data platform, not a web scraper.

As a worked example, the platform will ingest and process a filing such as Alphabet's Q2 2026 10-Q. That single filing contains the primary HTML document, exhibits, XBRL instance data, taxonomy files, and a full submission text file, which shows the mix of structured and unstructured data the project handles.

---

## Data Sources and Integration Details

The project uses the SEC EDGAR system as the primary source and a market-data API for enrichment. Ingestion uses the SEC's official APIs and archives rather than emulating browser activity, and respects the SEC's automated-access rules (10 requests per second, declared User-Agent header).

| Source / Endpoint | Auth | Rate limit | Format | Data | Update frequency |
|---|---|---|---|---|---|
| EDGAR Submissions API `data.sec.gov/submissions/CIK##########.json` | None; User-Agent required | 10 req/s | JSON | Filing history and metadata | Filings post through the day; refresh daily or on demand |
| XBRL company facts `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` | None; User-Agent required | 10 req/s | JSON | Extracted financial facts | Daily |
| EDGAR archives `sec.gov/Archives/edgar/data/...` | None; User-Agent required | 10 req/s | HTML / XML / XBRL / text | Full filing documents and exhibits | Per filing |
| Market Data API (Alpha Vantage, Finnhub, or yfinance) | API key | Tier dependent (approx. 5 to 60 req/min on free tiers) | JSON | Price, volume, market cap, history | Daily bars, intraday optional |

**Ingestion into Bronze:** a rate-limited PySpark ingestion job pulls submissions and XBRL facts per CIK, downloads the referenced filing documents from the archives, and writes each stream to raw Bronze Delta tables (`bronze_company_submissions`, `bronze_filings`, `bronze_filing_documents`, `bronze_xbrl_facts`, `bronze_filing_text`). Original filing documents are retained so SEC data can be reproduced and reprocessed. The market-data API is polled on a daily cadence and landed alongside the SEC data for enrichment in Silver.

---

## Spark Data Pipeline

The ingestion and transformation pipeline is built with Apache Spark and PySpark on Databricks using a medallion architecture.

**Bronze (raw):** raw EDGAR and market data, retained verbatim. Tables: `bronze_company_submissions`, `bronze_filings`, `bronze_filing_documents`, `bronze_xbrl_facts`, `bronze_filing_text`.

**Silver (cleaned and normalized):** key transformations include parsing XBRL instance and taxonomy files into typed facts, parsing filing HTML into sections, entity resolution to standardize company identifiers (CIK to ticker to name), deduplicating filings by accession number, normalizing financial concepts and reporting periods, extracting financial tables and values, and chunking filing text for search. Enrichment joins market data onto the company and period grain. Tables: `silver_companies`, `silver_filings`, `silver_filing_sections`, `silver_financial_facts`, `silver_financial_periods`, `silver_exhibits`.

**Gold (application-ready):** analytical datasets used by the app and the analytics layer. Tables: `gold_company_financials`, `gold_revenue_history`, `gold_income_statement_metrics`, `gold_balance_sheet_metrics`, `gold_cash_flow_metrics`, `gold_filing_activity`, `gold_company_comparisons`.

**Partitioning and quality checks:** fact and filing tables are partitioned by filing period (year and quarter) and clustered by CIK to keep company-level reads fast. Quality checks include not-null constraints on keys (CIK, accession number, period), deduplication on accession number, range checks on numeric facts, and referential checks that every fact resolves to a known company and filing before promotion to Gold.

---

## Lakebase Data Model

Lakebase serves as the operational relational database (managed PostgreSQL) for the application, giving the project a true transactional component instead of using Delta tables for everything. Core tables: `users`, `companies`, `watchlists`, `watchlist_companies`, `saved_filings`, `saved_research`, `agent_conversations`, `agent_actions`.

**watchlists:** `watchlist_id`, `user_id`, `name`, `created_at`

**watchlist_companies:** `watchlist_id`, `cik`, `ticker`, `added_at`

**saved_research:** `research_id`, `user_id`, `company_cik`, `filing_id`, `title`, `notes`, `created_at`

---

## Action-Taking AI Agent

The application includes an SEC Research Assistant, a tool-calling agent that both retrieves information and performs write actions. This makes it action-taking rather than a plain chatbot. Retrieval reads from Gold Delta and Lakebase; writes go to Lakebase.

**Retrieval tools:** `search_company()`, `search_filings()`, `get_filing()`, `get_financial_metric()`, `compare_companies()`, `search_filing_text()`, `get_saved_research()`

**Write / action tools:** `save_filing()`, `save_company_to_watchlist()`, `create_research_note()`, `update_research_note()`, `remove_from_watchlist()`

**Example interaction:**

- User: "Find Alphabet's most recent 10-Q and summarize the major changes in revenue and operating expenses." The agent retrieves the filing and financial data and answers.
- User: "Save this filing to my research list and add a note that I want to compare Google's cloud growth against Microsoft." The agent writes those records to Lakebase.

---

## Analytics Pipeline

Changes to the Lakebase operational tables are captured using Lakebase Change Data Feed and propagated into Delta analytics tables. (If CDF from Lakebase is unavailable in the target workspace, the same analytics tables will be populated with Delta Live Tables, which the requirements allow as an alternative.)

**Change events captured:** inserts and updates to watchlists, saved filings, research notes, agent conversations, and agent actions.

**Transformation to analytics Delta tables:** change rows are appended to append-only event tables, then aggregated into usage and activity marts.

**Metrics and dashboards:**

- Most searched companies, most viewed filings, most requested filing types
- Most viewed financial metrics
- Saved filings and watchlist activity over time
- AI agent usage, per-tool call counts, and agent action success rate
- Query latency and usage funnels (search to filing view to save)
- Research notes created and filings searched per user

---

## Frontend and Deployment

The frontend is deployed as a working Databricks App. Databricks Apps can use Lakebase directly as a managed PostgreSQL backend, which fits this architecture.

**Core screens and interactions:**

- **Company Search:** search by name or ticker (Alphabet, Microsoft, Apple, Amazon, Tesla) and view company info, recent filings, filing history, and headline financial metrics. Reads Gold Delta and Lakebase.
- **Filing Explorer:** open a specific filing (for example Alphabet 10-Q, Q2 2026) and view metadata, sections, financial statements, XBRL facts, exhibits, filing text, and a link to the SEC source document.
- **Financial Dashboard:** visualize revenue, net income, operating income, cash flow, assets, liabilities, debt, EPS, and year-over-year changes. Reads Gold Delta.
- **AI Research Assistant:** a chat panel that calls the tool-calling agent; the agent reads Gold Delta and Lakebase and writes user actions (watchlists, saved filings, notes) to Lakebase.

**Deployment plan:** package the app with its `app.yaml` and dependencies, register it as a Databricks App, bind it to the Lakebase instance and Unity Catalog resources, and use the workspace identity for auth so the app queries Gold Delta and Lakebase under governed permissions. Frontend calls the agent endpoint and Lakebase directly for reads and writes.

---

## Big Data Characteristics (meets Variety + Volume)

The project meets two of the three Vs: **Variety** and **Volume**.

**Variety (qualifies):** SEC filings arrive in JSON, HTML, XML, XBRL, plain text, financial tables, exhibits, and narrative sections. A single Alphabet 10-Q includes an HTML filing, full submission text, XBRL instance data, and XBRL schema, calculation, definition, label, and presentation files, plus multiple exhibits. The pipeline processes both structured and unstructured or semi-structured data.

**Volume (qualifies, >1M rows):** the row count is anchored in XBRL financial facts. Scoping to roughly 500 companies (S&P 500 constituents) across 10 or more years of 10-K, 10-Q, and 8-K filings gives on the order of 500 companies multiplied by about 40 filing periods multiplied by several hundred to a few thousand XBRL facts per filing. At a conservative 800 facts per filing, that is roughly **16 million rows** in `silver_financial_facts` alone. Two additional tables independently exceed one million rows:

- **Filing-text chunks:** hundreds of searchable chunks per filing across tens of thousands of filings.
- **Daily market-data bars:** about 500 companies multiplied by about 2,500 trading days is roughly **1.25 million rows**.

Even a reduced pilot of 100 companies clears one million XBRL facts, so the Volume claim is credible at full scope and at pilot scope. The target for the analytical Delta tables is well above one million rows.

---

## Technology Stack

| Component | Technology |
|---|---|
| Data Source | SEC EDGAR (Submissions API, XBRL API, filing archives) |
| External API | Market Data API (Alpha Vantage, Finnhub, or yfinance) |
| Processing | Apache Spark / PySpark |
| Lakehouse | Delta Lake (Bronze / Silver / Gold) |
| Operational DB | Lakebase (managed PostgreSQL) |
| Change Data Capture | Lakebase Change Data Feed (DLT as fallback) |
| AI Agent | Databricks tool-calling agent |
| Frontend | Databricks Apps |
| Analytics | Databricks SQL / Delta |
| Governance | Unity Catalog |
| Language | Python |
| Deployment | Databricks Apps |

---

## Expected Outcome

The final application provides a single interface for researching SEC filings without manually navigating EDGAR. A user can search for a company, view its historical filings, open and explore individual filings and extracted financials, compare metrics across periods or companies, ask an AI agent questions, save filings and research notes through the agent, create watchlists, and view analytics about their own research activity.

The project demonstrates an end-to-end data engineering and AI architecture using SEC EDGAR, Apache Spark, Delta Lake, Lakebase, Change Data Feed, an external market API, a tool-calling AI agent, and a deployed Databricks App.

## more info
https://www.sec.gov/search-filings/edgar-application-programming-interfaces


Capstone Project Proposal
Required Components
Your capstone must include:

Spark data pipeline: Ingest, clean, transform, or enrich project data with Spark.

Third-party API: Integrate at least one external API relevant to the project.

Lakebase data model: Store the application's relational and operational data in Lakebase.

Action-taking AI agent: Provide an agent with tools that can both retrieve information and perform at least one meaningful write action, such as saving, updating, or deleting application data.

Analytics pipeline: Use Change Data Feed from Lakebase (or Delta Live Table) to populate a Delta table that supports analytics about application usage, agent activity, or data changes.

Frontend: Provide a usable interface for the project's core workflow.

Deployed application: Deploy the frontend as a working Databricks App or on Render

Tackles at least 2 of the 3 Vs of Big Data: The project needs to have at least two of the following high volume (>1m rows), high velocity (<1 minute latency), or high variety (processes unstructured data)