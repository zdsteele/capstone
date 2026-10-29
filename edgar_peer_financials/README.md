# edgar_peer_financials

Rebuilds three Bloomberg peer screens — **Financials**, **Profitability**, **Balance
Sheet / leverage** — for a 23-company energy & industrial peer set, straight from
SEC EDGAR filings, as Delta tables in `engineering_playground_dev`.

Self-contained Databricks Asset Bundle (one serverless job), carved out of the
EDGAR capstone (`../`) — no app, no Lakebase, no vector search.

## What it reads

| Source | Used for |
|---|---|
| 10-K / 10-Q XBRL instance (`*_htm.xml`, inside each filing) | every financial number — includes company extension tags and dimensioned facts |
| SEC companyfacts API | fallback / history for standard us-gaap tags |
| 8-K Item 2.02 → EX-99.1 earnings release | **Adj. EBITDA** (non-GAAP, not in XBRL) — read by an LLM via `ai_query` |

Not from EDGAR: **1Y TSR** and **Stock Return** need market prices — NULL for now.

## Pipeline — job `edgar_peer_financials_<target>`

```
01_bronze_ingest ─┬─ 02_silver_facts ──┐
                  └─ 03_silver_adj_ebitda ─┴─ 04_silver_ltm ─ 05_gold_tables ─ 06_reconcile
```

| Notebook | Writes |
|---|---|
| `01_bronze_ingest` | Volume `bronze_edgar.raw_edgar/{cik}/{accession}/…` (submission .txt, primary doc, XBRL instance, EX-99.1); `bronze_submissions`, `bronze_companyfacts`, `bronze_filings`, `bronze_filing_documents`, `bronze_earnings_releases` |
| `02_silver_facts` | `silver_companies`, `silver_filings`, `silver_financial_facts` (instance ∪ companyfacts, `source` column) |
| `03_silver_adj_ebitda` | `silver_adj_ebitda` (one row per release; MERGE — only new releases hit the LLM) |
| `04_silver_ltm` | `silver_resolved_facts`, `silver_ltm_lineage` (which tag/filing fed each number), `silver_ltm_metrics` |
| `05_gold_tables` | `gold_peer_financials`, `gold_peer_profitability`, `gold_peer_leverage` + `*_latest` views |
| `06_reconcile` | `bronze_bloomberg_reference`, `gold_bloomberg_reconciliation` |

Incremental: 01 skips filings already downloaded; 03 skips releases already
extracted; 02/04/05/06 rebuild (small data — 23 companies).

## Definitions

- **LTM** for every flow metric: `YTD(as of) + FY(prior) − YTD(a year earlier)`;
  at fiscal Q4, the FY value. Fiscal quarter comes from the company's fiscal-year end.
- **EBIT** = operating income (fallback: pretax + interest). **EBITDA** (margin,
  leverage ratios) = EBIT + D&A as reported — Bloomberg's multiples use this, not Adj. EBITDA.
- **Adj. EBITDA** = sum of the last four quarterly company-reported values, or the FY value
  at Q4; `adj_ebitda_source = reported_fallback` when the company publishes none.
- **Total Debt** = short-term borrowings + commercial paper + current & long-term debt +
  finance-lease + operating-lease liabilities. **Net Debt** = Total Debt − cash − short-term investments.
- **CAPEX** is negative; **FCF** = OCF + CAPEX; **Cash flow per share** = LTM OCF / diluted shares.
- **Growth** = LTM vs LTM four quarters earlier, NULL on a non-positive base ("NM").
- **ROA / ROE** = LTM NI / average(current, year-ago) assets / equity.
  **ROIC** = (NI + interest × (1 − tax rate)) / average(equity + total debt).
- Units: USD millions; percentages as 13.4 = 13.4%; multiples as x.

## Tuning the mapping

All XBRL tag choices live in [`config/concept_map.json`](config/concept_map.json):
a default priority list per metric plus per-ticker `overrides`. The LTM is built
per candidate and the first candidate with a complete LTM wins (a tag switch
mid-year falls back to a per-part `mixed` candidate). Entry forms:

- `us-gaap:Revenues`: the consolidated (no-dimension) fact
- `us-gaap:X[axis=member]`: that exact dimension set (WHD/SEI Class A EPS)
- `A+B`: the sum; every term must exist (CAT's segment debt, AESI's D&A + amortization)
- an override of `[]`: the metric doesn't apply to that ticker (GEV's finance leases sit inside its debt tag)

Loop: run the job → `SELECT * FROM gold_edgar.gold_bloomberg_reconciliation WHERE status = 'miss'`
→ look the number up in `silver_edgar.silver_ltm_lineage` (concept, filing, period) →
find the right tag in `silver_financial_facts` → add an override → re-run 04–06.

## Known differences vs. the Bloomberg sample (2026 Q2)

First reconciliation: 462 of 549 compared values within tolerance (5% or a small absolute gap).
Revenue, OCF, EBIT, net income, EPS, capex, total debt and sales growth match for ~all
companies. The remaining misses, by cause:

| Cause | Affected | Notes |
|---|---|---|
| **AMRC filed its FY2025 10-K without XBRL** (`isXBRL=0`) | AMRC flows for 2025 Q4–2026 Q2 | no structured FY2025 numbers exist on EDGAR; balance-sheet items still match. Clears with a 10-K/A or the FY2026 10-K |
| **Bloomberg adjusts / defines differently** | Adj. EBITDA (8/23), ROIC (8/21), EBITDA/Interest (3/16) | ours is company-reported Adj. EBITDA; Bloomberg normalizes its own. Bloomberg's interest expense and invested capital don't match any single filed figure |
| **D&A not in one tag** | EBITDA margin for BE, AESI, LBRT, PUMP (−6 to −30%) | Bloomberg's D&A includes items those filers tag separately |
| **CAT financial-products business** | CAT net debt / capex | not yet verified: Bloomberg's implied cash (40.4bn) suggests it nets finance receivables, and its capex likely adds equipment leased to others (a separate CAT tag) |
| **Near-zero denominators** | BE net debt (−51 vs +1) | tiny absolute gap, huge % |
| **WHD net income** | WHD NI / margins / ROA / ROE | ours (163.4) traces to the filings: H1'26 81.9 + FY'25 166.0 − H1'25 84.6. Bloomberg's 81.9 equals H1 2026 alone |

## Run

```bash
cd edgar_peer_financials
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run peer_financials -t dev
```

The peer list is [`config/peers.json`](config/peers.json) (CIKs pinned). The
Bloomberg sample used for reconciliation is in [`config/bloomberg_reference/`](config/bloomberg_reference/).
