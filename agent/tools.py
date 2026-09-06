"""
Tools for the SEC Research Assistant.

Retrieval tools read the Gold/Silver Delta marts (`bootcamp_students.zdsteele.*`)
through the SQL warehouse (``lib.warehouse``). Write tools mutate the
``edgar.*`` operational tables in Lakebase (``lib.lakebase``).

Every tool call is logged to ``edgar.agent_actions`` — retrieval tools as
``SUCCESS``/``ERROR``; write tools start ``PENDING`` and flip on completion (the
``submit_receipt`` sink pattern from ``ai-agents-2026``). That table is the CDF
source for the usage-analytics pipeline.

``build_tools(ctx)`` binds the per-request user/conversation context and returns
the LangChain tool list for the LangGraph ``ToolNode``.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass

from langchain_core.tools import tool

from lib import lakebase, warehouse


@dataclass
class ToolContext:
    user_id: int
    conversation_id: int | None = None
    vs_endpoint: str = ""
    vs_index: str = ""
    embedding_endpoint: str = "databricks-gte-large-en"


# ---------------------------------------------------------------------------
# action logging
# ---------------------------------------------------------------------------

@contextmanager
def record_action(ctx: ToolContext, tool_name: str, kind: str, args: dict):
    started = time.time()
    action_id = None
    try:
        rows = lakebase.run_write(
            """
            INSERT INTO edgar.agent_actions
                (conversation_id, user_id, tool_name, tool_kind, args_json, status)
            VALUES (%(conv)s, %(uid)s, %(tool)s, %(kind)s, %(args)s::jsonb, 'PENDING')
            RETURNING action_id
            """,
            {
                "conv": ctx.conversation_id,
                "uid": ctx.user_id,
                "tool": tool_name,
                "kind": kind,
                "args": json.dumps(args, default=str),
            },
        )
        action_id = rows[0]["action_id"] if rows else None
    except Exception:
        action_id = None

    rec: dict = {"result": None}
    try:
        yield rec
    except Exception as exc:
        _finish(action_id, "ERROR", None, str(exc), started)
        raise
    else:
        _finish(action_id, "SUCCESS", rec.get("result"), None, started)


def _finish(action_id, status, result, error, started):
    if action_id is None:
        return
    try:
        lakebase.run_write(
            """
            UPDATE edgar.agent_actions
               SET status = %(status)s,
                   result_json = %(result)s::jsonb,
                   error = %(error)s,
                   latency_ms = %(ms)s
             WHERE action_id = %(id)s
            """,
            {
                "status": status,
                "result": json.dumps(result, default=str) if result is not None else None,
                "error": error,
                "ms": int((time.time() - started) * 1000),
                "id": action_id,
            },
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _rows_or_msg(rows: list[dict], empty: str) -> str:
    if not rows:
        return empty
    return json.dumps(rows, default=str, indent=2)


def _wq(sql: str, params: list | None = None) -> list[dict]:
    try:
        return warehouse.query(sql, params)
    except Exception as exc:
        return [{"_error": str(exc)}]


def _resolve_cik(token: str) -> str | None:
    """Accept a 10-digit CIK, a bare int CIK, or a ticker; return the 10-digit CIK."""
    if not token:
        return None
    t = token.strip().upper()
    if t.isdigit():
        return t.zfill(10)
    rows = _wq(
        f"SELECT cik FROM {warehouse.table('silver_companies')} WHERE upper(ticker) = ? LIMIT 1",
        [t],
    )
    if rows and "cik" in rows[0]:
        return rows[0]["cik"]
    try:
        rows = lakebase.run_query(
            "SELECT cik FROM edgar.companies WHERE upper(ticker) = %s LIMIT 1", (t,)
        )
        return rows[0]["cik"] if rows else None
    except Exception:
        return None


def _default_watchlist_id(ctx: ToolContext, name: str) -> int:
    rows = lakebase.run_write(
        """
        INSERT INTO edgar.watchlists (user_id, name)
        VALUES (%(uid)s, %(name)s)
        ON CONFLICT (user_id, name) DO UPDATE SET name = EXCLUDED.name
        RETURNING watchlist_id
        """,
        {"uid": ctx.user_id, "name": name},
    )
    return rows[0]["watchlist_id"]


# ---------------------------------------------------------------------------
# tool factory
# ---------------------------------------------------------------------------

def build_tools(ctx: ToolContext) -> list:
    T = warehouse.table

    # ---- retrieval --------------------------------------------------

    @tool
    def search_company(query: str) -> str:
        """Find companies by name, ticker, or CIK. Returns cik, ticker, name, sic."""
        with record_action(ctx, "search_company", "retrieval", {"query": query}) as rec:
            like = f"%{query}%"
            cik = query.zfill(10) if query.isdigit() else ""
            rows = _wq(
                f"""
                SELECT cik, ticker, name, sic, sic_description
                FROM {T('silver_companies')}
                WHERE name ILIKE ? OR ticker ILIKE ? OR cik = ?
                ORDER BY name LIMIT 25
                """,
                [like, like, cik],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No company matched '{query}'.")

    @tool
    def search_filings(company: str, form: str | None = None, limit: int = 20) -> str:
        """List a company's filings, newest first. `company` is a ticker or CIK;
        `form` optionally filters to a type like '10-K', '10-Q', '8-K'."""
        with record_action(
            ctx, "search_filings", "retrieval",
            {"company": company, "form": form, "limit": limit},
        ) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"""
                SELECT accession, form, filing_date, report_date, primary_document, sec_url
                FROM {T('silver_filings')}
                WHERE cik = ? AND (? IS NULL OR form = ?)
                ORDER BY filing_date DESC LIMIT {min(int(limit), 100)}
                """,
                [cik, form, form],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No filings found for {company}.")

    @tool
    def get_filing(accession: str) -> str:
        """Get one filing's metadata plus its parsed section headings and exhibit
        count. `accession` is the SEC accession number (dashed form)."""
        with record_action(ctx, "get_filing", "retrieval", {"accession": accession}) as rec:
            meta = _wq(f"SELECT * FROM {T('silver_filings')} WHERE accession = ?", [accession])
            sections = _wq(
                f"""
                SELECT section, heading, char_len FROM {T('silver_filing_sections')}
                WHERE accession = ? ORDER BY section_index
                """,
                [accession],
            )
            exhibits = _wq(
                f"SELECT count(*) AS n FROM {T('silver_exhibits')} WHERE accession = ?",
                [accession],
            )
            out = {
                "filing": meta[0] if meta and "_error" not in meta[0] else None,
                "sections": sections,
                "exhibit_count": exhibits[0].get("n") if exhibits and "_error" not in exhibits[0] else 0,
            }
            rec["result"] = out
            return json.dumps(out, default=str, indent=2) if out["filing"] else f"No filing {accession}."

    @tool
    def get_filing_intelligence(accession: str) -> str:
        """Get the AI-generated briefing for a 10-K / 10-Q: executive summary,
        revenue commentary, risk themes, management tone, and notable items.
        Prefer this over reading raw sections when the user wants a synthesis."""
        with record_action(
            ctx, "get_filing_intelligence", "retrieval", {"accession": accession}
        ) as rec:
            rows = _wq(
                f"""
                SELECT ticker, form, filing_date, executive_summary, revenue_commentary,
                       risk_themes, management_tone, notable_items
                FROM {T('gold_filing_intelligence')} WHERE accession = ?
                """,
                [accession],
            )
            rec["result"] = rows
            return _rows_or_msg(
                rows, f"No AI briefing for {accession} (only 10-K/10-Q are summarized)."
            )

    @tool
    def screen_companies(
        metric: str = "overall_score", worst_first: bool = False, limit: int = 10,
        direction: str | None = None, min_score: int | None = None,
        max_score: int | None = None, sector: str | None = None,
    ) -> str:
        """Rank / screen ALL covered companies by their AI health assessment.
        Use this for 'which companies are the healthiest / least healthy',
        'worst balance sheets', 'deteriorating companies', 'best cash generation
        in Technology', etc. — anything spanning the universe rather than one name.
        `metric`: overall_score | growth_quality | profitability | cash_generation
        | balance_sheet | capital_allocation | capital_efficiency.
        `worst_first=True` for the weakest. `direction`: Improving | Stable |
        Deteriorating. `min_score`/`max_score` filter overall_score. `sector`
        filters on the business-profile sector."""
        with record_action(
            ctx, "screen_companies", "retrieval",
            {"metric": metric, "worst_first": worst_first, "limit": limit,
             "direction": direction, "sector": sector},
        ) as rec:
            allowed = {"overall_score", "growth_quality", "profitability",
                       "cash_generation", "balance_sheet", "capital_allocation",
                       "capital_efficiency"}
            m = metric if metric in allowed else "overall_score"
            col = "h.overall_score" if m == "overall_score" else f"h.scores.{m}"
            # extra projected column only when ranking on a sub-score (avoids a
            # duplicate `overall_score` in the select list)
            extra = "" if m == "overall_score" else f", {col} AS {m}"
            order = "ASC" if worst_first else "DESC"
            lim = min(int(limit), 50)

            def _run(with_bp: bool):
                where = ["h.overall_score IS NOT NULL"]
                p: list = []
                if direction:
                    where.append("lower(h.direction) = ?"); p.append(direction.strip().lower())
                if min_score is not None:
                    where.append("h.overall_score >= ?"); p.append(int(min_score))
                if max_score is not None:
                    where.append("h.overall_score <= ?"); p.append(int(max_score))
                sel = "bp.sector" if with_bp else "cast(null as string) AS sector"
                join = f"LEFT JOIN {T('gold_business_profile')} bp ON bp.cik = h.cik" if with_bp else ""
                if with_bp and sector:
                    where.append("lower(bp.sector) LIKE ?"); p.append(f"%{sector.strip().lower()}%")
                return _wq(
                    f"""
                    SELECT h.ticker, h.name, {sel}, h.overall_score, h.overall_label,
                           h.direction, h.primary_strength, h.primary_risk{extra}
                    FROM {T('gold_company_health')} h {join}
                    WHERE {' AND '.join(where)}
                    ORDER BY {col} {order} NULLS LAST
                    LIMIT {lim}
                    """,
                    p,
                )

            rows = _run(with_bp=True)
            if rows and isinstance(rows[0], dict) and "_error" in rows[0]:
                rows = _run(with_bp=False)   # gold_business_profile not built yet
            rec["result"] = rows
            return _rows_or_msg(rows, "No health assessments yet (run notebook 10).")

    @tool
    def get_business_profile(company: str) -> str:
        """What the business actually is (analyst spec §2): primary business,
        revenue model, reportable segments, geographies, key customers &
        concentration, competitors, sector classification, cyclicality, capital
        intensity, regulatory exposure, economic drivers. From the latest 10-K.
        Call this FIRST for an unfamiliar company before analysing ratios."""
        with record_action(ctx, "get_business_profile", "retrieval", {"company": company}) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"SELECT * EXCEPT (cik, model) FROM {T('gold_business_profile')} WHERE cik = ?",
                [cik],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No business profile for {company} (run notebook 08).")

    @tool
    def get_8k_events(company: str, limit: int = 15) -> str:
        """Recent Form 8-K events for a company (analyst spec §2 timeline):
        event_type, plain-English summary, materiality (high/medium/low), and the
        key figures mentioned. 8-Ks announce material events between the periodic
        reports — earnings, M&A, executive changes, financing, guidance, legal."""
        with record_action(ctx, "get_8k_events", "retrieval", {"company": company, "limit": limit}) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"""
                SELECT accession, filing_date, event_type, materiality, event_summary, key_figures
                FROM {T('gold_8k_events')} WHERE cik = ?
                ORDER BY filing_date DESC LIMIT {min(int(limit), 40)}
                """,
                [cik],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No 8-K events for {company} (run notebook 08).")

    @tool
    def get_filing_changes(company: str, limit: int = 4) -> str:
        """What materially changed between a company's consecutive 10-K/10-Q
        filings (analyst spec §16): new risk factors, removed risks, topics that
        escalated (demand, pricing, liquidity, litigation, AI, going concern…),
        tone shift, materiality. Use for 'what changed', 'new risks', 'is
        management more cautious' questions."""
        with record_action(ctx, "get_filing_changes", "retrieval", {"company": company, "limit": limit}) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"""
                SELECT accession, form, filing_date, prev_filing_date, tone_shift, materiality,
                       change_summary, new_risks, removed_risks, escalated_topics
                FROM {T('gold_filing_language_changes')} WHERE cik = ?
                ORDER BY filing_date DESC LIMIT {min(int(limit), 12)}
                """,
                [cik],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No filing-change analysis for {company} (run notebook 12).")

    @tool
    def get_financial_ratios(
        company: str, fiscal_year: int | None = None, fiscal_period: str | None = None
    ) -> str:
        """Derived ratios + trend flags per period, covering analyst-spec §3-10:
        margins & growth; FCF, FCF margin/conversion and the FCF **bridge**
        (net income + D&A + SBC ± working-capital changes − capex); balance sheet
        (current ratio, total/net debt, debt & net-debt / EBITDA, interest
        coverage, goodwill % assets); capital allocation (dividend payout, FCF
        payout, buyback % of FCF, SBC vs buybacks); ROIC with an effective tax
        rate; working capital (DSO/DIO/DPO/CCC); per-share figures. `*_trend` is
        up/down/stable vs the same period a year earlier. Pin fiscal_year /
        fiscal_period to focus."""
        with record_action(
            ctx, "get_financial_ratios", "retrieval",
            {"company": company, "fiscal_year": fiscal_year, "fiscal_period": fiscal_period},
        ) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"""
                SELECT * EXCEPT (cik, ticker, name)
                FROM {T('gold_financial_ratios')}
                WHERE cik = ? AND (? IS NULL OR fiscal_year = ?)
                  AND (? IS NULL OR fiscal_period = ?)
                ORDER BY period_end DESC LIMIT 12
                """,
                [cik, fiscal_year, fiscal_year, fiscal_period, fiscal_period],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No ratios for {company} (run notebook 09).")

    @tool
    def get_valuation(company: str) -> str:
        """Get market-based valuation multiples for a company: price, market cap,
        enterprise value, P/E, EV/EBIT, EV/Revenue, Price/FCF, FCF yield,
        Price/Book. Market data is from yfinance; fundamentals from XBRL.
        This is STOCK VALUATION — keep it separate from company quality
        (get_company_health). A low multiple is not automatically 'cheap'."""
        with record_action(ctx, "get_valuation", "retrieval", {"company": company}) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"SELECT * EXCEPT (cik) FROM {T('gold_valuation')} WHERE cik = ?",
                [cik],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No valuation for {company} (run notebook 11).")

    @tool
    def get_company_health(company: str) -> str:
        """Get the AI investor health assessment for a company: 0-100 scores per
        dimension + overall, direction, and the full structured report (what
        changed, cash check, debt check, shareholder check, accounting check,
        risks, bull/base/bear, what to watch next, bottom line). Use this for
        'is this company healthy / improving / a good business' questions."""
        with record_action(ctx, "get_company_health", "retrieval", {"company": company}) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            rows = _wq(
                f"SELECT * EXCEPT (raw) FROM {T('gold_company_health')} WHERE cik = ?", [cik]
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No health assessment for {company} (run notebook 10).")

    @tool
    def get_financial_metric(
        company: str, metric: str, fiscal_year: int | None = None,
        fiscal_period: str | None = None,
    ) -> str:
        """Get a financial metric time series for a company. `metric` is one of:
        revenue, gross_profit, operating_income, net_income, eps_diluted,
        total_assets, total_liabilities, stockholders_equity, cash_and_equivalents,
        long_term_debt, operating_cash_flow, capital_expenditures, operating_expenses,
        research_development. Optionally pin fiscal_year and fiscal_period ('FY','Q1'..)."""
        with record_action(
            ctx, "get_financial_metric", "retrieval",
            {"company": company, "metric": metric, "fiscal_year": fiscal_year, "fiscal_period": fiscal_period},
        ) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            col = "".join(ch for ch in metric.lower().strip() if ch.isalnum() or ch == "_")
            rows = _wq(
                f"""
                SELECT fiscal_year, fiscal_period, period_end, form, {col} AS value
                FROM {T('gold_company_financials')}
                WHERE cik = ? AND {col} IS NOT NULL
                  AND (? IS NULL OR fiscal_year = ?)
                  AND (? IS NULL OR fiscal_period = ?)
                ORDER BY period_end DESC LIMIT 40
                """,
                [cik, fiscal_year, fiscal_year, fiscal_period, fiscal_period],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No '{metric}' values for {company}.")

    @tool
    def compare_companies(companies: list[str], metric: str) -> str:
        """Compare the latest value of `metric` across several companies (tickers
        or CIKs)."""
        with record_action(
            ctx, "compare_companies", "retrieval",
            {"companies": companies, "metric": metric},
        ) as rec:
            ciks = [c for c in (_resolve_cik(x) for x in companies) if c]
            if not ciks:
                return "Could not resolve any of the given companies."
            placeholders = ", ".join("?" for _ in ciks)
            rows = _wq(
                f"""
                SELECT name, ticker, metric, latest_value, latest_period_end, fiscal_year, fiscal_period
                FROM {T('gold_company_comparisons')}
                WHERE cik IN ({placeholders}) AND metric = ?
                ORDER BY latest_value DESC NULLS LAST
                """,
                [*ciks, metric.lower().strip()],
            )
            rec["result"] = rows
            return _rows_or_msg(rows, f"No comparison data for metric '{metric}'.")

    @tool
    def search_filing_text(query: str, company: str | None = None, k: int = 5) -> str:
        """Semantic + keyword (hybrid) search over parsed 10-K/10-Q filing text —
        business, risk factors, MD&A. Returns the top matching passages, each with
        its filing accession, section, and the FULL section text it came from
        (`parent_text`) so you can quote and reason from real context, not a
        fragment. Use this to answer 'what does <company> say about <topic>'."""
        with record_action(
            ctx, "search_filing_text", "retrieval",
            {"query": query, "company": company, "k": k},
        ) as rec:
            cik = _resolve_cik(company) if company else None
            hits = _vector_search(ctx, query, cik, k) if ctx.vs_index else None
            if not hits:
                like = f"%{query}%"
                hits = _wq(
                    f"""
                    SELECT chunk_id, cik, accession, section, heading,
                           substring(chunk_text, 1, 1200) AS chunk_text
                    FROM {T('silver_filing_text_chunks')}
                    WHERE chunk_text ILIKE ? AND (? IS NULL OR cik = ?)
                    LIMIT {min(int(k), 20)}
                    """,
                    [like, cik, cik],
                )
            rec["result"] = hits
            return _rows_or_msg(hits, f"No filing text matched '{query}'.")

    @tool
    def read_filing_section(accession: str, section: str) -> str:
        """Return the FULL text of one section of a filing — e.g.
        section='Item 1A' (risk factors), 'Item 7' (MD&A), 'Item 1' (business),
        'Item 8.01'. Use when the user wants a thorough read of a specific part
        rather than a keyword search. `accession` is the dashed SEC number."""
        with record_action(
            ctx, "read_filing_section", "retrieval",
            {"accession": accession, "section": section},
        ) as rec:
            want = section.strip().lower().replace("item", "").strip()
            rows = _wq(
                f"""
                SELECT section, heading, char_len,
                       substring(text, 1, 14000) AS text
                FROM {T('silver_filing_sections')}
                WHERE accession = ?
                  AND lower(replace(section, 'Item', '')) LIKE ?
                ORDER BY section_index
                """,
                [accession, f"%{want}%"],
            )
            rec["result"] = rows
            if not rows or "_error" in rows[0]:
                avail = _wq(
                    f"SELECT section FROM {T('silver_filing_sections')} WHERE accession = ? ORDER BY section_index",
                    [accession],
                )
                names = ", ".join(r.get("section", "?") for r in avail) if avail else "none"
                return f"No section matching '{section}' in {accession}. Available: {names}"
            return json.dumps(rows, default=str, indent=2)

    @tool
    def get_saved_research() -> str:
        """List the current user's saved research notes."""
        with record_action(ctx, "get_saved_research", "retrieval", {}) as rec:
            try:
                rows = lakebase.run_query(
                    """
                    SELECT research_id, title, company_cik, filing_id, notes, created_at, updated_at
                    FROM edgar.saved_research WHERE user_id = %s
                    ORDER BY updated_at DESC LIMIT 50
                    """,
                    (ctx.user_id,),
                )
            except Exception as exc:
                rows = [{"_error": str(exc)}]
            rec["result"] = rows
            return _rows_or_msg(rows, "You have no saved research notes yet.")

    # ---- write ----------------------------------------------------

    @tool
    def save_filing(accession: str, note: str | None = None) -> str:
        """Bookmark a filing to the user's saved list. Optionally attach a note."""
        with record_action(
            ctx, "save_filing", "write", {"accession": accession, "note": note}
        ) as rec:
            meta = _wq(
                f"SELECT cik, form, filing_date FROM {T('silver_filings')} WHERE accession = ?",
                [accession],
            )
            m = meta[0] if meta and "_error" not in meta[0] else {}
            if not m.get("cik"):
                return (
                    f"'{accession}' is not a known filing. Call search_filings first "
                    "to get a real accession number, then save that."
                )
            rows = lakebase.run_write(
                """
                INSERT INTO edgar.saved_filings
                    (user_id, company_cik, filing_id, form, filed_at, note)
                VALUES (%(uid)s, %(cik)s, %(acc)s, %(form)s, %(filed)s, %(note)s)
                ON CONFLICT (user_id, filing_id)
                    DO UPDATE SET note = COALESCE(EXCLUDED.note, edgar.saved_filings.note)
                RETURNING saved_filing_id
                """,
                {
                    "uid": ctx.user_id, "cik": m.get("cik"), "acc": accession,
                    "form": m.get("form"), "filed": m.get("filing_date"), "note": note,
                },
            )
            rec["result"] = {"saved_filing_id": rows[0]["saved_filing_id"] if rows else None}
            return f"Saved filing {accession} (saved_filing_id={rec['result']['saved_filing_id']})."

    @tool
    def save_company_to_watchlist(company: str, watchlist_name: str = "My Watchlist") -> str:
        """Add a company (ticker or CIK) to one of the user's watchlists,
        creating the watchlist if needed."""
        with record_action(
            ctx, "save_company_to_watchlist", "write",
            {"company": company, "watchlist_name": watchlist_name},
        ) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            tk = _wq(f"SELECT ticker FROM {T('silver_companies')} WHERE cik = ?", [cik])
            ticker = tk[0]["ticker"] if tk and "ticker" in tk[0] else None
            wl_id = _default_watchlist_id(ctx, watchlist_name)
            lakebase.run_write(
                """
                INSERT INTO edgar.watchlist_companies (watchlist_id, cik, ticker)
                VALUES (%(wl)s, %(cik)s, %(tk)s)
                ON CONFLICT (watchlist_id, cik) DO UPDATE SET ticker = EXCLUDED.ticker
                """,
                {"wl": wl_id, "cik": cik, "tk": ticker},
            )
            rec["result"] = {"watchlist_id": wl_id, "cik": cik, "ticker": ticker}
            return f"Added {ticker or cik} to '{watchlist_name}'."

    @tool
    def create_research_note(
        title: str, notes: str, company: str | None = None, accession: str | None = None
    ) -> str:
        """Create a research note. Optionally tie it to a company (ticker/CIK) and
        a filing accession."""
        with record_action(
            ctx, "create_research_note", "write",
            {"title": title, "company": company, "accession": accession},
        ) as rec:
            cik = _resolve_cik(company) if company else None
            rows = lakebase.run_write(
                """
                INSERT INTO edgar.saved_research
                    (user_id, company_cik, filing_id, title, notes)
                VALUES (%(uid)s, %(cik)s, %(acc)s, %(title)s, %(notes)s)
                RETURNING research_id
                """,
                {"uid": ctx.user_id, "cik": cik, "acc": accession, "title": title, "notes": notes},
            )
            rec["result"] = {"research_id": rows[0]["research_id"] if rows else None}
            return f"Created research note '{title}' (research_id={rec['result']['research_id']})."

    @tool
    def update_research_note(
        research_id: int, notes: str | None = None, title: str | None = None
    ) -> str:
        """Update an existing research note's title and/or notes."""
        with record_action(
            ctx, "update_research_note", "write",
            {"research_id": research_id, "has_notes": notes is not None, "has_title": title is not None},
        ) as rec:
            n = lakebase.run_write(
                """
                UPDATE edgar.saved_research
                   SET notes = COALESCE(%(notes)s, notes),
                       title = COALESCE(%(title)s, title),
                       updated_at = now()
                 WHERE research_id = %(id)s AND user_id = %(uid)s
                RETURNING research_id
                """,
                {"notes": notes, "title": title, "id": research_id, "uid": ctx.user_id},
            )
            ok = bool(n)
            rec["result"] = {"updated": ok}
            return f"Updated research note {research_id}." if ok else f"No note {research_id} for this user."

    @tool
    def remove_from_watchlist(company: str, watchlist_name: str = "My Watchlist") -> str:
        """Remove a company (ticker or CIK) from one of the user's watchlists."""
        with record_action(
            ctx, "remove_from_watchlist", "write",
            {"company": company, "watchlist_name": watchlist_name},
        ) as rec:
            cik = _resolve_cik(company)
            if not cik:
                return f"Could not resolve company '{company}'."
            n = lakebase.run_write(
                """
                DELETE FROM edgar.watchlist_companies wc
                USING edgar.watchlists w
                WHERE wc.watchlist_id = w.watchlist_id
                  AND w.user_id = %(uid)s AND w.name = %(name)s AND wc.cik = %(cik)s
                RETURNING wc.cik
                """,
                {"uid": ctx.user_id, "name": watchlist_name, "cik": cik},
            )
            rec["result"] = {"removed": bool(n)}
            return f"Removed {company} from '{watchlist_name}'." if n else f"{company} was not on '{watchlist_name}'."

    return [
        search_company, search_filings, get_filing, get_filing_intelligence,
        screen_companies, get_business_profile, get_8k_events, get_filing_changes,
        get_financial_ratios, get_valuation, get_company_health,
        get_financial_metric, compare_companies,
        search_filing_text, read_filing_section, get_saved_research,
        save_filing, save_company_to_watchlist, create_research_note,
        update_research_note, remove_from_watchlist,
    ]


# ---------------------------------------------------------------------------
# Vector Search — hybrid (dense + BM25) over context-enriched filing chunks.
# Falls back to keyword ILIKE when VS_INDEX is unset (see search_filing_text).
# ---------------------------------------------------------------------------

_VS_COLS_FULL = ["chunk_id", "cik", "accession", "ticker", "form",
                 "section", "heading", "chunk_text", "parent_text"]
_VS_COLS_BASE = _VS_COLS_FULL[:-1]  # for an index built before parent_text existed


def _vector_search(ctx: ToolContext, query: str, cik: str | None, k: int):
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    def _query(cols):
        body = {
            "columns": cols,
            "query_text": query,
            "num_results": min(int(k), 20),
            "query_type": "hybrid",
        }
        if cik:
            body["filters_json"] = json.dumps({"cik": cik})
        res = w.api_client.do(
            "POST", f"/api/2.0/vector-search/indexes/{ctx.vs_index}/query", body=body
        )
        rc = [c["name"] for c in res.get("manifest", {}).get("columns", [])]
        return [dict(zip(rc, row)) for row in res.get("result", {}).get("data_array", []) or []]

    try:
        return _query(_VS_COLS_FULL)
    except Exception:
        try:
            return _query(_VS_COLS_BASE)   # index predates the parent_text column
        except Exception:
            return None
