"""
EDGAR Intelligence Platform — Databricks App frontend.

Four screens (Company Search / Filing Explorer / Financial Dashboard / AI
Research Assistant), structured like ``ltap-cdc-day-2/app.py``: a Jinja shell +
a JSON API, session sign-in with a strict username guard, all errors as JSON.

Reads: Gold/Silver Delta via the SQL warehouse (``lib.warehouse``).
Writes / auth / chat state: Lakebase ``edgar.*`` (``lib.lakebase``).
The assistant runs the in-process LangGraph agent (``agent/graph.py``).
"""

import logging
import logging.handlers
import os
import re
import time

# Load .env for local dev BEFORE importing lib.* (they read os.environ at import).
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

from lib import lakebase, warehouse

def _setup_logging():
    """Console + rotating file (`logs/app.log`, next to this file). DEBUG when
    FLASK_DEBUG is set. Attached to the root logger so werkzeug / agent / lib
    all land in the same file for local debugging."""
    level = logging.DEBUG if os.environ.get("FLASK_DEBUG") else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S"
    )
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "app.log")
    if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
        fh = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)
    # keep chatty third-party libraries out of the app log even in debug mode
    for noisy in ("databricks.sql", "databricks.sdk", "urllib3", "py4j",
                  "mlflow", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return log_path


_LOG_PATH = _setup_logging()
logger = logging.getLogger("edgar-app")
logger.info("logging to %s (level=%s)", _LOG_PATH, logging.getLevelName(logging.getLogger().level))

app = Flask(__name__)


@app.after_request
def _log_request(resp):
    dur = (time.time() - getattr(request, "_t0", time.time())) * 1000
    logger.info("%s %s -> %s  %.0fms", request.method, request.full_path.rstrip("?"), resp.status_code, dur)
    return resp


@app.before_request
def _mark_start():
    request._t0 = time.time()
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)

VS_ENDPOINT = os.environ.get("VS_ENDPOINT", "")
VS_INDEX = os.environ.get("VS_INDEX", "")
EMBEDDING_ENDPOINT = os.environ.get("EMBEDDING_ENDPOINT", "databricks-gte-large-en")

_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,57}$")
_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_CIK_RE = re.compile(r"^\d{1,10}$")

T = warehouse.table  # bootcamp_students.zdsteele.<name>


def _validate_username(username: str) -> str:
    if not isinstance(username, str):
        raise ValueError("Username is required.")
    username = username.strip().lower()
    if not _USERNAME_RE.match(username):
        raise ValueError(
            "Username must be lowercase letters, digits, or underscores, "
            "and start with a letter or underscore."
        )
    return username


@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("unhandled")
    code = getattr(err, "code", 500)
    if not isinstance(code, int):
        code = 500
    return jsonify({"error": str(err)}), code


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------

def _current_user() -> dict | None:
    uid = session.get("user_id")
    if not uid:
        return None
    return {"user_id": uid, "username": session.get("username")}


def _require_user() -> dict:
    u = _current_user()
    if not u:
        from werkzeug.exceptions import Unauthorized

        raise Unauthorized("Not signed in.")
    return u


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.route("/login", methods=["GET"])
def login():
    if _current_user():
        return redirect(url_for("search_page"))
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def do_login():
    raw = (request.json or {}).get("username") if request.is_json else request.form.get("username")
    try:
        username = _validate_username(raw or "")
    except ValueError as exc:
        if request.is_json:
            return jsonify({"error": str(exc)}), 400
        return render_template("login.html", error=str(exc)), 400

    rows = lakebase.run_write(
        """
        INSERT INTO edgar.users (username)
        VALUES (%s)
        ON CONFLICT (username) DO UPDATE SET username = EXCLUDED.username
        RETURNING user_id
        """,
        (username,),
    )
    session["user_id"] = rows[0]["user_id"]
    session["username"] = username
    if request.is_json:
        return jsonify({"username": username, "user_id": session["user_id"]})
    return redirect(url_for("search_page"))


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    if request.is_json:
        return jsonify({"status": "ok"})
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return redirect(url_for("search_page") if _current_user() else url_for("login"))


@app.route("/search")
def search_page():
    if not _current_user():
        return redirect(url_for("login"))
    return render_template("search.html", **_nav())


@app.route("/filing/<accession>")
def filing_page(accession):
    if not _current_user():
        return redirect(url_for("login"))
    if not _ACCESSION_RE.match(accession):
        return render_template("filing.html", accession=None, error="Bad accession format.", **_nav())
    return render_template("filing.html", accession=accession, **_nav())


@app.route("/dashboard")
def dashboard_page():
    if not _current_user():
        return redirect(url_for("login"))
    return render_template("dashboard.html", **_nav())


@app.route("/assistant")
def assistant_page():
    if not _current_user():
        return redirect(url_for("login"))
    return render_template("assistant.html", **_nav())


@app.route("/workspace")
def workspace_page():
    if not _current_user():
        return redirect(url_for("login"))
    return render_template("workspace.html", **_nav())


def _nav():
    u = _current_user() or {}
    return {"username": u.get("username"), "catalog": warehouse.CATALOG, "schema": warehouse.SCHEMA}


# ---------------------------------------------------------------------------
# JSON API — reads (Gold/Silver Delta via warehouse)
# ---------------------------------------------------------------------------

@app.route("/api/companies")
def api_companies():
    """Every covered company + its AI health verdict — powers the landing grid."""
    _require_user()
    rows = warehouse.query(
        f"""
        SELECT c.cik, c.ticker, c.name, c.sic_description,
               (SELECT count(*) FROM {T('silver_filings')} f WHERE f.cik = c.cik) AS filing_count,
               h.overall_score, h.overall_label, h.direction,
               h.primary_strength, h.primary_risk, h.key_metric_next_quarter
        FROM {T('silver_companies')} c
        LEFT JOIN {T('gold_company_health')} h ON h.cik = c.cik
        ORDER BY h.overall_score DESC NULLS LAST, c.ticker
        """
    )
    return jsonify({"companies": rows})


@app.route("/api/search")
def api_search():
    _require_user()
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"companies": []})
    like = f"%{q}%"
    cik = q.zfill(10) if q.isdigit() else ""
    rows = warehouse.query(
        f"""
        SELECT c.cik, c.ticker, c.name, c.sic_description,
               (SELECT count(*) FROM {T('silver_filings')} f WHERE f.cik = c.cik) AS filing_count
        FROM {T('silver_companies')} c
        WHERE c.name ILIKE ? OR c.ticker ILIKE ? OR c.cik = ?
        ORDER BY c.name LIMIT 25
        """,
        [like, like, cik],
    )
    return jsonify({"companies": rows})


@app.route("/api/company/<cik>")
def api_company(cik):
    _require_user()
    if not _CIK_RE.match(cik):
        return jsonify({"error": "bad cik"}), 400
    cik = cik.zfill(10)
    company = warehouse.query(f"SELECT * FROM {T('silver_companies')} WHERE cik = ?", [cik])
    filings = warehouse.query(
        f"""
        SELECT accession, form, filing_date, report_date, primary_document, sec_url
        FROM {T('silver_filings')} WHERE cik = ? ORDER BY filing_date DESC LIMIT 50
        """,
        [cik],
    )
    metrics = warehouse.query(
        f"""
        SELECT fiscal_year, fiscal_period, period_end, revenue, net_income,
               operating_income, total_assets, eps_diluted
        FROM {T('gold_company_financials')} WHERE cik = ?
        ORDER BY period_end DESC LIMIT 8
        """,
        [cik],
    )
    return jsonify({"company": company[0] if company else None, "filings": filings, "metrics": metrics})


@app.route("/api/filing/<accession>")
def api_filing(accession):
    _require_user()
    if not _ACCESSION_RE.match(accession):
        return jsonify({"error": "bad accession"}), 400
    filing = warehouse.query(f"SELECT * FROM {T('silver_filings')} WHERE accession = ?", [accession])
    sections = warehouse.query(
        f"""
        SELECT section_index, section, heading, char_len
        FROM {T('silver_filing_sections')} WHERE accession = ? ORDER BY section_index
        """,
        [accession],
    )
    exhibits = warehouse.query(
        f"SELECT sequence, doc_type, filename, description FROM {T('silver_exhibits')} WHERE accession = ? ORDER BY sequence",
        [accession],
    )
    facts = warehouse.query(
        f"""
        SELECT concept, label, unit, period_start, period_end, value, fiscal_period
        FROM {T('silver_financial_facts')} WHERE accession = ?
          AND concept IN (
            'Revenues','RevenueFromContractWithCustomerExcludingAssessedTax',
            'CostOfRevenue','CostOfGoodsAndServicesSold','GrossProfit',
            'ResearchAndDevelopmentExpense','SellingGeneralAndAdministrativeExpense',
            'OperatingIncomeLoss','NetIncomeLoss','EarningsPerShareBasic',
            'EarningsPerShareDiluted','Assets','Liabilities','StockholdersEquity',
            'CashAndCashEquivalentsAtCarryingValue','LongTermDebtNoncurrent',
            'NetCashProvidedByUsedInOperatingActivities',
            'PaymentsToAcquirePropertyPlantAndEquipment')
        ORDER BY period_end DESC, concept
        LIMIT 40
        """,
        [accession],
    )
    try:
        intel = warehouse.query(
            f"""
            SELECT executive_summary, revenue_commentary, risk_themes,
                   management_tone, notable_items, model, generated_at
            FROM {T('gold_filing_intelligence')} WHERE accession = ?
            """,
            [accession],
        )
    except Exception:
        intel = []
    return jsonify(
        {
            "filing": filing[0] if filing else None,
            "intelligence": intel[0] if intel else None,
            "sections": sections,
            "exhibits": exhibits,
            "facts": facts,
        }
    )


@app.route("/api/health/<cik>")
def api_health(cik):
    _require_user()
    if not _CIK_RE.match(cik):
        return jsonify({"error": "bad cik"}), 400
    cik = cik.zfill(10)
    try:
        h = warehouse.query(
            f"SELECT * EXCEPT (raw) FROM {T('gold_company_health')} WHERE cik = ?", [cik]
        )
        ratios = warehouse.query(
            f"""
            SELECT fiscal_year, fiscal_period, period_end, revenue, revenue_growth_yoy,
                   gross_margin, operating_margin, net_margin, fcf, fcf_margin,
                   net_debt, return_on_equity, roic_approx,
                   operating_margin_trend, fcf_margin_trend, roic_approx_trend, net_debt_trend
            FROM {T('gold_financial_ratios')} WHERE cik = ? ORDER BY period_end DESC LIMIT 12
            """,
            [cik],
        )
    except Exception as exc:
        return jsonify({"health": None, "ratios": [], "note": f"run notebooks 09 + 10: {exc}"})
    return jsonify({"health": h[0] if h else None, "ratios": ratios})


@app.route("/api/intelligence")
def api_intelligence():
    _require_user()
    ticker = (request.args.get("ticker") or "").strip().upper()
    try:
        rows = warehouse.query(
            f"""
            SELECT accession, ticker, form, filing_date, management_tone,
                   executive_summary, risk_themes
            FROM {T('gold_filing_intelligence')}
            WHERE (? = '' OR upper(ticker) = ?)
            ORDER BY filing_date DESC LIMIT 40
            """,
            [ticker, ticker],
        )
    except Exception as exc:
        return jsonify({"briefings": [], "note": f"run notebook 08 first: {exc}"})
    return jsonify({"briefings": rows})


@app.route("/api/section/<accession>/<int:section_index>")
def api_section(accession, section_index):
    _require_user()
    if not _ACCESSION_RE.match(accession):
        return jsonify({"error": "bad accession"}), 400
    rows = warehouse.query(
        f"""
        SELECT section, heading, text FROM {T('silver_filing_sections')}
        WHERE accession = ? AND section_index = ?
        """,
        [accession, section_index],
    )
    return jsonify(rows[0] if rows else {})


@app.route("/api/dashboard/<cik>")
def api_dashboard(cik):
    _require_user()
    if not _CIK_RE.match(cik):
        return jsonify({"error": "bad cik"}), 400
    cik = cik.zfill(10)
    revenue = warehouse.query(
        f"""
        SELECT fiscal_year, fiscal_period, period_end, revenue, yoy_pct, qoq_pct
        FROM {T('gold_revenue_history')} WHERE cik = ? ORDER BY period_end
        """,
        [cik],
    )
    financials = warehouse.query(
        f"""
        SELECT fiscal_year, fiscal_period, period_end, revenue, gross_profit,
               operating_income, net_income, total_assets, total_liabilities,
               stockholders_equity, operating_cash_flow, eps_diluted
        FROM {T('gold_company_financials')} WHERE cik = ? ORDER BY period_end
        """,
        [cik],
    )
    return jsonify({"revenue": revenue, "financials": financials})


@app.route("/api/dashboard/activity")
def api_activity():
    _require_user()
    try:
        tool_stats = warehouse.query(
            f"SELECT tool_name, call_count, success_rate FROM {T('gold_agent_tool_stats')} ORDER BY call_count DESC"
        )
        events = warehouse.query(
            f"SELECT event_type, event_count, users FROM {T('gold_usage_funnel')} ORDER BY event_count DESC"
        )
    except Exception as exc:
        return jsonify({"tool_stats": [], "events": [], "note": f"analytics job hasn't run yet: {exc}"})
    return jsonify({"tool_stats": tool_stats, "events": events})


@app.route("/api/watchlist")
def api_watchlist():
    u = _require_user()
    rows = lakebase.run_query(
        """
        SELECT wc.cik, wc.ticker, wc.added_at, w.name AS watchlist
        FROM edgar.watchlist_companies wc
        JOIN edgar.watchlists w ON w.watchlist_id = wc.watchlist_id
        WHERE w.user_id = %s ORDER BY wc.added_at DESC
        """,
        (u["user_id"],),
    )
    return jsonify({"watchlist": rows})


@app.route("/api/saved")
def api_saved():
    u = _require_user()
    filings = lakebase.run_query(
        "SELECT filing_id, company_cik, form, filed_at, note, created_at FROM edgar.saved_filings WHERE user_id = %s ORDER BY created_at DESC",
        (u["user_id"],),
    )
    research = lakebase.run_query(
        "SELECT research_id, title, company_cik, filing_id, notes, updated_at FROM edgar.saved_research WHERE user_id = %s ORDER BY updated_at DESC",
        (u["user_id"],),
    )
    return jsonify({"filings": filings, "research": research})


# ---------------------------------------------------------------------------
# JSON API — AI Research Assistant (in-process agent)
# ---------------------------------------------------------------------------

@app.route("/api/assistant/message", methods=["POST"])
def api_assistant_message():
    u = _require_user()
    body = request.get_json(silent=True) or {}
    message = (body.get("message") or "").strip()
    history = body.get("history") or []
    conversation_id = body.get("conversation_id")
    if not message:
        return jsonify({"error": "message is required"}), 400

    if not conversation_id:
        rows = lakebase.run_write(
            "INSERT INTO edgar.agent_conversations (user_id, title) VALUES (%s, %s) RETURNING conversation_id",
            (u["user_id"], message[:60]),
        )
        conversation_id = rows[0]["conversation_id"]

    from agent.graph import run_agent
    from agent.tools import ToolContext

    ctx = ToolContext(
        user_id=u["user_id"],
        conversation_id=conversation_id,
        vs_endpoint=VS_ENDPOINT,
        vs_index=VS_INDEX,
        embedding_endpoint=EMBEDDING_ENDPOINT,
    )
    turn = history + [{"role": "user", "content": message}]
    logger.info("assistant conv=%s prompt=%r", conversation_id, message[:200])
    try:
        result = run_agent(turn, ctx)
    except Exception:
        logger.exception("run_agent failed conv=%s", conversation_id)
        raise
    logger.info(
        "assistant conv=%s tools=%s confidence=%s reply=%r",
        conversation_id, result.get("tool_calls"), result.get("confidence"),
        (result.get("reply") or "")[:200],
    )

    # log the final answer + its self-reported confidence (feeds analytics)
    import json as _json

    lakebase.run_write(
        """
        INSERT INTO edgar.agent_actions
            (conversation_id, user_id, tool_name, tool_kind, args_json, status, confidence, result_json)
        VALUES (%(conv)s, %(uid)s, 'final_answer', 'answer', %(args)s::jsonb, 'SUCCESS', %(conf)s, %(res)s::jsonb)
        """,
        {
            "conv": conversation_id,
            "uid": u["user_id"],
            "args": _json.dumps({"message": message}),
            "conf": result.get("confidence"),
            "res": _json.dumps(
                {"tool_calls": result["tool_calls"], "confidence_reason": result.get("confidence_reason")}
            ),
        },
    )
    lakebase.run_write(
        """
        UPDATE edgar.agent_conversations
           SET last_message_at = now(), message_count = message_count + 2
         WHERE conversation_id = %s
        """,
        (conversation_id,),
    )
    return jsonify(
        {
            "conversation_id": conversation_id,
            "reply": result["reply"],
            "confidence": result.get("confidence"),
            "confidence_reason": result.get("confidence_reason"),
            "tool_calls": result["tool_calls"],
        }
    )


if __name__ == "__main__":
    app.run(
        debug=bool(os.environ.get("FLASK_DEBUG")),
        host=os.environ.get("FLASK_RUN_HOST", "0.0.0.0"),
        port=int(os.environ.get("FLASK_RUN_PORT", "8000")),
    )
