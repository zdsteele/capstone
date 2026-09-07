"""
Offline eval harness for the SEC Research Assistant agent.

Runs every case in ``cases.jsonl`` through ``agent.graph.run_agent`` and scores:

  * tool use        — expect_tools_any / expect_tools_all appear in the calls made
  * must_include_*   — required substrings are present in the reply
  * must_not         — banned substrings (advice language, memorized facts) absent
  * confidence       — the "Confidence: <level>" line was emitted when required
  * judge (optional) — a separate LLM grades the answer against the case rubric

Usage (from the capstone/ dir, with the venv active):

    python -m agent.eval.run_eval                       # full suite, current model
    python -m agent.eval.run_eval --model databricks-claude-sonnet-5
    python -m agent.eval.run_eval --only health_basic,buy_redirect
    python -m agent.eval.run_eval --no-judge            # deterministic checks only
    python -m agent.eval.run_eval --compare databricks-meta-llama-3-3-70b-instruct,databricks-claude-sonnet-5

Writes agent/eval/report.json (and report_<model>.json per model in --compare).
Loads capstone/.env if present so warehouse + Lakebase are reachable locally.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:  # Windows consoles default to cp1252 and choke on report glyphs
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HERE = Path(__file__).resolve()
_CAPSTONE = _HERE.parents[2]
if str(_CAPSTONE) not in sys.path:
    sys.path.insert(0, str(_CAPSTONE))

# --- env bootstrap --------------------------------------------------------
try:
    from dotenv import load_dotenv

    load_dotenv(_CAPSTONE / ".env")
except Exception:
    pass

os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "edgar")
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "b15d3d6f837ba428")
os.environ.setdefault("UC_CATALOG", "bootcamp_students")
os.environ.setdefault("UC_SCHEMA", "zdsteele_capstone")
os.environ.setdefault("VS_ENDPOINT", "zachy_vs")
os.environ.setdefault("VS_INDEX", "bootcamp_students.zdsteele_capstone.filing_text_index")
os.environ.setdefault("EMBEDDING_ENDPOINT", "databricks-gte-large-en")
os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

CASES_FILE = _HERE.parent / "cases.jsonl"
HAS_LAKEBASE = bool(os.environ.get("LAKEBASE_URL") or os.environ.get("PGHOST"))
JUDGE_MODEL_DEFAULT = "databricks-claude-opus-5"


def load_cases(only: set[str] | None) -> list[dict]:
    cases = []
    for line in CASES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        c = json.loads(line)
        if only and c["id"] not in only:
            continue
        cases.append(c)
    return cases


def _contains_any(text: str, needles) -> bool:
    t = text.lower()
    return any(n.lower() in t for n in needles)


def _judge(case: dict, reply: str, judge_model: str) -> tuple[bool, str]:
    """Grade the reply against case['judge'] with a separate LLM. PASS/FAIL + reason."""
    from agent.graph import _make_chat, _temperature_for

    chat = _make_chat(judge_model, _temperature_for(judge_model))
    rubric = case["judge"]
    prompt = (
        "You are grading an equity-research assistant's answer. Be strict but fair.\n\n"
        f"USER QUESTION:\n{case['q']}\n\n"
        f"GRADING CRITERION (the answer must satisfy this):\n{rubric}\n\n"
        f"ASSISTANT ANSWER:\n{reply}\n\n"
        "Reply with exactly one line: `PASS - <short reason>` or `FAIL - <short reason>`."
    )
    try:
        out = chat.invoke(prompt)
        from agent.graph import _text_of

        line = _text_of(out.content).strip().splitlines()[0]
    except Exception as exc:
        return False, f"judge error: {exc}"
    verdict = line.strip().upper().startswith("PASS")
    return verdict, line.strip()


def run_case(case: dict, ctx, judge_model: str | None) -> dict:
    from agent.graph import run_agent

    if case.get("requires_lakebase") and not HAS_LAKEBASE:
        return {"id": case["id"], "skipped": "no Lakebase (set LAKEBASE_URL)"}

    t0 = time.time()
    try:
        r = run_agent([{"role": "user", "content": case["q"]}], ctx)
    except Exception as exc:
        return {"id": case["id"], "error": repr(exc), "secs": round(time.time() - t0, 1)}

    reply = r.get("reply") or ""
    tools = set(r.get("tool_calls") or [])
    checks: dict[str, bool] = {}

    if case.get("expect_tools_any"):
        checks["tools_any"] = bool(tools & set(case["expect_tools_any"]))
    if case.get("expect_tools_all"):
        checks["tools_all"] = set(case["expect_tools_all"]).issubset(tools)
    if case.get("must_include_any"):
        checks["include_any"] = _contains_any(reply, case["must_include_any"])
    if case.get("must_include_all"):
        checks["include_all"] = all(n.lower() in reply.lower() for n in case["must_include_all"])
    if case.get("must_not"):
        checks["must_not"] = not _contains_any(reply, case["must_not"])
    if case.get("confidence_required"):
        checks["confidence"] = (r.get("confidence") in ("high", "medium", "low"))

    judge_line = None
    if case.get("judge") and judge_model:
        jok, judge_line = _judge(case, reply, judge_model)
        checks["judge"] = jok

    passed = all(checks.values()) if checks else True
    return {
        "id": case["id"],
        "passed": passed,
        "checks": checks,
        "tools": sorted(tools),
        "confidence": r.get("confidence"),
        "judge": judge_line,
        "secs": round(time.time() - t0, 1),
        "reply_head": reply[:280].replace("\n", " "),
        "reply_len": len(reply),
    }


def run_suite(model: str, cases: list[dict], judge_model: str | None) -> dict:
    os.environ["LLM_ENDPOINT"] = model
    for mod in list(sys.modules):
        if mod.startswith("agent."):
            del sys.modules[mod]
    from agent.tools import ToolContext

    ctx = ToolContext(
        user_id=int(os.environ.get("EVAL_USER_ID", "1")),
        conversation_id=None,
        vs_endpoint=os.environ["VS_ENDPOINT"],
        vs_index=os.environ["VS_INDEX"],
        embedding_endpoint=os.environ["EMBEDDING_ENDPOINT"],
    )

    print(f"\n{'=' * 78}\nMODEL: {model}   ({len(cases)} cases, judge={judge_model or 'off'})\n{'=' * 78}")
    results = []
    for c in cases:
        res = run_case(c, ctx, judge_model)
        results.append(res)
        if "skipped" in res:
            print(f"  SKIP  {res['id']:<26} ({res['skipped']})")
        elif "error" in res:
            print(f"  ERR   {res['id']:<26} {res['error'][:90]} ({res['secs']}s)")
        else:
            mark = "PASS " if res["passed"] else "FAIL "
            fails = [k for k, v in res["checks"].items() if not v]
            print(f"  {mark} {res['id']:<26} {res['secs']:>5}s  "
                  f"tools={res['tools']}  conf={res['confidence']}"
                  + (f"  failed:{fails}" if fails else ""))
            if res["judge"]:
                print(f"        judge: {res['judge'][:150]}")

    graded = [r for r in results if "passed" in r]
    n_pass = sum(1 for r in graded if r["passed"])
    summary = {
        "model": model,
        "judge_model": judge_model,
        "total": len(cases),
        "graded": len(graded),
        "passed": n_pass,
        "failed": len(graded) - n_pass,
        "skipped": sum(1 for r in results if "skipped" in r),
        "errors": sum(1 for r in results if "error" in r),
        "pass_rate": round(n_pass / len(graded), 3) if graded else None,
        "avg_secs": round(sum(r["secs"] for r in graded) / len(graded), 1) if graded else None,
        "results": results,
    }
    print(f"\n  -> {n_pass}/{len(graded)} passed "
          f"({summary['pass_rate']}), {summary['skipped']} skipped, {summary['errors']} errors, "
          f"avg {summary['avg_secs']}s/case")
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-5"))
    ap.add_argument("--compare", help="comma-separated endpoints to run head-to-head")
    ap.add_argument("--only", help="comma-separated case ids")
    ap.add_argument("--no-judge", action="store_true")
    ap.add_argument("--judge-model", default=JUDGE_MODEL_DEFAULT)
    ap.add_argument("--out", default=str(_HERE.parent / "report.json"))
    args = ap.parse_args()

    only = set(x.strip() for x in args.only.split(",")) if args.only else None
    cases = load_cases(only)
    judge_model = None if args.no_judge else args.judge_model
    models = [m.strip() for m in args.compare.split(",")] if args.compare else [args.model]

    all_summaries = []
    for model in models:
        s = run_suite(model, cases, judge_model)
        all_summaries.append(s)
        out = args.out if len(models) == 1 else str(
            _HERE.parent / f"report_{model.split('/')[-1]}.json")
        Path(out).write_text(json.dumps(s, indent=2), encoding="utf-8")
        print(f"  wrote {out}")

    if len(all_summaries) > 1:
        print(f"\n{'=' * 78}\nHEAD-TO-HEAD\n{'=' * 78}")
        for s in all_summaries:
            print(f"  {s['model']:<44} {s['passed']}/{s['graded']}  "
                  f"rate={s['pass_rate']}  avg={s['avg_secs']}s")

    worst = min(all_summaries, key=lambda s: s["pass_rate"] or 0)
    sys.exit(0 if (worst["pass_rate"] or 0) >= 0.8 else 1)


if __name__ == "__main__":
    main()
