# Research-assistant eval

Offline regression + quality suite for the in-process agent (`agent/graph.py`).
Run it after any change to the model, prompt, tools, or graph.

## Run

From `capstone/` with the venv active (it loads `capstone/.env` for warehouse +
Lakebase access):

```bash
python -m agent.eval.run_eval                     # full suite, current model, LLM judge on
python -m agent.eval.run_eval --no-judge          # deterministic checks only (fast, free)
python -m agent.eval.run_eval --only health_basic,buy_redirect
python -m agent.eval.run_eval --model databricks-claude-sonnet-5
python -m agent.eval.run_eval --compare databricks-meta-llama-3-3-70b-instruct,databricks-claude-sonnet-5
```

Exit code is 0 when the (worst) model's pass rate is ≥ 0.80, else 1 — usable in CI.
Writes `report.json` (or `report_<model>.json` per model under `--compare`).

## What each case checks

`cases.jsonl`, one JSON object per line:

| field | meaning |
|---|---|
| `q` | the user turn |
| `expect_tools_any` / `expect_tools_all` | tool names that must appear in the calls made |
| `must_include_any` / `must_include_all` | substrings that must be in the reply |
| `must_not` | substrings that must **not** appear (advice language, memorized facts) |
| `confidence_required` | the `Confidence: <high\|medium\|low>` line must be parsed out |
| `judge` | rubric a separate LLM (`--judge-model`, default `databricks-claude-opus-5`) grades PASS/FAIL |
| `requires_lakebase` | skipped unless `LAKEBASE_URL` / `PGHOST` is set (write-tool cases) |

A case passes only if **every** applicable check passes.

## Adding cases

Append a line to `cases.jsonl`. Keep `must_not` lists tight (real failure
phrases, not anything that could appear innocently). Prefer a `judge` rubric for
anything about answer *quality*; use the deterministic checks for tool routing,
grounding guardrails, and the confidence line.
