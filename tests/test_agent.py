"""Plain-assert tests for the agent's pure helpers (no network).

    python tests/test_agent.py     (or: pytest tests/test_agent.py)

Guards the Claude-compatibility fixes in agent/graph.py: response-shape
flattening, temperature gating, and the trailing Confidence line parser.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.graph import _split_confidence, _temperature_for, _text_of


def test_text_of_plain_string():
    assert _text_of("hello world") == "hello world"
    assert _text_of("") == ""
    assert _text_of(None) == ""


def test_text_of_claude_block_list():
    # Claude / gpt-5 return content as a list of blocks; keep only the answer text
    content = [
        {"type": "reasoning", "summary": [{"type": "summary_text", "text": "", "signature": "x"}]},
        {"type": "text", "text": "The answer"},
        {"type": "text", "text": " continues."},
    ]
    assert _text_of(content) == "The answer\n continues."


def test_text_of_drops_thinking_blocks():
    content = [
        {"type": "thinking", "thinking": "internal chain of thought"},
        {"type": "text", "text": "Final."},
    ]
    assert _text_of(content) == "Final."
    # nothing but thinking -> empty
    assert _text_of([{"type": "thinking", "thinking": "..."}]) == ""


def test_text_of_string_blocks():
    assert _text_of(["a", "b"]) == "a\nb"


def test_temperature_for_families():
    os.environ.pop("LLM_TEMPERATURE", None)
    assert _temperature_for("databricks-meta-llama-3-3-70b-instruct") == 0.1
    assert _temperature_for("databricks-claude-sonnet-5") is None
    assert _temperature_for("databricks-claude-opus-5") is None
    assert _temperature_for("databricks-gpt-5") is None
    assert _temperature_for("databricks-gpt-5-mini") is None


def test_temperature_for_env_override():
    os.environ["LLM_TEMPERATURE"] = ""
    try:
        assert _temperature_for("databricks-meta-llama-3-3-70b-instruct") is None
        os.environ["LLM_TEMPERATURE"] = "0.7"
        assert _temperature_for("databricks-claude-sonnet-5") == 0.7
    finally:
        os.environ.pop("LLM_TEMPERATURE", None)


def test_split_confidence():
    body, level, why = _split_confidence(
        "Apple is healthy.\n\nConfidence: high - every figure came from a tool result."
    )
    assert level == "high"
    assert "every figure" in why
    assert "Confidence:" not in body
    assert body.strip() == "Apple is healthy."


def test_split_confidence_absent():
    body, level, why = _split_confidence("no confidence line here")
    assert level is None and why is None
    assert body == "no confidence line here"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
