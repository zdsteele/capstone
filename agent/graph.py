"""
LangGraph tool-calling agent — the ReAct loop from
``ai-agents-2026/day3/04_langgraph_agent.ipynb`` (`build_agent`):

    agent (LLM + bound tools) --should_continue--> tools (ToolNode) --> agent --> END

Runs in-process inside the Flask app. ``run_agent`` executes one user turn given
the full message history and a :class:`ToolContext`, and returns the assistant's
final text plus the list of tool calls made (for the UI / action log).
"""

from __future__ import annotations

import json
import os
import re
from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import (
    AIMessage, AnyMessage, HumanMessage, SystemMessage, ToolMessage,
)
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agent.prompt import SYSTEM_PROMPT
from agent.tools import ToolContext, build_tools

LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-5")

# Some served models (Anthropic Claude, OpenAI gpt-5*) reject an explicit
# `temperature` on the Databricks endpoint. Send it only where it's accepted.
# Override with LLM_TEMPERATURE="" to force-omit, or a float to force-send.
_NO_TEMPERATURE = ("claude", "gpt-5", "gpt-6", "o1", "o3")


def _temperature_for(endpoint: str):
    env = os.environ.get("LLM_TEMPERATURE")
    if env is not None:
        return float(env) if env.strip() != "" else None
    low = endpoint.lower()
    if any(tok in low for tok in _NO_TEMPERATURE):
        return None
    return 0.1


def _is_claude(endpoint: str) -> bool:
    return "claude" in endpoint.lower()


class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]


def _make_chat(endpoint: str, temperature):
    """ChatDatabricks with two fixes for the pinned 0.4.0 line + Claude endpoints:

    * `temperature` (and `n`) are stripped from the payload when `temperature is
      None` — Claude / gpt-5 endpoints 400 on an explicit temperature and 0.4.0
      hardcodes it into `_prepare_inputs`.
    * Claude endpoints get `thinking: {type: disabled}`. Extended thinking is on
      by default on databricks-claude-* and (a) langchain 0.3.x can't stream its
      block format and (b) the thinking block round-trips broken through a
      multi-turn tool loop → `400 each thinking block must contain thinking`.
      A tool-routing agent doesn't need it. Override: LLM_THINKING=adaptive|off.
    """
    from databricks_langchain import ChatDatabricks

    omit_sampling = temperature is None
    extra: dict = {}
    if _is_claude(endpoint) and os.environ.get("LLM_THINKING", "off").lower() != "adaptive":
        extra["thinking"] = {"type": "disabled"}

    class _Chat(ChatDatabricks):
        def _prepare_inputs(self, messages, stop=None, **kwargs):
            data = super()._prepare_inputs(messages, stop, **kwargs)
            if omit_sampling:
                data.pop("temperature", None)
                data.pop("n", None)
            return data

    kwargs = {"endpoint": endpoint}
    if temperature is not None:
        kwargs["temperature"] = temperature
    if extra:
        kwargs["extra_params"] = extra
    return _Chat(**kwargs)


def _llm():
    """LLM via ChatDatabricks (databricks-langchain, pinned) — it parses the
    Databricks tool-call format correctly, which raw ChatOpenAI against the
    endpoint does not (Llama emits `<function=...>` text instead of tool_calls)."""
    return _make_chat(LLM_ENDPOINT, _temperature_for(LLM_ENDPOINT))


def build_graph(ctx: ToolContext):
    tools = build_tools(ctx)
    model = _llm().bind_tools(tools)

    def _preprocess(state: AgentState):
        return [SystemMessage(content=SYSTEM_PROMPT), *state["messages"]]

    preprocessor = RunnableLambda(_preprocess)

    def call_model(state: AgentState, config: RunnableConfig):
        return {"messages": [(preprocessor | model).invoke(state, config)]}

    def should_continue(state: AgentState):
        last = state["messages"][-1]
        return "continue" if isinstance(last, AIMessage) and last.tool_calls else "end"

    g = StateGraph(AgentState)
    g.add_node("agent", RunnableLambda(call_model))
    g.add_node("tools", ToolNode(tools))
    g.set_entry_point("agent")
    g.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
    g.add_edge("tools", "agent")
    return g.compile()


def _text_of(content) -> str:
    """Flatten an LLM message's `content` to plain text.

    ChatDatabricks returns a str for Llama, but Claude / gpt-5 endpoints return a
    list of content blocks — `{"type": "text", "text": ...}` for the answer and
    `{"type": "reasoning"|"thinking", ...}` for chain-of-thought. Keep only the
    answer text; drop reasoning blocks (and never `str()` the whole list)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                btype = block.get("type")
                if btype in ("reasoning", "thinking", "redacted_thinking"):
                    continue
                txt = block.get("text")
                if isinstance(txt, str) and txt:
                    parts.append(txt)
        return "\n".join(p for p in parts if p).strip()
    return str(content or "")


_CONF_RE = re.compile(r"confidence:\s*(high|medium|low)\b[^\n]*", re.IGNORECASE)


def _split_confidence(reply: str) -> tuple[str, str | None, str | None]:
    """Pull the trailing 'Confidence: <level> - <why>' line out of the reply.
    Returns (reply_without_line, level, reason)."""
    m = _CONF_RE.search(reply or "")
    if not m:
        return reply, None, None
    line = m.group(0)
    level = m.group(1).lower()
    reason = None
    if "-" in line:
        reason = line.split("-", 1)[1].strip() or None
    cleaned = (reply[: m.start()] + reply[m.end():]).strip()
    return cleaned, level, reason


_WRITE_TOOLS = {
    "save_filing", "save_company_to_watchlist", "create_research_note",
    "update_research_note", "remove_from_watchlist",
}


def _extract_sources(out_msgs) -> list[dict]:
    """Pull the filings the retrieval tools actually returned, so the UI can show
    'sources'. Groups sections seen per accession."""
    by_acc: dict[str, dict] = {}
    for m in out_msgs:
        if not isinstance(m, ToolMessage) or (m.name or "") in _WRITE_TOOLS:
            continue
        content = m.content if isinstance(m.content, str) else str(m.content)
        try:
            data = json.loads(content)
        except Exception:
            continue
        rows = data if isinstance(data, list) else [data]
        for r in rows:
            if not isinstance(r, dict) or "_error" in r:
                continue
            acc = r.get("accession")
            if not acc and isinstance(r.get("filing"), dict):
                acc = r["filing"].get("accession")
            if not acc:
                continue
            e = by_acc.setdefault(acc, {
                "accession": acc,
                "ticker": r.get("ticker"),
                "form": r.get("form"),
                "filing_date": str(r.get("filing_date") or r.get("filed_at") or "")[:10] or None,
                "sections": [],
            })
            for k in ("ticker", "form", "filing_date"):
                if not e.get(k) and r.get(k):
                    e[k] = str(r[k])[:10] if k == "filing_date" else r[k]
            sec = r.get("section")
            if sec and sec not in e["sections"]:
                e["sections"].append(sec)
    return list(by_acc.values())


def _enable_mlflow():
    try:
        import mlflow

        mlflow.langchain.autolog()
    except Exception:
        pass


def _history_to_messages(history: list[dict]) -> list[AnyMessage]:
    msgs: list[AnyMessage] = []
    for m in history:
        if m["role"] == "user":
            msgs.append(HumanMessage(content=m["content"]))
        else:
            msgs.append(AIMessage(content=m["content"]))
    return msgs


def _finalize(out_msgs) -> dict:
    """Turn the final message list into the response dict the app/UI expects."""
    tool_calls = [
        tc["name"] for m in out_msgs for tc in getattr(m, "tool_calls", None) or []
    ]
    reply = ""
    for m in reversed(out_msgs):
        if isinstance(m, AIMessage) and not (getattr(m, "tool_calls", None)):
            reply = _text_of(m.content)
            break
    reply, confidence, confidence_reason = _split_confidence(reply)
    return {
        "reply": reply,
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "tool_calls": tool_calls,
        "sources": _extract_sources(out_msgs),
        "steps": len(out_msgs),
    }


def run_agent(history: list[dict], ctx: ToolContext) -> dict:
    """`history` is a list of ``{"role": "user"|"assistant", "content": str}``.
    Returns ``{reply, confidence, confidence_reason, tool_calls, sources, steps}``."""
    _enable_mlflow()
    graph = build_graph(ctx)
    result = graph.invoke(
        {"messages": _history_to_messages(history)}, config={"recursion_limit": 25}
    )
    return _finalize(result["messages"])


def run_agent_stream(history: list[dict], ctx: ToolContext):
    """Generator version of :func:`run_agent`. Yields event dicts:

      {"type": "tool_start", "names": [...]}   agent decided to call tools
      {"type": "tool_end",   "name": "..."}    one tool returned
      {"type": "token",      "text": "..."}    a piece of the answer (preview)
      {"type": "done",       **finalize dict}   authoritative final result
      {"type": "error",      "message": "..."}

    Streamed tokens are a live preview (they include any pre-tool chatter); the
    UI should treat ``done.reply`` as the source of truth and re-render it."""
    _enable_mlflow()
    graph = build_graph(ctx)
    inputs = {"messages": _history_to_messages(history)}
    cfg = {"recursion_limit": 25}

    final_msgs = None
    try:
        for mode, chunk in graph.stream(
            inputs, config=cfg, stream_mode=["updates", "messages", "values"]
        ):
            if mode == "values":
                final_msgs = chunk.get("messages", final_msgs)
            elif mode == "updates":
                for node, delta in (chunk or {}).items():
                    msgs = (delta or {}).get("messages", []) if isinstance(delta, dict) else []
                    if node == "agent":
                        for m in msgs:
                            names = [tc["name"] for tc in getattr(m, "tool_calls", None) or []]
                            if names:
                                yield {"type": "tool_start", "names": names}
                    elif node == "tools":
                        for m in msgs:
                            if isinstance(m, ToolMessage):
                                yield {"type": "tool_end", "name": m.name}
            elif mode == "messages":
                if isinstance(chunk, (list, tuple)) and len(chunk) == 2:
                    msg_chunk, meta = chunk
                else:
                    msg_chunk, meta = chunk, {}
                # `messages` mode also carries ToolMessage payloads (the raw tool
                # JSON) — those are not answer tokens.
                if isinstance(msg_chunk, ToolMessage):
                    continue
                if (meta or {}).get("langgraph_node") not in (None, "agent"):
                    continue
                txt = _text_of(getattr(msg_chunk, "content", ""))
                if txt:
                    yield {"type": "token", "text": txt}
    except Exception as exc:  # pragma: no cover - network/timeout
        yield {"type": "error", "message": str(exc)}
        return

    if final_msgs is None:
        yield {"type": "error", "message": "agent produced no output"}
        return
    yield {"type": "done", **_finalize(final_msgs)}
