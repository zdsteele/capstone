"""
LangGraph tool-calling agent — the ReAct loop from
``ai-agents-2026/day3/04_langgraph_agent.ipynb`` (`build_agent`):

    agent (LLM + bound tools) --should_continue--> tools (ToolNode) --> agent --> END

Runs in-process inside the Flask app. ``run_agent`` executes one user turn given
the full message history and a :class:`ToolContext`, and returns the assistant's
final text plus the list of tool calls made (for the UI / action log).
"""

from __future__ import annotations

import os
import re
from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agent.prompt import SYSTEM_PROMPT
from agent.tools import ToolContext, build_tools

LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")


class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]


def _llm():
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)


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


def run_agent(history: list[dict], ctx: ToolContext) -> dict:
    """`history` is a list of ``{"role": "user"|"assistant", "content": str}``.
    Returns ``{"reply", "confidence", "confidence_reason", "tool_calls", "steps"}``."""
    try:
        import mlflow

        mlflow.langchain.autolog()
    except Exception:
        pass

    graph = build_graph(ctx)

    msgs: list[AnyMessage] = []
    for m in history:
        if m["role"] == "user":
            msgs.append(HumanMessage(content=m["content"]))
        else:
            msgs.append(AIMessage(content=m["content"]))

    result = graph.invoke({"messages": msgs}, config={"recursion_limit": 25})
    out_msgs = result["messages"]

    tool_calls: list[str] = []
    for m in out_msgs:
        for tc in getattr(m, "tool_calls", None) or []:
            tool_calls.append(tc["name"])

    reply = ""
    for m in reversed(out_msgs):
        if isinstance(m, AIMessage) and not (getattr(m, "tool_calls", None)):
            reply = m.content if isinstance(m.content, str) else str(m.content)
            break

    reply, confidence, confidence_reason = _split_confidence(reply)
    return {
        "reply": reply,
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "tool_calls": tool_calls,
        "steps": len(out_msgs),
    }
