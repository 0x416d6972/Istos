"""Agent loop — plan → mesh tool → observe, until the model stops.

Call from a ``@channel`` (or a ``@worker``) so the agent is a service on a key
expression and its tools are other services on the fabric::

    tools = tools_from_handlers(app, prefixes=["math/add"])
    model = OpenAIChatModel(base_url="http://127.0.0.1:1234/v1", model="…")

    @app.channel("agent/chat", durable=True)
    async def chat(s: ChannelSession):
        await drive_channel(s, model, tools, system="You use tools when needed.")
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, Union

from istos.agent.model import Model, ModelReply, ToolCall
from istos.agent.tools import (
    MeshTool,
    format_tool_error,
    format_tool_result,
)
from istos.logging import get_logger
from istos.observability.tracing import set_span_attributes, span
from istos.primitives.channel import ChannelSession

_logger = get_logger("agent.loop")


@dataclass(frozen=True)
class AgentEvent:
    """One step the loop emits for the caller to forward (e.g. over a channel).

    ``kind`` is one of:

    - ``message`` — final assistant text for this turn
    - ``tool_call`` — model asked to run a mesh tool (``name``, ``arguments``)
    - ``tool_result`` — tool returned (``content``); ``error`` when it raised
    - ``handoff`` — active agent transferred to ``name`` (multi-agent loop)
    - ``done`` — turn finished (no more steps)
    """

    kind: str
    content: Any = None
    name: Optional[str] = None
    arguments: Optional[Dict[str, Any]] = None
    tool_call_id: Optional[str] = None
    error: bool = False


def user_text(msg: Any) -> str:
    """Pull a user string out of a channel message (str or common dict shapes)."""
    if isinstance(msg, str):
        return msg
    if isinstance(msg, dict):
        for key in ("text", "content", "message", "prompt"):
            val = msg.get(key)
            if isinstance(val, str) and val:
                return val
        return json.dumps(msg)
    return str(msg)


def _tool_call_message(frame: dict) -> dict:
    """Rebuild the assistant ``tool_calls`` message from a persisted frame."""
    return {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": frame.get("tool_call_id"),
                "type": "function",
                "function": {
                    "name": frame.get("name"),
                    "arguments": json.dumps(frame.get("arguments") or {}),
                },
            }
        ],
    }


def history_to_messages(
    history: Sequence[dict],
    *,
    system: Optional[str] = None,
    include_tools: bool = True,
) -> List[dict]:
    """Map a durable channel log (``[{dir, data, ts}, …]``) into chat messages.

    With ``include_tools`` (the default) the tool transcript is reconstructed:
    each persisted ``tool_call`` frame becomes an assistant ``tool_calls`` message
    followed by the ``tool`` message carrying its result, so a reconnecting agent
    sees the same context it had live. A ``tool_call`` with no matching
    ``tool_result`` in the log (a crash mid-tool) is dropped so the sequence stays
    valid. Set ``include_tools=False`` to rebuild plain text only.
    """
    results_by_id: Dict[str, dict] = {}
    if include_tools:
        for turn in history:
            if turn.get("dir") != "out":
                continue
            data = turn.get("data")
            if isinstance(data, dict) and data.get("kind") == "tool_result":
                tid = data.get("tool_call_id")
                if tid is not None:
                    results_by_id[tid] = data

    messages: List[dict] = []
    if system:
        messages.append({"role": "system", "content": system})
    for turn in history:
        data = turn.get("data")
        direction = turn.get("dir")
        if direction == "in":
            messages.append({"role": "user", "content": user_text(data)})
        elif direction == "out":
            if not isinstance(data, dict):
                if user_text(data):
                    messages.append({"role": "assistant", "content": user_text(data)})
                continue
            kind = data.get("kind")
            if kind == "tool_call":
                if not include_tools:
                    continue
                tid = data.get("tool_call_id")
                result = results_by_id.get(tid) if isinstance(tid, str) else None
                if result is None:
                    continue
                messages.append(_tool_call_message(data))
                messages.append({
                    "role": "tool",
                    "tool_call_id": tid,
                    "content": str(result.get("content") or ""),
                })
                continue
            if kind in ("tool_result", "done", "handoff"):
                continue
            text = data.get("content") if kind == "message" else user_text(data)
            if text:
                messages.append({"role": "assistant", "content": str(text)})
    return messages


def _assistant_message(reply: ModelReply) -> dict:
    msg: Dict[str, Any] = {
        "role": "assistant",
        "content": reply.content,
    }
    if reply.tool_calls:
        msg["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.name,
                    "arguments": json.dumps(tc.arguments),
                },
            }
            for tc in reply.tool_calls
        ]
    return msg


def _by_name(tools: Sequence[MeshTool]) -> Dict[str, MeshTool]:
    return {t.name: t for t in tools}


def _completion_attrs(reply: ModelReply) -> Dict[str, Any]:
    """GenAI + Istos span attributes for one model turn (``None`` values dropped
    by the tracing layer when a custom model leaves telemetry unset)."""
    usage = reply.usage or {}
    return {
        "istos.agent.tool_calls": len(reply.tool_calls),
        "istos.agent.has_content": bool(reply.content),
        "gen_ai.response.model": reply.model,
        "gen_ai.response.finish_reasons": [reply.finish_reason] if reply.finish_reason else None,
        "gen_ai.usage.input_tokens": usage.get("prompt_tokens"),
        "gen_ai.usage.output_tokens": usage.get("completion_tokens"),
    }


def _trim_messages(messages: List[dict], max_messages: int) -> None:
    """Bound a growing chat log in place, keeping a valid message sequence.

    Leading ``system`` messages are always kept; from the rest only the most
    recent ``max_messages`` are retained. A ``tool`` message is only valid after
    the assistant ``tool_calls`` it answers, so any left orphaned at the front of
    the retained tail are dropped — the window never begins mid tool-call.
    """
    if max_messages < 1:
        raise ValueError("max_messages must be >= 1")

    head = 0
    while head < len(messages) and messages[head].get("role") == "system":
        head += 1
    system = messages[:head]
    body = messages[head:]
    if len(body) <= max_messages:
        return

    tail = body[-max_messages:]
    while tail and tail[0].get("role") == "tool":
        tail.pop(0)
    messages[:] = system + tail


async def run_agent(
    model: Model,
    tools: Sequence[MeshTool],
    messages: List[dict],
    *,
    max_steps: int = 8,
    max_messages: Optional[int] = None,
    token: Optional[Union[bytes, str]] = None,
    timeout_s: float = 5.0,
) -> AsyncIterator[AgentEvent]:
    """Run plan → tool → observe until the model returns text or ``max_steps``.

    Mutates ``messages`` in place so the caller can keep a multi-turn
    conversation. Each mesh tool call forwards ``token`` on ``query_once``. Pass
    ``max_messages`` to bound the log before each completion (system prompt kept).
    """
    if max_steps < 1:
        raise ValueError("max_steps must be >= 1")

    catalog = _by_name(tools)
    schemas = [t.openai_schema() for t in tools] or None

    for step in range(max_steps):
        if max_messages is not None:
            _trim_messages(messages, max_messages)
        with span(
            "istos.agent.completion",
            {
                "gen_ai.operation.name": "chat",
                "istos.agent.step": step + 1,
                "istos.agent.messages": len(messages),
                "istos.agent.tools": len(tools),
            },
        ) as sp:
            reply = await model.complete(messages, tools=schemas)
            set_span_attributes(sp, _completion_attrs(reply))
        messages.append(_assistant_message(reply))

        if not reply.tool_calls:
            if reply.content:
                yield AgentEvent(kind="message", content=reply.content)
            else:
                # No text and no usable tool call — never end a turn silently.
                yield AgentEvent(
                    kind="message",
                    content="The model returned an empty response.",
                    error=True,
                )
            yield AgentEvent(kind="done")
            return

        for tc in reply.tool_calls:
            yield AgentEvent(
                kind="tool_call",
                name=tc.name,
                arguments=tc.arguments,
                tool_call_id=tc.id,
            )
            result_text, is_error = await _run_tool(
                catalog, tc, token=token, timeout_s=timeout_s,
            )
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text,
            })
            yield AgentEvent(
                kind="tool_result",
                name=tc.name,
                content=result_text,
                tool_call_id=tc.id,
                error=is_error,
            )

        _logger.debug(
            "Agent step %s finished %s tool call(s)",
            step + 1, len(reply.tool_calls),
            extra={"step": step + 1, "n_tools": len(reply.tool_calls)},
        )

    # Hit the ceiling while still tool-calling — surface what we have.
    yield AgentEvent(
        kind="message",
        content=f"Stopped after {max_steps} tool steps without a final answer.",
    )
    yield AgentEvent(kind="done")


async def _run_tool(
    catalog: Dict[str, MeshTool],
    tc: ToolCall,
    *,
    token: Optional[Union[bytes, str]],
    timeout_s: float,
) -> tuple[str, bool]:
    tool = catalog.get(tc.name)
    with span(
        "istos.agent.tool",
        {
            "istos.agent.tool.name": tc.name,
            "istos.agent.tool.prefix": tool.prefix if tool else None,
        },
    ) as sp:
        if tool is None:
            set_span_attributes(sp, {"istos.agent.tool.error": True})
            return f"unknown tool: {tc.name!r}", True
        try:
            value = await tool.call(tc.arguments, token=token, timeout_s=timeout_s)
            return format_tool_result(value), False
        except Exception as exc:
            _logger.info(
                "Tool %s raised: %s", tc.name, exc,
                extra={"tool": tc.name, "prefix": tool.prefix},
            )
            set_span_attributes(sp, {
                "istos.agent.tool.error": True,
                "istos.agent.tool.error_message": str(exc),
            })
            return format_tool_error(exc), True


async def drive_channel(
    session: ChannelSession,
    model: Model,
    tools: Sequence[MeshTool],
    *,
    system: Optional[str] = None,
    max_steps: int = 8,
    max_messages: Optional[int] = 40,
    token: Optional[Union[bytes, str]] = None,
    timeout_s: float = 5.0,
    send_events: bool = True,
) -> None:
    """Channel helper: reload history, then run :func:`run_agent` per inbound turn.

    By default each :class:`AgentEvent` is sent as a dict
    ``{"kind", "content", …}``. Set ``send_events=False`` to send only the final
    ``message`` content (plain string). ``max_messages`` bounds the reused log so
    a long-lived session does not grow unboundedly; pass ``None`` to disable.
    """
    history = await session.history()
    messages = history_to_messages(history, system=system)
    if system and not any(m.get("role") == "system" for m in messages):
        messages.insert(0, {"role": "system", "content": system})

    async for msg in session:
        messages.append({"role": "user", "content": user_text(msg)})
        async for event in run_agent(
            model, tools, messages,
            max_steps=max_steps, max_messages=max_messages,
            token=token, timeout_s=timeout_s,
        ):
            if event.kind == "done":
                continue
            if send_events:
                await session.send({
                    "kind": event.kind,
                    "content": event.content,
                    "name": event.name,
                    "arguments": event.arguments,
                    "tool_call_id": event.tool_call_id,
                    "error": event.error,
                })
            elif event.kind == "message" and event.content is not None:
                await session.send(event.content)
