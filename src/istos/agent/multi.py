"""Multi-agent handoff — Swarm-style transfer between agents on one conversation.

An :class:`Agent` bundles a model, its mesh tools, a system prompt, and the
agents it may hand off to. The model transfers by calling a synthetic
``transfer_to_<name>`` tool; the loop swaps the active agent but keeps the shared
message history, so context carries across. Handoffs may form cycles, so a
specialist can hand back to the router (triage → specialist → triage)::

    billing = Agent("billing", model, tools=[refund], system="You handle refunds.")
    router = Agent("router", model, handoffs=[billing])
    billing.handoffs = [router]            # return handoff

    @app.channel("agent/chat", durable=True)
    async def chat(s: ChannelSession):
        await drive_agents(s, router, token=jwt)

The caller's ``token`` is forwarded to whichever agent's tools run, so
authorizers see the original principal regardless of how many handoffs occurred.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional, Sequence, Union

from istos.agent.loop import (
    AgentEvent,
    _assistant_message,
    _by_name,
    _completion_attrs,
    _run_tool,
    _trim_messages,
    history_to_messages,
    user_text,
)
from istos.agent.model import Model
from istos.agent.tools import MeshTool, tool_name
from istos.logging import get_logger
from istos.observability.tracing import set_span_attributes, span
from istos.primitives.channel import ChannelSession

_logger = get_logger("agent.multi")


@dataclass
class Agent:
    """One agent in a handoff graph: a model, its tools, a system prompt, and the
    agents it may transfer to.

    ``name`` must be usable in a tool name (``[A-Za-z0-9_-]``); it is sanitized
    the same way key expressions are. ``description`` is surfaced to a router
    model in the ``transfer_to_<name>`` tool so it knows when to hand off.
    """

    name: str
    model: Model
    tools: Sequence[MeshTool] = field(default_factory=list)
    system: Optional[str] = None
    handoffs: Sequence["Agent"] = field(default_factory=list)
    description: str = ""

    @property
    def tool_name(self) -> str:
        return tool_name(self.name)


def _transfer_tool_name(agent: Agent) -> str:
    return f"transfer_to_{agent.tool_name}"


def _handoff_schema(target: Agent) -> dict:
    desc = target.description or f"the {target.name} agent"
    return {
        "type": "function",
        "function": {
            "name": _transfer_tool_name(target),
            "description": f"Hand the conversation off to {desc}.",
            "parameters": {"type": "object", "properties": {}},
        },
    }


def _with_system(messages: List[dict], system: Optional[str]) -> List[dict]:
    """Present the active agent's system prompt, replacing any stored one.

    The system prompt is dynamic (it changes on handoff), so it is never stored
    in ``messages``; each completion gets the current agent's prompt prepended.
    """
    body = [m for m in messages if m.get("role") != "system"]
    if system:
        return [{"role": "system", "content": system}, *body]
    return body


def build_registry(entry: Agent) -> Dict[str, Agent]:
    """Every agent reachable from ``entry`` through ``handoffs``, keyed by name."""
    registry: Dict[str, Agent] = {}
    stack = [entry]
    while stack:
        agent = stack.pop()
        if agent.name in registry:
            continue
        registry[agent.name] = agent
        stack.extend(agent.handoffs)
    return registry


async def run_multi_agent(
    active: Agent,
    messages: List[dict],
    *,
    max_steps: int = 8,
    max_messages: Optional[int] = None,
    token: Optional[Union[bytes, str]] = None,
    timeout_s: float = 5.0,
) -> AsyncIterator[AgentEvent]:
    """Run the loop starting at ``active``, switching agents on handoff.

    Mutates ``messages`` in place (shared history). Emits the same events as
    :func:`~istos.agent.loop.run_agent` plus ``handoff`` when the active agent
    changes; the last ``handoff`` event names the agent that should drive the
    next turn. ``token`` is forwarded to every tool call, across handoffs.
    """
    if max_steps < 1:
        raise ValueError("max_steps must be >= 1")

    for step in range(max_steps):
        if max_messages is not None:
            _trim_messages(messages, max_messages)

        handoff_by_tool = {_transfer_tool_name(t): t for t in active.handoffs}
        catalog = _by_name(active.tools)
        schemas = [t.openai_schema() for t in active.tools]
        schemas += [_handoff_schema(t) for t in active.handoffs]

        with span(
            "istos.agent.completion",
            {
                "gen_ai.operation.name": "chat",
                "istos.agent.name": active.name,
                "istos.agent.step": step + 1,
                "istos.agent.messages": len(messages),
                "istos.agent.tools": len(active.tools),
                "istos.agent.handoffs": len(active.handoffs),
            },
        ) as sp:
            reply = await active.model.complete(
                _with_system(messages, active.system), tools=schemas or None
            )
            set_span_attributes(sp, _completion_attrs(reply))
        messages.append(_assistant_message(reply))

        if not reply.tool_calls:
            if reply.content:
                yield AgentEvent(kind="message", content=reply.content)
            else:
                yield AgentEvent(
                    kind="message",
                    content="The model returned an empty response.",
                    error=True,
                )
            yield AgentEvent(kind="done")
            return

        # Resolve every tool call against the agent that produced them, so each
        # gets a tool response (the API requires one per call). A handoff wins at
        # the end of the batch; the last transfer sets the next active agent.
        next_active = active
        for tc in reply.tool_calls:
            target = handoff_by_tool.get(tc.name)
            if target is not None:
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": f"Transferred to {target.name}.",
                })
                yield AgentEvent(
                    kind="handoff",
                    name=target.name,
                    content=f"{active.name} -> {target.name}",
                    tool_call_id=tc.id,
                )
                next_active = target
                continue

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

        if next_active is not active:
            _logger.debug(
                "Handoff %s -> %s", active.name, next_active.name,
                extra={"from": active.name, "to": next_active.name},
            )
            active = next_active

    yield AgentEvent(
        kind="message",
        content=f"Stopped after {max_steps} steps without a final answer.",
    )
    yield AgentEvent(kind="done")


def _active_from_history(
    history: Sequence[dict], registry: Dict[str, Agent], entry: Agent
) -> Agent:
    """Restore the agent that was active before a reconnect from persisted
    ``handoff`` frames; fall back to ``entry`` when none were recorded."""
    active = entry
    for turn in history:
        if turn.get("dir") != "out":
            continue
        data = turn.get("data")
        if isinstance(data, dict) and data.get("kind") == "handoff":
            target = registry.get(data.get("name"))
            if target is not None:
                active = target
    return active


async def drive_agents(
    session: ChannelSession,
    entry: Agent,
    *,
    max_steps: int = 8,
    max_messages: Optional[int] = 40,
    token: Optional[Union[bytes, str]] = None,
    timeout_s: float = 5.0,
    send_events: bool = True,
) -> None:
    """Channel helper: reload history, then run :func:`run_multi_agent` per turn.

    The active agent persists across turns within a session, and is restored on
    reconnect from persisted ``handoff`` frames (``send_events=True``); otherwise
    a resumed session restarts at ``entry``. ``token`` forwards to tool calls
    under whichever agent is active. See :func:`~istos.agent.loop.drive_channel`
    for the ``send_events`` payload shapes.
    """
    registry = build_registry(entry)
    history = await session.history()
    messages = history_to_messages(history)
    active = _active_from_history(history, registry, entry)

    async for msg in session:
        messages.append({"role": "user", "content": user_text(msg)})
        async for event in run_multi_agent(
            active, messages,
            max_steps=max_steps, max_messages=max_messages,
            token=token, timeout_s=timeout_s,
        ):
            if event.kind == "handoff":
                target = registry.get(event.name)
                if target is not None:
                    active = target
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
