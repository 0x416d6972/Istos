"""Mesh agent loop: tools_from_handlers, run_agent, drive_channel."""

import asyncio

import pytest

from istos import (
    Agent,
    Istos,
    MeshTool,
    ModelReply,
    NotFoundError,
    ToolCall,
    build_registry,
    drive_agents,
    drive_channel,
    run_agent,
    run_multi_agent,
    tools_from_handlers,
)
from istos.agent.loop import _trim_messages, history_to_messages, user_text
from istos.agent.multi import _active_from_history
from istos.agent.tools import tool_name
from istos.messages.serialization import JsonSerializer
from istos.primitives.channel import ChannelSession


def _app() -> Istos:
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)

    @app.handle("math/add")
    async def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @app.handle("math/boom")
    async def boom() -> int:
        """Always fails."""
        raise NotFoundError("no boom")

    @app.handle(".istos/hidden")
    async def hidden() -> dict:
        return {}

    return app


class _ScriptedModel:
    """Returns a fixed sequence of replies — one per complete() call."""

    def __init__(self, replies: list) -> None:
        self._replies = list(replies)
        self.calls = 0

    async def complete(self, messages, *, tools=None) -> ModelReply:
        self.calls += 1
        if not self._replies:
            return ModelReply(content="fallback")
        return self._replies.pop(0)


def _session():
    sent = []

    async def sink(raw: bytes):
        sent.append(JsonSerializer().deserialize(raw))

    return ChannelSession(JsonSerializer(), sink), sent


# ---------------------------------------------------------------------------
# tools
# ---------------------------------------------------------------------------
def test_tool_name_maps_slash():
    assert tool_name("math/add") == "math-add"


def test_tools_from_handlers_skips_plumbing_and_builds_schema():
    tools = tools_from_handlers(_app())
    by_name = {t.name: t for t in tools}
    assert "math-add" in by_name
    assert "math-boom" in by_name
    assert not any(t.prefix.startswith(".istos/") for t in tools)
    assert by_name["math-add"].description == "Add two integers."
    assert by_name["math-add"].parameters["properties"]["a"]["type"] == "integer"


def test_tools_from_handlers_whitelist():
    tools = tools_from_handlers(_app(), prefixes=["math/add"])
    assert [t.prefix for t in tools] == ["math/add"]


@pytest.mark.asyncio
async def test_mesh_tool_invoke_override():
    async def local_add(a: int, b: int) -> int:
        return a + b

    tool = MeshTool("math/add", invoke=local_add, description="local")
    assert await tool.call({"a": 2, "b": 3}) == 5


def test_mesh_tool_requires_app_or_invoke():
    with pytest.raises(ValueError):
        MeshTool("math/add")


# ---------------------------------------------------------------------------
# loop helpers
# ---------------------------------------------------------------------------
def test_user_text_shapes():
    assert user_text("hi") == "hi"
    assert user_text({"text": "yo"}) == "yo"
    assert user_text({"content": "c"}) == "c"


def test_history_to_messages_drops_toolcall_without_result():
    # A tool_call with no matching tool_result (crash mid-tool) is dropped.
    history = [
        {"dir": "in", "data": "hello"},
        {"dir": "out", "data": {"kind": "tool_call", "name": "math-add"}},
        {"dir": "out", "data": {"kind": "message", "content": "3"}},
    ]
    msgs = history_to_messages(history, system="sys")
    assert msgs[0] == {"role": "system", "content": "sys"}
    assert msgs[1] == {"role": "user", "content": "hello"}
    assert msgs[2] == {"role": "assistant", "content": "3"}


def test_history_to_messages_rebuilds_tool_transcript():
    history = [
        {"dir": "in", "data": "2+3?"},
        {"dir": "out", "data": {
            "kind": "tool_call", "name": "math-add",
            "arguments": {"a": 2, "b": 3}, "tool_call_id": "1",
        }},
        {"dir": "out", "data": {
            "kind": "tool_result", "content": "5", "tool_call_id": "1",
        }},
        {"dir": "out", "data": {"kind": "message", "content": "sum is 5"}},
    ]
    msgs = history_to_messages(history)
    assert [m["role"] for m in msgs] == ["user", "assistant", "tool", "assistant"]
    call = msgs[1]
    assert call["tool_calls"][0]["id"] == "1"
    assert call["tool_calls"][0]["function"]["name"] == "math-add"
    assert call["tool_calls"][0]["function"]["arguments"] == '{"a": 2, "b": 3}'
    assert msgs[2] == {"role": "tool", "tool_call_id": "1", "content": "5"}
    assert msgs[3] == {"role": "assistant", "content": "sum is 5"}


def test_history_to_messages_include_tools_false_skips_transcript():
    history = [
        {"dir": "in", "data": "2+3?"},
        {"dir": "out", "data": {
            "kind": "tool_call", "name": "math-add",
            "arguments": {"a": 2, "b": 3}, "tool_call_id": "1",
        }},
        {"dir": "out", "data": {
            "kind": "tool_result", "content": "5", "tool_call_id": "1",
        }},
        {"dir": "out", "data": {"kind": "message", "content": "sum is 5"}},
    ]
    msgs = history_to_messages(history, include_tools=False)
    assert [m["role"] for m in msgs] == ["user", "assistant"]
    assert msgs[1]["content"] == "sum is 5"


# ---------------------------------------------------------------------------
# run_agent
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_run_agent_text_only():
    model = _ScriptedModel([ModelReply(content="pong")])
    messages = [{"role": "user", "content": "ping"}]
    events = [e async for e in run_agent(model, [], messages)]
    assert [e.kind for e in events] == ["message", "done"]
    assert events[0].content == "pong"
    assert messages[-1]["role"] == "assistant"
    assert model.calls == 1


@pytest.mark.asyncio
async def test_run_agent_calls_mesh_tool_then_answers():
    async def add(a: int, b: int) -> int:
        return a + b

    tools = [MeshTool("math/add", invoke=add, description="Add")]
    model = _ScriptedModel([
        ModelReply(
            tool_calls=[ToolCall(id="1", name="math-add", arguments={"a": 2, "b": 3})]
        ),
        ModelReply(content="sum is 5"),
    ])
    messages = [{"role": "user", "content": "2+3?"}]
    events = [e async for e in run_agent(model, tools, messages)]
    kinds = [e.kind for e in events]
    assert kinds == ["tool_call", "tool_result", "message", "done"]
    assert events[0].name == "math-add"
    assert events[1].content == "5"
    assert events[1].error is False
    assert events[2].content == "sum is 5"
    assert any(m.get("role") == "tool" for m in messages)


@pytest.mark.asyncio
async def test_run_agent_tool_error_is_fed_back():
    async def boom() -> int:
        raise NotFoundError("gone")

    tools = [MeshTool("math/boom", invoke=boom)]
    model = _ScriptedModel([
        ModelReply(tool_calls=[ToolCall(id="1", name="math-boom", arguments={})]),
        ModelReply(content="could not"),
    ])
    messages = [{"role": "user", "content": "boom"}]
    events = [e async for e in run_agent(model, tools, messages)]
    result = next(e for e in events if e.kind == "tool_result")
    assert result.error is True
    assert "not_found" in result.content


@pytest.mark.asyncio
async def test_run_agent_unknown_tool():
    model = _ScriptedModel([
        ModelReply(tool_calls=[ToolCall(id="1", name="nope", arguments={})]),
        ModelReply(content="ok"),
    ])
    messages = [{"role": "user", "content": "x"}]
    events = [e async for e in run_agent(model, [], messages)]
    result = next(e for e in events if e.kind == "tool_result")
    assert result.error is True
    assert "unknown tool" in result.content


@pytest.mark.asyncio
async def test_run_agent_empty_reply_is_not_silent():
    model = _ScriptedModel([ModelReply()])
    messages = [{"role": "user", "content": "x"}]
    events = [e async for e in run_agent(model, [], messages)]
    assert [e.kind for e in events] == ["message", "done"]
    assert events[0].error is True
    assert events[0].content


@pytest.mark.asyncio
async def test_run_agent_trims_messages():
    captured = []

    class _Capturing:
        async def complete(self, messages, *, tools=None):
            captured.append(len(messages))
            return ModelReply(content="ok")

    messages = [{"role": "system", "content": "sys"}]
    messages += [{"role": "user", "content": str(i)} for i in range(50)]
    events = [e async for e in run_agent(_Capturing(), [], messages, max_messages=10)]
    assert [e.kind for e in events] == ["message", "done"]
    # system kept + 10 body, then the appended assistant reply.
    assert captured == [11]
    assert messages[0]["role"] == "system"


def test_trim_messages_drops_orphaned_tool_frames():
    messages = [{"role": "system", "content": "s"}]
    messages += [
        {"role": "user", "content": "u"},
        {"role": "assistant", "tool_calls": [{"id": "1"}]},
        {"role": "tool", "tool_call_id": "1", "content": "r"},
        {"role": "assistant", "content": "done"},
    ]
    # Last 2 body messages are [tool, assistant]; the orphaned tool is dropped.
    _trim_messages(messages, 2)
    assert messages[0]["role"] == "system"
    assert [m["role"] for m in messages[1:]] == ["assistant"]
    assert messages[1]["content"] == "done"


@pytest.mark.asyncio
async def test_run_agent_respects_max_steps():
    async def add(a: int, b: int) -> int:
        return a + b

    tools = [MeshTool("math/add", invoke=add)]
    # Always asks for another tool call — should stop at max_steps.
    model = _ScriptedModel([
        ModelReply(
            tool_calls=[ToolCall(id=str(i), name="math-add", arguments={"a": 1, "b": 1})]
        )
        for i in range(10)
    ])
    messages = [{"role": "user", "content": "loop"}]
    events = [e async for e in run_agent(model, tools, messages, max_steps=2)]
    assert events[-1].kind == "done"
    assert events[-2].kind == "message"
    assert "2 tool steps" in events[-2].content


# ---------------------------------------------------------------------------
# drive_channel
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_drive_channel_sends_events():
    model = _ScriptedModel([ModelReply(content="hello back")])
    s, sent = _session()

    task = asyncio.create_task(drive_channel(s, model, [], system="sys"))
    await asyncio.sleep(0)
    s.feed(JsonSerializer().serialize({"text": "hi"}))
    for _ in range(100):
        if sent:
            break
        await asyncio.sleep(0.01)
    s.close()
    await task
    assert sent[0]["kind"] == "message"
    assert sent[0]["content"] == "hello back"


class _MemStore:
    """Minimal SessionStore stand-in: keeps the {dir, data, ts} log in memory."""

    def __init__(self) -> None:
        self.log: list = []

    async def append(self, conversation_id, direction, data):
        self.log.append({"dir": direction, "data": data, "ts": 0})

    async def history(self, conversation_id, limit=1000):
        return list(self.log)


@pytest.mark.asyncio
async def test_drive_channel_tool_transcript_survives_reconnect():
    async def add(a: int, b: int) -> int:
        return a + b

    tools = [MeshTool("math/add", invoke=add, description="Add")]
    store = _MemStore()

    def durable_session():
        sent = []

        async def sink(raw: bytes):
            sent.append(JsonSerializer().deserialize(raw))

        s = ChannelSession(
            JsonSerializer(), sink, store=store, conversation_id="c1",
        )
        return s, sent

    # First connection: one tool-using turn, persisted to the store.
    model = _ScriptedModel([
        ModelReply(
            tool_calls=[ToolCall(id="1", name="math-add", arguments={"a": 2, "b": 3})]
        ),
        ModelReply(content="sum is 5"),
    ])
    s1, _ = durable_session()
    task = asyncio.create_task(drive_channel(s1, model, tools, system="sys"))
    await asyncio.sleep(0)
    s1.feed(JsonSerializer().serialize({"text": "2+3?"}))

    def persisted_final_message():
        return any(
            isinstance(e["data"], dict) and e["data"].get("kind") == "message"
            for e in store.log
        )

    for _ in range(100):
        if persisted_final_message():
            break
        await asyncio.sleep(0.01)
    s1.close()
    await task

    # Reconnect: the rebuilt context carries the assistant tool_call + tool result.
    rebuilt = history_to_messages(await store.history("c1"), system="sys")
    roles = [m["role"] for m in rebuilt]
    assert "tool" in roles
    tool_msg = next(m for m in rebuilt if m["role"] == "tool")
    assert tool_msg["content"] == "5"
    call = next(m for m in rebuilt if m.get("tool_calls"))
    assert call["tool_calls"][0]["function"]["name"] == "math-add"


@pytest.mark.asyncio
async def test_drive_channel_plain_message_mode():
    model = _ScriptedModel([ModelReply(content="plain")])
    s, sent = _session()

    task = asyncio.create_task(drive_channel(s, model, [], send_events=False))
    await asyncio.sleep(0)
    s.feed(JsonSerializer().serialize("hi"))
    for _ in range(100):
        if sent:
            break
        await asyncio.sleep(0.01)
    s.close()
    await task
    assert sent == ["plain"]


# ---------------------------------------------------------------------------
# multi-agent handoff
# ---------------------------------------------------------------------------
class _RecordingApp:
    """query_once stand-in that records the token each mesh tool call forwards."""

    def __init__(self) -> None:
        self.tokens: list = []

    async def query_once(self, prefix, *, token=None, timeout_s=5.0, **kwargs):
        self.tokens.append(token)
        return kwargs.get("a", 0) + kwargs.get("b", 0)


def _transfer(target: str, cid: str = "1") -> ModelReply:
    return ModelReply(tool_calls=[ToolCall(id=cid, name=f"transfer_to_{target}", arguments={})])


def test_build_registry_handles_cycles():
    billing = Agent("billing", _ScriptedModel([]))
    router = Agent("router", _ScriptedModel([]), handoffs=[billing])
    billing.handoffs = [router]  # return handoff -> cycle
    reg = build_registry(router)
    assert set(reg) == {"router", "billing"}


@pytest.mark.asyncio
async def test_run_multi_agent_handoff_switches_agent():
    billing = Agent(
        "billing", _ScriptedModel([ModelReply(content="refund done")]),
        system="You handle refunds.",
    )
    router = Agent(
        "router", _ScriptedModel([_transfer("billing")]), handoffs=[billing],
    )
    messages = [{"role": "user", "content": "i want a refund"}]
    events = [e async for e in run_multi_agent(router, messages)]

    assert [e.kind for e in events] == ["handoff", "message", "done"]
    assert events[0].name == "billing"
    assert events[1].content == "refund done"
    assert billing.model.calls == 1 and router.model.calls == 1
    # The transfer is acknowledged with a tool message (API requires one per call).
    assert any(
        m.get("role") == "tool" and m["content"] == "Transferred to billing."
        for m in messages
    )


@pytest.mark.asyncio
async def test_run_multi_agent_return_handoff():
    router = Agent(
        "router",
        _ScriptedModel([_transfer("billing", "1"), ModelReply(content="all set")]),
    )
    billing = Agent("billing", _ScriptedModel([_transfer("router", "2")]))
    router.handoffs = [billing]
    billing.handoffs = [router]

    messages = [{"role": "user", "content": "refund then anything else"}]
    events = [e async for e in run_multi_agent(router, messages)]

    handoffs = [(e.content, e.name) for e in events if e.kind == "handoff"]
    assert handoffs == [("router -> billing", "billing"), ("billing -> router", "router")]
    assert events[-2].kind == "message" and events[-2].content == "all set"


@pytest.mark.asyncio
async def test_run_multi_agent_forwards_token_across_handoff():
    app = _RecordingApp()
    refund = MeshTool("billing/refund", app=app, description="Refund")
    billing = Agent(
        "billing",
        _ScriptedModel([
            ModelReply(tool_calls=[
                ToolCall(id="2", name="billing-refund", arguments={"a": 1, "b": 2})
            ]),
            ModelReply(content="refunded 3"),
        ]),
        tools=[refund],
    )
    router = Agent("router", _ScriptedModel([_transfer("billing")]), handoffs=[billing])

    messages = [{"role": "user", "content": "refund please"}]
    events = [e async for e in run_multi_agent(router, messages, token="jwt")]

    kinds = [e.kind for e in events]
    assert kinds == ["handoff", "tool_call", "tool_result", "message", "done"]
    assert app.tokens == ["jwt"]  # caller token reached the post-handoff agent's tool


@pytest.mark.asyncio
async def test_drive_agents_restores_active_agent_on_reconnect():
    store = _MemStore()

    def durable_session():
        sent = []

        async def sink(raw: bytes):
            sent.append(JsonSerializer().deserialize(raw))

        return ChannelSession(
            JsonSerializer(), sink, store=store, conversation_id="c1",
        ), sent

    # First connection: router hands off to billing, billing answers.
    billing1 = Agent("billing", _ScriptedModel([ModelReply(content="hi from billing")]))
    router1 = Agent("router", _ScriptedModel([_transfer("billing")]), handoffs=[billing1])
    s1, _ = durable_session()
    task = asyncio.create_task(drive_agents(s1, router1))
    await asyncio.sleep(0)
    s1.feed(JsonSerializer().serialize({"text": "refund"}))

    def persisted_message():
        return any(
            isinstance(e["data"], dict) and e["data"].get("kind") == "message"
            for e in store.log
        )

    for _ in range(100):
        if persisted_message():
            break
        await asyncio.sleep(0.01)
    s1.close()
    await task

    # A handoff frame was persisted, so a reconnect restores billing as active.
    router2 = Agent("router", _ScriptedModel([_transfer("billing")]))
    billing2 = Agent("billing", _ScriptedModel([ModelReply(content="second answer")]))
    router2.handoffs = [billing2]
    reg = build_registry(router2)
    assert _active_from_history(store.log, reg, router2) is billing2

    s2, sent2 = durable_session()
    task2 = asyncio.create_task(drive_agents(s2, router2))
    await asyncio.sleep(0)
    s2.feed(JsonSerializer().serialize({"text": "again"}))
    for _ in range(100):
        if any(e.get("kind") == "message" for e in sent2):
            break
        await asyncio.sleep(0.01)
    s2.close()
    await task2

    # billing2 handled the turn directly; the router model was never consulted.
    assert any(e["content"] == "second answer" for e in sent2 if e["kind"] == "message")
    assert router2.model.calls == 0


# ---------------------------------------------------------------------------
# tracing
# ---------------------------------------------------------------------------
class _FakeSpan:
    def __init__(self, name, attributes):
        self.name = name
        self.attributes = dict(attributes or {})

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeTracer:
    def __init__(self):
        self.spans = []

    def start_as_current_span(self, name, attributes=None):
        sp = _FakeSpan(name, attributes)
        self.spans.append(sp)
        return sp


@pytest.fixture
def fake_tracer(monkeypatch):
    from istos.observability import tracing

    tracer = _FakeTracer()
    monkeypatch.setattr(tracing, "_tracer", tracer)
    return tracer


@pytest.mark.asyncio
async def test_run_agent_emits_model_and_tool_spans(fake_tracer):
    async def add(a: int, b: int) -> int:
        return a + b

    tools = [MeshTool("math/add", invoke=add)]
    model = _ScriptedModel([
        ModelReply(tool_calls=[ToolCall(id="1", name="math-add", arguments={"a": 2, "b": 3})]),
        ModelReply(
            content="5", model="test-model", finish_reason="stop",
            usage={"prompt_tokens": 10, "completion_tokens": 4},
        ),
    ])
    messages = [{"role": "user", "content": "2+3?"}]
    _ = [e async for e in run_agent(model, tools, messages)]

    by_name = {}
    for sp in fake_tracer.spans:
        by_name.setdefault(sp.name, []).append(sp)

    assert len(by_name["istos.agent.completion"]) == 2
    assert len(by_name["istos.agent.tool"]) == 1

    tool_span = by_name["istos.agent.tool"][0]
    assert tool_span.attributes["istos.agent.tool.name"] == "math-add"
    assert tool_span.attributes["istos.agent.tool.prefix"] == "math/add"
    assert "istos.agent.tool.error" not in tool_span.attributes

    final = by_name["istos.agent.completion"][1]
    assert final.attributes["gen_ai.response.model"] == "test-model"
    assert final.attributes["gen_ai.response.finish_reasons"] == ["stop"]
    assert final.attributes["gen_ai.usage.input_tokens"] == 10
    assert final.attributes["gen_ai.usage.output_tokens"] == 4
    assert final.attributes["istos.agent.has_content"] is True


@pytest.mark.asyncio
async def test_tool_span_flags_errors(fake_tracer):
    model = _ScriptedModel([
        ModelReply(tool_calls=[ToolCall(id="1", name="nope", arguments={})]),
        ModelReply(content="done"),
    ])
    _ = [e async for e in run_agent(model, [], [{"role": "user", "content": "x"}])]

    tool_span = next(s for s in fake_tracer.spans if s.name == "istos.agent.tool")
    assert tool_span.attributes["istos.agent.tool.error"] is True


@pytest.mark.asyncio
async def test_multi_agent_completion_span_names_active_agent(fake_tracer):
    billing = Agent("billing", _ScriptedModel([ModelReply(content="refund done")]))
    router = Agent("router", _ScriptedModel([_transfer("billing")]), handoffs=[billing])
    _ = [e async for e in run_multi_agent(router, [{"role": "user", "content": "hi"}])]

    names = [
        s.attributes.get("istos.agent.name")
        for s in fake_tracer.spans if s.name == "istos.agent.completion"
    ]
    assert names == ["router", "billing"]


@pytest.mark.asyncio
async def test_no_spans_when_tracing_off():
    from istos.observability import tracing

    assert tracing._tracer is None  # default: tracing not configured
    model = _ScriptedModel([ModelReply(content="hi")])
    events = [e async for e in run_agent(model, [], [{"role": "user", "content": "x"}])]
    assert [e.kind for e in events] == ["message", "done"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mesh_tool_query_once_over_fabric():
    app = _app()
    tools = tools_from_handlers(app, prefixes=["math/add"])
    async with app.serving():
        await asyncio.sleep(0.4)
        assert await tools[0].call({"a": 4, "b": 5}) == 9
