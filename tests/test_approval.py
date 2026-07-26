"""Human-in-the-loop approval for irreversible tools."""

import asyncio
import warnings

import pytest

from istos import (
    ApprovalDenied,
    ApprovalGate,
    ApprovalState,
    ApprovalTimeout,
    Istos,
    IstosSecurityWarning,
    IstosTestClient,
    MeshTool,
    ModelReply,
    NotFoundError,
    Public,
    ToolCall,
    run_agent,
    tools_from_handlers,
    tools_from_manifest,
)
from istos.agent.approval import decide_approval, list_approvals
from istos.consistency.storage import InMemoryStoragePlugin


class _ScriptedModel:
    def __init__(self, replies: list) -> None:
        self._replies = list(replies)

    async def complete(self, messages, *, tools=None) -> ModelReply:
        if not self._replies:
            return ModelReply(content="done")
        return self._replies.pop(0)


def _refund_tool(*, approval=True, calls=None):
    async def refund(order_id: str) -> dict:
        if calls is not None:
            calls.append(order_id)
        return {"refunded": order_id}

    return MeshTool(
        "billing/refund",
        invoke=refund,
        description="Refund an order",
        parameters={
            "type": "object",
            "properties": {"order_id": {"type": "string"}},
            "required": ["order_id"],
        },
        approval=approval,
    )


def _wants_refund(order_id: str = "o-1") -> ModelReply:
    return ModelReply(
        tool_calls=[ToolCall(id="c1", name="billing-refund", arguments={"order_id": order_id})]
    )


# ---------------------------------------------------------------------------
# the gate
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_approve_returns_the_arguments_to_run():
    gate = ApprovalGate()
    req = await gate.request("billing-refund", {"order_id": "o-1"}, reason="moves money")
    assert req.state == ApprovalState.PENDING
    assert [r["id"] for r in await gate.pending()] == [req.id]

    await gate.approve(req.id, by="amir")
    decided = await gate.wait(req.id)
    assert decided.state == ApprovalState.APPROVED
    assert decided.decided_by == "amir"
    assert decided.arguments == {"order_id": "o-1"}
    assert await gate.pending() == []


@pytest.mark.asyncio
async def test_an_approver_can_correct_the_arguments():
    gate = ApprovalGate()
    req = await gate.request("billing-refund", {"order_id": "typo"})
    await gate.approve(req.id, by="amir", arguments={"order_id": "o-42"})
    assert (await gate.wait(req.id)).arguments == {"order_id": "o-42"}


@pytest.mark.asyncio
async def test_deny_raises_with_the_note():
    gate = ApprovalGate()
    req = await gate.request("billing-refund", {"order_id": "o-1"})
    await gate.deny(req.id, by="amir", note="wrong order")
    with pytest.raises(ApprovalDenied) as exc:
        await gate.wait(req.id)
    assert "wrong order" in str(exc.value)
    assert exc.value.code == "approval_denied"


@pytest.mark.asyncio
async def test_waiting_past_the_deadline_fails_closed():
    gate = ApprovalGate(timeout_s=0.05)
    req = await gate.request("billing-refund", {"order_id": "o-1"})
    with pytest.raises(ApprovalTimeout):
        await gate.wait(req.id)
    # Settled as expired, so it is not offered to an operator who is too late.
    assert await gate.pending() == []
    assert (await gate.get(req.id)).state == ApprovalState.EXPIRED
    # And a late yes cannot revive it.
    await gate.approve(req.id, by="amir")
    assert (await gate.get(req.id)).state == ApprovalState.EXPIRED


@pytest.mark.asyncio
async def test_expired_requests_are_not_listed_as_pending():
    gate = ApprovalGate(timeout_s=None)
    req = await gate.request("billing-refund", {"order_id": "o-1"}, timeout_s=0.01)
    await asyncio.sleep(0.02)
    assert await gate.pending() == []
    with pytest.raises(ApprovalTimeout):
        await gate.wait(req.id)


@pytest.mark.asyncio
async def test_first_decision_wins():
    gate = ApprovalGate()
    req = await gate.request("billing-refund", {})
    await gate.approve(req.id, by="first")
    await gate.deny(req.id, by="second")
    assert (await gate.get(req.id)).decided_by == "first"


@pytest.mark.asyncio
async def test_decide_is_none_for_a_request_this_gate_never_had():
    # This is what lets a fan-out decide ask every node and only the holder act.
    gate = ApprovalGate()
    assert await gate.decide("nope", approved=True) is None
    with pytest.raises(NotFoundError):
        await gate.wait("nope")


@pytest.mark.asyncio
async def test_no_deadline_waits_for_the_human():
    gate = ApprovalGate(timeout_s=None)
    req = await gate.request("billing-refund", {})

    async def approve_later():
        await asyncio.sleep(0.05)
        await gate.approve(req.id, by="amir")

    task = asyncio.create_task(approve_later())
    decided = await gate.wait(req.id)   # would hang if the deadline leaked in
    await task
    assert decided.state == ApprovalState.APPROVED


@pytest.mark.asyncio
async def test_pending_requests_survive_a_restart_but_their_waiter_does_not():
    storage = InMemoryStoragePlugin()
    old = ApprovalGate(storage=storage, service_name="agent")
    req = await old.request("billing-refund", {"order_id": "o-1"}, timeout_s=None)

    fresh = ApprovalGate(storage=storage, service_name="agent")
    assert [r["id"] for r in await fresh.pending()] == [req.id]
    # An operator can still see it, but nobody in this process is waiting on it,
    # so it cannot be handed back to an agent that no longer exists.
    with pytest.raises(ApprovalTimeout):
        await fresh.wait(req.id)


# ---------------------------------------------------------------------------
# the loop
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_gated_tool_runs_only_after_a_yes():
    calls: list = []
    gate = ApprovalGate()
    gate.on_request = lambda req: asyncio.ensure_future(gate.approve(req.id, by="amir"))
    tool = _refund_tool(calls=calls)
    model = _ScriptedModel([_wants_refund(), ModelReply(content="Refunded o-1.")])

    events = [
        e async for e in run_agent(model, [tool], [], approvals=gate, conversation_id="c-1")
    ]
    kinds = [e.kind for e in events]
    assert kinds == [
        "tool_call", "approval_request", "approval_decision", "tool_result",
        "message", "done",
    ]
    assert calls == ["o-1"]
    request_event = events[1]
    assert request_event.approval_id
    assert request_event.arguments == {"order_id": "o-1"}
    assert events[3].error is False


@pytest.mark.asyncio
async def test_a_denial_stops_the_call_and_tells_the_model():
    calls: list = []
    gate = ApprovalGate()
    gate.on_request = lambda req: asyncio.ensure_future(
        gate.deny(req.id, by="amir", note="not this order")
    )
    tool = _refund_tool(calls=calls)
    messages: list = []
    model = _ScriptedModel([_wants_refund(), ModelReply(content="I did not refund it.")])

    events = [e async for e in run_agent(model, [tool], messages, approvals=gate)]
    assert calls == []                                    # the tool never ran
    decision = next(e for e in events if e.kind == "approval_decision")
    assert decision.error is True
    result = next(e for e in events if e.kind == "tool_result")
    assert result.error is True
    assert "not this order" in str(result.content)
    # The refusal is in the transcript, so the model can answer the user.
    tool_msg = next(m for m in messages if m.get("role") == "tool")
    assert "not this order" in tool_msg["content"]


@pytest.mark.asyncio
async def test_timeout_stops_the_call_too():
    calls: list = []
    gate = ApprovalGate(timeout_s=0.05)
    tool = _refund_tool(calls=calls)
    model = _ScriptedModel([_wants_refund(), ModelReply(content="Nobody answered.")])

    events = [e async for e in run_agent(model, [tool], [], approvals=gate)]
    assert calls == []
    result = next(e for e in events if e.kind == "tool_result")
    assert result.error is True
    assert "approval_timeout" in str(result.content)


@pytest.mark.asyncio
async def test_an_approved_edit_is_what_runs():
    calls: list = []
    gate = ApprovalGate()
    gate.on_request = lambda req: asyncio.ensure_future(
        gate.approve(req.id, by="amir", arguments={"order_id": "o-99"})
    )
    model = _ScriptedModel([_wants_refund("o-1"), ModelReply(content="ok")])
    [e async for e in run_agent(model, [_refund_tool(calls=calls)], [], approvals=gate)]
    assert calls == ["o-99"]


@pytest.mark.asyncio
async def test_a_gated_tool_without_a_gate_is_an_error_not_a_pass():
    model = _ScriptedModel([_wants_refund()])
    with pytest.raises(ValueError, match="require human approval"):
        [e async for e in run_agent(model, [_refund_tool()], [])]


@pytest.mark.asyncio
async def test_ungated_tools_are_untouched():
    calls: list = []
    model = _ScriptedModel([_wants_refund(), ModelReply(content="ok")])
    events = [
        e async for e in run_agent(model, [_refund_tool(approval=False, calls=calls)], [])
    ]
    assert [e.kind for e in events] == ["tool_call", "tool_result", "message", "done"]
    assert calls == ["o-1"]


# ---------------------------------------------------------------------------
# declared by the tool's owner
# ---------------------------------------------------------------------------
def _billing_app() -> Istos:
    app = Istos(
        enable_health=False, enable_metrics=False, enable_discovery=False,
        service_name="billing",
    )

    @app.handle("billing/refund", approval="moves real money")
    async def refund(order_id: str) -> dict:
        """Refund an order."""
        return {"refunded": order_id}

    @app.handle("billing/status")
    async def status(order_id: str) -> dict:
        """Read-only."""
        return {"order_id": order_id}

    return app


def test_handle_approval_reaches_the_manifest_and_the_tool():
    app = _billing_app()
    manifest = app.export_capabilities()
    by_prefix = {c["prefix"]: c for c in manifest["capabilities"]}
    assert by_prefix["billing/refund"]["approval"] == "moves real money"
    assert "approval" not in by_prefix["billing/status"]

    tools = {t.prefix: t for t in tools_from_handlers(app)}
    assert tools["billing/refund"].requires_approval
    assert tools["billing/refund"].approval_reason == "moves real money"
    assert not tools["billing/status"].requires_approval

    # An agent elsewhere on the fabric inherits the flag from the manifest.
    remote = {t.prefix: t for t in tools_from_manifest(app, manifest)}
    assert remote["billing/refund"].requires_approval
    assert not remote["billing/status"].requires_approval


def test_a_caller_can_gate_a_prefix_the_owner_did_not():
    app = _billing_app()
    tools = {t.prefix: t for t in tools_from_handlers(app, approval=["billing/status"])}
    assert tools["billing/status"].requires_approval


# ---------------------------------------------------------------------------
# on the fabric
# ---------------------------------------------------------------------------
def _agent_app() -> Istos:
    return Istos(
        enable_health=False, enable_metrics=False, enable_discovery=False,
        service_name="agent",
    )


def test_an_open_decide_endpoint_warns():
    app = _agent_app()
    with pytest.warns(IstosSecurityWarning, match="approve a tool call"):
        app.approvals()


def test_an_authorized_gate_does_not_warn():
    app = _agent_app()
    with warnings.catch_warnings():
        warnings.simplefilter("error", IstosSecurityWarning)
        app.approvals(authorizer=Public)


@pytest.mark.asyncio
async def test_the_fabric_endpoints_list_and_settle():
    app = _agent_app()
    gate = app.approvals(authorizer=Public)
    client = IstosTestClient(app)

    req = await gate.request("billing-refund", {"order_id": "o-1"}, reason="money")
    listed = await client.query(gate.key)
    assert [r["id"] for r in listed["pending"]] == [req.id]
    assert listed["pending"][0]["reason"] == "money"

    # Not this node's request → matched false, so a fan-out decide is a no-op here.
    assert await client.query(gate.decide_key, request_id="other", approved=True) == {
        "matched": False
    }

    settled = await client.query(
        gate.decide_key, request_id=req.id, approved=True, by="amir"
    )
    assert settled["matched"] is True
    assert settled["request"]["state"] == "approved"
    assert (await gate.wait(req.id)).decided_by == "amir"


@pytest.mark.asyncio
async def test_a_string_no_does_not_read_as_a_yes():
    """Selector params arrive as text; the yes is the dangerous direction."""
    app = _agent_app()
    gate = app.approvals(authorizer=Public)
    handler = next(h for h in app._handlers if h.prefix == gate.decide_key)

    req = await gate.request("billing-refund", {})
    await handler(request_id=req.id, approved="false", by="amir")
    assert (await gate.get(req.id)).state == ApprovalState.DENIED


@pytest.mark.asyncio
async def test_the_agent_waits_while_an_operator_decides_over_the_fabric():
    """The realistic shape: the loop is suspended and the decision arrives as a
    query on this node's decide key."""
    app = _agent_app()
    gate = app.approvals(authorizer=Public)
    client = IstosTestClient(app)
    calls: list = []
    model = _ScriptedModel([_wants_refund(), ModelReply(content="Refunded.")])

    async def drive() -> list:
        return [
            e async for e in run_agent(
                model, [_refund_tool(calls=calls)], [], approvals=gate,
            )
        ]

    task = asyncio.create_task(drive())
    for _ in range(100):                       # wait for the loop to suspend
        pending = await gate.pending()
        if pending:
            break
        await asyncio.sleep(0.01)
    assert pending, "the agent never filed a request"
    assert calls == []                         # still gated

    await client.query(
        gate.decide_key, request_id=pending[0]["id"], approved=True, by="amir",
    )
    events = await task
    assert calls == ["o-1"]
    assert [e.kind for e in events][-2:] == ["message", "done"]


@pytest.mark.asyncio
async def test_decide_approval_needs_a_node_holding_the_request(monkeypatch):
    app = _agent_app()

    async def no_one(*args, **kwargs):
        return [{"matched": False}]

    monkeypatch.setattr(app, "query_once", no_one)
    with pytest.raises(NotFoundError):
        await decide_approval(app, "gone", approved=True)


@pytest.mark.asyncio
async def test_list_approvals_merges_every_node(monkeypatch):
    app = _agent_app()

    async def two_nodes(*args, **kwargs):
        return [
            {"service": "b", "pending": [{"id": "2", "tool": "t", "requested_at": 2}]},
            {"service": "a", "pending": [{"id": "1", "tool": "t", "requested_at": 1}]},
            {"__istos_error": True, "code": "unauthorized", "message": "no"},
        ]

    monkeypatch.setattr(app, "query_once", two_nodes)
    found = await list_approvals(app)
    assert [r["id"] for r in found] == ["1", "2"]       # oldest first
    assert [r["service"] for r in found] == ["a", "b"]
