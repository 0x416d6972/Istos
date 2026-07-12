"""Request envelope: cross-hop correlation_id + W3C traceparent propagation.

The Zenoh attachment carries an auth token *and* — across hops — the
correlation_id and traceparent, so one logical operation stays linked through a
chain of handlers. A bare-token attachment stays backward compatible.
"""

import asyncio

import pytest

from istos import AuthContext, Istos
from istos.context import (
    RequestContext,
    RequestEnvelope,
    get_request_context,
    peek_request_context,
    reset_request_context,
    set_request_context,
)
from istos.primitives.query import query_wrapper
from istos.messages.serialization import JsonSerializer


# ---------------------------------------------------------------------------
# 1. Envelope encode/decode
# ---------------------------------------------------------------------------
def test_bare_token_is_backward_compatible():
    # A plain string attachment (old wire form) is read as a token.
    env = RequestEnvelope.from_attachment(b"my-secret-token")
    assert env.token == "my-secret-token"
    assert env.correlation_id is None and env.traceparent is None


def test_token_only_stays_bare_on_the_wire():
    # No metadata → emit the simple bare-token form, not JSON.
    assert RequestEnvelope(token="t").to_attachment() == b"t"
    assert RequestEnvelope().to_attachment() is None


def test_envelope_roundtrip_with_metadata():
    env = RequestEnvelope(token="t", correlation_id="cid-1", traceparent="00-a-b-01")
    raw = env.to_attachment()
    assert raw.startswith(b"{")  # JSON envelope once metadata is present
    back = RequestEnvelope.from_attachment(raw)
    assert (back.token, back.correlation_id, back.traceparent) == ("t", "cid-1", "00-a-b-01")


def test_envelope_without_token_carries_metadata():
    raw = RequestEnvelope(correlation_id="cid-9").to_attachment()
    back = RequestEnvelope.from_attachment(raw)
    assert back.token is None and back.correlation_id == "cid-9"


def test_non_envelope_json_is_treated_as_token():
    # A JSON object lacking known keys is not an envelope — it's an opaque token.
    env = RequestEnvelope.from_attachment(b'{"user":"alice"}')
    assert env.token == '{"user":"alice"}'
    assert env.correlation_id is None


def test_authcontext_token_is_envelope_aware():
    raw = RequestEnvelope(token="jwt-here", correlation_id="c").to_attachment()
    assert AuthContext(prefix="p", key_expr="p", attachment=raw).token == "jwt-here"
    # Bare token still works.
    assert AuthContext(prefix="p", key_expr="p", attachment=b"plain").token == "plain"


# ---------------------------------------------------------------------------
# 2. Outbound forwarding from ambient context
# ---------------------------------------------------------------------------
def test_query_forwards_ambient_correlation_id():
    wrapper = query_wrapper(
        func=lambda d: d, prefix="k", serializer=JsonSerializer(),
        get_session=lambda: None, attachment=b"tok",
    )
    try:
        set_request_context(RequestContext(correlation_id="cid-parent", traceparent="00-t-s-01"))
        raw = wrapper._outbound_attachment()
        env = RequestEnvelope.from_attachment(raw)
        assert env.token == "tok"
        assert env.correlation_id == "cid-parent"
        assert env.traceparent == "00-t-s-01"
    finally:
        reset_request_context()


def test_query_outside_request_stays_bare():
    reset_request_context()
    assert peek_request_context() is None
    wrapper = query_wrapper(
        func=lambda d: d, prefix="k", serializer=JsonSerializer(),
        get_session=lambda: None, attachment=b"tok",
    )
    # No ambient context → bare token, unchanged wire form.
    assert wrapper._outbound_attachment() == b"tok"


# ---------------------------------------------------------------------------
# 3. Integration: correlation_id flows across a real handler→handler hop
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_correlation_id_propagates_across_hops():
    """Handler A queries handler B over Zenoh; B inherits A's correlation_id."""
    seen: dict = {}
    app = Istos(enable_health=False, enable_metrics=False)

    @app.handle("hop/b")
    async def b():
        seen["b_cid"] = get_request_context().correlation_id
        return {"ok": True}

    @app.handle("hop/a")
    async def a():
        seen["a_cid"] = get_request_context().correlation_id
        await app.query_once("hop/b", timeout_s=3.0)
        return {"ok": True}

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.2)
        await app.query_once("hop/a", timeout_s=3.0)
        await asyncio.sleep(0.3)
        assert "a_cid" in seen and "b_cid" in seen
        assert seen["a_cid"] == seen["b_cid"]  # one correlation across the chain
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
