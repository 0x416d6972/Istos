"""Regression tests for the framework review fixes (one test per defect)."""

import asyncio
import socket
import time
import warnings

import aiohttp
import pytest

from istos import Depends, Istos, current_principal
from istos.consistency.storage import InMemoryStoragePlugin
from istos.context import peek_request_context
from istos.discovery.asyncapi import get_function_schemas
from istos.errors import (
    ConflictError,
    IstosSecurityWarning,
    ReplyDecodeError,
    is_retryable,
)
from istos.http.health import HealthState
from istos.http.mcp import MCPServer
from istos.messages.serialization import JsonSerializer
from istos.middleware.ratelimit import RateLimitMiddleware
from istos.primitives.channel_fabric import FabricChannelServer, _ServerSession
from istos.primitives.clients import stream_client_wrapper
from istos.primitives.query import QueryResult, query_wrapper
from istos.primitives.stream import stream_wrapper
from istos.queue.store import JobState, QueueStore
from istos.queue.worker import worker_wrapper
from istos.retry import RetryPolicy
from istos.security.authz import Principal, TokenAuthorizer, require_roles
from istos.testing import IstosTestClient
from istos.validation import validate_params


class _Selector:
    def __init__(self, key, params=None):
        self.key_expr = key
        self.parameters = params or {}


class _Query:
    def __init__(self, key, params=None, attachment=None):
        self.selector = _Selector(key, params)
        self.attachment = attachment
        self.replies = []

    def reply(self, key, payload):
        self.replies.append((key, payload))

    def drop(self):
        pass


class _Session:
    def __init__(self):
        self.declared = []

    def declare_queryable(self, key, cb, complete=True):
        self.declared.append(key)
        return self

    def declare_subscriber(self, key, cb, **kw):
        self.declared.append(key)
        return self

    def liveliness(self):
        return self

    def undeclare(self):
        pass


def _handler(app, prefix):
    return next(h for h in app._handlers if h.prefix == prefix)


# --- @handle ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_void_handler_still_replies(istos):
    @istos.handle("jobs/kick")
    async def kick() -> None:
        return None

    query = _Query("jobs/kick")
    await _handler(istos, "jobs/kick").on_query(query)
    assert len(query.replies) == 1
    assert JsonSerializer().deserialize(query.replies[0][1]) is None


class _MarkFails(InMemoryStoragePlugin):
    async def mark_processed(self, idempotency_key, result):
        raise RuntimeError("ledger down")


@pytest.mark.asyncio
async def test_exactly_once_does_not_rerun_when_the_ledger_write_fails():
    app = Istos(storage=_MarkFails(), enable_health=False, enable_metrics=False)
    runs = []

    @app.handle("pay/charge", durability="exactly_once")
    async def charge(amount: int) -> dict:
        runs.append(amount)
        return {"ok": True}

    assert await charge(amount=5) == {"ok": True}
    # The claim stays held: a retry is told to wait, it does not charge again.
    with pytest.raises(ConflictError):
        await charge(amount=5)
    assert runs == [5]


def test_idempotency_key_counts_defaults(istos):
    @istos.handle("robot/move")
    async def move(distance: int = 10) -> int:
        return distance

    assert move._bound_params((), {}) == move._bound_params((), {"distance": 10})


def test_unresolvable_forward_ref_does_not_fail_decoration(istos):
    @istos.handle("odd/types")
    async def odd(x: "NotDefinedAnywhere") -> "AlsoMissing":  # type: ignore[name-defined]  # noqa: F821
        return x

    assert odd._return_type is None


# --- methods are refused on the network path -----------------------------------

@pytest.mark.asyncio
async def test_method_handlers_are_refused_at_bind(istos):
    class Robot:
        @istos.handle("robot/arm")
        async def arm(self, angle: int) -> int:
            return angle

    session = _Session()
    with pytest.raises(TypeError, match="taking 'self'"):
        await istos._bind_handlers(session)
    assert session.declared == []
    # In-process through an instance still works.
    assert await Robot().arm(3) == 3


@pytest.mark.asyncio
async def test_method_streams_channels_and_subscribers_are_refused(istos):
    class Svc:
        @istos.stream("svc/s")
        async def s(self):
            yield 1

    with pytest.raises(TypeError):
        await istos._bind_streams(_Session())

    app = Istos(enable_health=False, enable_metrics=False)

    class Chat:
        @app.channel("svc/chat")
        async def chat(self, session):
            pass

    with pytest.raises(TypeError):
        await app._bind_channels(_Session())

    app2 = Istos(enable_health=False, enable_metrics=False)

    class Sub:
        @app2.subscribe("svc/events")
        async def on(self, data):
            pass

    with pytest.raises(TypeError):
        await app2._bind_subscribers(_Session())


# --- validation and schemas ------------------------------------------------

def test_untyped_param_with_default_is_optional():
    def f(a: int, b=5):
        return a, b

    assert validate_params(f, {"a": "1"}) == {"a": 1, "b": 5}


def test_schema_leaves_out_injected_params():
    def principal_dep():
        return None

    async def h(x: int, db=None, who=Depends(principal_dep)) -> int:
        return x

    schema = get_function_schemas(h)["payload_schema"]
    assert set(schema["properties"]) == {"x"}


# --- db injection on @stream / @channel -----------------------------------

@pytest.mark.asyncio
async def test_stream_receives_db_from_storage_not_the_network():
    storage = InMemoryStoragePlugin()
    seen = []

    async def gen(n: int, db=None):
        seen.append(db)
        yield n

    wrapper = stream_wrapper(gen, "s/db", JsonSerializer(), storage=storage)
    query = _Query("s/db", {"n": "2"})
    await wrapper.on_query(query)
    assert seen == [storage]
    assert JsonSerializer().deserialize(query.replies[0][1]) == 2


@pytest.mark.asyncio
async def test_channel_receives_db():
    storage = InMemoryStoragePlugin()
    app = Istos(storage=storage, enable_health=False, enable_metrics=False)
    seen = []

    @app.channel("c/db")
    async def chat(session, db=None):
        seen.append(db)

    from istos.primitives.channel import ChannelSession

    async def sink(raw):
        pass

    wrapper = app._channels[0]
    await wrapper.run(ChannelSession(JsonSerializer(), sink), attachment=None, params={})
    assert seen == [storage]


# --- queue -----------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_worker_cannot_ack_a_reclaimed_job():
    s = QueueStore("q")
    jid = await s.add(b"x", max_attempts=5)
    first = await s.claim(lease_s=0.01)
    await asyncio.sleep(0.02)
    second = await s.claim(lease_s=30)  # lease lapsed → reclaimed and re-leased
    assert second is not None and second.id == jid and second.attempts == 2

    assert await s.ack(jid, attempt=first.attempts) is False
    assert await s.nack(jid, attempt=first.attempts, error="late") is None
    assert (await s.stats())[JobState.LEASED.value] == 1
    assert await s.ack(jid, attempt=second.attempts) is True


@pytest.mark.asyncio
async def test_ack_of_an_unleased_job_is_refused():
    s = QueueStore("q")
    jid = await s.add(b"x", max_attempts=5)
    assert await s.ack(jid) is False
    assert (await s.stats())[JobState.READY.value] == 1


@pytest.mark.asyncio
async def test_dead_letters_outlive_the_result_ttl():
    s = QueueStore("q", result_ttl_s=1.0, dead_letter_ttl_s=100.0)
    jid = await s.add(b"x", max_attempts=1)
    await s.claim(lease_s=30)
    assert await s.nack(jid, error="boom") == "dead"
    s._jobs[jid].completed_at = time.time() - 50  # past result TTL, inside DLQ TTL
    await s.sweep()
    assert [r.id for r in await s.dead_letters()] == [jid]
    s._jobs[jid].completed_at = time.time() - 200
    await s.sweep()
    assert await s.dead_letters() == []


@pytest.mark.asyncio
async def test_unfinished_chord_expires():
    s = QueueStore("q", chord_ttl_s=10.0)
    assert await s.chord_report("c1", 0, 2, None) is None
    s._chords["c1"]["created_at"] = time.time() - 60
    await s.sweep()
    assert "c1" not in s._chords


def test_worker_rejects_zero_poll_interval():
    async def body(job):
        return job

    with pytest.raises(ValueError):
        worker_wrapper(body, "q", poll_interval_s=0, serializer=JsonSerializer())


# --- @subscribe dedup ------------------------------------------------------

@pytest.mark.asyncio
async def test_failed_delivery_is_not_recorded_as_seen():
    app = Istos(enable_health=False, enable_metrics=False)
    attempts = []

    @app.subscribe("ev/x", dedup=True)
    async def on(data):
        attempts.append(data)
        if len(attempts) == 1:
            raise RuntimeError("transient")

    sub = app._subscribers[0]
    raw = JsonSerializer().serialize({"n": 1})
    with pytest.raises(RuntimeError):
        await sub._deliver(raw)
    await sub._deliver(raw)  # the redelivery must run
    await sub._deliver(raw)  # now it is a duplicate
    assert len(attempts) == 2


# --- @query ----------------------------------------------------------------

def test_decode_errors_are_not_retryable():
    assert is_retryable(ReplyDecodeError()) is False


@pytest.mark.asyncio
async def test_query_does_not_resend_on_decode_error_or_callback_error():
    calls = []

    def make(payload):
        def _blocking(session, selector):
            calls.append(selector)
            return [QueryResult("k", payload, JsonSerializer())]
        return _blocking

    def boom(result):
        raise ValueError("callback bug")

    q = query_wrapper(boom, "k", JsonSerializer(), get_session=lambda: object(), retry=3)
    q._blocking_query = make(b"1")  # type: ignore[method-assign]
    with pytest.raises(ValueError):
        await q()
    assert len(calls) == 1

    calls.clear()
    q2 = query_wrapper(lambda r: r, "k", JsonSerializer(), get_session=lambda: object(), retry=3)
    q2._blocking_query = make(b"\xff\xfe")  # type: ignore[method-assign]
    with pytest.raises(ReplyDecodeError):
        await q2()
    assert len(calls) == 1


def test_negative_retries_are_refused():
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)


# --- channels and stream clients ------------------------------------------

@pytest.mark.asyncio
async def test_teardown_cancels_a_handler_that_is_not_in_receive():
    loop = asyncio.get_running_loop()

    class _Wrapper:
        prefix = "c/busy"
        serializer = JsonSerializer()

    server = FabricChannelServer(_Session(), _Wrapper(), loop, cancel_grace_s=0.01)

    class _Chan:
        def close(self):
            pass

    busy = asyncio.ensure_future(asyncio.sleep(3600))
    state = _ServerSession(_Chan(), _Session())
    state.task = busy
    server._sessions["sid"] = state
    server._teardown("sid")
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(busy, timeout=1.0)


@pytest.mark.asyncio
async def test_stream_client_closes_the_iterator_on_early_exit():
    closed = []

    class _App:
        def stream_query(self, prefix, **kw):
            async def gen():
                try:
                    for i in range(100):
                        yield i
                finally:
                    closed.append(True)
            return gen()

    async def body(chunks):
        async for c in chunks:
            return c

    client = stream_client_wrapper(body, _App(), "s/x", serializer=None, timeout_s=1.0)
    assert await client() == 0
    assert closed == [True]


# --- rate limiting ---------------------------------------------------------

@pytest.mark.asyncio
async def test_exhausted_idle_buckets_are_evicted():
    rl = RateLimitMiddleware(rate=1, per=1.0)
    old = time.monotonic() - 3600
    for i in range(1100):
        rl._buckets[f"k{i}"] = (0.0, old)  # exhausted, long idle
    rl._evict(time.monotonic())
    assert len(rl._buckets) == 0


# --- auth ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_mapped_to_principal_satisfies_require_roles():
    from istos.security.authz import AuthContext, check_authorized, combine_authorizers
    from istos.context import RequestEnvelope

    app_auth = TokenAuthorizer({"t1": Principal("ops", roles=frozenset({"admin"}))})
    gate = combine_authorizers(app_auth, require_roles("admin"))
    ctx = AuthContext(
        prefix="admin/reset", key_expr="admin/reset",
        attachment=RequestEnvelope(token="t1").to_attachment(),
    )
    principal = await check_authorized(gate, ctx)
    assert principal.id == "ops"


@pytest.mark.asyncio
async def test_mcp_lists_only_callable_tools_and_rejects_collisions():
    app = Istos(authorizer=TokenAuthorizer("good"), enable_health=False, enable_metrics=False)

    @app.handle("a/b")
    async def ab() -> int:
        return 1

    server = MCPServer(app)
    denied = await server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, token="bad")
    assert denied["result"]["tools"] == []
    allowed = await server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, token="good")
    assert [t["name"] for t in allowed["result"]["tools"]] == ["a-b"]

    @app.handle("a.b")  # also maps to "a-b"
    async def a_dot_b() -> int:
        return 2

    with pytest.raises(ValueError, match="claimed by both"):
        server.validate()


# --- HTTP surface ----------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_http_body_limit_and_open_gateway_warning():
    port = _free_port()
    app = Istos(http_port=port, http_max_body_bytes=1024,
                enable_health=False, enable_metrics=False)

    @app.handle("echo", http=True)
    async def echo(x: str = "") -> str:
        return x

    with pytest.warns(IstosSecurityWarning, match="no app-wide authorizer"):
        runner = await app._start_http_server()
    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"http://127.0.0.1:{port}/echo", data="{" + "x" * 4096 + "}",
                headers={"Content-Type": "application/json"},
            ) as resp:
                assert resp.status == 413
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_readiness_does_not_leak_exception_text():
    checker = HealthState()
    checker.ready = True

    async def db_check():
        raise RuntimeError("postgres://admin:hunter2@db:5432 refused")

    checker.checks["db"] = db_check
    result = await checker.readiness()
    assert "hunter2" not in str(result)
    assert result["checks"]["db"]["error"] == "RuntimeError"


# --- TestClient ------------------------------------------------------------

@pytest.mark.asyncio
async def test_testclient_context_does_not_leak_and_publish_carries_principal():
    app = Istos(
        authorizer=TokenAuthorizer({"t": Principal("alice")}),
        enable_health=False, enable_metrics=False,
    )
    seen = []

    @app.handle("who")
    async def who(p=Depends(current_principal)) -> str:
        return p.id

    @app.subscribe("ev")
    async def on(data, p=Depends(current_principal)):
        seen.append(p.id if p else None)

    client = IstosTestClient(app)
    assert await client.query("who", token="t") == "alice"
    assert peek_request_context() is None
    await client.publish("ev", {"a": 1}, token="t")
    assert seen == ["alice"]
    assert peek_request_context() is None


# --- shutdown drain --------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_waits_for_in_flight_requests():
    app = Istos(enable_health=False, enable_metrics=False, shutdown_grace_s=1.0)
    loop = asyncio.get_running_loop()
    done = []

    async def work():
        await asyncio.sleep(0.05)
        done.append(True)

    app._submit(loop, work())
    await asyncio.sleep(0)
    await app._drain_inflight()
    assert done == [True]

    app._shutdown_grace_s = 0.05

    async def stuck():
        await asyncio.sleep(3600)

    app._submit(loop, stuck())
    await asyncio.sleep(0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        await app._drain_inflight()
    await asyncio.sleep(0.01)
    assert app._inflight == set()
