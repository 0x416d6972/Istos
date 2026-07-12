"""Path B — brokerless in-Python persistence for durable pub/sub.

Unit tests exercise the object store and the PersistRole logic without a network;
the integration test proves the writer + history queryable round-trip over a real
Zenoh session (samples survive into the store and replay back through get()).
"""

import asyncio

import pytest
import zenoh

from istos import Istos, InMemoryObjectStore, S3ObjectStore
from istos.communication.persist import (
    ObjectStore,
    PersistRole,
    parse_store_url,
)
from istos.core.subscribe import subscribe_wrapper
from istos.messages.serialization import JsonSerializer


# ---------------------------------------------------------------------------
# 1. Object store
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_inmemory_store_put_and_history_ordered():
    store = InMemoryObjectStore()
    await store.put("k/0002", b"b")
    await store.put("k/0001", b"a")
    hist = await store.history("k")
    assert [payload for _key, payload in hist] == [b"a", b"b"]  # sorted by key


@pytest.mark.asyncio
async def test_inmemory_store_history_prefix_isolation():
    store = InMemoryObjectStore()
    await store.put("orders/created/1", b"x")
    await store.put("orders/shipped/1", b"y")
    assert [p for _k, p in await store.history("orders/created")] == [b"x"]
    assert [p for _k, p in await store.history("orders")] == [b"x", b"y"]


def test_parse_store_url_dispatch():
    assert isinstance(parse_store_url("memory://anything"), InMemoryObjectStore)
    with pytest.raises(ValueError, match="Unsupported persistence URL scheme"):
        parse_store_url("ftp://nope")


def test_s3_store_url_parsing_without_credentials():
    # Constructs (needs aioboto3 from the [s3] extra); no network call yet.
    pytest.importorskip("aioboto3")
    store = S3ObjectStore.from_url("s3://my-bucket/streams")
    assert store._bucket == "my-bucket"
    assert store._prefix == "streams"
    assert store._object_key("orders/1") == "streams/orders/1"


def test_s3_store_url_requires_bucket():
    pytest.importorskip("aioboto3")
    with pytest.raises(ValueError, match="missing a bucket"):
        S3ObjectStore.from_url("s3:///no-bucket")


def test_s3_store_url_parses_endpoint_and_region():
    """MinIO / custom endpoint + region come from the URL query string."""
    pytest.importorskip("aioboto3")
    store = S3ObjectStore.from_url(
        "s3://bkt/pre?endpoint=http://localhost:9000&region=us-east-1"
    )
    assert store._endpoint_url == "http://localhost:9000"
    assert store._region_name == "us-east-1"


def test_s3_store_kwargs_win_over_query():
    pytest.importorskip("aioboto3")
    store = S3ObjectStore.from_url(
        "s3://bkt?region=us-east-1", region_name="eu-west-1"
    )
    assert store._region_name == "eu-west-1"


# ---------------------------------------------------------------------------
# 2. PersistRole logic (no network)
# ---------------------------------------------------------------------------
class _FakeSample:
    def __init__(self, key: str, payload: bytes):
        self.key_expr = key
        self.payload = payload


class _FakeQuery:
    def __init__(self, key: str):
        self.key_expr = key
        self.replies: list = []

    def reply(self, key, payload):
        self.replies.append((str(key), bytes(payload)))


@pytest.mark.asyncio
async def test_role_persist_mints_unique_stream_keys():
    store = InMemoryObjectStore()
    role = PersistRole("drone/telemetry", store)
    await role._persist("drone/telemetry", b"first")
    await role._persist("drone/telemetry", b"second")
    hist = await store.history("drone/telemetry")
    # Two distinct objects retained (log semantics, not last-value-wins).
    assert [p for _k, p in hist] == [b"first", b"second"]
    assert len({k for k, _ in hist}) == 2


@pytest.mark.asyncio
async def test_role_answer_replays_history_to_query():
    store = InMemoryObjectStore()
    role = PersistRole("orders/created", store)
    await role._persist("orders/created", b"e0")
    await role._persist("orders/created", b"e1")

    query = _FakeQuery("orders/created")
    await role._answer(query)
    assert [p for _k, p in query.replies] == [b"e0", b"e1"]
    # Each reply under its own minted sub-key (distinct → no consolidation collapse).
    assert all(k.startswith("orders/created/") for k, _ in query.replies)
    assert len({k for k, _ in query.replies}) == 2


@pytest.mark.asyncio
async def test_role_persist_never_raises_on_store_failure():
    class _Broken(ObjectStore):
        async def put(self, key, payload):
            raise IOError("s3 down")

        async def history(self, prefix):
            return []

    role = PersistRole("k", _Broken())
    await role._persist("k", b"x")  # logged, swallowed — producer keeps running


# ---------------------------------------------------------------------------
# 3. Registration wiring
# ---------------------------------------------------------------------------
def test_publish_persist_registers_role(istos: Istos):
    @istos.publish("orders/created", durable=True, persist="memory://")
    def created():
        return {"ok": True}

    assert len(istos._persist_roles) == 1
    assert istos._persist_roles[0].key_expr == "orders/created"
    assert isinstance(istos._persist_roles[0].store, InMemoryObjectStore)


def test_persist_accepts_ready_store(istos: Istos):
    store = InMemoryObjectStore()
    role = istos.persist("sensors/#", store)
    assert role.store is store
    assert istos._persist_roles == [role]


# ---------------------------------------------------------------------------
# 4. Subscriber-side history replay
# ---------------------------------------------------------------------------
class _FakeReply:
    def __init__(self, key: str, payload: bytes):
        self.ok = _FakeSample(key, payload)


class _FakeGetSession:
    def __init__(self, replies: list):
        self._replies = replies
        self.asked: list = []

    def get(self, selector: str):
        self.asked.append(selector)
        return list(self._replies)


@pytest.mark.asyncio
async def test_replay_history_delivers_in_publish_order():
    seen: list = []
    sw = subscribe_wrapper(
        lambda d: seen.append(d), "orders/created", JsonSerializer(),
        replay_persisted=True,
    )
    # Replies arrive out of order; minted keys sort chronologically.
    session = _FakeGetSession([
        _FakeReply("orders/created/0002", b'"b"'),
        _FakeReply("orders/created/0001", b'"a"'),
    ])
    await sw.replay_history(session)
    assert seen == ["a", "b"]
    assert session.asked == ["orders/created/**"]  # wildcard selector


@pytest.mark.asyncio
async def test_replay_history_empty_is_noop():
    seen: list = []
    sw = subscribe_wrapper(lambda d: seen.append(d), "k", JsonSerializer(),
                           replay_persisted=True)
    await sw.replay_history(_FakeGetSession([]))
    assert seen == []


def test_subscribe_replay_persisted_flag_propagates(istos: Istos):
    @istos.subscribe("orders/created", replay_persisted=True)
    def on_created(data):
        pass

    assert istos._subscribers[0].replay_persisted is True


# ---------------------------------------------------------------------------
# 5. Integration: writer + history queryable over a real Zenoh session
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_persist_writes_and_serves_history_brokerless():
    """Publish samples; the role persists them to the store, and a later get()
    replays them back through the history queryable — no broker, no plugin."""
    session = zenoh.open(zenoh.Config())
    store = InMemoryObjectStore()
    role = PersistRole("istos/test/persist", store)
    try:
        role.bind(session, asyncio.get_running_loop())
        await asyncio.sleep(0.2)

        for i in range(3):
            session.put("istos/test/persist", f'"e{i}"')
        await asyncio.sleep(0.6)  # let the writer persist

        assert len(await store.history("istos/test/persist")) == 3

        # A fresh consumer fetches full history via the queryable (run get() off
        # the loop so the queryable coroutine can answer).
        def _collect():
            out = []
            # Consumers fetch the stream via the wildcard; the queryable replies
            # each historical sample under its own minted key.
            for reply in session.get("istos/test/persist/**"):
                try:
                    out.append(bytes(reply.ok.payload).decode())
                except Exception:
                    pass
            return out

        got = await asyncio.to_thread(_collect)
        assert sorted(got) == ['"e0"', '"e1"', '"e2"']
    finally:
        await role.aclose()
        session.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_subscriber_replays_persisted_history_over_zenoh():
    """A subscriber with replay_persisted=True recovers the full stream from the
    persistence queryable — the producer-crash-durable path, end to end."""
    session = zenoh.open(zenoh.Config())
    store = InMemoryObjectStore()
    role = PersistRole("istos/test/subreplay", store)
    seen: list = []
    sw = subscribe_wrapper(
        lambda d: seen.append(d), "istos/test/subreplay", JsonSerializer(),
        replay_persisted=True,
    )
    try:
        role.bind(session, asyncio.get_running_loop())
        await asyncio.sleep(0.2)

        # Producer publishes, then "goes away" — data lives only in the store.
        for i in range(3):
            session.put("istos/test/subreplay", f'"e{i}"')
        await asyncio.sleep(0.6)

        await sw.replay_history(session)
        await asyncio.sleep(0.3)

        assert sorted(seen) == ["e0", "e1", "e2"]
    finally:
        await role.aclose()
        session.close()
