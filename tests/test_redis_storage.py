"""Redis storage plugin against a real server.

Skips when redis isn't installed or no server answers on REDIS_URL
(default redis://localhost:6379/15). Run one with: docker run -p 6379:6379 redis:7
"""

import asyncio
import os
import uuid

import pytest
import pytest_asyncio

from istos.consistency.redis_storage import RedisStoragePlugin
from istos.consistency.storage import ClaimState, Durability
from istos.errors import ConflictError
from istos.primitives.handler import handler_wrapper
from istos.messages.serialization import JsonSerializer

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")


@pytest_asyncio.fixture
async def redis_store():
    try:
        import redis.asyncio as aioredis  # noqa: F401
    except ImportError:
        pytest.skip("redis not installed")

    # Unique prefix per test so parallel runs / leftovers never collide.
    store = RedisStoragePlugin(url=REDIS_URL, prefix=f"istos-test:{uuid.uuid4().hex}:")
    try:
        client = await store._get_client()
        await client.ping()
    except Exception:
        pytest.skip(f"no redis server at {REDIS_URL}")
    try:
        yield store
    finally:
        await store.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_exactly_once_dedups(redis_store):
    calls = []

    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    h = handler_wrapper(
        double, prefix="math/double", storage=redis_store,
        serializer=JsonSerializer(), durability=Durability.EXACTLY_ONCE,
    )
    r1 = await h(x=21)
    r2 = await h(x=21)  # redelivery -> served from the Redis ledger
    assert r1 == r2 == {"result": 42}
    assert calls == [21]

    log = await redis_store.get_log("math/double")
    assert len(log) == 1  # logged exactly once despite two calls


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_kv_roundtrip(redis_store):
    await redis_store.put("k", {"a": 1})
    assert await redis_store.get("k") == {"a": 1}
    await redis_store.delete("k")
    assert await redis_store.get("k") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_claim_is_exclusive_and_leased(redis_store):
    assert (await redis_store.claim_processed("k", lease_s=0.1)).state is ClaimState.CLAIMED
    assert (await redis_store.claim_processed("k", lease_s=0.1)).state is ClaimState.IN_FLIGHT
    assert await redis_store.check_processed("k") is None   # claimed is not finished

    await asyncio.sleep(0.15)                               # the owner died
    assert (await redis_store.claim_processed("k", lease_s=30)).state is ClaimState.CLAIMED

    await redis_store.mark_processed("k", {"v": 1})
    done = await redis_store.claim_processed("k", lease_s=0.0)
    assert done.state is ClaimState.DONE and done.result == {"v": 1}

    # DONE is terminal: the TTL is dropped, and neither release nor a second
    # mark undoes a result.
    await redis_store.release_claim("k")
    await redis_store.mark_processed("k", {"v": 2})
    await asyncio.sleep(0.15)
    assert (await redis_store.claim_processed("k")).result == {"v": 1}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_exactly_once_under_concurrent_redelivery(redis_store):
    """Eight simultaneous redeliveries of one request, one execution."""
    calls = []

    async def charge(order_id: str):
        calls.append(order_id)
        await asyncio.sleep(0.05)
        return {"charged": order_id}

    h = handler_wrapper(
        charge, prefix="pay/charge", storage=redis_store,
        serializer=JsonSerializer(), durability=Durability.EXACTLY_ONCE,
    )
    results = await asyncio.gather(
        *(h(order_id="o1") for _ in range(8)), return_exceptions=True
    )

    assert calls == ["o1"]
    ok = [r for r in results if not isinstance(r, BaseException)]
    conflicts = [r for r in results if isinstance(r, ConflictError)]
    assert len(ok) + len(conflicts) == 8
    assert all(r == {"charged": "o1"} for r in ok)
    assert await h(order_id="o1") == {"charged": "o1"}   # cached, still one call
    assert calls == ["o1"]
    assert len(await redis_store.get_log("pay/charge")) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_record_written_before_claims_reads_as_done(redis_store):
    """An older Istos stored a bare result with no claim tag."""
    client = await redis_store._get_client()
    await client.set(redis_store._idemp_key("legacy"), b'{"old": true}')

    legacy = await redis_store.claim_processed("legacy")
    assert legacy.state is ClaimState.DONE and legacy.result == {"old": True}
    assert await redis_store.check_processed("legacy") == {"old": True}
