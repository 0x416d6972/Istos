"""Redis storage plugin — exactly-once ledger against a real server.

Skips cleanly when the ``redis`` package is missing or no server answers on
``REDIS_URL`` (default ``redis://localhost:6379/15``, a scratch DB we flush).
Run against a server with::

    docker run -p 6379:6379 redis:7
    uv run pytest tests/test_redis_storage.py -q
"""

import os
import uuid

import pytest
import pytest_asyncio

from istos.consistency.redis_storage import RedisStoragePlugin
from istos.consistency.storage import Durability
from istos.core.handler import handler_wrapper
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
