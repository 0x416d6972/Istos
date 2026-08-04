"""Shared test fixtures."""

import os
import uuid

import pytest
import pytest_asyncio

from istos import Istos

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def istos():
    """Istos instance without built-in health/metrics handlers for isolated tests."""
    return Istos(enable_health=False, enable_metrics=False)


@pytest_asyncio.fixture
async def redis_storage():
    """A RedisStoragePlugin against a real server, or skip.

    Run one with: docker run -p 6379:6379 redis:7-alpine
    """
    from istos.consistency.redis_storage import RedisStoragePlugin

    try:
        import redis.asyncio  # noqa: F401
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
