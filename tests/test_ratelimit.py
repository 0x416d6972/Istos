"""RateLimitMiddleware — token-bucket enforcement of RateLimitError."""

import asyncio

import pytest

from istos.context import RequestContext
from istos.core.errors import RateLimitError
from istos.middleware import RateLimitMiddleware
from istos.middleware.base import RequestScope


def _scope(principal=None, prefix="op"):
    return RequestScope(
        prefix=prefix, operation="handle",
        context=RequestContext(principal=principal),
    )


async def _ok(scope):
    return "ok"


@pytest.mark.asyncio
async def test_allows_within_burst_then_blocks():
    mw = RateLimitMiddleware(rate=100, per=1.0, burst=3)
    scope = _scope(principal="alice")
    assert [await mw(scope, _ok) for _ in range(3)] == ["ok", "ok", "ok"]
    with pytest.raises(RateLimitError) as ei:
        await mw(scope, _ok)
    assert "retry_after" in ei.value.details


@pytest.mark.asyncio
async def test_refills_over_time():
    mw = RateLimitMiddleware(rate=50, per=1.0, burst=1)  # ~1 token per 20ms
    scope = _scope(principal="bob")
    assert await mw(scope, _ok) == "ok"
    with pytest.raises(RateLimitError):
        await mw(scope, _ok)
    await asyncio.sleep(0.05)  # enough to refill a token
    assert await mw(scope, _ok) == "ok"


@pytest.mark.asyncio
async def test_buckets_are_per_key():
    mw = RateLimitMiddleware(rate=100, per=1.0, burst=1)
    await mw(_scope(principal="alice"), _ok)
    # bob has his own bucket, unaffected by alice
    assert await mw(_scope(principal="bob"), _ok) == "ok"


@pytest.mark.asyncio
async def test_custom_key_per_prefix():
    mw = RateLimitMiddleware(rate=100, per=1.0, burst=1, key=lambda s: s.prefix)
    await mw(_scope(prefix="a"), _ok)
    with pytest.raises(RateLimitError):
        await mw(_scope(prefix="a"), _ok)
    assert await mw(_scope(prefix="b"), _ok) == "ok"  # different endpoint


@pytest.mark.asyncio
async def test_rejects_bad_config():
    with pytest.raises(ValueError):
        RateLimitMiddleware(rate=0)
