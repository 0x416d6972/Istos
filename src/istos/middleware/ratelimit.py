"""Token-bucket rate limiting middleware.

``RateLimitError`` (429) already existed; this enforces it. Each key gets a
bucket that refills at ``rate`` tokens per ``per`` seconds up to ``burst``; a
request costs one token, and an empty bucket raises ``RateLimitError``.
"""

import asyncio
import time
from typing import Callable, Dict, Optional, Tuple

from istos.errors import RateLimitError
from istos.middleware.base import HandlerCallable, RequestScope


def _default_key(scope: RequestScope) -> str:
    """Limit per authenticated identity.

    Unauthenticated requests share one bucket per endpoint, not one bucket for
    the whole process. A principal with no usable id (an empty JWT ``sub``)
    is anonymous too — ``str(principal)`` would mint a fresh full bucket for
    every distinct claims blob.
    """
    principal = scope.context.principal
    if isinstance(principal, str) and principal:
        return principal
    ident = getattr(principal, "id", None) if principal is not None else None
    if ident:
        return str(ident)
    return f"anonymous:{scope.prefix}"


class RateLimitMiddleware:
    """Limit requests per key with a token bucket.

        app.add_middleware(RateLimitMiddleware(rate=10, per=1.0))          # 10/s per identity
        app.add_middleware(RateLimitMiddleware(rate=100, per=60,
                                               key=lambda s: s.prefix))    # 100/min per endpoint

    ``burst`` is the bucket capacity (defaults to ``rate``); it caps how many
    requests can arrive at once before the steady rate applies.
    """

    def __init__(
        self,
        rate: float,
        per: float = 1.0,
        *,
        burst: Optional[float] = None,
        key: Optional[Callable[[RequestScope], str]] = None,
    ) -> None:
        if rate <= 0 or per <= 0:
            raise ValueError("rate and per must be positive")
        self.rate = rate
        self.per = per
        self.burst = float(burst if burst is not None else rate)
        self._key = key or _default_key
        self._buckets: Dict[str, Tuple[float, float]] = {}
        self._lock = asyncio.Lock()
        self._idle_s = max(60.0, per * 10)

    def _evict(self, now: float) -> None:
        if len(self._buckets) < 1024:
            return
        stale = [
            key
            for key, (tokens, last) in self._buckets.items()
            if now - last >= self._idle_s and tokens >= self.burst
        ]
        for key in stale:
            del self._buckets[key]

    async def __call__(self, scope: RequestScope, call_next: HandlerCallable) -> object:
        key = self._key(scope)
        async with self._lock:
            now = time.monotonic()
            self._evict(now)
            tokens, last = self._buckets.get(key, (self.burst, now))
            tokens = min(self.burst, tokens + (now - last) * (self.rate / self.per))
            if tokens < 1.0:
                # Whole periods until the next token is available.
                retry_after = round((1.0 - tokens) * (self.per / self.rate), 3)
                self._buckets[key] = (tokens, now)
                raise RateLimitError(
                    f"Rate limit exceeded for {key!r}",
                    details={"retry_after": retry_after},
                )
            self._buckets[key] = (tokens - 1.0, now)
        return await call_next(scope)
