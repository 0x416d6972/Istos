"""Token-bucket rate limiting middleware.

``RateLimitError`` (429) already existed; this enforces it. Each key gets a
bucket that refills at ``rate`` tokens per ``per`` seconds up to ``burst``; a
request costs one token, and an empty bucket raises ``RateLimitError``.
"""

import asyncio
import time
from typing import Callable, Dict, Optional, Tuple

from istos.core.errors import RateLimitError
from istos.middleware.base import HandlerCallable, RequestScope


def _default_key(scope: RequestScope) -> str:
    """Limit per authenticated identity; unauthenticated requests share one bucket."""
    principal = scope.context.principal
    if principal is None:
        return "anonymous"
    return str(getattr(principal, "id", None) or principal)


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

    async def __call__(self, scope: RequestScope, call_next: HandlerCallable) -> object:
        key = self._key(scope)
        async with self._lock:
            now = time.monotonic()
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
