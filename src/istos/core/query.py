import asyncio
import inspect
import zenoh
from typing import Any, Callable, List, Optional, Union

from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.retry import RetryPolicy, execute_with_retry


class QueryResult:
    """Holds a single reply from a Zenoh query."""
    def __init__(self, key: str, raw_payload: bytes, serializer: Serialize):
        self.key = key
        self.raw_payload = raw_payload
        self._serializer = serializer

    def decode(self) -> Any:
        """Deserialize the raw payload."""
        return self._serializer.deserialize(self.raw_payload)

    def __repr__(self) -> str:
        return f"QueryResult(key={self.key!r})"


class bound_query_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "query_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class query_wrapper:
    """
    Descriptor that replaces the original function.
    On every call it queries Zenoh for data at the registered prefix,
    deserializes the reply, and passes it as the first argument to the
    decorated function.
    """
    def __init__(
        self,
        func: Callable,
        prefix: str,
        serializer: Serialize,
        get_session: Callable[[], Optional[zenoh.Session]],
        timeout_s: float = 5.0,
        retry: Optional[Union[int, RetryPolicy]] = None,
    ):
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self._get_session = get_session
        self.timeout_s = timeout_s
        self.calls = 0

        # Normalize retry parameter
        if retry is None:
            self.retry_policy = RetryPolicy(max_retries=0)
        elif isinstance(retry, int):
            self.retry_policy = RetryPolicy.from_int(retry)
        else:
            self.retry_policy = retry

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1

        session = self._get_session()
        if session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run() or istos.run_async() first."
            )

        # Build selector with kwargs
        import urllib.parse
        selector = self.prefix
        if kwargs:
            query_string = urllib.parse.urlencode(kwargs)
            selector = f"{selector}?{query_string}"
            # Consume kwargs so they aren't passed to the decorated func
            kwargs = {}

        async def _do_query():
            # Query Zenoh on a background thread (session.get is blocking)
            results: List[QueryResult] = await asyncio.to_thread(
                self._blocking_query, session, selector
            )

            # Decode the first result (most common case) or pass the full list
            decoded = [r.decode() for r in results]
            data = decoded[0] if len(decoded) == 1 else decoded

            # Pass the queried data after any bound instance args (e.g. self)
            # args may contain 'self' injected by bound_query_wrapper
            if inspect.iscoroutinefunction(self.func):
                return await self.func(*args, data, **kwargs)
            else:
                return self.func(*args, data, **kwargs)

        return await execute_with_retry(_do_query, self.retry_policy)

    def _blocking_query(self, session: zenoh.Session, selector: str) -> List[QueryResult]:
        """Synchronous Zenoh get — runs inside asyncio.to_thread."""
        results: List[QueryResult] = []
        replies = session.get(selector, timeout=self.timeout_s)

        for reply in replies:
            if reply.ok is not None:
                sample = reply.ok
                key = str(sample.key_expr)
                raw = bytes(sample.payload)
                results.append(QueryResult(key, raw, self.serializer))

        return results

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_query_wrapper(self, instance)
