import asyncio
import inspect
import zenoh
from typing import Any, Callable, List, Optional, Union

from istos.messages.serialization import Serialize
from istos.retry import RetryPolicy, execute_with_retry
from istos.context import RequestEnvelope, peek_request_context
from istos.di.depends import has_dependencies, invoke_with_dependencies, positional_param_names


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
        attachment: Optional[bytes] = None,
        dependency_overrides: Optional[dict] = None,
    ):
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self._get_session = get_session
        self.timeout_s = timeout_s
        self._attachment = attachment
        self.calls = 0

        if retry is None:
            self.retry_policy = RetryPolicy(max_retries=0)
        elif isinstance(retry, int):
            self.retry_policy = RetryPolicy.from_int(retry)
        else:
            self.retry_policy = retry

        # Reply fills the first positional; Depends fill the rest.
        self._has_depends = has_dependencies(func)
        self._skip_names = tuple(positional_param_names(func)[:1])
        self._dependency_overrides = dependency_overrides if dependency_overrides is not None else {}

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1

        zenoh_session = self._get_session()
        if zenoh_session is None:
            raise RuntimeError(
                "No active Zenoh session. Queries run over the service's shared "
                "session — start it with istos.run()/run_async() first."
            )

        import urllib.parse
        selector = self.prefix
        query_kwargs = dict(kwargs)
        if query_kwargs:
            # Zenoh separators are ';' — not '&'.
            query_string = ";".join(
                f"{urllib.parse.quote(str(k))}={urllib.parse.quote(str(v))}"
                for k, v in query_kwargs.items()
            )
            selector = f"{selector}?{query_string}"
            for k in query_kwargs:
                kwargs.pop(k)

        async def _do_query():
            # session.get blocks — keep it off the event loop.
            results: List[QueryResult] = await asyncio.to_thread(
                self._blocking_query, zenoh_session, selector
            )

            decoded = [r.decode() for r in results]
            data = decoded[0] if len(decoded) == 1 else decoded

            # args may already hold `self` from bound_query_wrapper.
            if self._has_depends:
                return await invoke_with_dependencies(
                    self.func, args=(*args, data), skip_names=self._skip_names,
                    overrides=self._dependency_overrides,
                )
            if inspect.iscoroutinefunction(self.func):
                return await self.func(*args, data, **kwargs)
            else:
                return await asyncio.to_thread(self.func, *args, data, **kwargs)

        return await execute_with_retry(_do_query, self.retry_policy)

    def _outbound_attachment(self) -> Optional[bytes]:
        """Build the attachment: the caller's token plus — when this query runs
        inside a request — the ambient correlation_id and traceparent, so the
        logical operation stays linked across hops."""
        token = RequestEnvelope.from_attachment(self._attachment).token
        ctx = peek_request_context()
        env = RequestEnvelope(
            token=token,
            correlation_id=ctx.correlation_id if ctx else None,
            traceparent=ctx.traceparent if ctx else None,
        )
        return env.to_attachment()

    def _blocking_query(self, session: zenoh.Session, selector: str) -> List[QueryResult]:
        """Synchronous Zenoh get — runs inside asyncio.to_thread."""
        results: List[QueryResult] = []
        get_kwargs: dict = {"timeout": self.timeout_s}
        attachment = self._outbound_attachment()
        if attachment is not None:
            get_kwargs["attachment"] = attachment
        replies = session.get(selector, **get_kwargs)

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
