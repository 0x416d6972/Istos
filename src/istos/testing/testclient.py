"""In-process test client for handlers, streams and subscribers — no network."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, List, Optional

from istos.app import Istos
from istos.context import RequestContext, RequestEnvelope, set_request_context
from istos.security.authz import AuthContext, check_authorized
from istos.primitives.channel import ChannelClosed, ChannelSession
from istos.validation import validate_params
from istos.di.depends import resolve_dependencies


class IstosTestClient:
    """
    Invoke handlers, streams and subscribers in-process. The authorizer gate,
    validation, DI and durability ledger all still run.

        istos = Istos()

        @istos.handle("robot/move")
        async def move(distance: int):
            return {"moved": distance}

        client = IstosTestClient(istos)
        assert await client.query("robot/move", distance=10) == {"moved": 10}

    Pass ``token=`` to drive an authorizer; a denied request raises
    ``UnauthorizedError``.
    """

    def __init__(self, app: Istos) -> None:
        self.app = app

    def _find_handler(self, prefix: str) -> Any:
        for handler in self.app._handlers:
            if handler.prefix == prefix:
                return handler
        raise KeyError(f"No handler registered for prefix: {prefix!r}")

    def _find_stream(self, prefix: str) -> Any:
        for wrapper in self.app._streams:
            if wrapper.prefix == prefix:
                return wrapper
        raise KeyError(f"No stream registered for prefix: {prefix!r}")

    def _find_subscribers(self, prefix: str) -> List[Any]:
        return [s for s in self.app._subscribers if s.prefix == prefix]

    def _find_channel(self, prefix: str) -> Any:
        for wrapper in self.app._channels:
            if wrapper.prefix == prefix:
                return wrapper
        raise KeyError(f"No channel registered for prefix: {prefix!r}")

    async def _gate(self, wrapper: Any, prefix: str, params: dict, token: Optional[str]) -> None:
        """Run the authorizer and set a request context so the body can inject
        the resolved principal/token. A denial raises ``UnauthorizedError``."""
        attachment = RequestEnvelope(token=token).to_attachment() if token is not None else None
        principal = await check_authorized(
            getattr(wrapper, "_authorizer", None),
            AuthContext(prefix=prefix, key_expr=prefix, params=params, attachment=attachment),
        )
        set_request_context(RequestContext(
            prefix=prefix, principal=principal, attachment=attachment,
        ))

    async def query(self, prefix: str, token: Optional[str] = None, **kwargs: Any) -> Any:
        """Invoke a handler in-process."""
        handler = self._find_handler(prefix)
        # db / Depends(...) are injected by the handler, not validated here.
        skip = getattr(handler, "_injected_params", None)
        validated = validate_params(handler.func, kwargs, skip_params=skip)
        validated.pop("db", None)
        validated.pop("session", None)
        await self._gate(handler, prefix, validated, token)
        return await handler(**validated)

    async def stream(
        self, prefix: str, token: Optional[str] = None, **kwargs: Any
    ) -> AsyncIterator[Any]:
        """Consume a ``@stream`` handler in-process, yielding each chunk.

            async for chunk in client.stream("llm/generate", prompt="hi"):
                ...
        """
        wrapper = self._find_stream(prefix)
        skip = getattr(wrapper, "_injected_params", None)
        validated = validate_params(wrapper.func, kwargs, skip_params=skip)
        validated.pop("db", None)
        await self._gate(wrapper, prefix, validated, token)

        async with AsyncExitStack() as di_stack:
            call_kwargs = dict(validated)
            if getattr(wrapper, "_has_depends", False):
                call_kwargs = await resolve_dependencies(
                    wrapper.func, call_kwargs, di_stack, cache={},
                    overrides=getattr(wrapper, "_dependency_overrides", {}),
                )
            agen = wrapper.func(**call_kwargs)
            try:
                async for chunk in agen:
                    yield chunk
            finally:
                if hasattr(agen, "aclose"):
                    await agen.aclose()

    async def publish(self, prefix: str, data: Any) -> None:
        """Deliver data to all matching subscribers in-process."""
        subscribers = self._find_subscribers(prefix)
        if not subscribers:
            raise KeyError(f"No subscribers registered for prefix: {prefix!r}")
        for sub in subscribers:
            await sub(data)

    def channel(
        self, prefix: str, token: Optional[str] = None,
        conversation_id: Optional[str] = None, **params: Any,
    ) -> "_TestChannel":
        """Open a ``@channel`` in-process and return a duplex handle. Use it as an
        async context manager; ``send`` / ``receive`` / ``async for`` from the
        caller's side while the handler runs::

            async with client.channel("agent/chat") as chan:
                await chan.send("hi")
                assert await chan.receive() == {"role": "system", "text": "ready"}

        The authorizer, validation and DI run exactly as on the WebSocket/fabric
        transport; a denied ``token`` raises ``UnauthorizedError`` on enter.
        """
        wrapper = self._find_channel(prefix)
        return _TestChannel(wrapper, token=token, conversation_id=conversation_id, params=params)

    def run_query(self, prefix: str, **kwargs: Any) -> Any:
        """Synchronous wrapper around query()."""
        return asyncio.run(self.query(prefix, **kwargs))

    def run_publish(self, prefix: str, data: Any) -> None:
        """Synchronous wrapper around publish()."""
        return asyncio.run(self.publish(prefix, data))


_DONE = object()


class _TestChannel:
    """Caller's end of an in-process ``@channel`` session. Runs the handler as a
    background task and mirrors the transport: caller ``send`` feeds the handler,
    caller ``receive`` drains what the handler sent back."""

    def __init__(self, wrapper: Any, *, token: Optional[str],
                 conversation_id: Optional[str], params: dict) -> None:
        self._wrapper = wrapper
        self._params = params
        self._conversation_id = conversation_id
        self._attachment = (
            RequestEnvelope(token=token).to_attachment() if token is not None else None
        )
        self._outbound: asyncio.Queue = asyncio.Queue()
        self._session: Optional[ChannelSession] = None
        self._task: Optional[asyncio.Task] = None

    @property
    def conversation_id(self) -> Optional[str]:
        return self._session.conversation_id if self._session else self._conversation_id

    async def __aenter__(self) -> "_TestChannel":
        # Authorize up front so a denial surfaces here, before the task starts.
        principal = await self._wrapper.authorize(self._attachment, self._params)

        async def sink(raw: bytes) -> None:
            await self._outbound.put(raw)

        self._session = ChannelSession(
            self._wrapper.serializer, sink,
            attachment=self._attachment,
            store=self._wrapper.session_store,
            conversation_id=self._conversation_id,
        )
        set_request_context(RequestContext(
            prefix=self._wrapper.prefix, principal=principal, attachment=self._attachment,
        ))

        async def _run() -> None:
            try:
                await self._wrapper.run(
                    self._session, attachment=self._attachment,
                    params=self._params, principal=principal,
                )
            finally:
                await self._outbound.put(_DONE)

        self._task = asyncio.ensure_future(_run())
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session is not None:
            self._session.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def send(self, data: Any) -> None:
        """Deliver a message to the handler."""
        assert self._session is not None, "use `async with client.channel(...)`"
        self._session.feed(self._wrapper.serializer.serialize(data))

    async def receive(self) -> Any:
        """Wait for the handler's next message, or raise ChannelClosed when it ends."""
        item = await self._outbound.get()
        if item is _DONE:
            self._outbound.put_nowait(_DONE)
            if self._task is not None and self._task.done():
                exc = self._task.exception()
                if exc is not None:
                    raise exc
            raise ChannelClosed()
        return self._wrapper.serializer.deserialize(item)

    def __aiter__(self) -> "_TestChannel":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration
