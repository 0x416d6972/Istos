"""Bidirectional channels — full-duplex sessions for interactive agents.

A ``@handle`` replies once and a ``@stream`` yields many; a ``@channel`` keeps a
session open so the handler can ``receive()`` inbound messages and ``send()``
outbound ones in any order — an agent that reads a turn, streams back tokens,
then waits for the next. WebSocket is the transport at the HTTP edge; the handler
still gets authorization, validation, DI and the request envelope, and can reach
the rest of the mesh through the app.

    @app.channel("agent/chat", ws="/chat")
    async def chat(s: ChannelSession):
        await s.send({"role": "system", "text": "ready"})
        async for msg in s:
            async for tok in llm.stream(msg):
                await s.send(tok)
            await s.send({"done": True})
"""

import asyncio
import inspect
from contextlib import AsyncExitStack
from typing import Any, Awaitable, Callable, Optional

from istos.context import RequestEnvelope, get_request_context
from istos.core.authz import AuthContext, Authorizer, check_authorized
from istos.core.errors import ExceptionHandlerRegistry, get_default_registry
from istos.core.validation import validate_params
from istos.di.depends import extract_depends, resolve_dependencies
from istos.messages.serialization import Serialize
from istos.logging import get_logger


class ChannelClosed(Exception):
    """Raised by receive()/send() once the peer has hung up."""


# Marks end-of-stream in the inbound queue so a blocked receive() wakes up.
_CLOSE = object()

# Sentinel for run(principal=...): distinguishes "not authorized yet" from None.
_UNSET = object()


class ChannelSession:
    """The handler's end of a duplex session. The transport feeds inbound bytes
    and drains outbound ones; the handler works in terms of decoded messages."""

    def __init__(
        self,
        serializer: Serialize,
        send_sink: Callable[[bytes], Awaitable[None]],
        *,
        principal: Any = None,
        correlation_id: Optional[str] = None,
        attachment: Optional[bytes] = None,
        store: Any = None,
        conversation_id: Optional[str] = None,
    ) -> None:
        self._serializer = serializer
        self._send = send_sink
        self.principal = principal
        self.correlation_id = correlation_id
        self.attachment = attachment
        self._store = store
        self._conversation_id = conversation_id
        self._inbound: asyncio.Queue = asyncio.Queue()
        self._closed = False

    @property
    def conversation_id(self) -> Optional[str]:
        return self._conversation_id

    async def history(self, limit: int = 1000) -> list:
        """Prior messages for this conversation, oldest-first. Empty unless the
        channel is durable (``@channel(durable=True)``)."""
        if self._store is None or self._conversation_id is None:
            return []
        return list(await self._store.history(self._conversation_id, limit=limit))

    async def send(self, data: Any) -> None:
        """Push a message to the peer."""
        if self._closed:
            raise ChannelClosed("channel is closed")
        await self._send(self._serializer.serialize(data))
        if self._store is not None and self._conversation_id is not None:
            await self._store.append(self._conversation_id, "out", data)

    async def receive(self) -> Any:
        """Wait for the next message from the peer, or raise ChannelClosed."""
        item = await self._inbound.get()
        if item is _CLOSE:
            self._inbound.put_nowait(_CLOSE)  # keep every later receive() closed too
            raise ChannelClosed()
        data = self._serializer.deserialize(item)
        if self._store is not None and self._conversation_id is not None:
            await self._store.append(self._conversation_id, "in", data)
        return data

    def __aiter__(self) -> "ChannelSession":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    # --- transport side ---

    def feed(self, raw: bytes) -> None:
        """Transport hook: hand an inbound message to the handler."""
        if not self._closed:
            self._inbound.put_nowait(raw)

    def close(self) -> None:
        """Transport hook: the peer is gone; unblock any pending receive()."""
        if not self._closed:
            self._closed = True
            self._inbound.put_nowait(_CLOSE)

    @property
    def closed(self) -> bool:
        return self._closed


class channel_wrapper:
    """Registers a duplex handler and drives one session through it."""

    def __init__(
        self,
        func: Callable,
        prefix: str,
        serializer: Serialize,
        *,
        authorizer: Optional[Authorizer] = None,
        exception_registry: Optional[ExceptionHandlerRegistry] = None,
        dependency_overrides: Optional[dict] = None,
        durable: bool = False,
        session_store: Any = None,
    ) -> None:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"@channel requires an async function; {getattr(func, '__name__', func)!r} "
                "is not one."
            )
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self.durable = durable
        self.session_store = session_store if durable else None
        self._authorizer = authorizer
        self._exception_registry = exception_registry or get_default_registry()
        self._logger = get_logger("channel")

        params = list(inspect.signature(func).parameters.values())
        # The first positional parameter is the ChannelSession; the rest may be
        # network params or Depends(...).
        self._session_param = params[0].name if params else "session"
        self._depends_params = {
            p.name for p in params if extract_depends(p) is not None
        }
        self._has_depends = bool(self._depends_params)
        self._injected_params = set(self._depends_params) | {self._session_param}
        self._dependency_overrides = dependency_overrides or {}

    async def authorize(self, attachment: Optional[bytes], params: dict) -> Any:
        """Run the authorizer gate and return the principal (or raise). The fabric
        open handshake calls this to accept/deny before a session exists."""
        return await check_authorized(
            self._authorizer,
            AuthContext(
                prefix=self.prefix, key_expr=self.prefix, params=params,
                attachment=attachment, operation="channel",
            ),
        )

    async def run(
        self,
        session: ChannelSession,
        *,
        attachment: Optional[bytes],
        params: dict,
        principal: Any = _UNSET,
    ) -> None:
        """Validate, resolve DI, then run the handler for the life of the session.
        Authorizes first unless ``principal`` is supplied (already gated by the
        caller — e.g. the fabric open handshake)."""
        if principal is _UNSET:
            principal = await self.authorize(attachment, params)
        session.principal = principal

        req_ctx = get_request_context()
        req_ctx.prefix = self.prefix
        req_ctx.operation = "channel"
        req_ctx.principal = principal
        req_ctx.attachment = attachment
        env = RequestEnvelope.from_attachment(attachment)
        if env.correlation_id:
            req_ctx.correlation_id = env.correlation_id
        req_ctx.traceparent = env.traceparent
        session.correlation_id = req_ctx.correlation_id

        validated = validate_params(
            self.func, params, skip_params=self._injected_params
        )
        validated.pop("db", None)

        async with AsyncExitStack() as di_stack:
            call_kwargs = {self._session_param: session, **validated}
            if self._has_depends:
                call_kwargs = await resolve_dependencies(
                    self.func, call_kwargs, di_stack, cache={},
                    overrides=self._dependency_overrides,
                )
            await self.func(**call_kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """In-process invocation (TestClient) calls the handler directly."""
        return self.func(*args, **kwargs)
