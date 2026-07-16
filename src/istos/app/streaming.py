"""Streaming and bidirectional verbs: @stream and @channel, their clients, and the fabric binds."""

import asyncio
import uuid
import zenoh
from typing import Any, AsyncIterator, Callable, Optional, Union

from istos.messages.serialization import Serialize, JsonSerializer
from istos.primitives.stream import stream_wrapper
from istos.primitives.channel import channel_wrapper
from istos.primitives.channel_fabric import ChannelClient, FabricChannelServer
from istos.primitives.clients import stream_client_wrapper, channel_client_wrapper
from istos.primitives.session_store import SessionStore
from istos.errors import (
    IstosError,
    error_from_payload,
    is_error_payload,
)
from istos.security.authz import Authorizer, combine_authorizers
from istos.context import RequestEnvelope, peek_request_context
from istos.http.gateway import parse_http_spec, build_selector

from istos.app._base import IstosBase


class _StreamingMixin(IstosBase):
    """Streaming and bidirectional verbs: @stream and @channel, their clients, and the fabric binds."""

    def stream(
        self,
        prefix: str,
        serializer: Optional[Serialize] = None,
        authorizer: Optional[Authorizer] = None,
        http: Optional[Union[bool, str]] = None,
        http_timeout_s: float = 60.0,
    ) -> Callable:
        """
        Register an async-generator handler. Each ``yield`` is one reply chunk
        on the same Zenoh query (unlike ``@handle``, which replies once)::

            @istos.stream("llm/generate")
            async def generate(prompt: str):
                async for token in model.stream(prompt):
                    yield token

        Consume with :meth:`stream_query`. Auth, validation, DI, middleware and
        the request envelope work like ``@handle`` (middleware wraps the whole
        stream — once at open, once when it ends); deps stay open for the stream.

        ``http=True`` (or ``http="GET /path"``) also serves the stream as SSE.
        Chunks are ``data:`` frames; the stream finishes with ``event: end``
        (or ``event: error``). ``http_timeout_s`` defaults to 60s.
        """
        if http is not None:
            self._http_routes.append(
                parse_http_spec(http, prefix, timeout_s=http_timeout_s, sse=True)
            )

        def decorator(func: Callable) -> stream_wrapper:
            wrapper = stream_wrapper(
                func, prefix, serializer or JsonSerializer(),
                authorizer=combine_authorizers(self._authorizer, authorizer),
                exception_registry=self._exception_registry,
                dependency_overrides=self.dependency_overrides,
                middleware=self._middleware_stack,
            )
            self._streams.append(wrapper)
            return wrapper
        return decorator

    def channel(
        self,
        prefix: str,
        serializer: Optional[Serialize] = None,
        authorizer: Optional[Authorizer] = None,
        ws: Optional[Union[bool, str]] = None,
        durable: bool = False,
    ) -> Callable:
        """
        Decorator for a **bidirectional** handler — a full-duplex session for
        interactive agents. The handler takes a :class:`ChannelSession` and uses
        ``send()`` / ``receive()`` (or ``async for``) in any order:

            @app.channel("agent/chat", ws="/chat")
            async def chat(s: ChannelSession):
                async for msg in s:
                    async for tok in llm.stream(msg):
                        await s.send(tok)

        ``ws=True`` (or ``ws="/path"``) exposes the channel as a WebSocket on the
        HTTP surface (needs ``Istos(http_port=...)``); the ``Authorization`` header
        and trace headers from the handshake feed the usual authorizer/envelope.
        Auth, validation, DI and middleware work like ``@handle`` (middleware
        wraps the whole session — once at open, once at close).

        ``durable=True`` persists every message to a conversation log (over the
        app's storage), so a session resumes after a disconnect: the caller
        reconnects with the same ``conversation_id`` and the handler reads prior
        turns with ``await session.history()``.
        """
        def decorator(func: Callable) -> channel_wrapper:
            wrapper = channel_wrapper(
                func, prefix, serializer or JsonSerializer(),
                authorizer=combine_authorizers(self._authorizer, authorizer),
                exception_registry=self._exception_registry,
                dependency_overrides=self.dependency_overrides,
                durable=durable, session_store=SessionStore(self._storage),
                middleware=self._middleware_stack,
            )
            self._channels.append(wrapper)
            if ws is not None:
                path = ws if isinstance(ws, str) else "/" + prefix.lstrip("/")
                if not path.startswith("/"):
                    path = "/" + path
                self._ws_channel_routes.append((path, wrapper))
            return wrapper
        return decorator

    async def stream_query(
        self,
        key_expr: str,
        *,
        timeout_s: float = 60.0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
        **params: Any,
    ) -> AsyncIterator[Any]:
        """Yield chunks from a ``@stream`` handler as they arrive::

            async for token in app.stream_query("llm/generate", prompt="hi"):
                print(token, end="")

        Uses ``consolidation=NONE`` so every reply is kept. Forwards the request
        envelope. Default timeout is 60s. Handler errors are raised here.
        """
        import threading
        import urllib.parse

        session = self._session_manager.session
        if session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run() or istos.run_async() first."
            )
        serializer = serializer or JsonSerializer()

        selector = key_expr
        if params:
            query_string = ";".join(
                f"{urllib.parse.quote(str(k))}={urllib.parse.quote(str(v))}"
                for k, v in params.items()
            )
            selector = f"{key_expr}?{query_string}"

        tok = None
        if token is not None:
            tok = token.decode("utf-8") if isinstance(token, bytes) else str(token)
        ctx = peek_request_context()
        outbound = RequestEnvelope(
            token=tok,
            correlation_id=ctx.correlation_id if ctx else None,
            traceparent=ctx.traceparent if ctx else None,
        ).to_attachment()

        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _END = object()
        cancel_token = zenoh.CancellationToken()

        def _pump() -> None:
            try:
                get_kwargs: dict = {
                    "consolidation": zenoh.ConsolidationMode.NONE,
                    "timeout": timeout_s,
                    "cancellation_token": cancel_token,
                }
                if outbound is not None:
                    get_kwargs["attachment"] = outbound
                for reply in session.get(selector, **get_kwargs):
                    if reply.ok is not None:
                        loop.call_soon_threadsafe(queue.put_nowait, bytes(reply.ok.payload))
            except Exception as e:  # surfaced to the async consumer
                loop.call_soon_threadsafe(queue.put_nowait, e)
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, _END)

        threading.Thread(target=_pump, daemon=True).start()

        try:
            while True:
                item = await queue.get()
                if item is _END:
                    break
                if isinstance(item, Exception):
                    raise item
                data = serializer.deserialize(item)
                if is_error_payload(data):
                    raise error_from_payload(data, default_code="stream_error")
                yield data
        finally:
            # Consumer stopped early (break / exception) — cancel the underlying
            # get so the pump thread unwinds instead of draining to completion.
            cancel_token.cancel()

    async def open_channel(
        self,
        prefix: str,
        *,
        token: Optional[Union[bytes, str]] = None,
        conversation_id: Optional[str] = None,
        timeout_s: float = 5.0,
        serializer: Optional[Serialize] = None,
        **params: Any,
    ) -> ChannelClient:
        """Open a session to a remote ``@channel`` over the fabric and return a
        :class:`ChannelClient` (``send`` / ``receive`` / ``async for`` / ``close``)::

            chan = await app.open_channel("agent/chat", token=jwt)
            await chan.send("hello")
            async for msg in chan:
                ...
            await chan.close()

        Runs the open handshake (authorized via ``token``), then keeps the session
        alive with a liveliness token until you ``close()`` (or the process exits).

        For a durable channel, pass a ``conversation_id`` to resume an earlier
        conversation (one is generated otherwise and set on the returned client);
        the handler then sees prior turns via ``session.history()``.
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run()/run_async()/serving() first."
            )
        serializer = serializer or JsonSerializer()
        sid = uuid.uuid4().hex
        conversation_id = conversation_id or uuid.uuid4().hex
        loop = asyncio.get_running_loop()

        tok = None
        if token is not None:
            tok = token.decode("utf-8") if isinstance(token, bytes) else str(token)
        ctx = peek_request_context()
        attachment = RequestEnvelope(
            token=tok,
            correlation_id=ctx.correlation_id if ctx else None,
            traceparent=ctx.traceparent if ctx else None,
        ).to_attachment()

        client = ChannelClient(session, prefix, sid, serializer, loop, conversation_id)
        # Subscribe to the down channel before opening so no early reply is lost.
        client._subscribe_down()

        open_params = dict(params)
        open_params["conversation_id"] = conversation_id
        selector = build_selector(f"{prefix}/{sid}/open", open_params)

        def _open() -> Optional[bytes]:
            kwargs: dict = {"timeout": timeout_s}
            if attachment is not None:
                kwargs["attachment"] = attachment
            for reply in session.get(selector, **kwargs):
                try:
                    return bytes(reply.ok.payload)
                except Exception:
                    continue
            return None

        payload = await asyncio.to_thread(_open)
        if payload is None:
            await client.close()
            raise IstosError(
                f"No channel server answered for {prefix!r}.", code="not_found", status=504
            )
        resp = serializer.deserialize(payload)
        if is_error_payload(resp):
            await client.close()
            raise error_from_payload(resp, default_code="unauthorized")
        client._declare_liveliness()
        return client

    def stream_client(
        self,
        prefix: str,
        serializer: Optional[Serialize] = None,
        timeout_s: float = 60.0,
        token: Optional[Union[bytes, str]] = None,
    ) -> Callable:
        """
        Client-side decorator for reaching a ``@stream`` — the streaming
        counterpart to ``@query``. The body receives the live chunk iterator:

            @app.stream_client("llm/generate")
            async def generate(chunks):
                async for tok in chunks:
                    print(tok, end="")

            await generate(prompt="hi")     # call kwargs → params

        Pass ``token=`` when calling, or set a default ``token=`` on the decorator.
        """
        def decorator(func: Callable) -> stream_client_wrapper:
            return stream_client_wrapper(
                func, self, prefix, serializer=serializer, timeout_s=timeout_s,
                token=token, dependency_overrides=self.dependency_overrides,
            )
        return decorator

    def channel_client(
        self,
        prefix: str,
        serializer: Optional[Serialize] = None,
        timeout_s: float = 5.0,
        token: Optional[Union[bytes, str]] = None,
    ) -> Callable:
        """
        Client-side decorator for reaching a ``@channel``. The body receives an
        open :class:`ChannelClient`; the session closes when the body returns:

            @app.channel_client("agent/chat")
            async def chat(session):
                await session.send("hi")
                async for msg in session:
                    print(msg)

            await chat(token=jwt)           # call kwargs → open params
        """
        def decorator(func: Callable) -> channel_client_wrapper:
            return channel_client_wrapper(
                func, self, prefix, serializer=serializer, timeout_s=timeout_s,
                token=token, dependency_overrides=self.dependency_overrides,
            )
        return decorator

    async def _bind_streams(self, session: zenoh.Session) -> None:
        """Bind streaming handlers as multi-reply queryables."""
        loop = asyncio.get_running_loop()
        for wrapper in self._streams:
            self._logger.info("Binding stream %s", wrapper.prefix, extra={"prefix": wrapper.prefix})

            def make_callback(w=wrapper):
                def _sync_callback(query: zenoh.Query):
                    if not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(w.on_query(query), loop)
                return _sync_callback

            queryable = session.declare_queryable(wrapper.prefix, make_callback(), complete=True)
            self._zenoh_queryables.append(queryable)

    async def _bind_channels(self, session: zenoh.Session) -> None:
        """Serve @channel handlers over the fabric: an open-handshake queryable
        plus liveliness-driven teardown, one server per channel."""
        loop = asyncio.get_running_loop()
        for wrapper in self._channels:
            self._logger.info("Binding channel %s", wrapper.prefix, extra={"prefix": wrapper.prefix})
            server = FabricChannelServer(session, wrapper, loop)
            server.bind()
            self._channel_servers.append(server)

    async def _unbind_channels(self) -> None:
        for server in self._channel_servers:
            server.unbind()
        self._channel_servers.clear()

