"""Cross-node channels over Zenoh — phase 2 of @channel.

A @channel handler can run on one node while a client (often a WebSocket
gateway) opens a session from another. There is no Zenoh "connection", so a
session is built from three keys under the channel prefix ``P`` and a session id
``S``:

* ``P/S/up``   — client → server messages (client puts, server subscribes)
* ``P/S/down`` — server → client messages (server puts, client subscribes)
* liveliness ``P/S`` — the client holds a token; when it drops (close or crash)
  the server tears the session down.

Opening is a one-shot query to ``P/S/open`` carrying the auth token as the
attachment, so the authorizer runs before any session exists. On success both
sides start pub/sub on up/down and the client declares its liveliness token.
"""

import asyncio
import contextlib
from typing import Any, Dict, Iterable, Optional, Tuple, cast

import zenoh

from istos.primitives.channel import ChannelClosed, ChannelSession, _CLOSE
from istos.errors import UnauthorizedError, is_error_payload, reply_err
from istos.http.gateway import decode_params
from istos.logging import get_logger
from istos.messages.serialization import Serialize


def _sid_of(key: str, prefix: str) -> str:
    """First segment after the prefix — the session id (works for both the
    ``P/S/open`` handshake key and the liveliness ``P/S`` key)."""
    return key[len(prefix) + 1:].split("/", 1)[0]


class ChannelClient:
    """Caller's end of a fabric channel. Same surface as ChannelSession
    (send/receive/async-for/close), backed by Zenoh pub/sub."""

    def __init__(
        self,
        session: Any,
        prefix: str,
        sid: str,
        serializer: Serialize,
        loop: asyncio.AbstractEventLoop,
        conversation_id: Optional[str] = None,
    ) -> None:
        self._session = session
        self._serializer = serializer
        self._loop = loop
        self.conversation_id = conversation_id
        self._up_key = f"{prefix}/{sid}/up"
        self._down_key = f"{prefix}/{sid}/down"
        self._live_key = f"{prefix}/{sid}"
        self._inbound: asyncio.Queue = asyncio.Queue()
        self._down_sub: Any = None
        self._live_token: Any = None
        self._closed = False

    def _subscribe_down(self) -> None:
        def cb(sample: zenoh.Sample) -> None:
            payload = bytes(sample.payload)
            self._loop.call_soon_threadsafe(self._inbound.put_nowait, payload)
        self._down_sub = self._session.declare_subscriber(self._down_key, cb)

    def _declare_liveliness(self) -> None:
        self._live_token = self._session.liveliness().declare_token(self._live_key)

    async def send(self, data: Any) -> None:
        if self._closed:
            raise ChannelClosed("channel is closed")
        raw = self._serializer.serialize(data)
        await asyncio.to_thread(self._session.put, self._up_key, raw)

    async def receive(self) -> Any:
        item = await self._inbound.get()
        if item is _CLOSE:
            self._inbound.put_nowait(_CLOSE)
            raise ChannelClosed()
        return self._serializer.deserialize(item)

    def __aiter__(self) -> "ChannelClient":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Dropping the liveliness token is what tells the server to tear down.
        if self._live_token is not None:
            with contextlib.suppress(Exception):
                self._live_token.undeclare()
        if self._down_sub is not None:
            with contextlib.suppress(Exception):
                self._down_sub.undeclare()
        self._inbound.put_nowait(_CLOSE)

    @property
    def closed(self) -> bool:
        return self._closed


class _ServerSession:
    __slots__ = ("session", "up_sub", "task")

    def __init__(self, session: ChannelSession, up_sub: Any) -> None:
        self.session = session
        self.up_sub = up_sub
        self.task: Optional[asyncio.Task] = None


class FabricChannelServer:
    """Serves one @channel over Zenoh: answers open handshakes, runs a handler
    per session, and tears sessions down when their liveliness token drops.

    Closing a session unblocks a handler waiting in ``receive()``. A handler busy
    elsewhere (a model call, a query) would never notice, so it is cancelled
    once ``cancel_grace_s`` has passed after the client went away."""

    def __init__(
        self, session: Any, wrapper: Any, loop: asyncio.AbstractEventLoop,
        *, cancel_grace_s: float = 5.0,
    ) -> None:
        self._cancel_grace_s = cancel_grace_s
        self._session = session
        self._wrapper = wrapper
        self._loop = loop
        self._prefix = wrapper.prefix
        self._serializer: Serialize = wrapper.serializer
        self._sessions: Dict[str, _ServerSession] = {}
        self._run_tasks: set[asyncio.Task] = set()
        self._queryable: Any = None
        self._live_sub: Any = None
        self._logger = get_logger("channel")

    def bind(self) -> None:
        self._queryable = self._session.declare_queryable(
            f"{self._prefix}/*/open", self._on_open, complete=True
        )
        self._live_sub = self._session.liveliness().declare_subscriber(
            f"{self._prefix}/*", self._on_liveliness, history=False
        )

    def unbind(self) -> None:
        for sid in list(self._sessions):
            self._teardown(sid, grace_s=0.0)
        if self._queryable is not None:
            with contextlib.suppress(Exception):
                self._queryable.undeclare()
        if self._live_sub is not None:
            with contextlib.suppress(Exception):
                self._live_sub.undeclare()

    # --- open handshake (Zenoh queryable thread) ---

    def _on_open(self, query: zenoh.Query) -> None:
        key = str(query.selector.key_expr)
        sid = _sid_of(key, self._prefix)
        attachment = _attachment_of(query)
        params: dict = {}
        if getattr(query.selector, "parameters", None):
            params = decode_params(
                dict(cast(Iterable[Tuple[str, str]], query.selector.parameters))
            )

        fut = asyncio.run_coroutine_threadsafe(
            self._open(sid, attachment, params), self._loop
        )
        try:
            reply = fut.result(timeout=10)
        except Exception as e:  # pragma: no cover - defensive
            self._logger.exception("channel open failed: %s", e)
            reply = reply_err("channel open failed", code="internal_error")
        with contextlib.suppress(Exception):
            query.reply(key, self._serializer.serialize(reply))

    async def _open(self, sid: str, attachment: Optional[bytes], params: dict) -> dict:
        conversation_id = params.pop("conversation_id", None)
        try:
            principal = await self._wrapper.authorize(attachment, params)
        except UnauthorizedError as e:
            return reply_err(str(e), code="unauthorized")

        down_key = f"{self._prefix}/{sid}/down"

        async def sink(raw: bytes) -> None:
            await asyncio.to_thread(self._session.put, down_key, raw)

        chan = ChannelSession(
            self._serializer, sink, principal=principal, attachment=attachment,
            store=self._wrapper.session_store, conversation_id=conversation_id,
        )

        def up_cb(sample: zenoh.Sample) -> None:
            payload = bytes(sample.payload)
            self._loop.call_soon_threadsafe(chan.feed, payload)

        up_sub = self._session.declare_subscriber(f"{self._prefix}/{sid}/up", up_cb)
        state = _ServerSession(chan, up_sub)
        self._sessions[sid] = state
        task = self._loop.create_task(self._run(sid, chan, attachment, params, principal))
        state.task = task
        self._run_tasks.add(task)
        task.add_done_callback(self._run_tasks.discard)
        return {"ok": True, "sid": sid}

    async def _run(
        self, sid: str, chan: ChannelSession, attachment: Optional[bytes],
        params: dict, principal: Any,
    ) -> None:
        try:
            await self._wrapper.run(
                chan, attachment=attachment, params=params, principal=principal
            )
        except Exception as e:
            self._logger.error(
                "Channel session %s on %s failed: %s", sid, self._prefix, e,
                exc_info=True, extra={"prefix": self._prefix},
            )
        finally:
            self._teardown(sid)

    # --- liveliness teardown (Zenoh subscriber thread) ---

    def _on_liveliness(self, sample: zenoh.Sample) -> None:
        if sample.kind == zenoh.SampleKind.DELETE:
            sid = _sid_of(str(sample.key_expr), self._prefix)
            self._loop.call_soon_threadsafe(self._teardown, sid)

    def _teardown(self, sid: str, *, grace_s: Optional[float] = None) -> None:
        state = self._sessions.pop(sid, None)
        if state is None:
            return
        state.session.close()  # unblocks the handler's receive() → it returns
        with contextlib.suppress(Exception):
            state.up_sub.undeclare()
        task = state.task
        if task is None or task.done():
            return
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        if task is current:
            return  # the handler finished and _run is tearing down after it
        grace = self._cancel_grace_s if grace_s is None else grace_s
        if grace <= 0:
            task.cancel()
        else:
            self._loop.call_later(grace, task.cancel)  # no-op once it has finished


def _attachment_of(query: zenoh.Query) -> Optional[bytes]:
    raw = getattr(query, "attachment", None)
    if raw is None:
        return None
    try:
        return bytes(raw)
    except (TypeError, ValueError):
        return None


__all__ = ["ChannelClient", "FabricChannelServer", "is_error_payload"]
