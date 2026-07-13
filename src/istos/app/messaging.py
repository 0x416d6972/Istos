"""RPC and pub/sub verbs: @handle, @query, @publish, @subscribe, liveliness, persistence, and their one-shot calls."""

import asyncio
import zenoh
from typing import Any, AsyncIterator, Callable, Optional, Union

from istos.communication.persist import ObjectStore, PersistRole, ReplayEvent, parse_store_url
from istos.consistency.storage import Durability
from istos.messages.serialization import Serialize, JsonSerializer
from istos.primitives.handler import handler_wrapper
from istos.primitives.query import query_wrapper
from istos.primitives.subscribe import subscribe_wrapper
from istos.primitives.publish import publish_wrapper
from istos.primitives.liveliness import liveliness_wrapper
from istos.retry import RetryPolicy
from istos.security.authz import Authorizer, combine_authorizers
from istos.context import RequestEnvelope, peek_request_context
from istos.http.gateway import parse_http_spec

from istos.app._base import IstosBase


class _MessagingMixin(IstosBase):
    """RPC and pub/sub verbs: @handle, @query, @publish, @subscribe, liveliness, persistence, and their one-shot calls."""

    def handle(
        self,
        prefix: str,
        serializer: Optional[Serialize] = None,
        retry: Optional[Union[int, RetryPolicy]] = None,
        durability: Union[str, "Durability"] = Durability.AT_MOST_ONCE,
        authorizer: Optional[Authorizer] = None,
        http: Optional[Union[bool, str]] = None,
    ) -> Callable:
        """
        Decorator that registers a function or method as an Istos handler.

            @istos.handle(prefix="robot/move")
            async def move(distance: int): ...

            @istos.handle("robot/move", http=True)          # POST /robot/move
            @istos.handle("robot/move", http="POST /move")  # custom method+path
            async def move(distance: int): ...

            @istos.handle("critical/op", durability="exactly_once")
            async def critical(x: int, db: StoragePlugin = None): ...

            @istos.handle("fast/op", retry=3, durability="at_least_once")
            async def fast(x: int): ...

            @istos.handle("admin/op", authorizer=TokenAuthorizer("secret"))
            async def admin(x: int): ...

        Durability writes go to the app-wide storage ledger configured on
        ``Istos(storage=...)`` / ``storage_config=`` / ``storage_database=``; a
        handler that declares ``db: StoragePlugin`` receives that same backend.

        Authorization is **layered**: the app-wide authorizer passed to
        ``Istos(authorizer=...)`` always applies, and a per-handler ``authorizer``
        adds an *additional* requirement on top of it (both must pass). Pass
        ``authorizer=Public`` to opt a single handler out of the app-wide gate.
        When neither is set, the handler is reachable by any peer on the fabric.

        HTTP ingress: pass ``http=True`` (or ``http="POST /path"``) to also expose
        the handler over HTTP via the gateway (requires ``Istos(http_port=…)``).
        The request body/query become the handler's params and the
        ``Authorization`` header is forwarded as the Zenoh attachment, so the
        authorizer gate still runs. Lets non-Zenoh callers (FastAPI, browsers)
        invoke the handler.
        """
        if http is not None:
            self._http_routes.append(parse_http_spec(http, prefix))

        def decorator(func: Callable) -> handler_wrapper:
            wrapper = handler_wrapper(
                func, prefix,
                self._storage,
                serializer or JsonSerializer(),
                retry=retry,
                durability=durability,
                middleware=self._middleware_stack,
                exception_registry=self._exception_registry,
                authorizer=combine_authorizers(self._authorizer, authorizer),
                dependency_overrides=self.dependency_overrides,
            )
            self._handlers.append(wrapper)
            
            return wrapper
        return decorator

    def query(self, prefix: str, timeout_s: float = 5.0, retry: Optional[Union[int, RetryPolicy]] = None, serializer: Optional[Serialize] = None, token: Optional[Union[bytes, str]] = None) -> Callable:
        """
        Decorator that queries a registered handler when the function is called.

            @istos.query("math/add", retry=5)
            def process(result):
                print(result)

            @istos.query("binary/data", serializer=MsgPackSerializer())
            def process_binary(result): ...

        Pass ``token`` (bytes or str) to carry an auth token on every call —
        symmetry with ``query_once`` for calling gated handlers:

            @istos.query("admin/op", token="secret")
            def op(result): ...
        """
        if isinstance(token, str):
            token = token.encode("utf-8")
        def decorator(func: Callable) -> query_wrapper:
            wrapper = query_wrapper(
                func, prefix, serializer or JsonSerializer(),
                get_session=lambda: self._session_manager.session,
                timeout_s=timeout_s,
                retry=retry,
                dependency_overrides=self.dependency_overrides,
                attachment=token,
            )
            self._queries.append(wrapper)
            return wrapper
        return decorator

    def publish(
        self,
        prefix: str,
        use_shm: bool = False,
        serializer: Optional[Serialize] = None,
        durable: bool = False,
        cache: int = 1000,
        heartbeat: float = 1.0,
        reliability: Optional["zenoh.Reliability"] = None,
        congestion_control: Optional["zenoh.CongestionControl"] = None,
        persist: Optional[str] = None,
    ) -> Callable:
        """
        Decorator that publishes the return value of a function to the network.

            @istos.publish("drone/telemetry")
            def get_telemetry():
                return {"battery": 85}

            @istos.publish("binary/feed", serializer=MsgPackSerializer())
            def get_feed(): ...

        With ``durable=True`` the message is published through Zenoh's
        AdvancedPublisher, which retains the last ``cache`` samples as a replay log
        and heartbeats every ``heartbeat`` seconds so late or recovering
        subscribers can fetch what they missed.

            @istos.publish("orders/created", durable=True, cache=1000)
            def created(): ...

        Durable publishers default to ``reliability=RELIABLE`` and
        ``congestion_control=BLOCK`` so samples are not silently dropped under
        backpressure; pass either explicitly to override.

        Producer-crash durability: pass ``persist="s3://bucket/prefix"`` and Istos
        co-locates a persistence role (see :meth:`persist`) that writes every
        sample to object storage and serves it back to history queries — so the
        stream survives the producer, not just subscriber disconnects. Brokerless:
        no ``zenohd`` and no native Zenoh storage plugin.

            @istos.publish("orders/created", durable=True, persist="s3://orders-log")
            def created(): ...
        """
        if persist is not None:
            self.persist(prefix, persist)

        def decorator(func: Callable) -> publish_wrapper:
            wrapper = publish_wrapper(
                func, prefix, serializer or JsonSerializer(),
                get_session=lambda: self._session_manager.session,
                use_shm=use_shm,
                get_shm_provider=self._get_or_init_shm,
                dependency_overrides=self.dependency_overrides,
                durable=durable,
                cache=cache,
                heartbeat=heartbeat,
                reliability=reliability,
                congestion_control=congestion_control,
            )
            self._publishers.append(wrapper)
            return wrapper
        return decorator

    def persist(
        self,
        key_expr: str,
        store: Union[str, ObjectStore],
    ) -> "PersistRole":
        """Persist every sample published on ``key_expr`` and serve it back.

        Declares a brokerless persistence role — a writer subscriber plus a
        history queryable — bound to an object store. Any ``session.get(key_expr)``
        (including a durable subscriber recovering history) is answered from the
        store, so the stream survives producer restarts without a broker, a
        ``zenohd`` router, or a native Zenoh storage plugin.

        ``store`` may be a URL (``"s3://bucket/prefix"``, ``"memory://"``) or a
        ready :class:`~istos.communication.persist.ObjectStore` instance.

        Call it directly to run a **standalone persistence node** — an Istos
        process with no publishers of its own whose only job is to durably retain
        and serve a stream::

            app = Istos()
            app.persist("orders/created", "s3://orders-log")
            app.run()

        or let ``@publish(persist="s3://…")`` register it for you.
        """
        obj_store = parse_store_url(store) if isinstance(store, str) else store
        role = PersistRole(key_expr, obj_store, logger=self._logger)
        self._persist_roles.append(role)
        return role

    async def replay(
        self,
        prefix: str,
        *,
        since: Optional[str] = None,
        serializer: Optional[Serialize] = None,
        timeout_s: float = 10.0,
    ) -> AsyncIterator["ReplayEvent"]:
        """Read a persisted stream back as a durable event log, oldest-first::

            cursor = load_cursor()            # None on first run
            async for event in app.replay("orders/created", since=cursor):
                process(event.data)
                cursor = event.position       # checkpoint to resume later
            save_cursor(cursor)

        Answered by a persistence role (see :meth:`persist`), so it works across
        producer restarts. ``since`` is a ``position`` from an earlier event;
        replay resumes strictly after it, so a consumer picks up where it stopped.
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run()/run_async()/serving() first."
            )
        serializer = serializer or JsonSerializer()

        selector = f"{prefix.rstrip('/')}/**"
        if since is not None:
            import urllib.parse
            selector = f"{selector}?_since={urllib.parse.quote(since)}"

        def _collect() -> list:
            out: list = []
            for reply in session.get(selector, timeout=timeout_s):
                try:
                    sample = reply.ok
                    out.append((str(sample.key_expr), bytes(sample.payload)))
                except Exception:
                    continue
            out.sort(key=lambda kv: kv[0])
            return out

        for position, raw in await asyncio.to_thread(_collect):
            yield ReplayEvent(position=position, data=serializer.deserialize(raw))

    def subscribe(
        self,
        prefix: str,
        retry: Optional[Union[int, RetryPolicy]] = None,
        serializer: Optional[Serialize] = None,
        durable: bool = False,
        replay: int = 1000,
        recover: bool = True,
        on_miss: Optional[Callable[[str, int], Any]] = None,
        authorizer: Optional[Authorizer] = None,
        replay_persisted: bool = False,
        dedup: Union[bool, int] = False,
    ) -> Callable:
        """
        Decorator that registers a function to be called when data is published
        to a prefix.

            @istos.subscribe("drone/telemetry", retry=3)
            def on_telemetry(data):
                print(data)

            @istos.subscribe("binary/events", serializer=MsgPackSerializer())
            def on_event(data): ...

        With ``durable=True`` the subscription uses Zenoh's AdvancedSubscriber,
        which replays up to ``replay`` historical samples from the producer's cache
        on join, and (when ``recover=True``) re-fetches samples missed during
        transient disconnects.

            @istos.subscribe("orders/created", durable=True, replay=1000)
            def on_created(event): ...

        With ``durable=True``, gaps that could not be recovered are always logged
        and passed to ``on_miss(source, nb)`` if supplied — ``source`` is the
        producer and ``nb`` the number of samples irrecoverably missed.

        Authorization applies to subscribers exactly as it does to handlers: the
        app-wide ``Istos(authorizer=...)`` gate and a per-subscriber ``authorizer``
        both run against the sample's attachment before the callback body. A denied
        sample is logged and dropped (pub/sub has no reply channel). Pass
        ``authorizer=Public`` to opt a single subscriber out of the app-wide gate.

        Producer-crash durability: pass ``replay_persisted=True`` and the
        subscriber pulls persisted history from the object-store queryable on join
        (see :meth:`persist`), so it recovers the stream even if the original
        producer is gone. Best-effort and at-least-once — combine with idempotent
        handlers.

        Recovery and history replay can deliver a sample twice. Pass
        ``dedup=True`` (or ``dedup=<window>``) to drop repeated payloads within a
        bounded window. It compares payload bytes, so only use it where identical
        payloads are safe to drop.
        """
        def decorator(func: Callable) -> subscribe_wrapper:
            wrapper = subscribe_wrapper(
                func, prefix, serializer or JsonSerializer(), retry=retry,
                dependency_overrides=self.dependency_overrides,
                durable=durable, replay=replay, recover=recover,
                on_miss=on_miss,
                middleware=self._middleware_stack,
                authorizer=combine_authorizers(self._authorizer, authorizer),
                replay_persisted=replay_persisted,
                dedup=dedup,
            )
            self._subscribers.append(wrapper)
            return wrapper
        return decorator

    def on_liveliness(self, prefix: str) -> Callable:
        """
        Decorator that registers a function to handle liveliness events on a network.
        Function signature should be: func(key_expr: str, is_alive: bool)
        """
        def decorator(func: Callable) -> liveliness_wrapper:
            wrapper = liveliness_wrapper(func, prefix, dependency_overrides=self.dependency_overrides)
            self._liveliness_subs.append(wrapper)
            return wrapper
        return decorator

    def declare_liveliness(self, prefix: str) -> None:
        """
        Announce liveliness on this prefix. Will be fully declared when runner starts.
        """
        self._liveliness_declares.append(prefix)


    async def query_once(
        self,
        key_expr: str,
        timeout_s: float = 5.0,
        serializer: Optional[Serialize] = None,
        token: Optional[Union[bytes, str]] = None,
        **kwargs: Any
    ) -> Any:
        """
        One-shot query without a decorator. Allows query parameters via kwargs.

            results = await istos.query_once("robot/move", distance=10)
            results = await istos.query_once("binary/data", serializer=MsgPackSerializer())

        Pass ``token`` (bytes or str) to carry an auth token to a handler
        protected by a TokenAuthorizer:

            await istos.query_once("admin/op", token="secret")
        """
        if self._session_manager.session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run() or istos.run_async() first."
            )
        if isinstance(token, str):
            token = token.encode("utf-8")
        wrapper = query_wrapper(
            func=lambda data: data,
            prefix=key_expr,
            serializer=serializer or JsonSerializer(),
            get_session=lambda: self._session_manager.session,
            timeout_s=timeout_s,
            attachment=token,
        )
        return await wrapper(**kwargs)

    async def publish_once(self, prefix: str, data: Any, use_shm: bool = False, serializer: Optional[Serialize] = None, token: Optional[Union[bytes, str]] = None) -> None:
        """
        One-shot publish without a decorator.

            await istos.publish_once("drone/status", {"ok": True})
            await istos.publish_once("binary/data", payload, serializer=MsgPackSerializer())

        Pass ``token`` (bytes or str) to carry an auth token to a gated
        subscriber. The current request's correlation_id / traceparent are
        forwarded too (see the request envelope).
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session.")
        _serializer = serializer or JsonSerializer()
        serialized = _serializer.serialize(data)

        tok = None
        if token is not None:
            tok = token.decode("utf-8") if isinstance(token, bytes) else str(token)
        ctx = peek_request_context()
        att = RequestEnvelope(
            token=tok,
            correlation_id=ctx.correlation_id if ctx else None,
            traceparent=ctx.traceparent if ctx else None,
        ).to_attachment()
        put_kwargs = {"attachment": att} if att is not None else {}

        def _do_put():
            if use_shm:
                provider = self._get_or_init_shm()
                payload = serialized.encode('utf-8') if isinstance(serialized, str) else serialized
                if not isinstance(payload, bytes):
                    payload = str(payload).encode('utf-8')
                sbuf = provider.alloc(len(payload))
                sbuf[:] = payload
                session.put(prefix, sbuf, **put_kwargs)
            else:
                session.put(prefix, serialized, **put_kwargs)

        await asyncio.to_thread(_do_put)

    async def delete_once(self, prefix: str) -> None:
        """
        Issue a network-wide DELETE operation for a given prefix.
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session.")
        await asyncio.to_thread(session.delete, prefix)


    async def _bind_handlers(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()
        
        for wrapper in self._handlers:
            self._logger.info("Binding handler %s", wrapper.prefix, extra={"prefix": wrapper.prefix})

            def make_callback(w=wrapper):
                def _sync_callback(query: zenoh.Query):
                    if not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(w.on_query(query), loop)
                return _sync_callback

            queryable = session.declare_queryable(
                wrapper.prefix,
                make_callback(),
                complete=True
            )
            self._zenoh_queryables.append(queryable)

    async def _unbind_handlers(self) -> None:
        for q in self._zenoh_queryables:
            q.undeclare()
        self._zenoh_queryables.clear()

    async def _bind_subscribers(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()

        for wrapper in self._subscribers:
            self._logger.info("Binding subscriber %s", wrapper.prefix, extra={"prefix": wrapper.prefix})

            def make_callback(w=wrapper):
                def _sync_callback(sample: zenoh.Sample):
                    if not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(w.on_sample(sample), loop)
                return _sync_callback

            if wrapper.durable:
                from istos.communication.durable import declare_durable_subscriber

                def make_miss_callback(w=wrapper):
                    def _miss(source: str, nb: int):
                        if not loop.is_closed():
                            asyncio.run_coroutine_threadsafe(w.handle_miss(source, nb), loop)
                    return _miss

                sub: Any = declare_durable_subscriber(
                    session, wrapper.prefix, make_callback(),
                    replay=wrapper.replay, recover=wrapper.recover,
                    on_miss=make_miss_callback(),
                )
            else:
                sub = session.declare_subscriber(wrapper.prefix, make_callback())
            self._zenoh_subscribers.append(sub)

            # History replay in the background so a slow get doesn't stall startup.
            if wrapper.replay_persisted:
                loop.create_task(wrapper.replay_history(session))

    async def _unbind_subscribers(self) -> None:
        for sub in self._zenoh_subscribers:
            sub.undeclare()
        self._zenoh_subscribers.clear()

    async def _bind_publishers(self, session: zenoh.Session) -> None:
        """Declare durable AdvancedPublishers at startup so their replay caches
        and heartbeats are live before the first message."""
        for wrapper in self._publishers:
            if wrapper.durable:
                self._logger.info(
                    "Binding durable publisher %s (cache=%d)",
                    wrapper.prefix, wrapper.cache, extra={"prefix": wrapper.prefix},
                )
                wrapper.declare(session)

    async def _unbind_publishers(self) -> None:
        for wrapper in self._publishers:
            wrapper.undeclare()

    async def _bind_persist(self, session: zenoh.Session) -> None:
        """Bind persistence roles (writer subscriber + history queryable) so
        published samples are durably retained and replayable after producer
        restarts."""
        loop = asyncio.get_running_loop()
        for role in self._persist_roles:
            role.bind(session, loop)

    async def _unbind_persist(self) -> None:
        for role in self._persist_roles:
            await role.aclose()

    async def _bind_liveliness(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()
        
        for prefix in self._liveliness_declares:
            token = session.liveliness().declare_token(prefix)
            self._zenoh_liveliness_tokens.append(token)
            self._logger.info("Declared liveliness token %s", prefix, extra={"prefix": prefix})
            
        for wrapper in self._liveliness_subs:
            def make_callback(w=wrapper):
                def _sync_callback(sample: zenoh.Sample):
                    if not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(w.on_sample(sample), loop)
                return _sync_callback

            sub = session.liveliness().declare_subscriber(wrapper.prefix, make_callback(), history=False)
            self._zenoh_liveliness_subs.append(sub)
            self._logger.info("Subscribed to liveliness %s", wrapper.prefix, extra={"prefix": wrapper.prefix})

    async def _unbind_liveliness(self) -> None:
        for sub in self._zenoh_liveliness_subs:
            sub.undeclare()
        self._zenoh_liveliness_subs.clear()
        
        for token in self._zenoh_liveliness_tokens:
            token.undeclare()
        self._zenoh_liveliness_tokens.clear()

