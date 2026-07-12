import asyncio
import contextlib
import inspect
import signal
import uuid
import warnings
import zenoh
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, Callable, List, Mapping, Optional, Type, Union, AsyncContextManager

from istos.communication.sessions import SessionManager, AsyncZenohSession, ZenohSession
from istos.communication.config import IstosZenohConfig
from istos.communication.persist import ObjectStore, PersistRole, parse_store_url
from istos.consistency.storage import StoragePlugin, InMemoryStoragePlugin, Durability
from istos.consistency.sqlalchemy_storage import SqlAlchemyStoragePlugin
from istos.consistency.config import DatabaseConfig
from istos.consistency.databases import DatabaseRegistry
from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.handler import handler_wrapper
from istos.core.query import query_wrapper
from istos.core.subscribe import subscribe_wrapper
from istos.core.publish import publish_wrapper
from istos.core.stream import stream_wrapper
from istos.core.liveliness import liveliness_wrapper
from istos.core.retry import RetryPolicy
from istos.core.asyncapi import AsyncApiGenerator, get_asyncapi_ui_html
from istos.core.errors import (
    ExceptionHandler,
    ExceptionHandlerRegistry,
    IstosError,
    IstosSecurityError,
    IstosSecurityWarning,
    get_default_registry,
)
from istos.core.authz import Authorizer, combine_authorizers
from istos.context import RequestContext, RequestEnvelope, peek_request_context, set_request_context
from istos.gateway import HttpRoute, parse_http_spec, build_selector, extract_bearer, status_for_reply, is_error_payload, sse_event
from istos.routing import IstosRouter
from istos.logging import configure_logging as _configure_logging, ensure_configured, get_logger
from istos.middleware.base import (
    CorrelationIdMiddleware,
    LoggingMiddleware,
    Middleware,
    MiddlewareStack,
)
from istos.health import HealthState, register_health_handlers
from istos.observability.metrics import MetricsCollector, PrometheusMiddleware
from istos.observability.tracing import TracingMiddleware, configure_tracing

class Istos:
    """
    Unified entry-point for the Istos framework.

    Usage:
        istos = Istos()

        # Or wire the network from a config; the session is built for you:
        istos = Istos(config=IstosZenohConfig(mode="client"))

        @istos.handle(prefix="robot/move")
        async def move(distance: int):
            return f"moved {distance}m"

        class Drone:
            @istos.handle(prefix="drone/fly")
            def fly(self, altitude: int):
                return f"flying at {altitude}m"

        istos.run()          # sync entry
        await istos.run_async()  # async entry
    """

    def __init__(
        self,
        session_manager: Optional[SessionManager] = None,
        storage: Optional[StoragePlugin] = None,
        lifespan: Optional[Callable[["Istos"], AsyncContextManager[None]]] = None,
        *,
        storage_config: Optional[DatabaseConfig] = None,
        databases: Optional[Mapping[str, DatabaseConfig]] = None,
        storage_database: Optional[str] = None,
        config: Optional[Union[IstosZenohConfig, zenoh.Config]] = None,
        log_level: str = "INFO",
        json_logs: bool = False,
        enable_health: bool = True,
        enable_metrics: bool = True,
        enable_discovery: bool = True,
        enable_tracing: bool = False,
        tracing_endpoint: Optional[str] = None,
        service_name: str = "istos",
        exception_registry: Optional[ExceptionHandlerRegistry] = None,
        authorizer: Optional[Authorizer] = None,
        require_auth: bool = False,
        configure_logging: Optional[bool] = None,
        http_port: Optional[int] = None,
    ):
        # configure_logging=True installs now; None defers to run(); False = never.
        self._log_level = log_level
        self._json_logs = json_logs
        self._configure_logging = configure_logging
        if configure_logging:
            _configure_logging(level=log_level, json_format=json_logs)
        self._logger = get_logger("app")

        # Build session manager from config= when callers don't pass one.
        self._config: Optional[IstosZenohConfig] = None
        if config is not None:
            if session_manager is not None:
                raise ValueError(
                    "Pass either 'config' or 'session_manager', not both."
                )
            if isinstance(config, IstosZenohConfig):
                self._config = config
                zenoh_conf = config.build()
                session_cls = AsyncZenohSession if config.session == "async" else ZenohSession
            else:
                # Raw zenoh.Config has no session= hint → async.
                zenoh_conf = config
                session_cls = AsyncZenohSession
            session_manager = session_cls(zenoh_conf)

        self._session_manager = session_manager or AsyncZenohSession()
        self._databases = DatabaseRegistry(databases or {})

        # Exactly one of storage / storage_config / storage_database.
        ledger_sources = [
            s for s in (storage, storage_config, storage_database) if s is not None
        ]
        if len(ledger_sources) > 1:
            raise ValueError(
                "Specify at most one of `storage` (a ready plugin), `storage_config` "
                "(a single DatabaseConfig), or `storage_database` (a name from "
                "`databases`) — they all define the durability ledger."
            )
        if storage_database is not None:
            if storage_database not in self._databases:
                raise ValueError(
                    f"storage_database={storage_database!r} is not in `databases` "
                    f"({self._databases.names()})."
                )
            # Registry owns disposal of the named engine.
            self._storage: StoragePlugin = SqlAlchemyStoragePlugin(
                self._databases.engine(storage_database)
            )
        elif storage_config is not None:
            self._storage = SqlAlchemyStoragePlugin.from_config(storage_config)
        else:
            self._storage = storage or InMemoryStoragePlugin()
        self._serializer = JsonSerializer()
        self.lifespan = lifespan
        self._service_name = service_name
        self._authorizer = authorizer
        # require_auth without an authorizer is a hard error, not a warning.
        if require_auth and authorizer is None:
            raise IstosSecurityError(
                "Istos(require_auth=True) requires an app-wide authorizer, but none "
                "was set. Pass Istos(authorizer=...) (e.g. JWTAuthorizer/TokenAuthorizer), "
                "or use Public per-handler to opt specific endpoints out."
            )
        self.dependency_overrides: dict = {}
        self._exception_registry = exception_registry or get_default_registry()
        self._middleware_stack = MiddlewareStack([
            CorrelationIdMiddleware(),
            LoggingMiddleware(self._logger),
        ])
        self._metrics = MetricsCollector()
        if enable_metrics:
            self._middleware_stack.add(PrometheusMiddleware(self._metrics))
        if enable_tracing:
            if configure_tracing(service_name=service_name, endpoint=tracing_endpoint):
                self._middleware_stack.add(TracingMiddleware())
        self._health = HealthState()
        self._enable_health = enable_health
        self._enable_metrics = enable_metrics
        self._enable_discovery = enable_discovery
        self._handlers: List[handler_wrapper] = []
        self._queries: List[query_wrapper] = []
        self._subscribers: List[subscribe_wrapper] = []
        self._publishers: List[publish_wrapper] = []
        self._streams: List[stream_wrapper] = []
        self._liveliness_subs: List[liveliness_wrapper] = []
        self._liveliness_declares: List[str] = []
        self._persist_roles: List["PersistRole"] = []
        self._http_routes: List[HttpRoute] = []
        self._zenoh_subscribers: List[zenoh.Subscriber] = []
        self._zenoh_queryables: List[zenoh.Queryable] = []
        self._zenoh_liveliness_subs: List[Any] = []
        self._zenoh_liveliness_tokens: List[Any] = []
        self._shm_provider: Optional[Any] = None
        self._docs_web_port: Optional[int] = None
        self._http_port: Optional[int] = http_port
        self._docs_prefix: Optional[str] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._builtin_handlers_registered = False
        self._lifecycle_stack: Optional[AsyncExitStack] = None
        self._web_runner: Any = None

    def _get_or_init_shm(self) -> Any:
        if self._shm_provider is None:
            self._shm_provider = zenoh.shm.ShmProvider.default_backend(10 * 1024 * 1024)
        return self._shm_provider

    def add_middleware(self, middleware: Middleware) -> None:
        """Add middleware to the request pipeline."""
        self._middleware_stack.add(middleware)

    def exception_handler(self, exc_type: Type[Exception]) -> Callable:
        """Register a custom exception handler for a given exception type."""

        def decorator(handler: ExceptionHandler) -> ExceptionHandler:
            self._exception_registry.register(exc_type, handler)
            return handler

        return decorator

    def add_health_check(self, name: str, check: Callable[[], Any]) -> None:
        """Register a custom readiness check."""
        self._health.checks[name] = check

    @property
    def metrics(self) -> MetricsCollector:
        """Access the in-process metrics collector."""
        return self._metrics

    @property
    def config(self) -> Optional[IstosZenohConfig]:
        """The IstosZenohConfig this app was built from, if any."""
        return self._config

    @property
    def databases(self) -> DatabaseRegistry:
        """The registry of named application databases (from `databases=`)."""
        return self._databases

    def db_session(self, name: str) -> Callable[[], AsyncIterator[Any]]:
        """
        A ``Depends`` provider that yields one ``AsyncSession`` per request from the
        named database configured in `databases=`. The engine (pool) is shared and
        app-lifetime; the session is per request:

            @app.handle("orders/create")
            async def create(item: str, db = Depends(app.db_session("app"))):
                db.add(Order(item=item))

        Overridable in tests: `app.dependency_overrides[app.db_session("app")] = ...`
        (the provider is cached per name, so the key is stable).
        """
        return self._databases.session_dependency(name)

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

    def query(self, prefix: str, timeout_s: float = 5.0, retry: Optional[Union[int, RetryPolicy]] = None, serializer: Optional[Serialize] = None, attachment: Optional[Union[bytes, str]] = None) -> Callable:
        """
        Decorator that queries a registered handler when the function is called.

            @istos.query("math/add", retry=5)
            def process(result):
                print(result)

            @istos.query("binary/data", serializer=MsgPackSerializer())
            def process_binary(result): ...

        Pass ``attachment`` (bytes or str) to carry an auth token on every call —
        symmetry with ``query_once`` for calling gated handlers:

            @istos.query("admin/op", attachment="secret")
            def op(result): ...
        """
        if isinstance(attachment, str):
            attachment = attachment.encode("utf-8")
        def decorator(func: Callable) -> query_wrapper:
            wrapper = query_wrapper(
                func, prefix, serializer or JsonSerializer(),
                get_session=lambda: self._session_manager.session,
                timeout_s=timeout_s,
                retry=retry,
                dependency_overrides=self.dependency_overrides,
                attachment=attachment,
            )
            self._queries.append(wrapper)
            return wrapper
        return decorator

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

        Consume with :meth:`stream_query`. Auth, validation, DI, and the request
        envelope work like ``@handle``; deps stay open until the stream ends.

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
            )
            self._streams.append(wrapper)
            return wrapper
        return decorator

    async def stream_query(
        self,
        key_expr: str,
        *,
        timeout_s: float = 60.0,
        serializer: Optional[Serialize] = None,
        attachment: Optional[Union[bytes, str]] = None,
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

        token = None
        if attachment is not None:
            token = attachment.decode("utf-8") if isinstance(attachment, bytes) else str(attachment)
        ctx = peek_request_context()
        outbound = RequestEnvelope(
            token=token,
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
                    raise IstosError(
                        data.get("message", "stream error"),
                        code=data.get("code", "stream_error"),
                    )
                yield data
        finally:
            # Consumer stopped early (break / exception) — cancel the underlying
            # get so the pump thread unwinds instead of draining to completion.
            cancel_token.cancel()


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
        attachment: Optional[Union[bytes, str]] = None,
        **kwargs: Any
    ) -> Any:
        """
        One-shot query without a decorator. Allows query parameters via kwargs.

            results = await istos.query_once("robot/move", distance=10)
            results = await istos.query_once("binary/data", serializer=MsgPackSerializer())

        Pass ``attachment`` (bytes or str) to carry an auth token to a handler
        protected by a TokenAuthorizer:

            await istos.query_once("admin/op", attachment="secret")
        """
        if self._session_manager.session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run() or istos.run_async() first."
            )
        if isinstance(attachment, str):
            attachment = attachment.encode("utf-8")
        wrapper = query_wrapper(
            func=lambda data: data,
            prefix=key_expr,
            serializer=serializer or JsonSerializer(),
            get_session=lambda: self._session_manager.session,
            timeout_s=timeout_s,
            attachment=attachment,
        )
        return await wrapper(**kwargs)

    async def publish_once(self, prefix: str, data: Any, use_shm: bool = False, serializer: Optional[Serialize] = None, attachment: Optional[Union[bytes, str]] = None) -> None:
        """
        One-shot publish without a decorator.

            await istos.publish_once("drone/status", {"ok": True})
            await istos.publish_once("binary/data", payload, serializer=MsgPackSerializer())

        Pass ``attachment`` (bytes or str) to carry an auth token to a gated
        subscriber. The current request's correlation_id / traceparent are
        forwarded too (see the request envelope).
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session.")
        _serializer = serializer or JsonSerializer()
        serialized = _serializer.serialize(data)

        token = None
        if attachment is not None:
            token = attachment.decode("utf-8") if isinstance(attachment, bytes) else str(attachment)
        ctx = peek_request_context()
        att = RequestEnvelope(
            token=token,
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


    def include_router(self, router: IstosRouter) -> None:
        """
        Includes a router's routes into the main application.
        """
        for action in router._actions:
            action(self)


    def export_asyncapi(self, title: str = "Istos Network", version: str = "1.0.0") -> str:
        """
        Generates and returns the AsyncAPI YAML specification for the network.
        """
        generator = AsyncApiGenerator(title=title, version=version)
        return generator.generate(self)

    def serve_docs(
        self,
        prefix: str = ".istos/docs",
        title: str = "Istos Network",
        version: str = "1.0.0",
        web_port: Optional[int] = None,
        authorizer: Optional[Authorizer] = None,
    ) -> None:
        """
        Registers a built-in handler to serve the AsyncAPI specification over Zenoh.
        If web_port is provided, it starts an embedded HTTP server to display the UI.

        The docs endpoint publishes your entire API surface. Protect it with an
        ``authorizer`` — which layers on top of the app-wide one — or rely on the
        app-wide authorizer alone. If neither is set a security warning is emitted
        because any peer can then enumerate your API.
        """
        # Used only to decide whether to warn about an ungated docs endpoint.
        effective = authorizer if authorizer is not None else self._authorizer
        if effective is None:
            warnings.warn(
                f"Docs endpoint {prefix!r} has no authorizer: it broadcasts your "
                "full AsyncAPI surface to every peer. Pass authorizer=... or set "
                "Istos(authorizer=...).",
                IstosSecurityWarning,
                stacklevel=2,
            )

        @self.handle(prefix=prefix, authorizer=authorizer)
        def _serve_docs() -> str:
            return self.export_asyncapi(title=title, version=version)

        if web_port is not None:
            self._docs_web_port = web_port
            self._docs_prefix = prefix


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

                sub = declare_durable_subscriber(
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

    def _http_server_port(self) -> Optional[int]:
        """The port for the embedded HTTP surface: explicit ``http_port`` wins,
        else the docs ``web_port`` (backward compatible)."""
        return self._http_port or self._docs_web_port

    async def _start_http_server(self) -> Any:
        """Start the embedded aiohttp server hosting the HTTP surface:
        K8s probes, Prometheus ``/metrics``, the ingress gateway routes, and
        (when configured) the docs UI. All share one port."""
        from aiohttp import web

        app = web.Application()

        async def _livez(request: web.Request) -> web.Response:
            return web.json_response(await self._health.liveness())

        async def _readyz(request: web.Request) -> web.Response:
            result = await self._health.readiness()
            status = 200 if result.get("status") == "ready" else 503
            return web.json_response(result, status=status)

        app.router.add_get('/livez', _livez)
        app.router.add_get('/healthz', _livez)   # common alias
        app.router.add_get('/readyz', _readyz)

        async def _metrics(request: web.Request) -> web.Response:
            return web.Response(
                text=self._metrics.export_prometheus(),
                content_type='text/plain', charset='utf-8',
            )

        app.router.add_get('/metrics', _metrics)

        for route in self._http_routes:
            handler = (
                self._make_sse_handler(route) if route.sse
                else self._make_gateway_handler(route)
            )
            app.router.add_route(route.method, route.path, handler)

        if self._docs_prefix is not None:
            html = get_asyncapi_ui_html(title="Istos Network Docs", schema_url="/asyncapi.yaml")

            async def web_ui_handler(request: web.Request) -> web.Response:
                return web.Response(text=html, content_type='text/html')

            async def asyncapi_yaml_handler(request: web.Request) -> web.Response:
                try:
                    results = await self.query_once(self._docs_prefix or ".istos/docs", timeout_s=2.0)
                    if results:
                        yaml_content = results[0] if isinstance(results, list) else results
                        return web.Response(text=yaml_content, content_type='application/yaml')
                    return web.Response(text="Docs not found on network", status=404)
                except Exception as e:
                    return web.Response(text=f"Error querying network: {e}", status=500)

            app.router.add_get('/', web_ui_handler)
            app.router.add_get('/asyncapi.yaml', asyncapi_yaml_handler)

        port = self._http_server_port()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        self._logger.info(
            "HTTP surface on http://localhost:%s (probes /livez /readyz, /metrics, "
            "%d gateway route(s))", port, len(self._http_routes),
            extra={"port": port, "gateway_routes": len(self._http_routes)},
        )
        return runner

    def _make_gateway_handler(self, route: HttpRoute) -> Any:
        """Build an aiohttp handler that bridges an HTTP request to a Zenoh query
        against ``route.key_expr``, forwarding the Authorization header as the
        query attachment (so the authorizer gate runs)."""
        import json as _json

        from aiohttp import web

        async def _handler(request: web.Request) -> web.Response:
            params: dict = dict(request.query)
            if request.body_exists:
                text = await request.text()
                if text.strip():
                    try:
                        data = _json.loads(text)
                    except _json.JSONDecodeError:
                        return web.json_response(
                            {"error": "bad_request", "code": "bad_request",
                             "message": "Request body must be valid JSON."},
                            status=400,
                        )
                    if not isinstance(data, dict):
                        return web.json_response(
                            {"error": "bad_request", "code": "bad_request",
                             "message": "JSON body must be an object of params."},
                            status=400,
                        )
                    params.update(data)

            token = extract_bearer(request.headers.get("Authorization"))
            selector = build_selector(route.key_expr, params)
            # Keep one cid / traceparent from HTTP into the Zenoh hop.
            envelope = RequestEnvelope(
                token=token,
                correlation_id=(request.headers.get("X-Correlation-ID")
                                or request.headers.get("X-Request-ID")),
                traceparent=request.headers.get("traceparent"),
            )
            outbound_attachment = envelope.to_attachment()

            def _query() -> Optional[bytes]:
                session = self._session_manager.session
                if session is None:
                    return None
                kwargs: dict = {"timeout": route.timeout_s}
                if outbound_attachment is not None:
                    kwargs["attachment"] = outbound_attachment
                for reply in session.get(selector, **kwargs):
                    try:
                        return bytes(reply.ok.payload)
                    except Exception:
                        continue  # skip error replies from other queryables
                return None

            try:
                payload = await asyncio.to_thread(_query)
            except Exception as e:
                self._logger.error(
                    "Gateway query failed for %s: %s", route.key_expr, e,
                    exc_info=True, extra={"prefix": route.key_expr},
                )
                return web.json_response(
                    {"error": "gateway_error", "code": "gateway_error",
                     "message": "Upstream query failed."},
                    status=502,
                )

            if payload is None:
                return web.json_response(
                    {"error": "not_found", "code": "not_found",
                     "message": f"No handler replied for {route.key_expr!r}."},
                    status=504,
                )

            try:
                parsed = _json.loads(payload)
            except Exception:
                return web.Response(body=payload, content_type='application/octet-stream')
            return web.json_response(parsed, status=status_for_reply(parsed))

        return _handler

    def _make_sse_handler(self, route: HttpRoute) -> Any:
        """aiohttp handler that relays a ``@stream`` handler's chunks as SSE.
        Forwards the Authorization and trace headers into the Zenoh envelope."""
        import json as _json

        from aiohttp import web

        async def _handler(request: web.Request) -> web.StreamResponse:
            params: dict = dict(request.query)
            if request.body_exists:
                text = await request.text()
                if text.strip():
                    try:
                        data = _json.loads(text)
                    except _json.JSONDecodeError:
                        return web.json_response(
                            {"error": "bad_request", "code": "bad_request",
                             "message": "Request body must be valid JSON."},
                            status=400,
                        )
                    if isinstance(data, dict):
                        params.update(data)

            token = extract_bearer(request.headers.get("Authorization"))
            # stream_query reads cid/trace from the ambient context.
            set_request_context(RequestContext(
                correlation_id=(request.headers.get("X-Correlation-ID")
                                or request.headers.get("X-Request-ID")
                                or str(uuid.uuid4())),
                traceparent=request.headers.get("traceparent"),
                prefix=route.key_expr,
                operation="stream",
            ))

            response = web.StreamResponse(
                status=200,
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",  # disable proxy buffering (nginx)
                },
            )
            await response.prepare(request)

            try:
                async for chunk in self.stream_query(
                    route.key_expr, timeout_s=route.timeout_s, attachment=token, **params
                ):
                    text_chunk = chunk if isinstance(chunk, str) else _json.dumps(chunk)
                    await response.write(sse_event(text_chunk).encode("utf-8"))
                await response.write(sse_event("", event="end").encode("utf-8"))
            except IstosError as e:
                err = _json.dumps({"code": e.code, "message": e.message})
                await response.write(sse_event(err, event="error").encode("utf-8"))
            except asyncio.CancelledError:
                raise  # client disconnected
            except Exception as e:
                self._logger.error(
                    "SSE stream failed for %s: %s", route.key_expr, e,
                    exc_info=True, extra={"prefix": route.key_expr},
                )
                err = _json.dumps({"code": "stream_error", "message": "Upstream stream failed."})
                try:
                    await response.write(sse_event(err, event="error").encode("utf-8"))
                except Exception:
                    pass
            finally:
                with contextlib.suppress(Exception):
                    await response.write_eof()
            return response

        return _handler

    def _register_builtin_handlers(self) -> None:
        if self._builtin_handlers_registered:
            return
        self._builtin_handlers_registered = True

        # Warn if built-ins would be open (they inherit the app-wide authorizer).
        if self._authorizer is None and (self._enable_health or self._enable_metrics or self._enable_discovery):
            exposed = []
            if self._enable_health:
                exposed += [".istos/health", ".istos/ready"]
            if self._enable_metrics:
                exposed.append(".istos/metrics")
            if self._enable_discovery:
                exposed.append(".istos/capabilities")
            warnings.warn(
                f"Built-in endpoints {exposed} are reachable by any peer with no "
                "authorization. Set Istos(authorizer=...) to protect them.",
                IstosSecurityWarning,
                stacklevel=2,
            )

        if self._enable_health:
            register_health_handlers(self, self._health)

        if self._enable_metrics:
            @self.handle(".istos/metrics")
            def _metrics() -> str:
                return self._metrics.export_prometheus()

        if self._enable_discovery:
            @self.handle(".istos/capabilities")
            def _capabilities() -> dict:
                return self.export_capabilities()

    def export_capabilities(self) -> dict:
        """What this node exposes — handlers/streams with schemas when available.

        Served at ``.istos/capabilities``. Query ``**/.istos/capabilities`` (or
        per node) to inventory the fabric. Each entry: ``prefix``, ``kind``,
        optional ``description``, and ``params_schema`` / ``return_schema``.
        """
        from istos.core.asyncapi import get_function_schemas

        def _describe(prefix: str, kind: str, func: Callable) -> dict:
            schemas = get_function_schemas(func)
            entry: dict = {
                "prefix": prefix,
                "kind": kind,
                "description": (inspect.getdoc(func) or "").strip() or None,
            }
            if schemas.get("payload_schema"):
                entry["params_schema"] = schemas["payload_schema"]
            if schemas.get("return_schema"):
                entry["return_schema"] = schemas["return_schema"]
            return entry

        capabilities: List[dict] = []
        # Skip .istos/* plumbing endpoints.
        for h in self._handlers:
            if not h.prefix.startswith(".istos/"):
                capabilities.append(_describe(h.prefix, "handle", h.func))
        for s in self._streams:
            capabilities.append(_describe(s.prefix, "stream", s.func))
        for p in self._publishers:
            capabilities.append(_describe(p.prefix, "publish", p.func))
        for sub in self._subscribers:
            capabilities.append(_describe(sub.prefix, "subscribe", sub.func))
        return {"service": self._service_name, "capabilities": capabilities}

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()

        def _request_shutdown() -> None:
            self._logger.info("Shutdown signal received")
            if self._shutdown_event is not None:
                self._shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except (NotImplementedError, RuntimeError):
                pass

    async def _startup(self, *, serve_http: bool) -> Any:
        """Open the session and bind every registry. Shared by run_async and the
        serving() context manager. No signal handlers, no blocking."""
        if self._configure_logging is not False:
            ensure_configured(self._log_level, self._json_logs)
        self._register_builtin_handlers()

        stack = AsyncExitStack()
        self._lifecycle_stack = stack
        if self.lifespan:
            await stack.enter_async_context(self.lifespan(self))

        # close() is optional (Redis/SQLAlchemy); InMemory has none.
        storage_close = getattr(self._storage, "close", None)
        if callable(storage_close):
            stack.push_async_callback(storage_close)
        stack.push_async_callback(self._databases.dispose_all)

        # session="sync" managers use __enter__, not __aenter__.
        if hasattr(self._session_manager, "__aenter__"):
            session = await stack.enter_async_context(self._session_manager)  # type: ignore
        else:
            session = stack.enter_context(self._session_manager)  # type: ignore

        await self._bind_handlers(session)
        await self._bind_streams(session)
        await self._bind_persist(session)
        await self._bind_publishers(session)
        await self._bind_subscribers(session)
        await self._bind_liveliness(session)

        self._health.ready = True
        prefixes = [a.prefix for a in self._handlers]
        subs = [s.prefix for s in self._subscribers]
        self._logger.info(
            "Service started with %d handler(s) and %d subscriber(s)",
            len(prefixes), len(subs),
            extra={"handlers": prefixes, "subscribers": subs},
        )

        self._web_runner = None
        if serve_http and self._http_server_port() is not None:
            self._web_runner = await self._start_http_server()
        return session

    async def _shutdown(self) -> None:
        """Reverse of _startup: unbind, then close session/storage/databases."""
        self._health.ready = False
        self._logger.info("Service stopping")
        if self._web_runner is not None:
            await self._web_runner.cleanup()
            self._web_runner = None
        await self._unbind_liveliness()
        await self._unbind_subscribers()
        await self._unbind_publishers()
        await self._unbind_persist()
        await self._unbind_handlers()
        if self._lifecycle_stack is not None:
            await self._lifecycle_stack.aclose()
            self._lifecycle_stack = None
        self._logger.info("Service stopped")

    @contextlib.asynccontextmanager
    async def serving(self, *, serve_http: bool = False) -> "AsyncIterator[Istos]":
        """Run the mesh for as long as the block is entered, without owning the
        process (no signal handlers, no blocking loop). This is the co-host hook:
        an ASGI host or a test drives the lifecycle.

            async with app.serving():
                reply = await app.query_once("robot/move", distance=5)

        Pass ``serve_http=True`` to also start the embedded aiohttp surface;
        under FastAPI/Starlette leave it off and let the ASGI host serve HTTP."""
        await self._startup(serve_http=serve_http)
        try:
            yield self
        finally:
            await self._shutdown()

    async def run_async(self) -> None:
        """
        Async entry-point.
        Opens a Zenoh session, binds registries, and keeps the loop alive.
        """
        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)
        async with self.serving(serve_http=True):
            try:
                if self._shutdown_event is not None:
                    await self._shutdown_event.wait()
                else:
                    while True:
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass

    def run(self) -> None:
        """
        Sync entry-point.
        Detects whether an event loop is already running and adapts.
        """
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.run_async())
        except RuntimeError:
            asyncio.run(self.run_async())
