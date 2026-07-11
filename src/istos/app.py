import asyncio
import signal
import warnings
import zenoh
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, Callable, List, Mapping, Optional, Type, Union, AsyncContextManager

from istos.communication.sessions import SessionManager, AsyncZenohSession, ZenohSession
from istos.communication.config import IstosZenohConfig
from istos.consistency.storage import StoragePlugin, InMemoryStoragePlugin, Durability
from istos.consistency.sqlalchemy_storage import SqlAlchemyStoragePlugin
from istos.consistency.config import DatabaseConfig
from istos.consistency.databases import DatabaseRegistry
from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.handler import handler_wrapper
from istos.core.query import query_wrapper
from istos.core.subscribe import subscribe_wrapper
from istos.core.publish import publish_wrapper
from istos.core.liveliness import liveliness_wrapper
from istos.core.retry import RetryPolicy
from istos.core.asyncapi import AsyncApiGenerator, get_asyncapi_ui_html
from istos.core.errors import (
    ExceptionHandler,
    ExceptionHandlerRegistry,
    IstosSecurityWarning,
    get_default_registry,
)
from istos.core.authz import Authorizer, combine_authorizers
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
        enable_tracing: bool = False,
        tracing_endpoint: Optional[str] = None,
        service_name: str = "istos",
        exception_registry: Optional[ExceptionHandlerRegistry] = None,
        authorizer: Optional[Authorizer] = None,
        configure_logging: Optional[bool] = None,
    ):
        # Logging follows the library convention: don't touch output config by
        # default. configure_logging=True installs Istos' handler now; None (the
        # default) defers to run(), which only installs a default handler if the
        # app hasn't configured one; False leaves logging entirely to the app.
        self._log_level = log_level
        self._json_logs = json_logs
        self._configure_logging = configure_logging
        if configure_logging:
            _configure_logging(level=log_level, json_format=json_logs)
        self._logger = get_logger("app")

        # config= is a convenience: build the session manager from an
        # IstosZenohConfig (or a raw zenoh.Config) so callers don't have to write
        # AsyncZenohSession(config.build()) themselves. The build happens here, and
        # the sync/async flavor is taken from config.session.
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
                # Raw zenoh.Config: no session-flavor hint, default to async.
                zenoh_conf = config
                session_cls = AsyncZenohSession
            session_manager = session_cls(zenoh_conf)

        self._session_manager = session_manager or AsyncZenohSession()
        # Named application databases: app-lifetime engines Istos manages and
        # hands to handlers per request via Depends(app.db_session(name)).
        self._databases = DatabaseRegistry(databases or {})

        # The durability ledger can be specified exactly one of three ways.
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
            # Borrow the named engine; the registry owns its disposal.
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
        # Dependency overrides for testing: map a dependency
        # callable to a replacement. Mutate app.dependency_overrides in tests.
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
        self._handlers: List[handler_wrapper] = []
        self._queries: List[query_wrapper] = []
        self._subscribers: List[subscribe_wrapper] = []
        self._publishers: List[publish_wrapper] = []
        self._liveliness_subs: List[liveliness_wrapper] = []
        self._liveliness_declares: List[str] = []
        self._zenoh_subscribers: List[zenoh.Subscriber] = []
        self._zenoh_queryables: List[zenoh.Queryable] = []
        self._zenoh_liveliness_subs: List[Any] = []
        self._zenoh_liveliness_tokens: List[Any] = []
        self._shm_provider: Optional[Any] = None
        self._docs_web_port: Optional[int] = None
        self._docs_prefix: Optional[str] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._builtin_handlers_registered = False

    def _get_or_init_shm(self) -> Any:
        if self._shm_provider is None:
            self._shm_provider = zenoh.shm.ShmProvider.default_backend(10 * 1024 * 1024)
        return self._shm_provider

    # ------------------------------------------------------------------
    # Middleware & Exception Handlers
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Decorator
    # ------------------------------------------------------------------

    def handle(
        self,
        prefix: str,
        serializer: Optional[Serialize] = None,
        retry: Optional[Union[int, RetryPolicy]] = None,
        durability: Union[str, "Durability"] = Durability.AT_MOST_ONCE,
        authorizer: Optional[Authorizer] = None,
    ) -> Callable:
        """
        Decorator that registers a function or method as an Istos handler.

            @istos.handle(prefix="robot/move")
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
        """
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

    def query(self, prefix: str, timeout_s: float = 5.0, retry: Optional[Union[int, RetryPolicy]] = None, serializer: Optional[Serialize] = None) -> Callable:
        """
        Decorator that queries a registered handler when the function is called.

            @istos.query("math/add", retry=5)
            def process(result):
                print(result)

            @istos.query("binary/data", serializer=MsgPackSerializer())
            def process_binary(result): ...
        """
        def decorator(func: Callable) -> query_wrapper:
            wrapper = query_wrapper(
                func, prefix, serializer or JsonSerializer(),
                get_session=lambda: self._session_manager.session,
                timeout_s=timeout_s,
                retry=retry,
                dependency_overrides=self.dependency_overrides,
            )
            self._queries.append(wrapper)
            return wrapper
        return decorator

    # ------------------------------------------------------------------
    # Pub/Sub & Advanced Features
    # ------------------------------------------------------------------

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
    ) -> Callable:
        """
        Decorator that publishes the return value of a function to the network.

            @istos.publish("drone/telemetry")
            def get_telemetry():
                return {"battery": 85}

            @istos.publish("binary/feed", serializer=MsgPackSerializer())
            def get_feed(): ...

        Brokerless durability: with ``durable=True`` the message is published
        through Zenoh's AdvancedPublisher, which retains the last ``cache`` samples
        as a replay log and heartbeats every ``heartbeat`` seconds so late or
        recovering subscribers can fetch what they missed — no broker required.

            @istos.publish("orders/created", durable=True, cache=1000)
            def created(): ...

        Durable publishers default to ``reliability=RELIABLE`` and
        ``congestion_control=BLOCK`` so samples are not silently dropped under
        backpressure; pass either explicitly to override.
        """
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
    ) -> Callable:
        """
        Decorator that registers a function to be called when data is published
        to a prefix.

            @istos.subscribe("drone/telemetry", retry=3)
            def on_telemetry(data):
                print(data)

            @istos.subscribe("binary/events", serializer=MsgPackSerializer())
            def on_event(data): ...

        Brokerless durability: with ``durable=True`` the subscription uses Zenoh's
        AdvancedSubscriber, which replays up to ``replay`` historical samples from
        the producer's cache on join, and (when ``recover=True``) re-fetches
        samples missed during transient disconnects — no broker required.

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
        """
        def decorator(func: Callable) -> subscribe_wrapper:
            wrapper = subscribe_wrapper(
                func, prefix, serializer or JsonSerializer(), retry=retry,
                dependency_overrides=self.dependency_overrides,
                durable=durable, replay=replay, recover=recover,
                on_miss=on_miss,
                middleware=self._middleware_stack,
                authorizer=combine_authorizers(self._authorizer, authorizer),
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

    # ------------------------------------------------------------------
    # Querying / Publishing directly
    # ------------------------------------------------------------------

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

    async def publish_once(self, prefix: str, data: Any, use_shm: bool = False, serializer: Optional[Serialize] = None) -> None:
        """
        One-shot publish without a decorator.

            await istos.publish_once("drone/status", {"ok": True})
            await istos.publish_once("binary/data", payload, serializer=MsgPackSerializer())
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session.")
        _serializer = serializer or JsonSerializer()
        serialized = _serializer.serialize(data)
        
        def _do_put():
            if use_shm:
                provider = self._get_or_init_shm()
                payload = serialized.encode('utf-8') if isinstance(serialized, str) else serialized
                if not isinstance(payload, bytes):
                    payload = str(payload).encode('utf-8')
                sbuf = provider.alloc(len(payload))
                sbuf[:] = payload
                session.put(prefix, sbuf)
            else:
                session.put(prefix, serialized)

        await asyncio.to_thread(_do_put)

    async def delete_once(self, prefix: str) -> None:
        """
        Issue a network-wide DELETE operation for a given prefix.
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session.")
        await asyncio.to_thread(session.delete, prefix)

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def include_router(self, router: IstosRouter) -> None:
        """
        Includes a router's routes into the main application.
        """
        for action in router._actions:
            action(self)

    # ------------------------------------------------------------------
    # Auto-Documentation (AsyncAPI)
    # ------------------------------------------------------------------

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
        # For the warning only: is the endpoint gated at all once layering applies?
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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

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
                # Brokerless durable subscription: replay history on join +
                # recover missed samples peer-to-peer from the producer's cache.
                # Unrecoverable gaps are bridged back to the loop via handle_miss.
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

    async def _bind_liveliness(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()
        
        for prefix in self._liveliness_declares:
            # Zenoh API: session.liveliness().declare_token(...)
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

    async def _start_web_docs(self) -> Any:
        try:
            from aiohttp import web
        except ImportError:
            self._logger.warning("aiohttp required for web docs: uv pip install aiohttp")
            return None

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

        app = web.Application()
        app.router.add_get('/', web_ui_handler)
        app.router.add_get('/asyncapi.yaml', asyncapi_yaml_handler)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self._docs_web_port)
        await site.start()
        url = f"http://localhost:{self._docs_web_port}"
        self._logger.info("Serving docs UI at %s", url, extra={"url": url})
        return runner

    def _register_builtin_handlers(self) -> None:
        if self._builtin_handlers_registered:
            return
        self._builtin_handlers_registered = True

        # Built-in endpoints register through self.handle and therefore inherit
        # the app-wide authorizer. Warn if they will be network-reachable with
        # no authorization at all.
        if self._authorizer is None and (self._enable_health or self._enable_metrics):
            exposed = []
            if self._enable_health:
                exposed += [".istos/health", ".istos/ready"]
            if self._enable_metrics:
                exposed.append(".istos/metrics")
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

    async def run_async(self) -> None:
        """
        Async entry-point.
        Opens a Zenoh session, binds registries, and keeps the loop alive.
        """
        # Standalone convenience: install a default log handler only if neither
        # Istos nor the embedding app already configured one (unless opted out).
        if self._configure_logging is not False:
            ensure_configured(self._log_level, self._json_logs)
        self._register_builtin_handlers()
        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)

        async with AsyncExitStack() as stack:
            if self.lifespan:
                await stack.enter_async_context(self.lifespan(self))

            # Tie the storage backend's teardown to the service lifecycle: whatever
            # connection pool / engine it holds is disposed on shutdown (normal or
            # error). close() is optional — only backends that hold resources define
            # it (Redis, SQLAlchemy); InMemory does not.
            storage_close = getattr(self._storage, "close", None)
            if callable(storage_close):
                stack.push_async_callback(storage_close)

            # Dispose all named application-database engines on shutdown too.
            stack.push_async_callback(self._databases.dispose_all)

            # Support both the async session manager and a sync one (session="sync").
            if hasattr(self._session_manager, "__aenter__"):
                session = await stack.enter_async_context(self._session_manager)  # type: ignore
            else:
                session = stack.enter_context(self._session_manager)  # type: ignore

            await self._bind_handlers(session)
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

            web_runner = None
            if self._docs_web_port is not None:
                web_runner = await self._start_web_docs()

            try:
                if self._shutdown_event is not None:
                    await self._shutdown_event.wait()
                else:
                    while True:
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
            finally:
                self._health.ready = False
                self._logger.info("Service stopping")
                if web_runner:
                    await web_runner.cleanup()
                await self._unbind_liveliness()
                await self._unbind_subscribers()
                await self._unbind_publishers()
                await self._unbind_handlers()
                self._logger.info("Service stopped")

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
