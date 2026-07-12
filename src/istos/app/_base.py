"""Shared state for the Istos mixins: the constructor and small accessors.

Every mixin inherits this, so ``__init__`` here is the single source of truth
for instance state and mypy resolves ``self._x`` for the mixin methods."""

import asyncio
import zenoh
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, Callable, List, Mapping, Optional, Type, Union, AsyncContextManager, cast

from istos.communication.sessions import SessionManager, AsyncZenohSession, ZenohSession
from istos.communication.config import IstosZenohConfig
from istos.communication.persist import PersistRole
from istos.consistency.storage import StoragePlugin, InMemoryStoragePlugin
from istos.consistency.sqlalchemy_storage import SqlAlchemyStoragePlugin
from istos.consistency.config import DatabaseConfig
from istos.consistency.databases import DatabaseRegistry
from istos.messages.serialization import JsonSerializer
from istos.primitives.handler import handler_wrapper
from istos.primitives.query import query_wrapper
from istos.primitives.subscribe import subscribe_wrapper
from istos.primitives.publish import publish_wrapper
from istos.primitives.stream import stream_wrapper
from istos.primitives.channel import channel_wrapper
from istos.primitives.channel_fabric import FabricChannelServer
from istos.queue import QueueRole, worker_wrapper
from istos.primitives.liveliness import liveliness_wrapper
from istos.errors import (
    ExceptionHandler,
    ExceptionHandlerRegistry,
    IstosSecurityError,
    get_default_registry,
)
from istos.security.authz import Authorizer
from istos.http.gateway import HttpRoute
from istos.routing import IstosRouter
from istos.logging import configure_logging as _configure_logging, get_logger
from istos.middleware.base import (
    CorrelationIdMiddleware,
    LoggingMiddleware,
    Middleware,
    MiddlewareStack,
)
from istos.http.health import HealthState
from istos.observability.metrics import MetricsCollector, PrometheusMiddleware
from istos.observability.tracing import TracingMiddleware, configure_tracing
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from istos.app import Istos


class IstosBase:
    """State and construction shared by every Istos mixin."""

    if TYPE_CHECKING:
        # Implemented by sibling mixins on the composed Istos class; declared
        # here so mypy resolves the cross-mixin calls from lifecycle and web.
        handle: Callable[..., Any]
        query_once: Callable[..., Any]
        stream_query: Callable[..., Any]
        export_capabilities: Callable[..., Any]
        _register_builtin_handlers: Callable[..., Any]
        _http_server_port: Callable[..., Any]
        _start_http_server: Callable[..., Any]
        _bind_handlers: Callable[..., Any]
        _unbind_handlers: Callable[..., Any]
        _bind_streams: Callable[..., Any]
        _bind_channels: Callable[..., Any]
        _unbind_channels: Callable[..., Any]
        _bind_subscribers: Callable[..., Any]
        _unbind_subscribers: Callable[..., Any]
        _bind_publishers: Callable[..., Any]
        _unbind_publishers: Callable[..., Any]
        _bind_persist: Callable[..., Any]
        _unbind_persist: Callable[..., Any]
        _bind_queues: Callable[..., Any]
        _unbind_queues: Callable[..., Any]
        _bind_liveliness: Callable[..., Any]
        _unbind_liveliness: Callable[..., Any]

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
        enable_mcp: bool = False,
        mcp_path: str = "/mcp",
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
        self._channels: List[channel_wrapper] = []
        self._ws_channel_routes: List[tuple] = []
        self._channel_servers: List[FabricChannelServer] = []
        self._queue_roles: List[QueueRole] = []
        self._workers: List[worker_wrapper] = []
        self._schedules: List[dict] = []
        self._schedule_tasks: List[asyncio.Task] = []
        self._enable_mcp = enable_mcp
        self._mcp_path = mcp_path

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

    def include_router(self, router: IstosRouter) -> None:
        """
        Includes a router's routes into the main application.
        """
        app = cast("Istos", self)
        for action in router._actions:
            action(app)


