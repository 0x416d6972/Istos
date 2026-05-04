import asyncio
import inspect
import zenoh
from contextlib import AsyncExitStack
from typing import Any, Callable, List, Optional, Union, AsyncContextManager

from istos.communication.sessions import SessionManager, AsyncZenohSession, ZenohSession
from istos.consistency.register import AbstractRegistery
from istos.consistency.storage import StoragePlugin, InMemoryStoragePlugin
from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.handler import handler_wrapper
from istos.core.query import query_wrapper
from istos.core.subscribe import subscribe_wrapper
from istos.core.publish import publish_wrapper
from istos.core.liveliness import liveliness_wrapper
from istos.core.retry import RetryPolicy
from istos.core.asyncapi import AsyncApiGenerator, get_asyncapi_ui_html
from istos.routing import IstosRouter

class Istos:
    """
    Unified entry-point for the Istos framework.

    Usage:
        istos = Istos()

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
        serializer: Optional[Serialize] = None,
        lifespan: Optional[Callable[["Istos"], AsyncContextManager[None]]] = None,
    ):
        self._session_manager = session_manager or AsyncZenohSession()
        self._storage = storage or InMemoryStoragePlugin()
        self._serializer = serializer or JsonSerializer()
        self.lifespan = lifespan
        self._registries: List[AbstractRegistery] = []
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

    def _get_or_init_shm(self) -> Any:
        if self._shm_provider is None:
            self._shm_provider = zenoh.shm.ShmProvider.default_backend(10 * 1024 * 1024)
        return self._shm_provider

    # ------------------------------------------------------------------
    # Decorator
    # ------------------------------------------------------------------

    def handle(self, prefix: str) -> Callable:
        """
        Decorator that registers a function or method as an Istos handler.

            @istos.handle(prefix="robot/move")
            async def move(distance: int): ...
        """
        def decorator(func: Callable) -> handler_wrapper:
            wrapper = handler_wrapper(func, prefix, self._storage, self._serializer)
            self._handlers.append(wrapper)
            
            return wrapper
        return decorator

    # ------------------------------------------------------------------
    # Registry management
    # ------------------------------------------------------------------

    def add_registry(self, registry: AbstractRegistery) -> None:
        """Bind a PrefixRegistery to be connected on startup."""
        self._registries.append(registry)

    def query(self, prefix: str, timeout_s: float = 5.0, retry: Optional[Union[int, RetryPolicy]] = None) -> Callable:
        """
        Decorator that queries a registered handler when the function is called.

            @istos.query("math/add", retry=5)
            def process(result):
                print(result)
        """
        def decorator(func: Callable) -> query_wrapper:
            wrapper = query_wrapper(
                func, prefix, self._serializer,
                get_session=lambda: self._session_manager.session,
                timeout_s=timeout_s,
                retry=retry,
            )
            self._queries.append(wrapper)
            return wrapper
        return decorator

    # ------------------------------------------------------------------
    # Pub/Sub & Advanced Features
    # ------------------------------------------------------------------

    def publish(self, prefix: str, use_shm: bool = False) -> Callable:
        """
        Decorator that publishes the return value of a function to the network.

            @istos.publish("drone/telemetry")
            def get_telemetry():
                return {"battery": 85}
        """
        def decorator(func: Callable) -> publish_wrapper:
            wrapper = publish_wrapper(
                func, prefix, self._serializer,
                get_session=lambda: self._session_manager.session,
                use_shm=use_shm,
                get_shm_provider=self._get_or_init_shm
            )
            self._publishers.append(wrapper)
            return wrapper
        return decorator

    def subscribe(self, prefix: str, retry: Optional[Union[int, RetryPolicy]] = None) -> Callable:
        """
        Decorator that registers a function to be called when data is published
        to a prefix.

            @istos.subscribe("drone/telemetry", retry=3)
            def on_telemetry(data):
                print(data)
        """
        def decorator(func: Callable) -> subscribe_wrapper:
            wrapper = subscribe_wrapper(func, prefix, self._serializer, retry=retry)
            self._subscribers.append(wrapper)
            return wrapper
        return decorator

    def on_liveliness(self, prefix: str) -> Callable:
        """
        Decorator that registers a function to handle liveliness events on a network.
        Function signature should be: func(key_expr: str, is_alive: bool)
        """
        def decorator(func: Callable) -> liveliness_wrapper:
            wrapper = liveliness_wrapper(func, prefix)
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
        **kwargs: Any
    ) -> List[Any]:
        """
        One-shot query without a decorator. Allows query parameters via kwargs.

            results = await istos.query_once("robot/move", distance=10)
        """
        if self._session_manager.session is None:
            raise RuntimeError(
                "No active Zenoh session. Call istos.run() or istos.run_async() first."
            )
        wrapper = query_wrapper(
            func=lambda data: data,
            prefix=key_expr,
            serializer=self._serializer,
            get_session=lambda: self._session_manager.session,
            timeout_s=timeout_s,
        )
        return await wrapper(**kwargs)

    async def publish_once(self, prefix: str, data: Any, use_shm: bool = False) -> None:
        """
        One-shot publish without a decorator.
        """
        session = self._session_manager.session
        if session is None:
            raise RuntimeError("No active Zenoh session.")
        serialized = self._serializer.serialize(data)
        
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

    def serve_docs(self, prefix: str = ".istos/docs", title: str = "Istos Network", version: str = "1.0.0", web_port: Optional[int] = None) -> None:
        """
        Registers a built-in handler to serve the AsyncAPI specification over Zenoh.
        If web_port is provided, it starts an embedded HTTP server to display the UI.
        """
        @self.handle(prefix=prefix)
        def _serve_docs() -> str:
            return self.export_asyncapi(title=title, version=version)
            
        if web_port is not None:
            self._docs_web_port = web_port
            self._docs_prefix = prefix

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _bind_registries(self, session: zenoh.Session) -> None:
        for registry in self._registries:
            print(f"[Istos] Binding registry: {registry._prefix}")
            await registry.register(session)

    async def _unbind_registries(self) -> None:
        for registry in self._registries:
            await registry.unregister()

    async def _bind_handlers(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()
        
        for wrapper in self._handlers:
            print(f"[Istos] Binding handler to: {wrapper.prefix}")

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
            print(f"[Istos] Binding subscriber to: {wrapper.prefix}")

            def make_callback(w=wrapper):
                def _sync_callback(sample: zenoh.Sample):
                    if not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(w.on_sample(sample), loop)
                return _sync_callback

            sub = session.declare_subscriber(wrapper.prefix, make_callback())
            self._zenoh_subscribers.append(sub)

    async def _unbind_subscribers(self) -> None:
        for sub in self._zenoh_subscribers:
            sub.undeclare()
        self._zenoh_subscribers.clear()

    async def _bind_liveliness(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()
        
        for prefix in self._liveliness_declares:
            # Zenoh API: session.liveliness().declare_token(...)
            token = session.liveliness().declare_token(prefix)
            self._zenoh_liveliness_tokens.append(token)
            print(f"[Istos] Declared Liveliness token on: {prefix}")
            
        for wrapper in self._liveliness_subs:
            def make_callback(w=wrapper):
                def _sync_callback(sample: zenoh.Sample):
                    if not loop.is_closed():
                        asyncio.run_coroutine_threadsafe(w.on_sample(sample), loop)
                return _sync_callback

            sub = session.liveliness().declare_subscriber(wrapper.prefix, make_callback(), history=False)
            self._zenoh_liveliness_subs.append(sub)
            print(f"[Istos] Subscribed to Liveliness events on: {wrapper.prefix}")

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
            print("[Istos] aiohttp is required for web docs. Install it using: uv pip install aiohttp")
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
        print(f"[Istos] 🌍 AsyncAPI Web Docs running at: http://localhost:{self._docs_web_port}")
        return runner

    async def run_async(self) -> None:
        """
        Async entry-point.
        Opens a Zenoh session, binds registries, and keeps the loop alive.
        """
        async with AsyncExitStack() as stack:
            if self.lifespan:
                await stack.enter_async_context(self.lifespan(self))
                
            session = await stack.enter_async_context(self._session_manager)  # type: ignore

            await self._bind_registries(session)
            await self._bind_handlers(session)
            await self._bind_subscribers(session)
            await self._bind_liveliness(session)

            prefixes = [a.prefix for a in self._handlers]
            print(f"[Istos] Active handlers: {prefixes}")
            subs = [s.prefix for s in self._subscribers]
            print(f"[Istos] Active subscribers: {subs}")
            print("[Istos] Running (async). Press Ctrl+C to stop.")

            web_runner = None
            if self._docs_web_port is not None:
                web_runner = await self._start_web_docs()

            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
            finally:
                if web_runner:
                    await web_runner.cleanup()
                await self._unbind_liveliness()
                await self._unbind_subscribers()
                await self._unbind_handlers()
                await self._unbind_registries()

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
