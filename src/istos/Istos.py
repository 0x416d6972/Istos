import asyncio
import inspect
import zenoh
from typing import Any, Callable, List, Optional, Union

from istos.communication.sessions import SessionManager, AsyncZenohSession, ZenohSession
from istos.consistency.register import AbstractRegistery
from istos.consistency.storage import StoragePlugin, InMemoryStoragePlugin
from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.agent import agent_wrapper
from istos.core.query import query_wrapper
from istos.core.subscribe import subscribe_wrapper
from istos.core.publish import publish_wrapper
from istos.core.liveliness import liveliness_wrapper
from istos.core.retry import RetryPolicy

class Istos:
    """
    Unified entry-point for the Istos framework.

    Usage:
        istos = Istos()

        @istos.agent(prefix="robot/move")
        async def move(distance: int):
            return f"moved {distance}m"

        class Drone:
            @istos.agent(prefix="drone/fly")
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
    ):
        self._session_manager = session_manager or AsyncZenohSession()
        self._storage = storage or InMemoryStoragePlugin()
        self._serializer = serializer or JsonSerializer()
        self._registries: List[AbstractRegistery] = []
        self._agents: List[agent_wrapper] = []
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

    def _get_or_init_shm(self) -> Any:
        if self._shm_provider is None:
            self._shm_provider = zenoh.shm.ShmProvider.default_backend(10 * 1024 * 1024)
        return self._shm_provider

    # ------------------------------------------------------------------
    # Decorator
    # ------------------------------------------------------------------

    def agent(self, prefix: str) -> Callable:
        """
        Decorator that registers a function or method as an Istos agent.

            @istos.agent(prefix="robot/move")
            async def move(distance: int): ...
        """
        def decorator(func: Callable) -> agent_wrapper:
            wrapper = agent_wrapper(func, prefix, self._storage, self._serializer)
            self._agents.append(wrapper)
            
            # Since Istos previously bound the agent's logic to storage automatically 
            # somewhere else or it didn't at all (it seems PrefixRegistery just looks up storage),
            # let's look at how PrefixRegistery gets registered.
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
        Decorator that queries a registered agent when the function is called.

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
    # Lifecycle
    # ------------------------------------------------------------------

    async def _bind_registries(self, session: zenoh.Session) -> None:
        for registry in self._registries:
            print(f"[Istos] Binding registry: {registry._prefix}")
            await registry.register(session)

    async def _unbind_registries(self) -> None:
        for registry in self._registries:
            await registry.unregister()

    async def _bind_agents(self, session: zenoh.Session) -> None:
        loop = asyncio.get_running_loop()
        
        for wrapper in self._agents:
            print(f"[Istos] Binding agent to: {wrapper.prefix}")

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

    async def _unbind_agents(self) -> None:
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

    async def run_async(self) -> None:
        """
        Async entry-point.
        Opens a Zenoh session, binds registries, and keeps the loop alive.
        """
        async with self._session_manager as session:  # type: ignore
            await self._bind_registries(session)
            await self._bind_agents(session)
            await self._bind_subscribers(session)
            await self._bind_liveliness(session)

            prefixes = [a.prefix for a in self._agents]
            print(f"[Istos] Active agents: {prefixes}")
            subs = [s.prefix for s in self._subscribers]
            print(f"[Istos] Active subscribers: {subs}")
            print("[Istos] Running (async). Press Ctrl+C to stop.")

            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
            finally:
                await self._unbind_liveliness()
                await self._unbind_subscribers()
                await self._unbind_agents()
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
