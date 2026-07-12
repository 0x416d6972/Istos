import asyncio
import inspect
import zenoh
from typing import Any, Callable, Optional

from istos.messages.serialization import Serialize
from istos.context import RequestEnvelope, peek_request_context
from istos.di.depends import has_dependencies, invoke_with_dependencies, positional_param_names

class bound_publish_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "publish_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class publish_wrapper:
    """
    Descriptor that replaces the original function.
    On every call, it calculates the return value of the function,
    serializes it, and publishes it via Zenoh to the given prefix.
    """
    def __init__(
        self,
        func: Callable,
        prefix: str,
        serializer: Serialize,
        get_session: Callable[[], Optional[zenoh.Session]],
        use_shm: bool = False,
        get_shm_provider: Optional[Callable[[], Any]] = None,
        dependency_overrides: Optional[dict] = None,
        durable: bool = False,
        cache: int = 1000,
        heartbeat: float = 1.0,
        reliability: Optional["zenoh.Reliability"] = None,
        congestion_control: Optional["zenoh.CongestionControl"] = None,
    ):
        if durable and use_shm:
            raise ValueError(
                "durable=True and use_shm=True cannot be combined: durable publishing "
                "goes through Zenoh's advanced publisher, which manages its own buffers."
            )
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self._get_session = get_session
        self.use_shm = use_shm
        self._get_shm_provider = get_shm_provider
        self.calls = 0
        # Brokerless durability: publish through an AdvancedPublisher that retains
        # a replay cache. The publisher is declared once at startup (see
        # Istos._bind_publishers) and reused for every put.
        self.durable = durable
        self.cache = cache
        self.heartbeat = heartbeat
        self.reliability = reliability
        self.congestion_control = congestion_control
        self._advanced_pub: Optional[Any] = None
        # Dependency injection: any caller args fill leading positional params;
        # the rest may be Depends(...), resolved per publish.
        self._has_depends = has_dependencies(func)
        self._positional_names = positional_param_names(func)
        self._dependency_overrides = dependency_overrides if dependency_overrides is not None else {}

    def declare(self, session: "zenoh.Session") -> None:
        """Declare the durable AdvancedPublisher (called once at service startup)."""
        if not self.durable or self._advanced_pub is not None:
            return
        from istos.communication.durable import declare_durable_publisher
        self._advanced_pub = declare_durable_publisher(
            session, self.prefix, cache=self.cache, heartbeat=self.heartbeat,
            reliability=self.reliability, congestion_control=self.congestion_control,
        )

    def undeclare(self) -> None:
        """Undeclare the durable publisher on shutdown (releases its cache)."""
        if self._advanced_pub is not None:
            self._advanced_pub.undeclare()
            self._advanced_pub = None

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1

        zenoh_session = self._get_session()
        if zenoh_session is None:
            raise RuntimeError(
                "No active Zenoh session. Publishing uses the service's shared "
                "session — start it with istos.run()/run_async() first."
            )
        if self.durable and self._advanced_pub is None:
            raise RuntimeError(
                "Durable publisher not declared yet. Durable publishing requires the "
                "service to be running — start it with istos.run()/run_async() first."
            )

        # Calculate the result of the function (resolving any dependencies)
        if self._has_depends:
            result = await invoke_with_dependencies(
                self.func,
                args=args,
                context=kwargs,
                skip_names=tuple(self._positional_names[:len(args)]),
                overrides=self._dependency_overrides,
            )
        elif inspect.iscoroutinefunction(self.func):
            result = await self.func(*args, **kwargs)
        else:
            result = await asyncio.to_thread(self.func, *args, **kwargs)

        # Publish the result
        serialized = self.serializer.serialize(result)

        # When publishing from inside a request, carry its correlation_id /
        # traceparent so subscribers continue the same logical operation.
        ctx = peek_request_context()
        attachment = None
        if ctx is not None and (ctx.correlation_id or ctx.traceparent):
            attachment = RequestEnvelope(
                correlation_id=ctx.correlation_id, traceparent=ctx.traceparent
            ).to_attachment()

        put_kwargs = {"attachment": attachment} if attachment is not None else {}

        def _do_put():
            if self.durable:
                # Through the AdvancedPublisher (key-bound): cached for replay.
                self._advanced_pub.put(serialized, **put_kwargs)
            elif self.use_shm:
                if self._get_shm_provider is None:
                    raise RuntimeError("SHM provider callback not provided.")
                provider = self._get_shm_provider()
                if provider is None:
                    raise RuntimeError("SHM provider not initialized. Cannot publish via SHM.")
                payload = serialized.encode('utf-8') if isinstance(serialized, str) else serialized
                if not isinstance(payload, bytes):
                    payload = str(payload).encode('utf-8')
                sbuf = provider.alloc(len(payload))
                sbuf[:] = payload
                zenoh_session.put(self.prefix, sbuf, **put_kwargs)
            else:
                zenoh_session.put(self.prefix, serialized, **put_kwargs)

        # Zenoh's put is synchronous, so offload it just in case
        await asyncio.to_thread(_do_put)

        return result

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_publish_wrapper(self, instance)
