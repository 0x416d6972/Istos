import asyncio
import inspect
import zenoh
from typing import Any, Callable, Optional

from istos.messages.serialization import Serialize, JsonSerializer

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
    ):
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self._get_session = get_session
        self.use_shm = use_shm
        self._get_shm_provider = get_shm_provider
        self.calls = 0

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1

        # Calculate the result of the function
        if inspect.iscoroutinefunction(self.func):
            result = await self.func(*args, **kwargs)
        else:
            result = self.func(*args, **kwargs)

        # Publish the result
        session = self._get_session()
        if session is None:
            raise RuntimeError(
                f"No active Zenoh session to publish '{self.prefix}'. "
                f"Call istos.run() or istos.run_async() first."
            )

        serialized = self.serializer.serialize(result)
        
        def _do_put():
            if self.use_shm:
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
                session.put(self.prefix, sbuf)
            else:
                session.put(self.prefix, serialized)

        # Zenoh's put is synchronous, so offload it just in case
        await asyncio.to_thread(_do_put)

        return result

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_publish_wrapper(self, instance)
