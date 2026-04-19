import asyncio
import inspect
import zenoh
from typing import Any, Callable, Optional


class bound_liveliness_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "liveliness_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class liveliness_wrapper:
    """
    Descriptor that wraps a function to become a liveliness callback.
    It takes the payload from Zenoh, determines if a node joined or dropped,
    and passes the key expression and status to the function.
    """
    def __init__(self, func: Callable, prefix: str):
        self.func = func
        self.prefix = prefix
        self.calls = 0

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args, **kwargs)
        else:
            return self.func(*args, **kwargs)

    async def on_sample(self, sample: zenoh.Sample, instance: Optional[Any] = None) -> None:
        """Called by Zenoh when a liveliness event occurs."""
        self.calls += 1
        try:
            key_expr = str(sample.key_expr)
            is_alive = sample.kind == zenoh.SampleKind.PUT

            if instance is not None:
                if inspect.iscoroutinefunction(self.func):
                    await self.func(instance, key_expr, is_alive)
                else:
                    self.func(instance, key_expr, is_alive)
            else:
                if inspect.iscoroutinefunction(self.func):
                    await self.func(key_expr, is_alive)
                else:
                    self.func(key_expr, is_alive)
        except Exception as e:
            print(f"[Istos Liveliness] Error processing sample on {self.prefix}: {e}")

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_liveliness_wrapper(self, instance)
