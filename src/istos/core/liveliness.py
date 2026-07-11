import asyncio
import inspect
import zenoh
from typing import Any, Callable, Optional

from istos.di.depends import has_dependencies, invoke_with_dependencies, positional_param_names


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
    and passes the key expression and status to the function. The callback may
    declare Depends(...) dependencies, resolved per event.
    """
    def __init__(self, func: Callable, prefix: str, dependency_overrides: Optional[dict] = None):
        self.func = func
        self.prefix = prefix
        self.calls = 0
        # key_expr and is_alive fill the first two positional slots.
        self._has_depends = has_dependencies(func)
        self._skip_names = tuple(positional_param_names(func)[:2])
        self._dependency_overrides = dependency_overrides if dependency_overrides is not None else {}

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args, **kwargs)
        return await asyncio.to_thread(self.func, *args, **kwargs)

    async def on_sample(self, sample: zenoh.Sample, instance: Optional[Any] = None) -> None:
        """Called by Zenoh when a liveliness event occurs."""
        self.calls += 1
        try:
            key_expr = str(sample.key_expr)
            is_alive = sample.kind == zenoh.SampleKind.PUT
            args = (instance, key_expr, is_alive) if instance is not None else (key_expr, is_alive)

            if self._has_depends:
                await invoke_with_dependencies(
                    self.func, args=args, skip_names=self._skip_names,
                    overrides=self._dependency_overrides,
                )
            elif inspect.iscoroutinefunction(self.func):
                await self.func(*args)
            else:
                await asyncio.to_thread(self.func, *args)
        except Exception as e:
            print(f"[Istos Liveliness] Error processing sample on {self.prefix}: {e}")

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_liveliness_wrapper(self, instance)
