import asyncio
import inspect
import zenoh
from typing import Any, Callable, List, Optional, Union

from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.retry import RetryPolicy, execute_with_retry

class bound_subscribe_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "subscribe_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class subscribe_wrapper:
    """
    Descriptor that wraps a function to become a subscriber callback.
    It takes the payload from Zenoh, deserializes it, and passes it to the function.
    """
    def __init__(
        self,
        func: Callable,
        prefix: str,
        serializer: Serialize,
        retry: Optional[Union[int, RetryPolicy]] = None,
    ):
        self.func = func
        self.prefix = prefix
        self.serializer = serializer
        self.calls = 0

        # Normalize retry parameter
        if retry is None:
            self.retry_policy = RetryPolicy(max_retries=0)
        elif isinstance(retry, int):
            self.retry_policy = RetryPolicy.from_int(retry)
        else:
            self.retry_policy = retry

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # Note: the actual callback logic is handled in Istos.py when binding.
        # This wrapper allows the user to manually call the function if they want,
        # but typically it's triggered internally by `on_sample`.
        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args, **kwargs)
        else:
            return self.func(*args, **kwargs)

    async def on_sample(self, sample: zenoh.Sample, instance: Optional[Any] = None) -> None:
        """Called by Zenoh when a new sample arrives."""
        self.calls += 1
        try:
            raw_payload = bytes(sample.payload)
            data = self.serializer.deserialize(raw_payload)

            async def _process():
                # If it's a bound method (class instance), pass the instance as first arg
                if instance is not None:
                    if inspect.iscoroutinefunction(self.func):
                        await self.func(instance, data)
                    else:
                        self.func(instance, data)
                else:
                    if inspect.iscoroutinefunction(self.func):
                        await self.func(data)
                    else:
                        self.func(data)

            await execute_with_retry(_process, self.retry_policy)
        except Exception as e:
            print(f"[Istos Subscribe] Error processing sample on {self.prefix}: {e}")

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_subscribe_wrapper(self, instance)

