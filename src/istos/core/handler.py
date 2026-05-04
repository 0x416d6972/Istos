import inspect
import zenoh
from typing import Any, Callable, Optional
from istos.consistency.storage import StoragePlugin
from istos.messages.serialization import Serialize, JsonSerializer
from istos.core.validation import validate_params, SchemaValidationError


class bound_handler_wrapper:
    """Bound-method proxy that injects `self` (the instance) into calls."""
    def __init__(self, desc: "handler_wrapper", subj: Any):
        self.desc = desc
        self.subj = subj

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.desc(self.subj, *args, **kwargs)


class handler_wrapper:
    """
    Descriptor that replaces the original function.
    Tracks invocations and writes serialized metadata to storage.
    """
    def __init__(self, func: Callable, prefix: str, storage: StoragePlugin, serializer: Serialize):
        self.func = func
        self.prefix = prefix
        self.storage = storage
        self.serializer = serializer
        self.calls = 0

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1

        metadata = {"func_name": self.func.__name__, "total_calls": self.calls}
        serialized = self.serializer.serialize(metadata)
        await self.storage.put(self.prefix, serialized)

        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args, **kwargs)
        else:
            return self.func(*args, **kwargs)

    async def on_query(self, query: zenoh.Query) -> None:
        try:
            key = str(query.selector.key_expr)
            
            # Extract parameters from query
            params = {}
            if hasattr(query.selector, "parameters") and query.selector.parameters:
                # parameters is a zenoh Map, extract as dict
                for k, v in query.selector.parameters.items():
                    params[k] = v
            
            # Validate and coerce parameters against function signature
            try:
                validated_params = validate_params(self.func, params)
            except SchemaValidationError as e:
                print(f"[handler_wrapper] Validation error on '{self.prefix}': {e}")
                error_payload = self.serializer.serialize({
                    "error": "validation_error",
                    "details": str(e.errors),
                })
                query.reply(key, error_payload)
                return

            # Execute function
            result = await self(**validated_params)
            
            # reply
            if result is not None:
                payload = self.serializer.serialize(result)
                query.reply(key, payload)
        except Exception as e:
            print(f"[handler_wrapper] Error executing handler '{self.prefix}': {e}")
            
    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return bound_handler_wrapper(self, instance)

