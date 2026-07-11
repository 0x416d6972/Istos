import inspect
from typing import Any, Callable, List, Optional, Union, TYPE_CHECKING
from istos.core.retry import RetryPolicy
from istos.core.authz import Authorizer
from istos.messages.serialization import Serialize

if TYPE_CHECKING:
    from istos.app import Istos

class RouterProxy:
    """
    A proxy object returned by router decorators.
    Delegates calls to the actual Istos wrapper once the router is included
    in the main application.
    """
    def __init__(self, name: str):
        self._real_wrapper: Optional[Callable] = None
        self._name = name
        
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._real_wrapper is None:
            raise RuntimeError(
                f"Router has not been included in the Istos app yet. "
                f"Cannot invoke '{self._name}'."
            )
        result = self._real_wrapper(*args, **kwargs)
        if inspect.iscoroutine(result):
            return result
        return result

    def __get__(self, instance: Any, owner: Any) -> Any:
        if self._real_wrapper is None:
            return self
        # Delegate binding if it's a descriptor/method
        if hasattr(self._real_wrapper, "__get__"):
            return self._real_wrapper.__get__(instance, owner)
        return self


class IstosRouter:
    """
    A router to group Istos decorators.
    Routes defined here will be applied to the main Istos app when 
    `istos.include_router(router)` is called.
    """
    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        self._actions: List[Callable[["Istos"], None]] = []

    def _apply_prefix(self, prefix: str) -> str:
        """Combines the router's prefix with the endpoint's prefix."""
        if self.prefix:
            base = self.prefix.rstrip('/')
            sub = prefix.lstrip('/')
            return f"{base}/{sub}" if base and sub else (base or sub)
        return prefix

    def handle(self, prefix: str, serializer: Optional[Serialize] = None, retry: Optional[Union[int, RetryPolicy]] = None, durability: str = "at_most_once", authorizer: Optional[Authorizer] = None) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.handle(full_prefix, serializer=serializer, retry=retry, durability=durability, authorizer=authorizer)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def query(self, prefix: str, timeout_s: float = 5.0, retry: Optional[Union[int, RetryPolicy]] = None, serializer: Optional[Serialize] = None) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.query(full_prefix, timeout_s=timeout_s, retry=retry, serializer=serializer)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def publish(self, prefix: str, use_shm: bool = False, serializer: Optional[Serialize] = None, durable: bool = False, cache: int = 1000, heartbeat: float = 1.0) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.publish(full_prefix, use_shm=use_shm, serializer=serializer, durable=durable, cache=cache, heartbeat=heartbeat)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def subscribe(self, prefix: str, retry: Optional[Union[int, RetryPolicy]] = None, serializer: Optional[Serialize] = None, durable: bool = False, replay: int = 1000, recover: bool = True, authorizer: Optional[Authorizer] = None) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.subscribe(full_prefix, retry=retry, serializer=serializer, durable=durable, replay=replay, recover=recover, authorizer=authorizer)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def on_liveliness(self, prefix: str) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.on_liveliness(full_prefix)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def declare_liveliness(self, prefix: str) -> None:
        full_prefix = self._apply_prefix(prefix)
        def action(app: "Istos"):
            app.declare_liveliness(full_prefix)
        self._actions.append(action)
