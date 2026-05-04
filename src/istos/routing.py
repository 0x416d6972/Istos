from typing import Any, Callable, List, Optional, Union, TYPE_CHECKING
from istos.core.retry import RetryPolicy

if TYPE_CHECKING:
    from istos.Istos import Istos

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
        return self._real_wrapper(*args, **kwargs)

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

    def handle(self, prefix: str) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                # Register on the app and store the real wrapper in the proxy
                proxy._real_wrapper = app.handle(full_prefix)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def query(self, prefix: str, timeout_s: float = 5.0, retry: Optional[Union[int, RetryPolicy]] = None) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.query(full_prefix, timeout_s=timeout_s, retry=retry)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def publish(self, prefix: str, use_shm: bool = False) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.publish(full_prefix, use_shm=use_shm)(func)
            self._actions.append(action)
            return proxy
        return decorator

    def subscribe(self, prefix: str, retry: Optional[Union[int, RetryPolicy]] = None) -> Callable:
        full_prefix = self._apply_prefix(prefix)
        def decorator(func: Callable) -> Callable:
            proxy = RouterProxy(func.__name__)
            def action(app: "Istos"):
                proxy._real_wrapper = app.subscribe(full_prefix, retry=retry)(func)
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
