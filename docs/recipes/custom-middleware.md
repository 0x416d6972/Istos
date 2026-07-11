# Recipe: Custom middleware

Time every `@handle` invocation and attach the duration to logs.

```python
import time
from istos import Istos, get_logger
from istos.middleware import RequestScope
from istos.middleware.base import HandlerCallable

log = get_logger("myapp.timing")
istos = Istos()

class TimingMiddleware:
    async def __call__(self, scope: RequestScope, call_next: HandlerCallable):
        start = time.perf_counter()
        try:
            return await call_next(scope)
        finally:
            ms = (time.perf_counter() - start) * 1000
            log.info(
                "handler timed",
                extra={
                    "prefix": scope.prefix,
                    "duration_ms": round(ms, 2),
                    "correlation_id": scope.context.correlation_id,
                },
            )

istos.add_middleware(TimingMiddleware())

@istos.handle("echo")
async def echo(message: str):
    return {"echo": message}

if __name__ == "__main__":
    istos.run()
```

See [Middleware](../user-guide/middleware.md).
