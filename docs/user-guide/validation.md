# Schema Validation

Istos validates and coerces parameters at the **network boundary** — before your handler or subscriber runs. Bad data is rejected with a structured error reply.

## Three modes

=== "Type hints"

    ```python
    @istos.handle("robot/move")
    async def move(distance: int, speed: str = "normal"):
        # Zenoh may send distance="15" (string) → coerced to int(15)
        # distance="hello" → SchemaValidationError reply
        return {"moved": distance, "speed": speed}
    ```

=== "Pydantic models"

    ```python
    from pydantic import BaseModel

    class MoveRequest(BaseModel):
        distance: int
        speed: str = "normal"

    @istos.handle("robot/move")
    async def move(request: MoveRequest):
        return {"moved": request.distance}
    ```

=== "Passthrough"

    ```python
    @istos.handle("robot/echo")
    async def echo(message):
        # No annotation → no coercion / schema check
        return {"echo": message}
    ```

## Where it applies

| Decorator | Validated input |
|-----------|-----------------|
| `@handle` | Query parameters / payload mapped to arguments |
| `@stream` | Query parameters / payload mapped to arguments (same as `@handle`) |
| `@subscribe` | Published payload (when typed) |
| `@query` / `@publish` | Return processing as configured; outbound kwargs build selectors |

Return-type annotations can also drive response validation when configured via the handler wrapper (see [Validation API](../api/validation.md)).

## Errors on the wire

Validation failures become a standardized error response (`code`, `message`, `correlation_id`, …) instead of crashing the process. Customize handling with [`@istos.exception_handler`](middleware.md).

## AsyncAPI schemas

Type hints and Pydantic models feed the [AsyncAPI](../api/discovery/asyncapi.md) generator used by `export_asyncapi()` / `serve_docs()`.

## Next Steps

- [Handlers & Queries (RPC)](rpc.md)
- [Dependency Injection](dependency-injection.md)
- [API: Validation](../api/validation.md)
