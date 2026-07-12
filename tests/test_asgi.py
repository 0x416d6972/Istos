"""serving() context manager + the ASGI lifespan for FastAPI/Starlette co-host."""

import pytest

from istos import Istos
from istos.asgi import lifespan


def _mesh() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


def test_lifespan_returns_asgi_context_manager():
    cm = lifespan(_mesh())
    assert callable(cm)
    # FastAPI calls it with the app; result is an async context manager.
    ctx = cm(object())
    assert hasattr(ctx, "__aenter__") and hasattr(ctx, "__aexit__")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_serving_runs_mesh_in_process():
    app = _mesh()

    @app.handle("math/inc")
    async def inc(x: int):
        return {"y": x + 1}

    async with app.serving():
        assert app._health.ready is True
        reply = await app.query_once("math/inc", x=1, timeout_s=3.0)
        value = reply[0] if isinstance(reply, list) else reply
        assert value == {"y": 2}

    assert app._health.ready is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lifespan_starts_and_stops_mesh():
    app = _mesh()

    @app.handle("ping")
    async def ping():
        return {"pong": True}

    async with lifespan(app)(object()):
        reply = await app.query_once("ping", timeout_s=3.0)
        value = reply[0] if isinstance(reply, list) else reply
        assert value == {"pong": True}

    assert app._health.ready is False
