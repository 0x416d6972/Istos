"""DI integration tests: Depends resolved through real @istos.handle handlers."""

import threading
from contextlib import AsyncExitStack
from typing import Annotated

import pytest

from istos import Istos, Depends, DependencyCycleError
from istos.di.depends import resolve_dependencies
from istos.testing import IstosTestClient


def _app():
    return Istos(enable_health=False, enable_metrics=False)


@pytest.mark.asyncio
async def test_handler_resolves_depends_and_subdeps():
    app = _app()

    def config():
        return {"scale": 10}

    def scale(cfg: dict = Depends(config)):
        return cfg["scale"]

    @app.handle("svc/compute")
    async def compute(x: int, factor: int = Depends(scale)):
        return {"result": x * factor}

    client = IstosTestClient(app)
    assert await client.query("svc/compute", x=5) == {"result": 50}


@pytest.mark.asyncio
async def test_handler_annotated_depends():
    app = _app()

    def get_service():
        return "service-obj"

    @app.handle("svc/annotated")
    async def h(svc: Annotated[str, Depends(get_service)]):
        return svc

    client = IstosTestClient(app)
    assert await client.query("svc/annotated") == "service-obj"


@pytest.mark.asyncio
async def test_yield_dependency_teardown_runs():
    app = _app()
    events = []

    def get_db():
        events.append("open")
        yield "DB"
        events.append("close")

    @app.handle("svc/db")
    async def h(db: str = Depends(get_db)):
        events.append(f"use:{db}")
        return db

    client = IstosTestClient(app)
    assert await client.query("svc/db") == "DB"
    assert events == ["open", "use:DB", "close"]


@pytest.mark.asyncio
async def test_dependency_cached_per_request():
    app = _app()
    calls = {"n": 0}

    def expensive():
        calls["n"] += 1
        return calls["n"]

    @app.handle("svc/cache")
    async def h(a: int = Depends(expensive), b: int = Depends(expensive)):
        return {"a": a, "b": b}

    client = IstosTestClient(app)
    result = await client.query("svc/cache")
    assert result == {"a": 1, "b": 1}  # resolved once, shared
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_dependency_override_via_app():
    app = _app()

    def real_db():
        return "REAL"

    @app.handle("svc/ov")
    async def h(db: str = Depends(real_db)):
        return db

    client = IstosTestClient(app)
    assert await client.query("svc/ov") == "REAL"

    app.dependency_overrides[real_db] = lambda: "MOCK"
    assert await client.query("svc/ov") == "MOCK"

    app.dependency_overrides.clear()
    assert await client.query("svc/ov") == "REAL"


@pytest.mark.asyncio
async def test_sync_dependency_offloaded_to_thread():
    app = _app()
    main_thread = threading.get_ident()
    seen = {}

    def blocking():
        seen["thread"] = threading.get_ident()
        return "ok"

    @app.handle("svc/sync")
    async def h(v: str = Depends(blocking)):
        return v

    client = IstosTestClient(app)
    assert await client.query("svc/sync") == "ok"
    assert seen["thread"] != main_thread  # ran off the event loop


@pytest.mark.asyncio
async def test_depends_excluded_from_network_validation():
    app = _app()

    def dep():
        return 99

    # x is a validated/coerced network param; injected is a dependency.
    @app.handle("svc/mix")
    async def h(x: int, injected: int = Depends(dep)):
        return x + injected

    client = IstosTestClient(app)
    # x arrives as a string over the wire and must still coerce to int
    assert await client.query("svc/mix", x="1") == 100


@pytest.mark.asyncio
async def test_subscribe_resolves_dependencies_per_message():
    app = _app()
    events = []

    def sink():
        events.append("open")
        yield "SINK"
        events.append("close")

    @app.subscribe("telemetry")
    async def on_telemetry(data, sink: str = Depends(sink)):
        events.append(f"{data}->{sink}")

    client = IstosTestClient(app)
    await client.publish("telemetry", {"t": 1})
    await client.publish("telemetry", {"t": 2})

    # dependency + yield teardown run for each message independently
    assert events == [
        "open", "{'t': 1}->SINK", "close",
        "open", "{'t': 2}->SINK", "close",
    ]


@pytest.mark.asyncio
async def test_subscribe_dependency_override():
    app = _app()

    def source():
        return "REAL"

    seen = []

    @app.subscribe("evt")
    async def on_evt(data, s: str = Depends(source)):
        seen.append(s)

    app.dependency_overrides[source] = lambda: "MOCK"
    client = IstosTestClient(app)
    await client.publish("evt", {})
    assert seen == ["MOCK"]


@pytest.mark.asyncio
async def test_publish_resolves_dependencies():
    from unittest.mock import MagicMock
    app = _app()

    def sensor():
        return 42

    @app.publish("reading")
    async def emit(src: int = Depends(sensor)):
        return {"value": src}

    # publishing needs an active session; inject a fake one
    app._session_manager._internal_session = MagicMock()
    assert await emit() == {"value": 42}


@pytest.mark.asyncio
async def test_cycle_detection():
    # Build a genuine self-referential cycle via a late-bound default.
    def d(x=None):
        return x
    d.__defaults__ = (Depends(d),)

    async def target(v=Depends(d)):
        return v

    async with AsyncExitStack() as stack:
        with pytest.raises(DependencyCycleError, match="Circular dependency"):
            await resolve_dependencies(target, {}, stack)
