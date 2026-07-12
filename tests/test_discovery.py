"""Capability discovery — the .istos/capabilities tool manifest."""

import asyncio

import pytest

from istos import Istos


def test_export_capabilities_describes_all_kinds():
    app = Istos(enable_health=False, enable_metrics=False, service_name="fleet")

    @app.handle("robot/move")
    async def move(distance: int) -> dict:
        """Move the robot."""
        return {"ok": distance}

    @app.stream("llm/gen")
    async def gen(prompt: str):
        """Stream tokens."""
        yield prompt

    @app.publish("drone/status")
    async def status() -> dict:
        return {"up": True}

    @app.subscribe("drone/telemetry")
    async def on_tel(data):
        pass

    manifest = app.export_capabilities()
    assert manifest["service"] == "fleet"
    by_prefix = {c["prefix"]: c for c in manifest["capabilities"]}

    assert by_prefix["robot/move"]["kind"] == "handle"
    assert by_prefix["robot/move"]["description"] == "Move the robot."
    assert by_prefix["robot/move"]["params_schema"]["properties"]["distance"]["type"] == "integer"
    assert by_prefix["llm/gen"]["kind"] == "stream"
    assert by_prefix["drone/status"]["kind"] == "publish"
    assert by_prefix["drone/telemetry"]["kind"] == "subscribe"


def test_capabilities_excludes_builtin_endpoints():
    app = Istos(enable_health=True, enable_metrics=True, enable_discovery=True)
    app._register_builtin_handlers()

    @app.handle("app/thing")
    async def thing():
        return {}

    prefixes = {c["prefix"] for c in app.export_capabilities()["capabilities"]}
    assert "app/thing" in prefixes
    assert not any(p.startswith(".istos/") for p in prefixes)  # plumbing hidden


def test_discovery_can_be_disabled():
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    app._register_builtin_handlers()
    assert not any(h.prefix == ".istos/capabilities" for h in app._handlers)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_capabilities_reachable_over_network():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.handle("robot/move")
    async def move(distance: int) -> dict:
        """Move it."""
        return {"ok": distance}

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.2)
        result = await app.query_once(".istos/capabilities", timeout_s=3.0)
        manifest = result[0] if isinstance(result, list) else result
        prefixes = {c["prefix"] for c in manifest["capabilities"]}
        assert "robot/move" in prefixes
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
