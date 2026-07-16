"""Capability discovery — the .istos/capabilities tool manifest."""

import asyncio

import pytest

from istos import Istos
from istos.communication.config import IstosZenohConfig
from istos.discovery.capabilities import capabilities_key


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

    from istos import ChannelSession

    @app.channel("agent/chat", ws="/chat")
    async def chat(s: ChannelSession):
        """Multi-turn chat."""
        ...

    manifest = app.export_capabilities()
    assert manifest["service"] == "fleet"
    by_prefix = {c["prefix"]: c for c in manifest["capabilities"]}

    assert by_prefix["robot/move"]["kind"] == "handle"
    assert by_prefix["robot/move"]["description"] == "Move the robot."
    assert by_prefix["robot/move"]["params_schema"]["properties"]["distance"]["type"] == "integer"
    assert by_prefix["llm/gen"]["kind"] == "stream"
    assert by_prefix["drone/status"]["kind"] == "publish"
    assert by_prefix["drone/telemetry"]["kind"] == "subscribe"
    # Channels are discoverable, with their WebSocket path, and the injected
    # ChannelSession param doesn't leak into (or break) the schema.
    assert by_prefix["agent/chat"]["kind"] == "channel"
    assert by_prefix["agent/chat"]["description"] == "Multi-turn chat."
    assert by_prefix["agent/chat"]["websocket"] == "/chat"
    assert "params_schema" not in by_prefix["agent/chat"]


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
    """Isolated from the LAN on purpose.

    Every node answers `.istos/capabilities` on the same key and `@handle`
    declares its queryable `complete=True`, so Zenoh asks exactly one responder.
    With multicast scouting on, any other node on the machine or LAN can be the
    one asked and this node's manifest never comes back. A longer wait does not
    help; the query is answered, just by someone else.
    """
    app = Istos(
        enable_health=False,
        enable_metrics=False,
        config=IstosZenohConfig(multicast_scouting=False),
    )

    @app.handle("robot/move")
    async def move(distance: int) -> dict:
        """Move it."""
        return {"ok": distance}

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.2)
        result = await app.query_once(".istos/capabilities", timeout_s=3.0)
        manifests = result if isinstance(result, list) else [result]
        prefixes = {c["prefix"] for m in manifests for c in m["capabilities"]}
        assert "robot/move" in prefixes
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_the_capabilities_key_is_per_service():
    assert capabilities_key("clients") == ".istos/capabilities/clients"


def test_a_service_name_is_made_safe_for_a_key():
    """Service names are free text; key chunks are not."""
    assert capabilities_key("my service/v2") == ".istos/capabilities/my-service-v2"
    assert capabilities_key("a*b?c#d$e") == ".istos/capabilities/a-b-c-d-e"
    assert capabilities_key("") == ".istos/capabilities/istos"
    assert capabilities_key("///") == ".istos/capabilities/istos"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_discover_capabilities_reaches_every_service():
    """Two nodes on a private loopback mesh, so the LAN cannot answer for them.

    The bare `.istos/capabilities` is one key on both nodes and answers for
    whichever Zenoh picked. The per-service keys are distinct, so the wildcard
    reaches both.
    """
    ep = f"tcp/127.0.0.1:{_free_port()}"
    a = Istos(
        service_name="clients", enable_health=False, enable_metrics=False,
        config=IstosZenohConfig(multicast_scouting=False, listen_endpoints=[ep]),
    )
    b = Istos(
        service_name="cdc", enable_health=False, enable_metrics=False,
        config=IstosZenohConfig(multicast_scouting=False, connect_endpoints=[ep]),
    )

    @a.handle("clients/list")
    async def clients_list() -> dict:
        """List clients."""
        return {}

    @b.handle("cdc/status")
    async def cdc_status() -> dict:
        """CDC health."""
        return {}

    ta = asyncio.create_task(a.run_async())
    await asyncio.sleep(1.5)
    tb = asyncio.create_task(b.run_async())
    await asyncio.sleep(2.5)
    try:
        fleet = await a.discover_capabilities()
        assert sorted(fleet) == ["cdc", "clients"]
        assert [c["prefix"] for c in fleet["cdc"]["capabilities"]] == ["cdc/status"]
        assert [c["prefix"] for c in fleet["clients"]["capabilities"]] == ["clients/list"]

        # The old key still answers, for one node.
        bare = await a.query_once(".istos/capabilities", timeout_s=3)
        assert not isinstance(bare, list)
        assert bare["service"] in ("clients", "cdc")
    finally:
        for t in (ta, tb):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
