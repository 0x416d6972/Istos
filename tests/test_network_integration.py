import pytest
import asyncio
import json
from unittest.mock import patch

from istos import Istos
from istos.communication.sessions import AsyncZenohSession
from istos.communication.config import IstosZenohConfig

@pytest.mark.integration
@pytest.mark.asyncio
async def test_network_integration_endpoints_modes():
    """
    Test connecting endpoints, modes, and configs for several services that find each other.
    We create Service A (peer) and Service B (client) using IstosZenohConfig.
    Service B will query Service A.
    """
    port = 17449
    
    # Config for Service A (Peer)
    config_a = IstosZenohConfig(
        mode="peer",
        listen_endpoints=[f"tcp/127.0.0.1:{port}"]
    )
    session_manager_a = AsyncZenohSession(config=config_a.build())
    istos_a = Istos(session_manager=session_manager_a)

    @istos_a.handle(prefix="integration/service_a/greet")
    async def greet():
        return "Greetings, Istos!"

    # Config for Service B (Client)
    config_b = IstosZenohConfig(
        mode="client",
        connect_endpoints=[f"tcp/127.0.0.1:{port}"]
    )
    session_manager_b = AsyncZenohSession(config=config_b.build())
    istos_b = Istos(session_manager=session_manager_b)

    task_a = asyncio.create_task(istos_a.run_async())
    
    # Wait for Service A to open its port
    await asyncio.sleep(1.0)

    task_b = asyncio.create_task(istos_b.run_async())

    # Wait for the network to establish
    await asyncio.sleep(2.0)

    try:
        # B queries A without kwargs
        results = await istos_b.query_once("integration/service_a/greet")
        
        assert "Greetings, Istos!" in str(results), f"Expected greeting not found in results: {results}"
        
    finally:
        task_a.cancel()
        task_b.cancel()
        import contextlib
        with contextlib.suppress(asyncio.CancelledError):
            await task_a
        with contextlib.suppress(asyncio.CancelledError):
            await task_b

def test_config_builder_auth():
    """
    Test that IstosZenohConfig correctly configures auth when provided.
    """
    with patch("zenoh.Config.from_json5") as mock_from_json5:
        config = IstosZenohConfig(
            mode="client",
            username="testuser",
            password="testpassword"
        )
        config.build()
        
        mock_from_json5.assert_called_once()
        json_str = mock_from_json5.call_args[0][0]
        conf_dict = json.loads(json_str)
        
        assert "transport" in conf_dict
        assert "auth" in conf_dict["transport"]
        assert "usrpwd" in conf_dict["transport"]["auth"]
        assert conf_dict["transport"]["auth"]["usrpwd"]["user"] == "testuser"
        assert conf_dict["transport"]["auth"]["usrpwd"]["password"] == "testpassword"
