import pytest
from istos import Istos, IstosRouter

def test_router_prefixes():
    router = IstosRouter(prefix="users")
    
    @router.handle("create")
    def create_user():
        pass
        
    assert len(router._actions) == 1

@pytest.mark.asyncio
async def test_include_router():
    istos = Istos()
    router = IstosRouter(prefix="api/v1")
    
    @router.handle("status")
    def status():
        return "ok"
        
    @router.publish("alerts")
    def alerts():
        return "alert"
        
    istos.include_router(router)
    
    # Verify the actions were applied to the main app
    assert len(istos._handlers) == 1
    assert istos._handlers[0].prefix == "api/v1/status"
    
    assert len(istos._publishers) == 1
    assert istos._publishers[0].prefix == "api/v1/alerts"

@pytest.mark.asyncio
async def test_router_lazy_proxy():
    istos = Istos()
    router = IstosRouter(prefix="sensor")
    
    @router.publish("temperature")
    def get_temperature():
        return {"temp": 25}
        
    # Before inclusion, calling the proxy raises an error
    with pytest.raises(RuntimeError, match="Router has not been included"):
        get_temperature()
        
    # Include the router
    istos.include_router(router)
    
    # Now the proxy should delegate to the real publish_wrapper
    # Note: we are not mocking zenoh here, so calling it will raise a Zenoh session error
    # but it shouldn't raise the RouterProxy RuntimeError.
    with pytest.raises(RuntimeError, match="No active Zenoh session"):
        await get_temperature()
