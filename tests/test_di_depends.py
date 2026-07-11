import pytest
from typing import AsyncGenerator, Generator
from istos.di.depends import Depends, inject_and_run

pytestmark = pytest.mark.asyncio

async def test_simple_dependency():
    """Test basic dependency resolution."""
    def get_number() -> int:
        return 42

    async def my_handler(num: int = Depends(get_number)):
        return num

    result = await inject_and_run(my_handler)
    assert result == 42


async def test_context_injection():
    """Test that existing context variables are injected seamlessly to nested dependencies."""
    def sub_dep(header: str) -> str:
        return f"Bearer {header}"

    async def my_handler(auth: str = Depends(sub_dep), payload: dict = {}):
        return auth, payload

    context = {"header": "token123", "payload": {"foo": "bar"}}
    result = await inject_and_run(my_handler, context=context)
    assert result == ("Bearer token123", {"foo": "bar"})


async def test_caching_behavior():
    """Test that a dependency executed multiple times per-request is only evaluated once."""
    count = 0
    def expensive_calc():
        nonlocal count
        count += 1
        return count

    # `intermediate` requires expensive_calc
    def intermediate(val: int = Depends(expensive_calc)):
        return val * 10

    # Handler requires both `expensive_calc` and `intermediate`
    async def my_handler(
        val1: int = Depends(expensive_calc), 
        val2: int = Depends(intermediate)
    ):
        return val1, val2

    result = await inject_and_run(my_handler)
    assert result == (1, 10)
    assert count == 1  # Should only be executed ONCE


async def test_no_caching():
    """Test that use_cache=False works exactly as expected."""
    count = 0
    def expensive_calc():
        nonlocal count
        count += 1
        return count

    def intermediate(val: int = Depends(expensive_calc, use_cache=False)):
        return val * 10

    async def my_handler(
        val1: int = Depends(expensive_calc, use_cache=False), 
        val2: int = Depends(intermediate)
    ):
        return val1, val2

    result = await inject_and_run(my_handler)
    # val1 calculates first (count=1), intermediate calculates it second (count=2)
    assert result[0] in (1, 2)
    assert result[1] in (10, 20)
    assert count == 2


async def test_generators_and_teardown():
    """Test that async and sync generators properly yield their values AND run their cleanup logic."""
    state = []
    
    async def get_db() -> AsyncGenerator[str, None]:
        state.append("connected")
        yield "db_connection"
        state.append("disconnected")

    def sync_gen() -> Generator[str, None, None]:
        state.append("sync_open")
        yield "file_handle"
        state.append("sync_close")

    async def my_handler(
        db: str = Depends(get_db),
        file: str = Depends(sync_gen)
    ):
        state.append("running")
        return f"{db} + {file}"

    result = await inject_and_run(my_handler)
    assert result == "db_connection + file_handle"
    
    # Assert Order: Setup -> Running -> Teardown
    assert "connected" in state[:2]
    assert "sync_open" in state[:2]
    assert state[2] == "running"
    
    # Teardown should run in reverse order (LIFO) of setup via AsyncExitStack
    assert "disconnected" in state[3:]
    assert "sync_close" in state[3:]


async def test_overrides():
    """Test that dependency overrides function correctly for mocking services."""
    def production_db():
        return "postgres"

    def mock_db():
        return "sqlite"

    async def my_handler(db: str = Depends(production_db)):
        return db

    # Normal run
    result = await inject_and_run(my_handler)
    assert result == "postgres"

    # Override run
    result = await inject_and_run(my_handler, overrides={production_db: mock_db})
    assert result == "sqlite"


async def test_sync_handler_fallback():
    """Test that inject_and_run safely works with standard synchronous handlers."""
    def get_data():
        return "pure_sync"
        
    def sync_handler(data: str = Depends(get_data)):
        return data.upper()
        
    result = await inject_and_run(sync_handler)
    assert result == "PURE_SYNC"
