# Recipe: Scaffold a service

Use the CLI to create a starter project and test it in-process.

```bash
istos new inventory
cd inventory
uv pip install istos pytest pytest-asyncio
pytest test_main.py
python main.py
```

Generated `main.py`:

```python
"""Istos service: inventory"""

from istos import Istos

istos = Istos()


@istos.handle("service/status")
async def status() -> dict:
    return {"service": "inventory", "status": "ok"}


if __name__ == "__main__":
    istos.run()
```

See [CLI](../user-guide/cli.md) and [Testing](../user-guide/testing.md).
