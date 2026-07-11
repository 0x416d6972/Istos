# Recipe: Redis storage

Use Redis as a shared durability / idempotency ledger across processes.

```bash
pip install 'istos[redis]'
docker compose up -d redis   # from the Istos repo, or any Redis
```

```python
from istos import Istos
from istos.consistency import RedisStoragePlugin

istos = Istos(
    storage=RedisStoragePlugin(
        url="redis://127.0.0.1:6379/0",
        prefix="istos:",
    )
)

@istos.handle("payments/charge", durability="exactly_once")
async def charge(payment_id: str, amount: float):
    # Retries with the same idempotency context return the cached result
    return {"payment_id": payment_id, "charged": amount}

if __name__ == "__main__":
    istos.run()
```

See [Storage](../user-guide/storage.md).
