# Recipe: RPC with lifespan

Call a `@query` only after the Zenoh session is open.

```python
from contextlib import asynccontextmanager
from istos import Istos

# --- Server (run in one process) ---
server = Istos()

@server.handle("math/add")
async def add(a: int, b: int):
    return {"sum": a + b}

# --- Client (run in another process on the same fabric) ---
client = Istos()

@client.query("math/add")
async def add_remote(result):
    return result

@asynccontextmanager
async def on_start(app):
    reply = await add_remote(a=2, b=3)
    print(reply)  # {"sum": 5}
    yield

client.lifespan = on_start

if __name__ == "__main__":
    # Pick one: server.run() or client.run()
    client.run()
```

Imperative alternative inside lifespan:

```python
@asynccontextmanager
async def on_start(app):
    reply = await app.query_once("math/add", a=2, b=3)
    print(reply)
    yield

client = Istos(lifespan=on_start)
```

See [Handlers & Queries (RPC)](../user-guide/rpc.md).
