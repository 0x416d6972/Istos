# Recipe: Durable orders

Late subscribers replay recent events from the producer's cache — no Kafka/NATS broker.

```python
from istos import Istos

app = Istos()

@app.publish("orders/created", durable=True, cache=1000, heartbeat=1.0)
async def created(order: dict):
    return order

@app.subscribe("orders/created", durable=True, replay=1000, recover=True)
async def on_created(event: dict):
    print(f"processing order {event['id']}")

# Producer side — after the session is open (e.g. lifespan):
# await created({"id": "ord_1", "total": 42.0})
```

Pair with handler durability for effectively-once processing:

```python
@app.handle("orders/charge", durability="exactly_once")
async def charge(order_id: str, amount: float):
    return {"charged": amount, "order_id": order_id}
```

See [Brokerless Durable Messaging](../user-guide/durable-messaging.md) and [Storage](../user-guide/storage.md).
