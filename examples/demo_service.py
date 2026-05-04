import asyncio
from pydantic import BaseModel
from istos import Istos

# ---------------------------------------------------------
# 1. Define Data Schemas
# ---------------------------------------------------------
class MoveRequest(BaseModel):
    distance: int
    speed: str = "normal"

class Telemetry(BaseModel):
    battery: float
    altitude: int

class SensorRequest(BaseModel):
    sensor_id: str

class SensorResponse(BaseModel):
    temperature: float

# ---------------------------------------------------------
# 2. Init Istos
# ---------------------------------------------------------
istos = Istos()

# ---------------------------------------------------------
# 3. Create Network Nodes (Agents, Pub, Sub)
# ---------------------------------------------------------
@istos.handle("robot/move")
async def move(request: MoveRequest) -> dict:
    """Moves the robot and returns the status."""
    print(f"[Robot] Moving {request.distance}m at {request.speed} speed!")
    return {"status": "success", "distance_moved": request.distance}

@istos.subscribe("drone/telemetry")
async def on_telemetry(data: Telemetry) -> None:
    """Listens for telemetry updates from drones."""
    print(f"[Drone] Received telemetry: {data}")

@istos.publish("drone/status")
async def publish_status() -> dict:
    """Broadcasts generic status information."""
    return {"status": "online", "uptime": 999}

@istos.query("drone/sensor")
async def get_sensor_data(request: SensorRequest) -> SensorResponse:
    """Queries a drone for its specific sensor readings (1-to-1 RPC Client)."""
    pass

# ---------------------------------------------------------
# 4. Expose the built-in Zenoh doc handler & Web Server
# ---------------------------------------------------------
# Passing web_port will natively boot an aiohttp server 
# in the background alongside Istos!
istos.serve_docs(
    prefix=".istos/docs", 
    title="Istos Robot Network", 
    version="1.0.0",
    web_port=8080
)

# ---------------------------------------------------------
# 5. Run it
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(istos.run_async())
    except KeyboardInterrupt:
        print("Shutting down...")
