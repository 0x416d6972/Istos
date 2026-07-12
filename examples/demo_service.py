from pydantic import BaseModel
from istos import Istos, AuthContext, Principal, Public, Depends, current_principal

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
# 2. Init Istos with an authorizer that resolves *identity*
# ---------------------------------------------------------
# The app-wide authorizer is the gate: it decides allow/deny at the network
# boundary. Instead of a bare True/False it returns a `Principal` describing WHO
# the caller is — Istos stashes it on the request so any handler can inject it
# with `Depends(current_principal)`. Unknown/absent token -> None -> denied.
_IDENTITIES = {
    "pilot-key": Principal(id="pilot-1", roles=frozenset({"pilot"})),
    "ops-key": Principal(id="ops-1", roles=frozenset({"pilot", "admin"})),
}

def authenticate(ctx: AuthContext) -> Principal | None:
    return _IDENTITIES.get(ctx.token)

istos = Istos(authorizer=authenticate)

# ---------------------------------------------------------
# 3. Create Network Nodes (Agents, Pub, Sub)
# ---------------------------------------------------------
@istos.handle("robot/move")
async def move(
    request: MoveRequest,
    user: Principal = Depends(current_principal),  # injected by the gate
) -> dict:
    """Moves the robot and returns the status."""
    print(f"[Robot] {user.id} moving {request.distance}m at {request.speed} speed!")
    return {"status": "success", "distance_moved": request.distance, "by": user.id}

@istos.subscribe("drone/telemetry", authorizer=Public)
async def on_telemetry(data: Telemetry) -> None:
    """Listens for telemetry updates from drones.

    Subscribers inherit the app-wide authorizer too, so an untokened telemetry
    sample would otherwise be dropped. Telemetry is broadcast fan-out, so we opt
    this stream out of the gate with `authorizer=Public`.
    """
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
# A caller reaches `robot/move` by presenting a token in the request attachment:
#     await client.query_once("robot/move", token="pilot-key", distance=5)
if __name__ == "__main__":
    try:
        istos.run()
    except KeyboardInterrupt:
        print("Shutting down...")
