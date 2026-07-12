import pytest
from pydantic import BaseModel
from istos import Istos
from istos.core.asyncapi import get_function_schemas
import yaml

class DemoModel(BaseModel):
    id: int
    name: str

def dummy_handler(req: DemoModel) -> dict:
    pass

def dummy_subscribe(event_data: DemoModel) -> None:
    pass

@pytest.mark.asyncio
async def test_get_function_schemas():
    schemas = get_function_schemas(dummy_handler)
    assert schemas["payload_schema"] is not None
    assert "properties" in schemas["payload_schema"]
    assert "id" in schemas["payload_schema"]["properties"]
    assert "name" in schemas["payload_schema"]["properties"]
    
    assert schemas["return_schema"] is not None

def test_asyncapi_generator():
    istos = Istos()
    
    @istos.handle("robot/move")
    def move(distance: int, speed: str = "normal") -> dict:
        return {"status": "ok"}
        
    @istos.subscribe("drone/telemetry")
    def on_telemetry(data: DemoModel):
        pass
        
    @istos.publish("drone/status")
    def publish_status() -> DemoModel:
        return DemoModel(id=1, name="status")
        
    istos.serve_docs()
    
    yaml_doc = istos.export_asyncapi()
    doc = yaml.safe_load(yaml_doc)
    
    assert doc["asyncapi"] == "3.0.0"
    assert "robot_move" in doc["channels"]
    assert "drone_telemetry" in doc["channels"]
    assert "drone_status" in doc["channels"]
    assert ".istos_docs" in doc["channels"]
    
    # Check operations
    ops = doc["operations"]
    
    # Handler (Receive + Reply)
    handle_op = next(op for name, op in ops.items() if "handle_move" in name)
    assert handle_op["action"] == "receive"
    assert "reply" in handle_op
    
    # Subscriber (Receive)
    sub_op = next(op for name, op in ops.items() if "subscribe_on_telemetry" in name)
    assert sub_op["action"] == "receive"
    assert "reply" not in sub_op
    
    # Publisher (Send)
    pub_op = next(op for name, op in ops.items() if "publish_publish_status" in name)
    assert pub_op["action"] == "send"
    assert "reply" not in pub_op


def test_asyncapi_includes_streams_and_channels():
    from istos import ChannelSession

    istos = Istos()

    @istos.stream("llm/generate")
    async def generate(prompt: str):
        yield prompt

    @istos.channel("agent/chat")
    async def chat(session: ChannelSession):
        async for msg in session:
            await session.send(msg)

    doc = yaml.safe_load(istos.export_asyncapi())

    assert "llm_generate" in doc["channels"]
    assert "agent_chat" in doc["channels"]

    ops = doc["operations"]
    stream_op = next(op for name, op in ops.items() if "stream_generate" in name)
    assert {"name": "@stream"} in stream_op["tags"]

    # A ChannelSession parameter has no JSON Schema; the channel must still appear
    # rather than sinking the whole document.
    channel_op = next(op for name, op in ops.items() if "channel_chat" in name)
    assert {"name": "@channel"} in channel_op["tags"]
