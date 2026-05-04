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
