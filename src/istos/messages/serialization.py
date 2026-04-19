import json
import msgpack  # type: ignore
import pickle
import base64
import yaml  # type: ignore
from typing import Protocol, Any, Type, TypeVar, Generic
from pydantic import BaseModel # type: ignore

T = TypeVar("T", bound=BaseModel)


class Serialize(Protocol):
    """
    Interface for serializing and deserializing messages.
    """
    def serialize(self, message: Any) -> bytes:
        """
        Serializes a message into bytes.
        """
        ...

    def deserialize(self, data: bytes) -> Any:
        """
        Deserializes bytes into a message.
        """
        ...


class JsonSerializer:
    """
    Serializes and deserializes messages using JSON.
    """
    def serialize(self, message: Any) -> bytes:
        return json.dumps(message).encode('utf-8')

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode('utf-8'))


class MsgPackSerializer:
    """
    Serializes and deserializes messages using MessagePack.
    """
    def serialize(self, message: Any) -> bytes:
        return msgpack.packb(message)

    def deserialize(self, data: bytes) -> Any:
        return msgpack.unpackb(data)


class PickleSerializer:
    """
    Serializes and deserializes messages using Pickle.
    """
    def serialize(self, message: Any) -> bytes:
        return pickle.dumps(message)

    def deserialize(self, data: bytes) -> Any:
        return pickle.loads(data)


class ProtobufSerializer:
    """
    Serializes and deserializes messages using Protocol Buffers.
    """
    def __init__(self, message_type: Any):
        self.message_type = message_type

    def serialize(self, message: Any) -> bytes:
        return message.SerializeToString()

    def deserialize(self, data: bytes) -> Any:
        message = self.message_type()
        message.ParseFromString(data)
        return message


class PydanticSerializer(Generic[T]):
    """
    Serializes and deserializes Pydantic models.
    Provides automatic validation on deserialization.
    """
    def __init__(self, model: Type[T]):
        self.model = model

    def serialize(self, message: T) -> bytes:
        return message.model_dump_json().encode('utf-8')

    def deserialize(self, data: bytes) -> T:
        return self.model.model_validate_json(data.decode('utf-8'))


class YamlSerializer:
    """
    Serializes and deserializes using YAML. 
    Useful for configurations or cross-language readable messages.
    """
    def serialize(self, message: Any) -> bytes:
        return yaml.dump(message).encode('utf-8')

    def deserialize(self, data: bytes) -> Any:
        return yaml.safe_load(data.decode('utf-8'))


class Base64Serializer:
    """
    A wrapper serializer that encodes binary data into Base64 strings.
    Useful for passing binary payloads through JSON-only or HTTP/text transport.
    """
    def __init__(self, inner_serializer: Serialize):
        self.inner = inner_serializer

    def serialize(self, message: Any) -> bytes:
        binary_data = self.inner.serialize(message)
        return base64.b64encode(binary_data)

        binary_data = base64.b64decode(data)
        return self.inner.deserialize(binary_data)
