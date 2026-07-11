import json
import msgpack  # type: ignore
import base64
import yaml  # type: ignore
from typing import Protocol, Any, Type, TypeVar, Generic
from pydantic import BaseModel  # type: ignore

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

    ``default=str`` lets common non-JSON-native types (datetime, Decimal, UUID,
    Path, etc.) serialize to their string form instead of raising TypeError.
    """

    def serialize(self, message: Any) -> bytes:
        return json.dumps(message, default=str).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class RawSerializer:
    """
    Passthrough serializer for already-encoded payloads.

    Zenoh payloads are raw bytes; this serializer moves ``bytes`` (or ``str``,
    encoded as UTF-8) across the network without wrapping them in JSON/msgpack.
    Use it for opaque or pre-encoded data: binary sensor frames, images, tokens,
    or the output of another serializer. Deserialization always yields ``bytes``.
    """

    def serialize(self, message: Any) -> bytes:
        if isinstance(message, bytes):
            return message
        if isinstance(message, (bytearray, memoryview)):
            return bytes(message)
        if isinstance(message, str):
            return message.encode("utf-8")
        raise TypeError(
            f"RawSerializer expects bytes or str, got {type(message).__name__}"
        )

    def deserialize(self, data: bytes) -> bytes:
        return bytes(data)


class MsgPackSerializer:
    """
    Serializes and deserializes messages using MessagePack.

    ``raw=False`` pins string decoding so peers on different msgpack versions
    agree (str stays str, binary stays bytes) rather than depending on the
    library default.
    """

    def serialize(self, message: Any) -> bytes:
        return msgpack.packb(message, use_bin_type=True)

    def deserialize(self, data: bytes) -> Any:
        return msgpack.unpackb(data, raw=False)


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
        return message.model_dump_json().encode("utf-8")

    def deserialize(self, data: bytes) -> T:
        return self.model.model_validate_json(data.decode("utf-8"))


class YamlSerializer:
    """
    Serializes and deserializes using YAML.
    Useful for configurations or cross-language readable messages.
    """

    def serialize(self, message: Any) -> bytes:
        return yaml.dump(message).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return yaml.safe_load(data.decode("utf-8"))


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

    def deserialize(self, data: bytes) -> Any:
        binary_data = base64.b64decode(data)
        return self.inner.deserialize(binary_data)
