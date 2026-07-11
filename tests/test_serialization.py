"""Serializer tests: raw passthrough, JSON hardening, msgpack round-trip."""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from istos.messages import (
    JsonSerializer,
    RawSerializer,
    MsgPackSerializer,
    Base64Serializer,
)


# ---------------------------------------------------------------------------
# RawSerializer
# ---------------------------------------------------------------------------

def test_raw_serializer_bytes_roundtrip():
    ser = RawSerializer()
    payload = b"\x00\x01\x02\xff binary frame"
    assert ser.serialize(payload) == payload
    assert ser.deserialize(ser.serialize(payload)) == payload


def test_raw_serializer_str_encodes_utf8():
    ser = RawSerializer()
    assert ser.serialize("héllo") == "héllo".encode("utf-8")
    # deserialize always yields bytes
    assert ser.deserialize(ser.serialize("héllo")) == "héllo".encode("utf-8")


def test_raw_serializer_bytearray_and_memoryview():
    ser = RawSerializer()
    assert ser.serialize(bytearray(b"abc")) == b"abc"
    assert ser.serialize(memoryview(b"abc")) == b"abc"


def test_raw_serializer_rejects_non_bytes():
    ser = RawSerializer()
    with pytest.raises(TypeError):
        ser.serialize({"not": "bytes"})


# ---------------------------------------------------------------------------
# JsonSerializer hardening (default=str)
# ---------------------------------------------------------------------------

def test_json_serializer_handles_datetime():
    ser = JsonSerializer()
    dt = datetime(2026, 7, 4, 12, 0, tzinfo=timezone.utc)
    out = ser.deserialize(ser.serialize({"ts": dt}))
    assert out["ts"] == str(dt)


def test_json_serializer_handles_decimal_uuid():
    ser = JsonSerializer()
    uid = uuid.uuid4()
    out = ser.deserialize(ser.serialize({"amount": Decimal("1.50"), "id": uid}))
    assert out["amount"] == "1.50"
    assert out["id"] == str(uid)


def test_json_serializer_basic_roundtrip():
    ser = JsonSerializer()
    data = {"a": 1, "b": [1, 2, 3], "c": "text"}
    assert ser.deserialize(ser.serialize(data)) == data


# ---------------------------------------------------------------------------
# MsgPackSerializer round-trip with pinned raw=False
# ---------------------------------------------------------------------------

def test_msgpack_str_stays_str():
    ser = MsgPackSerializer()
    out = ser.deserialize(ser.serialize({"name": "robot"}))
    assert out == {"name": "robot"}
    assert isinstance(out["name"], str)


def test_msgpack_bytes_stay_bytes():
    ser = MsgPackSerializer()
    out = ser.deserialize(ser.serialize({"blob": b"\x00\xff"}))
    assert out["blob"] == b"\x00\xff"
    assert isinstance(out["blob"], bytes)


# ---------------------------------------------------------------------------
# Base64 wrapper still composes with RawSerializer
# ---------------------------------------------------------------------------

def test_base64_wraps_raw():
    ser = Base64Serializer(RawSerializer())
    payload = b"\x00\x01\x02"
    encoded = ser.serialize(payload)
    assert b"\x00" not in encoded  # base64 is text-safe
    assert ser.deserialize(encoded) == payload
