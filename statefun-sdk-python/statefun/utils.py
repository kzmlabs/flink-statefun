# SPDX-License-Identifier: Apache-2.0

import typing
import json

from statefun.request_reply_pb2 import TypedValue
from statefun.core import Type, simple_type


def to_typed_value_deduce(value: typing.Union[str, bytes, bytearray],
                          value_type: typing.Union[None, Type] = None) -> bytes:
    if value_type:
        ser = value_type.serializer()
        return ser.serialize(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    try:
        return value.encode('utf-8')
    except AttributeError:
        pass
    raise TypeError(
        "Unable to convert value to bytes. strings, bytes, and anytype that has __bytes__ attribute is supported.")


def from_typed_value(tpe: Type, typed_value: typing.Optional[TypedValue]):
    if not typed_value or not typed_value.has_value:
        return None
    if tpe.typename != typed_value.typename:
        raise TypeError(
            f"Type mismatch: "
            f"type {typed_value.typename} (remote), yet it is specified locally "
            f"as {tpe.typename} ")
    ser = tpe.serializer()
    return ser.deserialize(typed_value.value)


def to_typed_value(type, value):
    typed_value = TypedValue()
    typed_value.typename = type.typename
    if value is None:
        typed_value.has_value = False
        return typed_value
    typed_value.has_value = True
    ser = type.serializer()
    typed_value.value = ser.serialize(value)
    return typed_value


def make_protobuf_type(cls, namespace: str = None) -> Type:
    """
    Create a StateFun type that is backed by Protobuf.
    """

    def deserialize_fn(b):
        v = cls()
        v.ParseFromString(b)
        return v

    def serialize_fn(v):
        return v.SerializeToString()

    if not namespace:
        namespace = "type.googleapis.com"
    if not cls:
        raise ValueError("The Protobuf generated class is missing.")
    name = cls().DESCRIPTOR.full_name
    if not name:
        raise TypeError("Unable to deduce the Protobuf Message full name.")
    return simple_type(typename=f"{namespace}/{name}", serialize_fn=serialize_fn, deserialize_fn=deserialize_fn)


def _serialize_json_utf8(obj) -> bytes:
    """
    serialize the given object as a JSON utf-8 bytes.
    """
    str = json.dumps(obj, ensure_ascii=False)
    return str.encode('utf-8')


def make_json_type(typename: str) -> Type:
    """
    Create a StateFun type named @typename, that is backed by Python's json module.
    """
    return simple_type(typename=typename,
                       serialize_fn=_serialize_json_utf8,
                       deserialize_fn=json.loads)
