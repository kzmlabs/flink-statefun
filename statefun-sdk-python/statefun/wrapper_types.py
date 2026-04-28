# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation


from statefun.core import Type, TypeSerializer
from statefun.types_pb2 import *


class ProtobufWrappingTypeSerializer(TypeSerializer):
    __slots__ = ("wrapper",)

    def __init__(self, wrapper):
        self.wrapper = wrapper

    def serialize(self, value):
        instance = self.wrapper()
        instance.value = value
        return instance.SerializeToString()

    def deserialize(self, string):
        instance = self.wrapper()
        instance.ParseFromString(string)
        return instance.value


class ProtobufWrappingType(Type):
    __slots__ = ("wrapper",)

    def __init__(self, typename, wrapper_message_type):
        super().__init__(typename)
        self.wrapper = wrapper_message_type

    def serializer(self) -> TypeSerializer:
        return ProtobufWrappingTypeSerializer(self.wrapper)


BoolType = ProtobufWrappingType("io.statefun.types/bool", BooleanWrapper)
IntType = ProtobufWrappingType("io.statefun.types/int", IntWrapper)
FloatType = ProtobufWrappingType("io.statefun.types/float", FloatWrapper)
LongType = ProtobufWrappingType("io.statefun.types/long", LongWrapper)
DoubleType = ProtobufWrappingType("io.statefun.types/double", DoubleWrapper)
StringType = ProtobufWrappingType("io.statefun.types/string", StringWrapper)

PY_TYPE_TO_WRAPPER_TYPE = {
    int: IntType,
    bool: BoolType,
    float: FloatType,
    str: StringType
}

WRAPPER_TYPE_TO_PY_TYPE = {
    IntType: int,
    BoolType: bool,
    FloatType: float,
    StringType: str
}
