from types import NoneType
from typing import Self

import msgspec


class MsgpackSerializer:
    _primary_type: type | NoneType = NoneType

    @classmethod
    def with_type(cls, _type: type) -> Self:
        inst = cls()
        inst._primary_type = _type

        return inst

    def serialize[T](self, data: T) -> bytes:
        return msgspec.msgpack.Encoder().encode(data)

    def deserialize[T](self, data: bytes, into: type | NoneType = NoneType) -> T:
        if self._primary_type is NoneType and into is NoneType:
            raise ValueError("unknown type to decode")

        return msgspec.msgpack.Decoder(into or self._primary_type).decode(data)


class MsgpackJsonSerializer:
    def serialize[T](self, data: T) -> bytes:
        return msgspec.msgpack.Encoder().encode(data)

    def deserialize[T](self, data: bytes, into: type | NoneType = NoneType) -> T:
        return msgspec.json.decode(data)
