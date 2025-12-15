from typing import Protocol

from msgspec.inspect import NoneType


class QSerializerProtocol(Protocol):
    def serialize[T](self, data: T) -> bytes: ...

    def deserialize[T](self, data: bytes, into: type | NoneType = NoneType) -> T: ...
