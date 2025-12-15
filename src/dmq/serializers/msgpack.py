from typing import Any, Self

import msgspec


class MsgpackSerializer:
    _primary_type: type | None = None

    @classmethod
    def with_type(cls, _type: type) -> Self:
        inst = cls()
        inst._primary_type = _type
        return inst

    def serialize(self, data: Any) -> bytes:
        return msgspec.msgpack.encode(data)

    def deserialize(self, data: bytes, into: type | None = None) -> Any:
        target_type = into or self._primary_type
        if target_type is not None:
            return msgspec.msgpack.decode(data, type=target_type)
        return msgspec.msgpack.decode(data)
