from typing import Any

from msgspec.inspect import NoneType
from msgspec.json import decode, encode


class JsonSerializer:
    def serialize(self, data: Any) -> bytes:
        return encode(data)

    def deserialize(self, data: bytes, into: type | NoneType = NoneType) -> Any:
        return decode(data)
