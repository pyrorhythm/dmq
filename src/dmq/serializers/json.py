from typing import Any

from msgspec.json import decode, encode


class JsonSerializer:
    def serialize(self, data: Any) -> bytes:
        return encode(data)

    def deserialize(self, data: bytes, into: type | None = None) -> Any:
        if into is not None:
            return decode(data, type=into)
        return decode(data)
