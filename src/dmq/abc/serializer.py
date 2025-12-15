from typing import Any, Protocol


class QSerializerProtocol(Protocol):
    def serialize(self, data: Any) -> bytes: ...

    def deserialize(self, data: bytes, into: type | None = None) -> Any: ...
