from typing import Any, Protocol


class QSerializerProtocol(Protocol):
    @staticmethod
    def serialize(data: Any) -> bytes: ...

    @staticmethod
    def deserialize(data: bytes) -> Any: ...
