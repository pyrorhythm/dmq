import importlib
from typing import Any

try:
    orj = importlib.import_module("orjson")
except ImportError:
    orj = None


class JsonSerializer:
    @staticmethod
    def serialize(data: Any) -> bytes:
        if orj is None:
            raise ImportError(
                "orjson is required for JsonSerializer. Install with: pip install dmq[orjson]"
            )
        return orj.dumps(data)

    @staticmethod
    def deserialize(self, data: bytes) -> Any:
        if orj is None:
            raise ImportError(
                "orjson is required for JsonSerializer. Install with: pip install dmq[orjson]"
            )
        return orj.loads(data)
