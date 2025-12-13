import pickle
import warnings
from typing import Any

from urllib3.exceptions import SecurityWarning


class PickleSerializer:
    @staticmethod
    def serialize(data: Any) -> bytes:
        warnings.warn(
            "PickleSerializer is not secure and should only be used in trusted environments. "
            "Pickle can execute arbitrary code during deserialization. "
            "Consider using MsgpackSerializer or JsonSerializer instead.",
            SecurityWarning,
            stacklevel=2,
        )

        return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        return pickle.loads(data)
