import pickle
import warnings
from typing import Any


class SecurityWarning(Warning):
    pass


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
    def deserialize(data: bytes, into: type | None = None) -> Any:
        return pickle.loads(data)  # noqa
