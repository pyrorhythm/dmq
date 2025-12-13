import msgspec

from ..types import TaskMessage


class MsgpackSerializer:
    _encoder = msgspec.msgpack.Encoder()
    _decoder = msgspec.msgpack.Decoder(TaskMessage)

    @classmethod
    def serialize(cls, data: TaskMessage) -> bytes:
        """Serialize TaskMessage to msgpack bytes."""
        return cls._encoder.encode(data)

    @classmethod
    def deserialize(cls, data: bytes) -> TaskMessage:
        """Deserialize msgpack bytes to TaskMessage."""
        return cls._decoder.decode(data)
