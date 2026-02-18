from typing import Literal, Self, overload

import msgspec


class MsgpackSerializer:
	_primary_type: type | None = None

	@classmethod
	def with_type(cls, _type: type) -> Self:
		inst = cls()
		inst._primary_type = _type
		return inst

	def serialize(self, data: object) -> bytes:
		return msgspec.msgpack.encode(data)

	@overload
	def deserialize[T](self, data: bytes, into: type[T]) -> T: ...
	@overload
	def deserialize[T](self, data: bytes, into: Literal[None] = None) -> object: ...

	def deserialize[T](self, data: bytes, into: type[T] | None = None) -> T | object:
		target_type = into or self._primary_type
		if target_type is not None:
			return msgspec.msgpack.decode(data, type=target_type)
		return msgspec.msgpack.decode(data)
