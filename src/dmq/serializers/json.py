from typing import Any, Literal, Self, overload

from msgspec.json import decode, encode


class JsonSerializer:
	_primary_type: type | None = None

	@classmethod
	def with_type(cls, _type: type) -> Self:
		inst = cls()
		inst._primary_type = _type
		return inst

	def serialize(self, data: Any) -> bytes:
		return encode(data)

	@overload
	def deserialize[T](self, data: bytes, into: type[T]) -> T: ...
	@overload
	def deserialize[T](self, data: bytes, into: Literal[None] = None) -> object: ...

	def deserialize[T](self, data: bytes, into: type[T] | None = None) -> T | object:
		if into is not None:
			return decode(data, type=into or self._primary_type)
		return decode(data)
