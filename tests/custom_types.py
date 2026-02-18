"""Custom types for type_serialization tests."""

from __future__ import annotations

import msgspec


class UserResult(msgspec.Struct, frozen=True):
	name: str
	age: int
	email: str | None = None


class OrderResult(msgspec.Struct, frozen=True):
	order_id: str
	total: float
	items: list[str]


class NestedResult(msgspec.Struct, frozen=True):
	user: UserResult
	order: OrderResult
