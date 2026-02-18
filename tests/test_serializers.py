from __future__ import annotations

import warnings

import msgspec
import pytest

from dmq.serializers.json import JsonSerializer
from dmq.serializers.msgpack import MsgpackSerializer
from dmq.serializers.pickle import PickleSerializer


class SampleStruct(msgspec.Struct):
	x: int
	y: str


class TestMsgpackSerializer:
	def test_roundtrip_primitives(self):
		s = MsgpackSerializer()
		for val in [42, "hello", [1, 2, 3], {"a": 1}]:
			assert s.deserialize(s.serialize(val)) == val

	def test_roundtrip_with_type(self):
		s = MsgpackSerializer()
		obj = SampleStruct(x=1, y="test")
		data = s.serialize(obj)
		result = s.deserialize(data, into=SampleStruct)
		assert result == obj

	def test_with_type_classmethod(self):
		s = MsgpackSerializer.with_type(SampleStruct)
		obj = SampleStruct(x=5, y="hi")
		data = s.serialize(obj)
		result = s.deserialize(data)
		assert result == obj

	def test_deserialize_without_type(self):
		s = MsgpackSerializer()
		data = s.serialize({"key": "value"})
		result = s.deserialize(data)
		assert result == {"key": "value"}


class TestJsonSerializer:
	def test_roundtrip_primitives(self):
		s = JsonSerializer()
		for val in [42, "hello", [1, 2, 3], {"a": 1}]:
			assert s.deserialize(s.serialize(val)) == val

	def test_roundtrip_with_type(self):
		s = JsonSerializer()
		obj = SampleStruct(x=1, y="test")
		data = s.serialize(obj)
		result = s.deserialize(data, into=SampleStruct)
		assert result == obj

	def test_deserialize_without_type(self):
		s = JsonSerializer()
		data = s.serialize({"key": "value"})
		result = s.deserialize(data)
		assert result == {"key": "value"}


class TestPickleSerializer:
	def test_roundtrip(self):
		s = PickleSerializer()
		for val in [42, "hello", [1, 2, 3], {"a": 1}]:
			with warnings.catch_warnings():
				warnings.simplefilter("ignore")
				assert s.deserialize(s.serialize(val)) == val

	def test_serialize_warns(self):
		s = PickleSerializer()
		with pytest.warns(match="not secure"):
			s.serialize("test")

	def test_deserialize_accepts_into(self):
		s = PickleSerializer()
		with warnings.catch_warnings():
			warnings.simplefilter("ignore")
			data = s.serialize({"x": 1})
		# into is accepted but ignored
		result = s.deserialize(data, into=dict)
		assert result == {"x": 1}
