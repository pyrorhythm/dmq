import os
import pickle
import warnings


class SecurityWarning(Warning):
	pass


pickle_allowed = os.getenv("DMQ_ALLOW_PICKLE", "").strip().lower() == "yes"


class PickleSerializer:
	@staticmethod
	def serialize(data: object) -> bytes:
		if not pickle_allowed:
			warnings.warn(
				"PickleSerializer is not secure and should only be used in trusted environments. "
				"Pickle can execute arbitrary code during deserialization. "
				"Consider using MsgpackSerializer or JsonSerializer instead.\n\n"
				"This warning could be turned off by setting envvar DMQ_ALLOW_PICKLE=yes.",
				SecurityWarning,
				stacklevel=2,
			)

		return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

	@staticmethod
	def deserialize(data: bytes, into: type | None = None) -> object:
		if not pickle_allowed:
			warnings.warn(
				"PickleSerializer is not secure and should only be used in trusted environments. "
				"Pickle can execute arbitrary code during deserialization. "
				"Consider using MsgpackSerializer or JsonSerializer instead.\n\n"
				"This warning could be turned off by setting envvar DMQ_ALLOW_PICKLE=yes.",
				SecurityWarning,
				stacklevel=2,
			)

		return pickle.loads(data)
