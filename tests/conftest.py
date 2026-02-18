from __future__ import annotations

from collections.abc import Generator

import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from dmq.backends.memory import InMemoryResultBackend
from dmq.brokers.memory import InMemoryBroker
from dmq.event_router import EventRouter
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer]:
	with RedisContainer("redis:8-alpine") as container:
		yield container


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer]:
	with PostgresContainer("postgres:18-alpine") as container:
		yield container


@pytest.fixture
def broker():
	return InMemoryBroker()


@pytest.fixture
def result_backend():
	return InMemoryResultBackend()


@pytest.fixture
def serializer():
	return MsgpackSerializer()


@pytest.fixture
def event_router():
	return EventRouter()


@pytest.fixture
def manager(broker, result_backend, serializer):
	return QManager(broker=broker, result_backend=result_backend, serializer=serializer)
