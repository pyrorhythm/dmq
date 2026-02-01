from __future__ import annotations

import pytest

from dmq.backends.memory import InMemoryResultBackend
from dmq.brokers.memory import InMemoryBroker
from dmq.event_router import EventRouter
from dmq.manager import QManager
from dmq.serializers.msgpack import MsgpackSerializer


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
