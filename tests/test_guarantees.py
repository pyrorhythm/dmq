from __future__ import annotations

import time

import pytest

from dmq.guarantees import DeliveryConfig, DeliveryGuarantee, IdempotencyStore


class TestDeliveryConfig:
	def test_default_is_at_least_once(self):
		config = DeliveryConfig()
		assert config.guarantee == DeliveryGuarantee.AT_LEAST_ONCE

	def test_at_most_once_acks_before(self):
		config = DeliveryConfig(guarantee=DeliveryGuarantee.AT_MOST_ONCE)
		assert config.should_ack_before_processing() is True
		assert config.should_ack_after_processing() is False

	def test_at_least_once_acks_after(self):
		config = DeliveryConfig(guarantee=DeliveryGuarantee.AT_LEAST_ONCE)
		assert config.should_ack_before_processing() is False
		assert config.should_ack_after_processing() is True

	def test_exactly_once_requires_idempotency(self):
		with pytest.raises(ValueError, match="idempotency"):
			DeliveryConfig(guarantee=DeliveryGuarantee.EXACTLY_ONCE)

	def test_exactly_once_with_idempotency(self):
		config = DeliveryConfig(guarantee=DeliveryGuarantee.EXACTLY_ONCE, enable_idempotency=True)
		assert config.should_ack_after_processing() is True
		assert config.should_check_idempotency() is True

	def test_idempotency_disabled_by_default(self):
		config = DeliveryConfig()
		assert config.should_check_idempotency() is False


class TestIdempotencyStore:
	async def test_mark_and_check(self):
		store = IdempotencyStore()
		assert await store.is_processed("t1") is False
		await store.mark_processed("t1", time.time())
		assert await store.is_processed("t1") is True

	async def test_cleanup_expired(self):
		store = IdempotencyStore()
		old_time = time.time() - 100
		await store.mark_processed("old", old_time)
		await store.mark_processed("new", time.time())

		await store.cleanup_expired(ttl=50)
		assert await store.is_processed("old") is False
		assert await store.is_processed("new") is True
