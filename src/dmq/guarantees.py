from __future__ import annotations

from enum import StrEnum


class DeliveryGuarantee(StrEnum):
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class DeliveryConfig:
    def __init__(
        self,
        guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
        enable_idempotency: bool = False,
        idempotency_ttl: int = 86400,
    ) -> None:
        self.guarantee = guarantee
        self.enable_idempotency = enable_idempotency
        self.idempotency_ttl = idempotency_ttl

        if guarantee == DeliveryGuarantee.EXACTLY_ONCE and not enable_idempotency:
            raise ValueError("exactly_once guarantee requires enable_idempotency=true")

    def should_ack_before_processing(self) -> bool:
        return self.guarantee == DeliveryGuarantee.AT_MOST_ONCE

    def should_ack_after_processing(self) -> bool:
        return self.guarantee in (DeliveryGuarantee.AT_LEAST_ONCE, DeliveryGuarantee.EXACTLY_ONCE)

    def should_check_idempotency(self) -> bool:
        return self.enable_idempotency


class IdempotencyStore:
    def __init__(self) -> None:
        self._processed: dict[str, float] = {}

    async def is_processed(self, task_id: str) -> bool:
        return task_id in self._processed

    async def mark_processed(self, task_id: str, timestamp: float) -> None:
        self._processed[task_id] = timestamp

    async def cleanup_expired(self, ttl: int) -> None:
        import time

        now = time.time()
        expired = [task_id for task_id, ts in self._processed.items() if now - ts > ttl]
        for task_id in expired:
            del self._processed[task_id]
