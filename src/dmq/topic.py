import msgspec


class TopicConfig(msgspec.Struct, frozen=True):
    name: str
    partition_count: int
    replication_factor: int = 1
    retention_ms: int | None = None
