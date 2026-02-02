# dmq

### --> Distributed Message Queue,

> **fully python-native**, based on _pluggable architecture_ and _combination of
parallelism & concurrency_

---

## install

```bash
pip install dmq

# with redis broker/backend
pip install dmq[redis]

# with postgres backend
pip install dmq[postgres]

# everything
pip install dmq[all]
```

## quickstart

define a manager and register tasks:

```python
# app.py
from dmq import QManager
from dmq.brokers.redis import RedisBroker
from dmq.backends.redis_backend import RedisResultBackend
from dmq.serializers.msgpack import MsgpackSerializer

broker = RedisBroker(redis_url="redis://localhost:6379")
backend = RedisResultBackend(redis_url="redis://localhost:6379")

q = QManager(
	broker=RedisBroker(redis_url="redis://localhost:6379"), 
	result_backend=RedisResultBackend(redis_url="redis://localhost:6379"), 
	serializer=MsgpackSerializer()
)
```

```python
# tasks.py
from app import q

@q.register
async def add(x: int, y: int) -> int:
    return x + y

@q.register(qname="emails.send")
async def send_email(to: str, body: str) -> None:
    ...
```

enqueue from anywhere:

```python
from tasks import add

task = await add.q(2, 3)
result = await task.result(timeout=10.0)  # 5
```

run the worker:

```bash
dmq --broker app:q --tasks tasks --workers 4
```

---

## architecture

![arch](./dmq-arch.png)

everything is a protocol — swap any layer without touching the rest.

---

## brokers

the broker handles task queuing and delivery.

| broker | use case | backend |
|--------|----------|---------|
| `InMemoryBroker` | dev/testing | asyncio.Queue |
| `RedisBroker` | production | redis lists + sorted sets |

```python
from dmq.brokers.memory import InMemoryBroker
from dmq.brokers.redis import RedisBroker

# in-memory, no deps
broker = InMemoryBroker()

# redis, production
broker = RedisBroker(redis_url="redis://localhost:6379", poll_interval=0.5)
```

## result backends

store and retrieve task results.

| backend | use case | storage |
|---------|----------|---------|
| `InMemoryResultBackend` | dev/testing | dict |
| `RedisResultBackend` | production | redis keys with ttl |
| `PostgresResultBackend` | production, durable | postgres with schema + triggers |

```python
from dmq.backends.redis_backend import RedisResultBackend
from dmq.backends.postgres_backend import PostgresResultBackend

redis_backend = RedisResultBackend(
    redis_url="redis://localhost:6379",
    default_ttl=3600,
    type_serialization=True,  # preserves result types via FQN
)

pg_backend = PostgresResultBackend(
    dsn="postgresql://user:pass@localhost/mydb",
    type_serialization=True,
)
await pg_backend.connect()
```

`type_serialization` round-trips custom types (msgspec structs, etc) by storing the fully qualified name alongside the result. requires worker and client to share the same codebase.

## serializers

| serializer | format | notes |
|------------|--------|-------|
| `MsgpackSerializer` | binary | default, fast |
| `JsonSerializer` | text | human readable |
| `PickleSerializer` | binary | deprecated, legacy only |

---

## scheduling

### one-shot scheduling

delay, eta, or cron... fire once:

```python
from datetime import datetime, UTC, timedelta

# delay by 30 seconds
await my_task.sched(delay=30.0, arg1, arg2)

# fire at a specific time
await my_task.sched(eta=datetime(2025, 6, 1, tzinfo=UTC))

# next cron match
await my_task.sched(cron="0 */4 * * *")
```

### periodic scheduling

register tasks that fire on a recurring schedule for the lifetime of the program:

```python
from dmq.types import CronSchedule, DelaySchedule

# every 5 minutes
@q.periodic(DelaySchedule(delay_seconds=300))
async def heartbeat():
    ...

# every 4 hours, cron
@q.periodic(CronSchedule(cron_expr="0 */4 * * *"))
async def cleanup():
    ...

# every 4 hours, but first fire delayed 1 hour from program start
@q.periodic(CronSchedule(cron_expr="0 */4 * * *", delay_seconds=3600))
async def sync_data():
    ...
```

periodic tasks are started automatically when `QWorkerPool.start()` is called. the `PeriodicScheduler` runs as a background asyncio task, computing next fire times and enqueuing via `broker.send_task()` at each tick.

`delay_seconds` on `CronSchedule` offsets the base time forward before computing the next cron match — useful for staggering periodic work or deferring from program start.

---

## delivery guarantees

```python
from dmq.guarantees import DeliveryConfig, DeliveryGuarantee

config = DeliveryConfig(
    guarantee=DeliveryGuarantee.AT_LEAST_ONCE,  # default
)
```

| guarantee | behavior |
|-----------|----------|
| `AT_MOST_ONCE` | ack before processing — fast, may lose tasks |
| `AT_LEAST_ONCE` | ack after processing — default, may reprocess |
| `EXACTLY_ONCE` | ack after + idempotency store — requires `enable_idempotency=True` |

## retry policies

```python
from dmq.retry import RetryPolicy, ExponentialBackoff, LinearBackoff, FixedDelayBackoff

policy = RetryPolicy(
    max_retries=5,
    backoff=ExponentialBackoff(base=2.0, max_delay=3600.0, jitter=True),
    retry_on=(ConnectionError, TimeoutError),
    dont_retry_on=(ValueError,),
)
```

backoff strategies: `ExponentialBackoff`, `LinearBackoff`, `FixedDelayBackoff`

---

## worker pool

```python
from dmq import QWorkerPool, ExecutionMode

pool = QWorkerPool(
    manager=q,
    worker_count=4,
    max_tasks_per_worker=10,
    execution_mode=ExecutionMode.ASYNC_ONLY,
)

await pool.start()
await pool.run_forever()
```

### execution modes

| mode | python build | worker type | recommended workers |
|------|-------------|-------------|-------------------|
| `ASYNC_ONLY` | standard cpython | `QAsyncWorker` | `min(cpu * 2, 8)` |
| `THREADED` | free-threaded 3.13+ | `QThreadedWorker` | `cpu_count` |

auto-detected at runtime via `detect_execution_mode()`. free-threaded mode requires `sys._is_gil_enabled() == False`.

---

## events & callbacks

hook into task lifecycle:

```python
from dmq.callback import Callback, CallbackRule
from dmq.events import QEventType

@q.callback
class LogFailures(Callback):
    rules = [CallbackRule(event_types=[QEventType.TASK_FAILED])]

    async def on_event(self, event):
        print(f"task {event.task_id} failed: {event.exception}")
```

event types: 
 - `TASK_QUEUED`
 - `TASK_STARTED`
 - `TASK_COMPLETED`
 - `TASK_FAILED`
 - `TASK_RETRY`
 - `TASK_NOT_FOUND`

## partitioning

```python
from dmq import HashPartitionStrategy, KeyBasedPartitionStrategy, RoundRobinPartitionStrategy

strategy = HashPartitionStrategy(num_partitions=8)
partition = strategy.get_partition("task-id-123")
```

---

## cli

```bash
dmq --broker app:q --tasks tasks --workers 4 --concurrency 20 --guarantee at_least_once

# options
  --broker        		python path to QManager instance [module:variable]
  --tasks         		task modules to import [space separated]
  --workers, -w			number of workers [default: auto]
  --concurrency, -c  	max concurrent tasks per worker [default: 10]
  --guarantee, -g    	[at_most_once | at_least_once | exactly_once]
  --execution-mode, -e  [auto | async | threaded]
  --log-level, -l    	[debug | info | warning]
```

---

## development

```bash
# install
uv sync --all-extras --dev

# test
just test

# test with testcontainers (redis + postgres)
just test-lima

# lint & format
just lint

# type check
just type-check
```

## license

apache 2.0 - [see here](./LICENSE)
