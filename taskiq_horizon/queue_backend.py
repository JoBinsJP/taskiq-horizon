"""Queue inspection at the broker boundary; Horizon state stays in Redis."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from copy import copy
from dataclasses import replace

from redis.asyncio import Redis
from taskiq.message import TaskiqMessage


class QueueBackend(ABC):
    connection: str

    def __init__(self, broker):
        self.broker = broker

    @abstractmethod
    def queue_for(self, message: TaskiqMessage) -> str: ...

    @abstractmethod
    async def size(self, queue: str) -> int: ...

    async def purge(self, queue: str) -> int | None:
        raise NotImplementedError("This backend does not support clearing queued jobs")


class RedisQueueBackend(QueueBackend):
    connection = "redis"

    def queue_for(self, message: TaskiqMessage) -> str:
        return message.labels.get("queue_name") or self.broker.queue_name

    async def size(self, queue: str) -> int:
        # Use the broker's pool, which can differ from Horizon's metadata Redis.
        async with Redis(connection_pool=self.broker.connection_pool) as client:
            return await client.llen(queue)

    async def purge(self, queue: str) -> int:
        async with Redis(connection_pool=self.broker.connection_pool) as client:
            async with client.pipeline(transaction=True) as pipe:
                pipe.llen(queue)
                pipe.delete(queue)
                result = await pipe.execute()
            return int(result[0])


class DatabaseQueueBackend(QueueBackend):
    connection = "database"

    def queue_for(self, message: TaskiqMessage) -> str:
        return message.labels.get("queue_name") or self.broker.queues[0]

    async def size(self, queue: str) -> int:
        import sqlalchemy as sa
        from alata.db.engine import get_async_engine

        jobs = sa.table(
            self.broker.table,
            sa.column("queue"),
            sa.column("reserved_at"),
            sa.column("available_at"),
        )
        now = round(self.broker.now())
        # Count runnable jobs, including expired reservations, just as a Redis
        # list counts work available to a worker rather than jobs in flight.
        runnable = sa.or_(
            sa.and_(jobs.c.reserved_at.is_(None), jobs.c.available_at <= now),
            jobs.c.reserved_at <= now - self.broker.retry_after,
        )
        statement = sa.select(sa.func.count()).select_from(jobs).where(jobs.c.queue == queue, runnable)
        async with get_async_engine(self.broker.connection).connect() as connection:
            return int((await connection.execute(statement)).scalar_one())

    async def purge(self, queue: str) -> int:
        import sqlalchemy as sa
        from alata.db.engine import get_async_engine

        jobs = sa.table(self.broker.table, sa.column("queue"))
        async with get_async_engine(self.broker.connection).begin() as connection:
            result = await connection.execute(sa.delete(jobs).where(jobs.c.queue == queue))
            return result.rowcount


class SqsQueueBackend(QueueBackend):
    connection = "sqs"

    def queue_for(self, message: TaskiqMessage) -> str:
        # SQS uses "queue", whereas Redis and database use "queue_name".
        return str(message.labels.get("queue", self.broker._default_queue.name))

    def _client(self, service):
        return self.broker._session.create_client(
            service,
            region_name=self.broker._aws_region,
            endpoint_url=self.broker._aws_endpoint_url,
            aws_access_key_id=self.broker._aws_access_key_id,
            aws_secret_access_key=self.broker._aws_secret_access_key,
        )

    async def size(self, queue: str) -> int:
        # A separate, scoped client works in the master and HTTP processes
        # without starting the broker or sharing a client's event loop.
        async with self._client("sqs") as client:
            url = (await client.get_queue_url(QueueName=queue))["QueueUrl"]
            result = await client.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])
            return int(result["Attributes"]["ApproximateNumberOfMessages"])

    async def purge(self, queue: str) -> None:
        async with self._client("sqs") as client:
            url = (await client.get_queue_url(QueueName=queue))["QueueUrl"]
            await client.purge_queue(QueueUrl=url)
        # SQS accepts the request asynchronously and returns no deletion count.

    async def retry(self, message, queue: str) -> None:
        # Use a scoped producer view so dashboard retries neither mutate the
        # shared broker's routing nor start worker, heartbeat or batch tasks.
        broker = copy(self.broker)
        broker._queue_urls = {}
        broker._sqs_queue_url = None
        destination = replace(
            broker._queues.get(queue, broker._default_queue), name=queue, is_fifo=queue.endswith(".fifo")
        )
        async with AsyncExitStack() as stack:
            broker._sqs_client = await stack.enter_async_context(self._client("sqs"))
            if broker.s3_extended_bucket_name:
                broker._s3_client = await stack.enter_async_context(self._client("s3"))
            await broker._send_single_message(message, queue=destination)


class InlineQueueBackend(QueueBackend):
    """Track inline jobs, but never launch workers for them."""

    def __init__(self, broker, connection: str):
        super().__init__(broker)
        self.connection = connection

    def queue_for(self, message: TaskiqMessage) -> str:
        return message.labels.get("queue_name") or "default"

    async def size(self, queue: str) -> int:
        return 0


def queue_backend(broker) -> QueueBackend:
    from alata.queue.brokers.database_broker import DatabaseBroker
    from alata.queue.brokers.null_broker import NullBroker
    from alata.queue.brokers.sync_broker import SyncBroker
    from taskiq_aio_sqs import SQSBroker
    from taskiq_redis import ListQueueBroker

    if isinstance(broker, ListQueueBroker):
        return RedisQueueBackend(broker)
    if isinstance(broker, DatabaseBroker):
        return DatabaseQueueBackend(broker)
    if isinstance(broker, SQSBroker):
        return SqsQueueBackend(broker)
    if isinstance(broker, (SyncBroker, NullBroker)):
        return InlineQueueBackend(broker, "sync" if isinstance(broker, SyncBroker) else "null")
    raise ValueError(f"Horizon cannot inspect queues for {type(broker).__name__}.")
