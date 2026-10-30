"""Queue depth and routing must follow the job broker, not metadata Redis."""

import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import fakeredis.aioredis
import pytest
import sqlalchemy as sa
from aiobotocore.session import get_session
from aiobotocore.stub import AioStubber
from alata.container import ServiceContainer
from alata.queue.brokers.database_broker import DatabaseBroker
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq_aio_sqs import SQSBroker
from taskiq_redis import ListQueueBroker

from taskiq_horizon.queue_backend import DatabaseQueueBackend, SqsQueueBackend, queue_backend
from taskiq_horizon.service_provider import mount_dashboard, register_services


def message(labels=None):
    return TaskiqMessage(task_id="audit", task_name="audit", labels=labels or {}, args=[], kwargs={})


@pytest.mark.asyncio
async def test_redis_reporting_and_workload_use_broker_connection(redis, config):
    broker_server = fakeredis.FakeServer()
    broker_redis = fakeredis.aioredis.FakeRedis(server=broker_server)
    broker = ListQueueBroker(url="redis://localhost:6379/1", queue_name="critical")
    container = ServiceContainer()
    container.instance("horizon.config", config)
    container.instance("horizon.redis", redis)
    container.instance("queue.broker", broker)
    register_services(container)
    broker.add_middlewares(container.make("horizon.middleware"))

    @broker.task(task_name="audit:critical")
    async def task():
        pass

    try:
        with patch("taskiq_redis.redis_broker.Redis", return_value=broker_redis):
            queued = await task.kiq()
        job = (await container.make("horizon.jobs").get_jobs([queued.task_id]))[0]
        assert job["queue"] == "critical"
        assert job["connection"] == "redis"
        assert await redis.llen("critical") == 0
        assert await broker_redis.llen("critical") == 1
        await container.make("horizon.metrics").increment_queue("critical", 1000)
        with patch("taskiq_horizon.queue_backend.Redis", return_value=broker_redis):
            assert await container.make("horizon.wait_time").calculate_for("critical") == 1
            workload = await container.make("horizon.workload").get()
            assert workload[0]["name"] == "critical"
            assert workload[0]["length"] == 1
    finally:
        await broker_redis.aclose()
        await broker.shutdown()


DATABASE_URLS = ["sqlite+aiosqlite://"]
if os.environ.get("HORIZON_TEST_DATABASE_URL"):
    DATABASE_URLS.append(os.environ["HORIZON_TEST_DATABASE_URL"])


@pytest.mark.asyncio
@pytest.mark.parametrize("url", DATABASE_URLS, ids=lambda url: url.split(":")[0])
async def test_database_backlog_excludes_delayed_and_active_jobs(url):
    engine = create_async_engine(url)
    table = sa.Table(
        f"horizon_audit_{uuid4().hex[:12]}",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("queue", sa.String(255)),
        sa.Column("available_at", sa.BigInteger),
        sa.Column("reserved_at", sa.BigInteger),
    )
    broker = DatabaseBroker(table=table.name, connection="audit", queues=["critical"], retry_after=60)
    broker.now = lambda: 1000
    backend = DatabaseQueueBackend(broker)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(table.metadata.create_all)
            await connection.execute(
                table.insert(),
                [
                    {"queue": "critical", "available_at": 900, "reserved_at": None},
                    {"queue": "critical", "available_at": 900, "reserved_at": 939},  # expired
                    {"queue": "critical", "available_at": 900, "reserved_at": 999},  # in flight
                    {"queue": "critical", "available_at": 1100, "reserved_at": None},  # delayed
                    {"queue": "other", "available_at": 900, "reserved_at": None},
                ],
            )
        with patch("alata.db.engine.get_async_engine", return_value=engine) as resolve:
            assert await backend.size("critical") == 2
            resolve.assert_called_once_with("audit")
            assert await backend.size("other") == 1
            assert await backend.size("critical' OR 1=1 --") == 0
            assert await backend.purge("critical' OR 1=1 --") == 0
            assert await backend.purge("critical") == 4
            assert await backend.size("critical") == 0
            assert await backend.size("other") == 1
        assert backend.queue_for(message()) == "critical"
        assert backend.queue_for(message({"queue_name": "other"})) == "other"
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(table.metadata.drop_all)
        await engine.dispose()


@pytest.mark.asyncio
async def test_sqs_uses_visible_message_count_and_actual_broker_destination(monkeypatch):
    broker = SQSBroker("critical", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    backend = SqsQueueBackend(broker)
    assert backend.queue_for(message({"queue_name": "ignored-by-sqs"})) == "critical"
    assert backend.queue_for(message({"queue": "urgent", "queue_name": "stale"})) == "urgent"
    async with get_session().create_client(
        "sqs", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    ) as client:
        stubber = AioStubber(client)
        url = "https://sqs.us-east-1.amazonaws.com/123456789012/critical"
        stubber.add_response("get_queue_url", {"QueueUrl": url}, {"QueueName": "critical"})
        stubber.add_response(
            "get_queue_attributes",
            {"Attributes": {"ApproximateNumberOfMessages": "7"}},
            {"QueueUrl": url, "AttributeNames": ["ApproximateNumberOfMessages"]},
        )

        @asynccontextmanager
        async def client_context(*args, **kwargs):
            assert args == ("sqs",)
            assert kwargs["region_name"] == "us-east-1"
            yield client

        monkeypatch.setattr(broker._session, "create_client", client_context)
        with stubber:
            assert await backend.size("critical") == 7
            stubber.assert_no_pending_responses()
            stubber.add_response("get_queue_url", {"QueueUrl": url}, {"QueueName": "critical"})
            stubber.add_response("purge_queue", {}, {"QueueUrl": url})
            assert await backend.purge("critical") is None
            stubber.assert_no_pending_responses()


@pytest.mark.asyncio
@pytest.mark.parametrize("queue", ["critical", "critical.fifo"])
async def test_sqs_retry_targets_original_queue_without_mutating_shared_broker(monkeypatch, queue):
    broker = SQSBroker(
        "default",
        use_task_id_for_deduplication=True,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    backend = SqsQueueBackend(broker)
    async with get_session().create_client(
        "sqs", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    ) as client:
        stubber = AioStubber(client)
        url = f"https://sqs.us-east-1.amazonaws.com/123456789012/{queue}"
        stubber.add_response("get_queue_url", {"QueueUrl": url}, {"QueueName": queue})
        expected = {"QueueUrl": url, "MessageBody": "{}"}
        if queue.endswith(".fifo"):
            expected.update(MessageGroupId="audit:job", MessageDeduplicationId="retry-id")
        stubber.add_response("send_message", {"MessageId": "retry-id"}, expected)

        @asynccontextmanager
        async def client_context(*args, **kwargs):
            yield client

        monkeypatch.setattr(broker._session, "create_client", client_context)
        with stubber:
            await backend.retry(
                BrokerMessage(task_id="retry-id", task_name="audit:job", message=b"{}", labels={}), queue
            )
            stubber.assert_no_pending_responses()
        assert broker._sqs_queue_name == "default"
        assert broker._sqs_queue_url is None
        assert broker._queue_urls == {}
        assert list(broker._queues) == ["default"]
        assert not hasattr(broker, "_sqs_client")


@pytest.mark.asyncio
async def test_stats_reports_actual_connection(redis, config):
    from fastapi import FastAPI

    container = ServiceContainer()
    container.instance("horizon.config", config)
    container.instance("horizon.redis", redis)
    container.instance("queue.broker", DatabaseBroker())
    register_services(container)
    backend = container.make("horizon.queue_backend")
    backend.size = AsyncMock(return_value=2)
    await container.make("horizon.metrics").increment_queue("default", 1000)
    app = FastAPI()
    mount_dashboard(app, container)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/horizon/api/stats")
    assert response.status_code == 200
    assert response.json()["wait"] == {"database:default": 2.0}


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("HORIZON_TEST_REDIS_URL"), reason="Set HORIZON_TEST_REDIS_URL for live Redis")
async def test_live_redis_dispatch_and_depth():
    queue = f"horizon-audit-{uuid4().hex}"
    broker = ListQueueBroker(url=os.environ["HORIZON_TEST_REDIS_URL"], queue_name=queue)
    backend = queue_backend(broker)

    @broker.task(task_name="audit:live")
    async def task():
        pass

    try:
        await task.kiq()
        assert await backend.size(queue) == 1
    finally:
        async with Redis(connection_pool=broker.connection_pool) as client:
            await client.delete(queue)
        await broker.shutdown()


def test_unknown_broker_is_not_misreported_as_redis():
    with pytest.raises(ValueError, match="cannot inspect"):
        queue_backend(object())


@pytest.mark.asyncio
async def test_workload_includes_supervised_queue_before_first_completed_job(redis, config):
    container = ServiceContainer()
    container.instance("horizon.config", config)
    container.instance("horizon.redis", redis)
    container.instance("queue.broker", DatabaseBroker())
    register_services(container)
    container.make("horizon.queue_backend").size = AsyncMock(return_value=5)
    container.make("horizon.supervisors").all_supervisors = AsyncMock(return_value=[{"processes": {"new-queue": 1}}])
    workload = await container.make("horizon.workload").get()
    assert workload[0]["name"] == "new-queue"
    assert workload[0]["length"] == 5


@pytest.mark.asyncio
async def test_redis_purge_clears_only_the_selected_broker_queue(redis, monkeypatch):
    broker = ListQueueBroker(url="redis://localhost:6379/1", queue_name="critical")
    backend = queue_backend(broker)
    await redis.rpush("critical", "job1", "job2")
    await redis.rpush("other", "job3")
    monkeypatch.setattr("taskiq_horizon.queue_backend.Redis", lambda **kwargs: redis)
    assert await backend.purge("critical") == 2
    assert await redis.llen("critical") == 0
    assert await redis.llen("other") == 1
    await broker.shutdown()
