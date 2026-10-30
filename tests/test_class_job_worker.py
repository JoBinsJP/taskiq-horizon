"""Class discovery and retries through real Horizon-managed TaskIQ processes."""

import os
from uuid import uuid4

import pytest
from alata.queue.brokers.sync_broker import SyncBroker
from redis.asyncio import Redis
from taskiq.kicker import AsyncKicker
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.middleware import HorizonMiddleware
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository
from taskiq_horizon.worker_process import WorkerProcess


@pytest.mark.asyncio
async def test_class_dispatch_has_named_horizon_history(redis, config, monkeypatch):
    from alata.container import container
    from alata.queue import ShouldQueue

    jobs = RedisJobRepository(redis, config)
    broker = SyncBroker()
    broker.add_middlewares(
        HorizonMiddleware(config, jobs, RedisMetricsRepository(redis, config), RedisTagRepository(redis, config))
    )
    original_make = container.make
    monkeypatch.setattr(container, "make", lambda name: broker if name == "queue.broker" else original_make(name))

    class SendEmail(ShouldQueue):
        tags = ("mail",)

        async def handle(self, user_id: int):
            return user_id

    try:
        dispatched = await SendEmail.dispatch(user_id=42)
        record = (await jobs.get_jobs([dispatched.task_id]))[0]
        assert record["status"] == "completed"
        assert record["name"] == SendEmail.get_task_name()
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("HORIZON_TEST_REDIS_URL"), reason="Requires isolated Redis")
async def test_worker_discovers_job_class_without_patterns_and_tracks_retry(tmp_path, monkeypatch):
    url = os.environ["HORIZON_TEST_REDIS_URL"]
    queue = "alata-class-job-" + uuid4().hex
    prefix = queue + ":horizon:"
    package = tmp_path / "app" / "jobs" / "billing"
    package.mkdir(parents=True)
    (tmp_path / "app" / "__init__.py").write_text("")
    (tmp_path / "app" / "jobs" / "__init__.py").write_text("")
    (package / "invoices.py").write_text("""
import os
from alata.queue import ShouldQueue
from redis.asyncio import Redis

class GenerateInvoice(ShouldQueue):
    max_retries = 2
    retry_delay = 0
    tags = ('billing',)

    async def handle(self, invoice_id: int) -> int:
        async with Redis.from_url(os.environ['HORIZON_TEST_REDIS_URL']) as redis:
            attempts = await redis.incr(os.environ['ALATA_WORKER_QUEUE'] + ':attempts')
        if attempts == 1:
            raise ValueError('retry once')
        return invoice_id * 2
""")
    (tmp_path / "class_worker.py").write_text("""
import os
from alata.queue import broker as factory
from redis.asyncio import Redis
from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.middleware import HorizonMiddleware
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository
url = os.environ['HORIZON_TEST_REDIS_URL']
queue = os.environ['ALATA_WORKER_QUEUE']
factory._queue_config = lambda: {
    'default': 'redis', 'queue': queue, 'max_retries': 3, 'retry_delay': 0,
    'connections': {'redis': {'broker_url': url, 'result_url': url}},
}
broker = factory.build_broker()
config = HorizonConfig(prefix=queue + ':horizon:')
redis = Redis.from_url(url)
broker.add_middlewares(HorizonMiddleware(config, RedisJobRepository(redis, config),
    RedisMetricsRepository(redis, config), RedisTagRepository(redis, config)))
""")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    redis = Redis.from_url(url)
    config = HorizonConfig(prefix=prefix)
    jobs = RedisJobRepository(redis, config)
    broker = ListQueueBroker(url=url, queue_name=queue).with_result_backend(RedisAsyncResultBackend(redis_url=url))
    broker.add_middlewares(
        HorizonMiddleware(config, jobs, RedisMetricsRepository(redis, config), RedisTagRepository(redis, config))
    )
    worker = WorkerProcess("class_worker:broker", queue, ["--max-async-tasks", "1"])
    task = None
    try:
        worker.start()
        # A different producer process does not need the worker's Python class.
        # Send the exact public class identity and serialized arguments.
        task = await AsyncKicker(
            task_name="app.jobs.billing.invoices:GenerateInvoice",
            broker=broker,
            labels={"retry_on_error": True, "max_retries": 2, "retry_delay": 0, "tags": ["billing"]},
        ).kiq(invoice_id=21)
        result = await task.wait_result(timeout=20, check_interval=0.05)
        assert not result.is_err
        assert result.return_value == 42
        assert await redis.get(queue + ":attempts") == b"2"
        record = (await jobs.get_jobs([task.task_id]))[0]
        assert record["status"] == "completed"
        assert record["name"] == "app.jobs.billing.invoices:GenerateInvoice"
        assert await jobs.count_failed() == 0
    finally:
        worker.kill()
        await broker.shutdown()
        keys = [key async for key in redis.scan_iter(match=queue + "*")]
        if task:
            keys.append(task.task_id)
        if keys:
            await redis.delete(*keys)
        await redis.aclose()
