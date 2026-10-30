"""Exercise TaskIQ's real child processes against an isolated Redis queue."""

import asyncio
import os
import time
from uuid import uuid4

import psutil
import pytest
from redis.asyncio import Redis
from taskiq import TaskiqMessage
from taskiq_redis import ListQueueBroker

from taskiq_horizon.process_pool import ProcessPool
from taskiq_horizon.worker_process import WorkerProcess

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not os.getenv("HORIZON_TEST_REDIS_URL"), reason="Requires isolated Redis"),
]


async def eventually(check, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    pytest.fail("Worker lifecycle did not reach the expected state")


@pytest.fixture
def worker_app(tmp_path, monkeypatch):
    queue = f"horizon-audit-{uuid4().hex}"
    module = "horizon_audit_worker"
    (tmp_path / f"{module}.py").write_text("""
import asyncio
import os
from redis.asyncio import Redis
from taskiq_redis import ListQueueBroker
url = os.environ['HORIZON_TEST_REDIS_URL']
queue = os.environ['ALATA_WORKER_QUEUE']
broker = ListQueueBroker(url=url, queue_name=queue)
from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.middleware import HorizonMiddleware
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository
config = HorizonConfig(prefix=queue + ':horizon:')
metadata = Redis.from_url(url)
broker.add_middlewares(HorizonMiddleware(config, RedisJobRepository(metadata, config), RedisMetricsRepository(metadata, config), RedisTagRepository(metadata, config)))
@broker.task(task_name='audit:hold')
async def hold(token: str):
    async with Redis.from_url(url) as redis:
        await redis.rpush(queue + ':started', token)
        while not await redis.exists(queue + ':release'):
            await asyncio.sleep(0.05)
        await redis.rpush(queue + ':finished', token)
""")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    return queue, f"{module}:broker"


async def test_pause_finishes_inflight_work_then_resume_consumes_again(worker_app):
    queue, broker_path = worker_app
    url = os.environ["HORIZON_TEST_REDIS_URL"]
    redis = Redis.from_url(url)
    broker = ListQueueBroker(url=url, queue_name=queue)
    from taskiq_horizon.config import HorizonConfig
    from taskiq_horizon.middleware import HorizonMiddleware
    from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
    from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository
    from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository

    config = HorizonConfig(prefix=queue + ":horizon:")
    jobs = RedisJobRepository(redis, config)
    tracker = HorizonMiddleware(config, jobs, RedisMetricsRepository(redis, config), RedisTagRepository(redis, config))
    broker.add_middlewares(tracker)
    pool = ProcessPool(broker_path, queue, ["--max-async-tasks", "1"])
    created = []

    async def send(token):
        message = TaskiqMessage(task_id=token, task_name="audit:hold", labels={}, args=[token], kwargs={})
        await tracker.pre_send(message)
        await broker.kick(broker.formatter.dumps(message))

    try:
        pool.scale(1)
        created.extend(pool.workers)
        await send("first")
        await eventually(lambda: redis.exists(queue + ":started"))
        parent = psutil.Process(pool.workers[0].pid)
        assert parent.children(recursive=True), "Must exercise the real TaskIQ parent/child topology"
        assert pool.workers[0].memory_usage_mb() >= parent.memory_info().rss / (1024 * 1024)
        pool.pause()
        assert pool.active_process_count == 0
        await redis.set(queue + ":release", "1")
        await eventually(lambda: redis.exists(queue + ":finished"))

        async def drained():
            pool.prune_terminating(timeout=10)
            return pool.total_process_count == 0

        await eventually(drained)
        await send("second")
        pool.monitor()
        assert await redis.llen(queue) == 1
        assert await redis.lrange(queue + ":finished", 0, -1) == [b"first"]
        pool.resume()
        created.extend(pool.workers)

        async def both_finished():
            return await redis.llen(queue + ":finished") == 2 and await jobs.count_completed() == 2

        await eventually(both_finished)
        pool.terminate_all()
        await eventually(drained)
    finally:
        for worker in created:
            worker.kill()
        await broker.shutdown()
        keys = [key async for key in redis.scan_iter(match=queue + ":horizon:*")]
        if keys:
            await redis.delete(*keys)
        await redis.delete(queue, queue + ":started", queue + ":finished", queue + ":release")
        await redis.aclose()


async def test_force_kill_removes_taskiq_children(worker_app):
    queue, broker_path = worker_app
    redis = Redis.from_url(os.environ["HORIZON_TEST_REDIS_URL"])
    broker = ListQueueBroker(url=os.environ["HORIZON_TEST_REDIS_URL"], queue_name=queue)
    worker = WorkerProcess(broker_path, queue)
    children = []
    try:
        worker.start()
        message = TaskiqMessage(task_id="held", task_name="audit:hold", labels={}, args=["held"], kwargs={})
        await broker.kick(broker.formatter.dumps(message))
        await eventually(lambda: redis.exists(queue + ":started"))
        children = psutil.Process(worker.pid).children(recursive=True)
        assert children
        worker.kill()

        async def children_exited():
            return all(not child.is_running() or child.status() == psutil.STATUS_ZOMBIE for child in children)

        await eventually(children_exited)
        assert not worker.is_alive()
    finally:
        worker.kill()
        for child in children:
            if child.is_running():
                child.kill()
        await broker.shutdown()
        keys = [key async for key in redis.scan_iter(match=queue + ":horizon:*")]
        if keys:
            await redis.delete(*keys)
        await redis.delete(queue, queue + ":started", queue + ":finished", queue + ":release")
        await redis.aclose()
