import json

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from taskiq.message import TaskiqMessage

from taskiq_horizon.api.routes import router
from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository


class DummySerializer:
    def dumpb(self, value):
        return json.dumps(value).encode("utf-8")

    def loadb(self, value):
        return json.loads(value.decode("utf-8"))


class DummyBroker:
    def __init__(self):
        self.serializer = DummySerializer()
        self.kicked = []

    async def kick(self, message):
        self.kicked.append(message)


class DummyWaitTime:
    async def calculate(self):
        return {}


class DummyWorkload:
    async def get(self):
        return {}


class DummyBatchRepo:
    def __init__(self):
        self.last_broker = None

    async def get_batches(self, limit=50, before_id=None, search_query=None):
        if before_id == "b-2":
            return {"batches": [{"id": "b-1"}], "total": 1}
        return {"batches": [{"id": "b-2"}, {"id": "b-1"}], "total": 2}

    async def find_batch(self, batch_id, broker=None):
        self.last_broker = broker
        if batch_id == "missing":
            return None
        return {"id": batch_id, "batch": {"failedJobIds": ["f-1"]}}


def make_app(redis, config):
    app = FastAPI()
    app.state.horizon_config = config
    app.state.horizon_job_repo = RedisJobRepository(redis, config)
    app.state.horizon_metrics_repo = RedisMetricsRepository(redis, config)
    app.state.horizon_tag_repo = RedisTagRepository(redis, config)
    app.state.horizon_wait_time = DummyWaitTime()
    app.state.horizon_workload_repo = DummyWorkload()
    app.state.horizon_master_repo = None
    app.state.horizon_super_repo = None
    app.state.horizon_command_queue = None
    app.state.broker = DummyBroker()
    app.include_router(router)
    return app


def make_message(task_id: str, tags: list[str] | None = None) -> TaskiqMessage:
    return TaskiqMessage(
        task_id=task_id,
        task_name="memory:write",
        labels={"queue_name": "default", "tags": tags or []},
        args=[1, 2],
        kwargs={"customer_id": 42},
    )


async def seed_failed_job(app, task_id: str, tags: list[str] | None = None):
    job_repo = app.state.horizon_job_repo
    tag_repo = app.state.horizon_tag_repo
    config = app.state.horizon_config

    message = make_message(task_id, tags=tags)
    payload = TaskPayload(message).prepare(config)

    await job_repo.pushed("redis", "default", payload)
    await job_repo.reserved("redis", "default", payload)
    await job_repo.failed(RuntimeError("boom"), "redis", "default", payload)

    if tags:
        await tag_repo.add(task_id, tags)
        await tag_repo.add(task_id, [f"failed:{tag}" for tag in tags])


async def seed_recent_job(app, task_id: str):
    job_repo = app.state.horizon_job_repo
    config = app.state.horizon_config

    message = make_message(task_id)
    payload = TaskPayload(message).prepare(config)
    await job_repo.pushed("redis", "default", payload)


@pytest.mark.asyncio
async def test_retry_job_requeues_failed_message(redis, config):
    app = make_app(redis, config)
    await seed_failed_job(app, "failed-001", tags=["tenant:42"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post("/api/jobs/retry/failed-001")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "retry_dispatched"
    assert len(app.state.broker.kicked) == 1

    broker_message = app.state.broker.kicked[0]
    assert broker_message.task_name == "memory:write"
    assert broker_message.labels["retry_of"] == "failed-001"

    pending = await app.state.horizon_job_repo.get_pending()
    assert any(job["id"] == body["id"] for job in pending)

    failed_job = await app.state.horizon_job_repo.find_failed("failed-001")
    assert failed_job["retried_by"][0]["id"] == body["id"]


@pytest.mark.asyncio
async def test_monitoring_endpoint_returns_tagged_jobs(redis, config):
    app = make_app(redis, config)
    await seed_failed_job(app, "failed-002", tags=["tenant:42"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/monitoring/tenant:42")

    body = response.json()
    assert response.status_code == 200
    assert body["total"] == 1
    assert body["jobs"][0]["id"] == "failed-002"


@pytest.mark.asyncio
async def test_failed_jobs_can_filter_by_tag(redis, config):
    app = make_app(redis, config)
    await seed_failed_job(app, "failed-003", tags=["tenant:42"])
    await seed_failed_job(app, "failed-004", tags=["tenant:43"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/jobs/failed?tag=tenant:42")

    body = response.json()
    assert response.status_code == 200
    assert body["total"] == 1
    assert [job["id"] for job in body["jobs"]] == ["failed-003"]


@pytest.mark.asyncio
async def test_metrics_indexes_return_slug_lists(redis, config):
    app = make_app(redis, config)
    metrics = app.state.horizon_metrics_repo
    await metrics.increment_job("memory:write", 10.0)
    await metrics.increment_queue("default", 20.0)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        jobs_response = await client.get("/api/metrics/jobs")
        queues_response = await client.get("/api/metrics/queues")

    assert jobs_response.json() == ["memory:write"]
    assert queues_response.json() == ["default"]


@pytest.mark.asyncio
async def test_recent_jobs_route_returns_recent_jobs(redis, config):
    app = make_app(redis, config)
    await seed_recent_job(app, "recent-001")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/jobs/recent")

    body = response.json()
    assert response.status_code == 200
    assert body["total"] == 1
    assert body["jobs"][0]["id"] == "recent-001"


@pytest.mark.asyncio
async def test_delete_failed_job_route_removes_failed_job(redis, config):
    app = make_app(redis, config)
    await seed_failed_job(app, "failed-delete-001")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.delete("/api/jobs/failed/failed-delete-001")

    assert response.status_code == 200
    assert response.json()["status"] == "deleted"
    assert await app.state.horizon_job_repo.find_failed("failed-delete-001") is None


@pytest.mark.asyncio
async def test_purge_queue_route_removes_pending_jobs(redis, config):
    app = make_app(redis, config)
    await seed_recent_job(app, "purge-default-001")
    message = TaskiqMessage(
        task_id="purge-high-001",
        task_name="memory:write",
        labels={"queue_name": "high"},
        args=[],
        kwargs={},
    )
    payload = TaskPayload(message).prepare(config)
    await app.state.horizon_job_repo.pushed("redis", "high", payload)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post("/api/queues/default/purge")

    assert response.status_code == 200
    assert response.json()["count"] == 1

    pending = await app.state.horizon_job_repo.get_pending()
    assert [job["id"] for job in pending] == ["purge-high-001"]


@pytest.mark.asyncio
async def test_get_batches_pagination_with_before_id(redis, config):
    app = make_app(redis, config)
    app.state.horizon_batch_repo = DummyBatchRepo()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/batches?before_id=b-2")

    assert response.status_code == 200
    assert response.json()["batches"][0]["id"] == "b-1"
    assert response.json()["total"] == 1


@pytest.mark.asyncio
async def test_get_batch_detail_returns_404_when_missing(redis, config):
    app = make_app(redis, config)
    app.state.horizon_batch_repo = DummyBatchRepo()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/batches/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


@pytest.mark.asyncio
async def test_get_batch_detail_returns_501_when_not_configured(redis, config):
    app = make_app(redis, config)
    # Don't set horizon_batch_repo to simulate missing config

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/batches/some-batch")

    assert response.status_code == 501
    assert response.json()["detail"] == "Not Configured"


@pytest.mark.asyncio
async def test_retry_batch_forwards_broker_and_retries_jobs(redis, config):
    app = make_app(redis, config)
    batch_repo = DummyBatchRepo()
    app.state.horizon_batch_repo = batch_repo
    await seed_failed_job(app, "f-1")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post("/api/batches/retry/b-1")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "retry_dispatched"
    assert body["retried"] == 1
    assert batch_repo.last_broker is app.state.broker
    assert len(app.state.broker.kicked) == 1


@pytest.mark.asyncio
async def test_retry_batch_returns_404_when_all_failed_jobs_are_missing(redis, config):
    app = make_app(redis, config)
    app.state.horizon_batch_repo = DummyBatchRepo()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post("/api/batches/retry/b-1")

    assert response.status_code == 404
    assert response.json()["detail"]["message"] == "1 failed job could not be found"


@pytest.mark.asyncio
async def test_retry_batch_returns_422_when_retries_cannot_run(redis, config):
    app = make_app(redis, config)
    app.state.horizon_batch_repo = DummyBatchRepo()
    app.state.broker = None
    await seed_failed_job(app, "f-1")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post("/api/batches/retry/b-1")

    assert response.status_code == 422
    assert response.json()["detail"]["message"] == "1 failed job could not be retried"
