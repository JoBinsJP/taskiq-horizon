import base64
import json
import pickle
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from alata.db import engine as db_engine
from alata.db.connections import ConnectionManager
from alata.queue.batch_middleware import BatchMiddleware
from alata.queue.batch_repository import BatchRepository
from alata.queue.brokers.sync_broker import SyncBroker
from alata.queue.models import JobBatch
from taskiq import TaskiqMessage
from test_alata_integration import services
from test_api import make_app, seed_failed_job

from taskiq_horizon.jobs.retry_failed_job import RetryFailedJob
from taskiq_horizon.payload import TaskPayload


class ExecutablePayload:
    def __reduce__(self):
        return eval, ("{'args': [], 'kwargs': {}}",)


@pytest.mark.asyncio
async def test_retry_rejects_executable_pickle_globals(redis, config):
    app = make_app(redis, config)
    await seed_failed_job(app, "unsafe")
    jobs = app.state.horizon_job_repo
    failed = await jobs.find_failed("unsafe")
    failed["payload"]["taskiq"]["state"] = base64.b64encode(pickle.dumps(ExecutablePayload())).decode()
    await redis.hset(jobs._key("unsafe"), "payload", json.dumps(failed["payload"]))
    handler = RetryFailedJob(app.state.broker, jobs, app.state.horizon_tag_repo, config)
    with pytest.raises(RuntimeError, match="Retry payload unavailable"):
        await handler.handle("unsafe")
    assert app.state.broker.kicked == []
    assert await jobs.count_pending() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch_fails", [False, True])
async def test_dashboard_retry_updates_real_batch_without_negative_counts(
    redis, config, tmp_path, monkeypatch, dispatch_fails
):
    connections = ConnectionManager()
    monkeypatch.setattr(db_engine, "_connections", connections)
    connections.init_async_engine("default", f"sqlite+aiosqlite:///{tmp_path / 'batches.sqlite'}")
    engine = db_engine.get_async_engine()
    async with engine.begin() as connection:
        await connection.run_sync(JobBatch.__table__.create)
    batch_id = str(uuid4())
    jobs, tags, middleware = services(redis, config)
    broker = SyncBroker().with_middlewares(BatchMiddleware(), middleware)
    calls = []

    @broker.task(task_name="audit:batch_retry")
    async def task():
        calls.append("ran")

    await JobBatch.acreate(
        {
            "id": batch_id,
            "name": "Failed batch",
            "total_jobs": 1,
            "pending_jobs": 0,
            "failed_jobs": 1,
            "failed_job_ids": ["original"],
            "options": {},
            "created_at": 100,
            "finished_at": 101,
        }
    )
    message = TaskiqMessage(
        task_id="original", task_name="audit:batch_retry", labels={"batch_id": batch_id}, args=[], kwargs={}
    )
    await jobs.failed(RuntimeError("old failure"), "sync", "default", TaskPayload(message).prepare(config))
    if dispatch_fails:
        monkeypatch.setattr(broker, "kick", AsyncMock(side_effect=OSError("broker unavailable")))
    try:
        handler = RetryFailedJob(broker, jobs, tags, config)
        if dispatch_fails:
            with pytest.raises(OSError, match="broker unavailable"):
                await handler.handle("original")
        else:
            retry_id = await handler.handle("original")
            assert (await jobs.get_jobs([retry_id]))[0]["status"] == "completed"
        batch = await BatchRepository.find(batch_id)
        assert batch.pending_jobs == 0
        assert batch.failed_jobs == int(dispatch_fails)
        assert batch.failed_job_ids == (["original"] if dispatch_fails else [])
        assert batch.finished_at is not None
        assert "_manual_retries" not in batch.options
        assert calls == ([] if dispatch_fails else ["ran"])
        assert await jobs.count_pending() == 0
    finally:
        await broker.shutdown()
        await engine.dispose()
