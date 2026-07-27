"""Tests for Redis repository flush_expired methods."""

import time
from types import SimpleNamespace

import pytest
from taskiq.message import TaskiqMessage

from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_master_repository import (
    RedisMasterSupervisorRepository,
    RedisSupervisorRepository,
)
from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository


class TestRedisMasterSupervisorRepository:
    """Tests for master supervisor heartbeat and expiry."""

    @pytest.fixture
    def repo(self, redis):
        return RedisMasterSupervisorRepository(redis, prefix="test-horizon:")

    @pytest.mark.asyncio
    async def test_update_and_retrieve_master(self, repo):
        """update_master() stores state that all_masters() can retrieve."""
        await repo.update_master(
            name="master-1",
            pid=12345,
            status="running",
            supervisors=["sup-1", "sup-2"],
            environment="local",
        )

        masters = await repo.all_masters()
        assert len(masters) == 1
        assert masters[0]["name"] == "master-1"
        assert masters[0]["pid"] == "12345"
        assert masters[0]["status"] == "running"
        assert masters[0]["supervisors"] == ["sup-1", "sup-2"]

    @pytest.mark.asyncio
    async def test_flush_expired_removes_stale(self, repo, redis):
        """flush_expired() removes masters that haven't heartbeated > 14s."""
        # Register a master with a stale timestamp
        key = "test-horizon:masters"
        stale_time = time.time() - 20  # 20 seconds ago
        await redis.zadd(key, {b"stale-master": stale_time})
        await redis.zadd(key, {b"fresh-master": time.time()})

        await repo.flush_expired()

        names = await repo.get_master_names()
        assert "fresh-master" in names
        assert "stale-master" not in names

    @pytest.mark.asyncio
    async def test_flush_expired_keeps_fresh(self, repo):
        """flush_expired() keeps masters that heartbeated recently."""
        await repo.update_master("fresh", 1, "running", [], "local")

        await repo.flush_expired()

        names = await repo.get_master_names()
        assert "fresh" in names


class TestRedisSupervisorRepository:
    """Tests for supervisor heartbeat and expiry."""

    @pytest.fixture
    def repo(self, redis):
        return RedisSupervisorRepository(redis, prefix="test-horizon:")

    @pytest.mark.asyncio
    async def test_update_and_retrieve_supervisor(self, repo):
        """update_supervisor() stores state that all_supervisors() can retrieve."""
        await repo.update_supervisor(
            name="sup-1",
            master="master-1",
            pid=6789,
            status="running",
            processes={"default": 3},
            options={"queue": "default", "maxProcesses": 10},
        )

        sups = await repo.all_supervisors()
        assert len(sups) == 1
        assert sups[0]["name"] == "sup-1"
        assert sups[0]["master"] == "master-1"
        assert sups[0]["processes"] == {"default": 3}

    @pytest.mark.asyncio
    async def test_flush_expired_removes_stale(self, repo, redis):
        """flush_expired() removes supervisors that haven't heartbeated > 29s."""
        key = "test-horizon:supervisors"
        stale_time = time.time() - 35
        await redis.zadd(key, {b"stale-sup": stale_time})
        await redis.zadd(key, {b"fresh-sup": time.time()})

        await repo.flush_expired()

        names = await repo.get_supervisor_names()
        assert "fresh-sup" in names
        assert "stale-sup" not in names


class TestRedisMetricsRepository:
    @pytest.fixture
    def repo(self, redis, config):
        return RedisMetricsRepository(redis, config)

    @pytest.fixture
    def wait_time_calculator(self):
        class StubWaitTime:
            async def calculate_for(self, queue: str) -> float:
                return 12.5 if queue == "default" else 0.0

        return StubWaitTime()

    @pytest.mark.asyncio
    async def test_increment_and_read_metrics(self, repo):
        await repo.increment_job("memory:write", 50.0)
        await repo.increment_job("memory:write", 150.0)
        await repo.increment_queue("default", 100.0)

        assert await repo.measured_jobs() == ["memory:write"]
        assert await repo.measured_queues() == ["default"]
        assert await repo.throughput_for_job("memory:write") == 2
        assert await repo.runtime_for_job("memory:write") == 100.0
        assert await repo.throughput_for_queue("default") == 1
        assert await repo.runtime_for_queue("default") == 100.0

    @pytest.mark.asyncio
    async def test_snapshot_uses_sorted_sets_and_wait_times(self, repo, wait_time_calculator):
        repo.set_wait_time_calculator(wait_time_calculator)
        await repo.increment_job("memory:write", 50.0)
        await repo.increment_queue("default", 100.0)

        await repo.snapshot()

        job_snapshots = await repo.snapshots_for_job("memory:write")
        queue_snapshots = await repo.snapshots_for_queue("default")

        assert len(job_snapshots) == 1
        assert job_snapshots[0]["throughput"] == 1
        assert queue_snapshots[0]["wait"] == 12.5
        assert queue_snapshots[0]["runtime"] == 100.0

    @pytest.mark.asyncio
    async def test_jobs_processed_per_minute_uses_snapshot_timestamp(self, repo, redis):
        await repo.increment_queue("default", 100.0)
        await redis.set("test-horizon:metrics:last_snapshot_at", int(time.time()) - 120)

        assert await repo.jobs_processed_per_minute() == 1

    @pytest.mark.asyncio
    async def test_clear_removes_metric_keys(self, repo):
        await repo.increment_job("memory:write", 50.0)
        await repo.increment_queue("default", 100.0)
        await repo.snapshot()

        await repo.clear()

        assert await repo.measured_jobs() == []
        assert await repo.measured_queues() == []
        assert await repo.snapshots_for_job("memory:write") == []


class TestRedisJobRepository:
    @pytest.fixture
    def repo(self, redis, config):
        return RedisJobRepository(redis, config)

    def _payload(self, task_id: str, queue_name: str = "default", labels: dict | None = None) -> TaskPayload:
        message = TaskiqMessage(
            task_id=task_id,
            task_name="memory:write",
            labels={"queue_name": queue_name, **(labels or {})},
            args=[],
            kwargs={},
        )
        return TaskPayload(message).prepare(SimpleNamespace(silenced=[], silenced_tags=[]))

    @pytest.mark.asyncio
    async def test_failed_stores_full_stacktrace(self, repo):
        """failed() stores the formatted traceback so the dashboard stacktrace panel works."""
        payload = self._payload("failing-job-001")
        await repo.pushed("redis", "default", payload)

        def _raise():
            raise RuntimeError("boom with trace")

        try:
            _raise()
        except RuntimeError as exc:
            caught = exc

        await repo.failed(caught, "redis", "default", payload)

        job = await repo.find_failed("failing-job-001")
        assert job is not None
        assert "Traceback (most recent call last):" in job["exception"]
        assert "_raise" in job["exception"]
        assert "RuntimeError: boom with trace" in job["exception"]

    @pytest.mark.asyncio
    async def test_failed_retry_marks_parent_retry_entry_failed(self, repo):
        """A failed retry must flip its entry on the parent job to 'failed', not stay 'pending'."""
        parent = self._payload("parent-job-001")
        await repo.pushed("redis", "default", parent)
        await repo.store_retry_reference("parent-job-001", "retry-job-001")

        retry_payload = self._payload("retry-job-001", labels={"retry_of": "parent-job-001"})
        await repo.failed(RuntimeError("retry failed"), "redis", "default", retry_payload)

        parent_job = (await repo.get_jobs(["parent-job-001"]))[0]
        assert parent_job["retried_by"] == [
            {"id": "retry-job-001", "status": "failed", "retried_at": pytest.approx(int(time.time()), abs=5)}
        ]

    @pytest.mark.asyncio
    async def test_completed_retry_marks_parent_retry_entry_completed(self, repo):
        """A successful retry flips its entry on the parent job to 'completed'."""
        parent = self._payload("parent-job-002")
        await repo.pushed("redis", "default", parent)
        await repo.store_retry_reference("parent-job-002", "retry-job-002")

        retry_payload = self._payload("retry-job-002", labels={"retry_of": "parent-job-002"})
        await repo.completed(retry_payload)

        parent_job = (await repo.get_jobs(["parent-job-002"]))[0]
        assert parent_job["retried_by"][0]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_purge_removes_pending_and_reserved_jobs_for_queue(self, repo):
        pending_default = self._payload("pending-default", "default")
        reserved_default = self._payload("reserved-default", "default")
        pending_high = self._payload("pending-high", "high")

        await repo.pushed("redis", "default", pending_default)
        await repo.pushed("redis", "default", reserved_default)
        await repo.pushed("redis", "high", pending_high)
        await repo.reserved("redis", "default", reserved_default)

        removed = await repo.purge("default")

        assert removed == 2
        pending = await repo.get_pending()
        assert len(pending) == 1
        assert pending[0]["id"] == "pending-high"
        assert pending[0]["queue"] == "high"
