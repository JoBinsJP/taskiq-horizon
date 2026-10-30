"""Tests for Redis repository flush_expired methods."""

import asyncio
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
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository


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
    async def test_failed_stores_diagnostic_context_automatically(self, repo):
        """failed() always stores an exception context for the dashboard card."""
        payload = self._payload("failing-job-ctx")
        await repo.pushed("redis", "high", payload)

        await repo.failed(RuntimeError("ctx boom"), "redis", "high", payload)

        job = await repo.find_failed("failing-job-ctx")
        assert job is not None
        context = job["context"]
        assert isinstance(context, dict)
        assert context["exception_type"] == "RuntimeError"
        assert context["queue"] == "high"
        assert context["connection"] == "redis"
        assert context["task_name"] == "memory:write"
        assert context["attempts"] == 1
        assert isinstance(context["labels"], dict)

    @pytest.mark.asyncio
    async def test_failed_merges_exception_provided_context(self, repo):
        """An exception exposing an upstream-style context dict is merged in."""
        payload = self._payload("failing-job-ctx2")
        await repo.pushed("redis", "default", payload)

        class ContextualError(RuntimeError):
            def context(self):
                return {"order_id": 42}

        await repo.failed(ContextualError("boom"), "redis", "default", payload)

        job = await repo.find_failed("failing-job-ctx2")
        assert job is not None
        assert job["context"]["order_id"] == 42
        assert job["context"]["exception_type"] == "ContextualError"

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

    @pytest.mark.asyncio
    async def test_get_jobs_by_type_returns_newest_first(self, repo):
        """Job lists must render newest-first (ascending zrange over negated-timestamp scores)."""
        older = self._payload("order-older-001")
        newer = self._payload("order-newer-002")

        await repo.pushed("redis", "default", older)
        await asyncio.sleep(0.002)  # guarantee distinct timestamp scores
        await repo.pushed("redis", "default", newer)

        recent = await repo.get_recent()
        assert [job["id"] for job in recent] == ["order-newer-002", "order-older-001"]

        pending = await repo.get_pending()
        assert [job["id"] for job in pending] == ["order-newer-002", "order-older-001"]

    @pytest.mark.asyncio
    async def test_failed_jobs_returned_newest_first(self, repo):
        older = self._payload("failed-older-001")
        newer = self._payload("failed-newer-002")

        await repo.pushed("redis", "default", older)
        await repo.failed(RuntimeError("first"), "redis", "default", older)
        await asyncio.sleep(0.002)
        await repo.pushed("redis", "default", newer)
        await repo.failed(RuntimeError("second"), "redis", "default", newer)

        failed = await repo.get_failed()
        assert [job["id"] for job in failed] == ["failed-newer-002", "failed-older-001"]

    @pytest.mark.asyncio
    async def test_completed_and_silenced_jobs_returned_newest_first(self, repo):
        older = self._payload("done-older-001")
        newer = self._payload("done-newer-002")

        await repo.pushed("redis", "default", older)
        await repo.completed(older)
        await asyncio.sleep(0.002)
        await repo.pushed("redis", "default", newer)
        await repo.completed(newer)

        completed = await repo.get_completed()
        assert [job["id"] for job in completed] == ["done-newer-002", "done-older-001"]

        silenced_older = self._payload("quiet-older-001")
        silenced_newer = self._payload("quiet-newer-002")
        await repo.pushed("redis", "default", silenced_older)
        await repo.completed(silenced_older, silenced=True)
        await asyncio.sleep(0.002)
        await repo.pushed("redis", "default", silenced_newer)
        await repo.completed(silenced_newer, silenced=True)

        silenced = await repo.get_silenced()
        assert [job["id"] for job in silenced] == ["quiet-newer-002", "quiet-older-001"]


class TestRedisTagRepository:
    @pytest.fixture
    def repo(self, redis, config):
        return RedisTagRepository(redis, config)

    @pytest.fixture
    def job_repo(self, redis, config):
        return RedisJobRepository(redis, config)

    def _payload(self, task_id: str) -> TaskPayload:
        message = TaskiqMessage(
            task_id=task_id,
            task_name="memory:write",
            labels={"queue_name": "default"},
            args=[],
            kwargs={},
        )
        return TaskPayload(message).prepare(SimpleNamespace(silenced=[], silenced_tags=[]))

    @pytest.mark.asyncio
    async def test_add_stores_negated_timestamp_scores(self, repo, redis):
        before = time.time() * -1
        await repo.add("tag-job-001", ["tenant:42"])
        after = time.time() * -1

        score = await redis.zscore("test-horizon:tags:tenant:42", "tag-job-001")
        assert score is not None
        assert after <= score <= before

    @pytest.mark.asyncio
    async def test_tagged_jobs_returned_newest_first(self, repo, job_repo):
        older = self._payload("tagged-older-001")
        newer = self._payload("tagged-newer-002")

        await job_repo.pushed("redis", "default", older)
        await repo.add("tagged-older-001", ["tenant:42"])
        await asyncio.sleep(0.002)
        await job_repo.pushed("redis", "default", newer)
        await repo.add("tagged-newer-002", ["tenant:42"])

        jobs = await job_repo.get_jobs_by_tag("tenant:42")
        assert [job["id"] for job in jobs] == ["tagged-newer-002", "tagged-older-001"]

    @pytest.mark.asyncio
    async def test_get_jobs_by_tag_uses_start_index_pagination(self, repo, job_repo):
        """Tag pagination is start-index based: after_index=0 starts at the first job."""
        for task_id in ["page-job-001", "page-job-002"]:
            await job_repo.pushed("redis", "default", self._payload(task_id))
            await repo.add(task_id, ["paged"])
            await asyncio.sleep(0.002)

        all_jobs = await job_repo.get_jobs_by_tag("paged", 0)
        assert [job["id"] for job in all_jobs] == ["page-job-002", "page-job-001"]

        second_page = await job_repo.get_jobs_by_tag("paged", 1)
        assert [job["id"] for job in second_page] == ["page-job-001"]


@pytest.mark.asyncio
async def test_monitored_tag_indexes_are_trimmed(redis, config):
    from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository

    jobs = RedisJobRepository(redis, config)
    tags = RedisTagRepository(redis, config)
    await tags.add("new", ["billing", "failed:billing"])
    for tag in ["billing", "failed:billing"]:
        key = f"{config.prefix}tags:{tag}"
        await redis.zadd(key, {"expired": -(time.time() - 1000)})
        assert await redis.ttl(key) > 0
    await jobs.trim_monitored_jobs()
    assert await tags.count("billing") == 1
    assert await tags.count("failed:billing") == 1


@pytest.mark.asyncio
async def test_queue_snapshot_computes_wait_before_resetting_runtime(redis, config):
    from unittest.mock import AsyncMock

    from taskiq_horizon.wait_time import WaitTimeCalculator

    metrics = RedisMetricsRepository(redis, config)
    metrics.set_wait_time_calculator(
        WaitTimeCalculator(
            metrics,
            queue_length_fetcher=AsyncMock(return_value=10),
            supervisor_fetcher=AsyncMock(return_value=[{"processes": {"default": 2}}]),
        )
    )
    await metrics.increment_queue("default", 100)
    await metrics.snapshot()
    snapshots = await metrics.snapshots_for_queue("default")
    assert snapshots[0]["wait"] == 0.5
    assert snapshots[0]["runtime"] == 100
