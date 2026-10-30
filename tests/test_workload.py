"""Tests for RedisWorkloadRepository and WaitTimeCalculator."""

import pytest
import pytest_asyncio

from taskiq_horizon.repositories.redis_master_repository import RedisSupervisorRepository
from taskiq_horizon.repositories.redis_workload_repository import RedisWorkloadRepository
from taskiq_horizon.wait_time import WaitTimeCalculator

# ── Fake MetricsRepository ───────────────────────────────────────────────────


class FakeMetrics:
    """Stub metrics repo for testing — returns fixed values."""

    def __init__(self, queues=None, runtimes=None):
        self._queues = queues or []
        self._runtimes = runtimes or {}

    async def measured_queues(self):
        return self._queues

    async def runtime_for_queue(self, queue):
        return self._runtimes.get(queue, 100.0)  # default 100ms per job


# ── WaitTimeCalculator Tests ─────────────────────────────────────────────────


class TestWaitTimeCalculator:
    """Tests for WaitTimeCalculator — covers the upstream wait-time behavior."""

    @pytest.mark.asyncio
    async def test_calculate_empty_queues(self):
        """No measured queues → empty result."""
        calc = WaitTimeCalculator(FakeMetrics())
        result = await calc.calculate()
        assert result == {}

    @pytest.mark.asyncio
    async def test_calculate_with_zero_queue_depth(self):
        """Queues exist but are empty → wait time is 0."""
        metrics = FakeMetrics(queues=["default"], runtimes={"default": 50.0})
        calc = WaitTimeCalculator(metrics)

        result = await calc.calculate()
        assert result == {"default": 0.0}

    @pytest.mark.asyncio
    async def test_calculate_with_jobs_no_processes(self):
        """Queue has jobs but no processes → raw time (no division)."""
        queue_depths = {"default": 10}

        async def fetch_len(q):
            return queue_depths.get(q, 0)

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 200.0})
        calc = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)

        result = await calc.calculate()
        # 10 jobs * 200ms = 2000ms / 1000 = 2.0 seconds
        assert result["default"] == 2.0

    @pytest.mark.asyncio
    async def test_calculate_divides_by_process_count(self):
        """Wait time is divided by total processes for the queue."""
        queue_depths = {"default": 10}
        supervisors = [
            {"processes": {"default": 2}},
            {"processes": {"default": 3}},
        ]

        async def fetch_len(q):
            return queue_depths.get(q, 0)

        async def fetch_supervisors():
            return supervisors

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 200.0})
        calc = WaitTimeCalculator(
            metrics,
            queue_length_fetcher=fetch_len,
            supervisor_fetcher=fetch_supervisors,
        )

        result = await calc.calculate()
        # 10 jobs * 200ms = 2000ms / 5 processes = 400ms / 1000 = 0.4 seconds
        assert result["default"] == 0.4

    @pytest.mark.asyncio
    async def test_calculate_sorted_descending(self):
        """Results are sorted by wait time descending (longest first)."""
        queue_depths = {"high": 5, "low": 20}

        async def fetch_len(q):
            return queue_depths.get(q, 0)

        metrics = FakeMetrics(
            queues=["high", "low"],
            runtimes={"high": 100.0, "low": 100.0},
        )
        calc = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)

        result = await calc.calculate()
        keys = list(result.keys())
        # low has 20*100=2000ms=2.0s, high has 5*100=500ms=0.5s
        assert keys[0] == "low"
        assert keys[1] == "high"

    @pytest.mark.asyncio
    async def test_calculate_for_single_queue(self):
        """calculate_for() returns a single float for one queue."""
        queue_depths = {"default": 8}

        async def fetch_len(q):
            return queue_depths.get(q, 0)

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 250.0})
        calc = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)

        result = await calc.calculate_for("default")
        # 8 * 250 = 2000ms / 1000 = 2.0s
        assert result == 2.0

    @pytest.mark.asyncio
    async def test_calculate_time_to_clear_split_queue(self):
        """Split queues (comma-separated) sum up sub-queue times."""
        queue_depths = {"high": 3, "default": 7}

        async def fetch_len(q):
            return queue_depths.get(q, 0)

        metrics = FakeMetrics(
            queues=["high,default"],
            runtimes={"high": 100.0, "default": 100.0},
        )
        calc = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)

        result = await calc.calculate_time_to_clear("high,default")
        # high: 3*100=300ms, default: 7*100=700ms, total: 1000ms / 1000 = 1.0s
        assert result == 1.0

    @pytest.mark.asyncio
    async def test_queue_length_fetcher_failure_does_not_report_empty_queue(self):
        """An outage must not tell the autoscaler to shrink a busy queue."""

        async def broken_fetcher(q):
            raise ConnectionError("Redis down")

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 100.0})
        calc = WaitTimeCalculator(metrics, queue_length_fetcher=broken_fetcher)

        with pytest.raises(ConnectionError, match="Redis down"):
            await calc.calculate()


# ── RedisWorkloadRepository Tests ────────────────────────────────────────────


class TestRedisWorkloadRepository:
    """Tests for RedisWorkloadRepository — covers the upstream workload behavior."""

    @pytest_asyncio.fixture
    async def supervisor_repo(self, redis):
        return RedisSupervisorRepository(redis, prefix="test-horizon:")

    @pytest.fixture
    def wait_time(self):
        """Basic WaitTimeCalculator with fake metrics."""
        return WaitTimeCalculator(
            FakeMetrics(queues=["default"], runtimes={"default": 100.0}),
        )

    @pytest.fixture
    def repo(self, redis, wait_time, supervisor_repo):
        return RedisWorkloadRepository(redis, wait_time, supervisor_repo)

    @pytest.mark.asyncio
    async def test_get_empty_workload(self, repo):
        """No measured queues → empty list."""
        # Override with empty metrics
        repo.wait_time = WaitTimeCalculator(FakeMetrics())
        result = await repo.get()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_workload_with_queue_depth(self, redis, supervisor_repo):
        """Queue depth is read via LLEN."""
        # Push 5 fake jobs onto the 'default' queue
        for i in range(5):
            await redis.lpush("default", f"job-{i}")

        async def fetch_len(q):
            return await redis.llen(q)

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 200.0})
        wait_time = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)
        repo = RedisWorkloadRepository(redis, wait_time, supervisor_repo)

        result = await repo.get()
        assert len(result) == 1
        assert result[0]["name"] == "default"
        assert result[0]["length"] == 5
        assert result[0]["split_queues"] is None

    @pytest.mark.asyncio
    async def test_get_workload_includes_process_count(self, redis, supervisor_repo):
        """Process count is aggregated from all supervisors."""
        # Register two supervisors with processes on 'default'
        await supervisor_repo.update_supervisor(
            "sup-1",
            "master-1",
            100,
            "running",
            {"default": 3},
            {},
        )
        await supervisor_repo.update_supervisor(
            "sup-2",
            "master-1",
            101,
            "running",
            {"default": 2},
            {},
        )

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 100.0})
        wait_time = WaitTimeCalculator(metrics)
        repo = RedisWorkloadRepository(redis, wait_time, supervisor_repo)

        result = await repo.get()
        assert result[0]["processes"] == 5  # 3 + 2

    @pytest.mark.asyncio
    async def test_get_workload_zero_processes_when_no_supervisors(self, repo):
        """No supervisors registered → processes = 0."""
        result = await repo.get()
        assert len(result) == 1
        assert result[0]["processes"] == 0

    @pytest.mark.asyncio
    async def test_get_workload_split_queues(self, redis, supervisor_repo):
        """Comma-separated queue names produce split_queues breakdown."""
        # Push jobs onto sub-queues
        for i in range(3):
            await redis.lpush("high", f"job-h-{i}")
        for i in range(7):
            await redis.lpush("default", f"job-d-{i}")

        async def fetch_len(q):
            return await redis.llen(q)

        metrics = FakeMetrics(
            queues=["high,default"],
            runtimes={"high": 100.0, "default": 100.0},
        )
        wait_time = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)
        repo = RedisWorkloadRepository(redis, wait_time, supervisor_repo)

        result = await repo.get()
        assert len(result) == 1
        assert result[0]["name"] == "high,default"
        assert result[0]["length"] == 10  # 3 + 7
        assert result[0]["split_queues"] is not None
        assert len(result[0]["split_queues"]) == 2
        assert result[0]["split_queues"][0]["name"] == "high"
        assert result[0]["split_queues"][0]["length"] == 3
        assert result[0]["split_queues"][1]["name"] == "default"
        assert result[0]["split_queues"][1]["length"] == 7

    @pytest.mark.asyncio
    async def test_get_workload_multiple_queues(self, redis, supervisor_repo):
        """Multiple separate queues each get their own workload entry."""
        for i in range(4):
            await redis.lpush("high", f"job-{i}")
        for i in range(2):
            await redis.lpush("low", f"job-{i}")

        async def fetch_len(q):
            return await redis.llen(q)

        metrics = FakeMetrics(
            queues=["high", "low"],
            runtimes={"high": 100.0, "low": 100.0},
        )
        wait_time = WaitTimeCalculator(metrics, queue_length_fetcher=fetch_len)
        repo = RedisWorkloadRepository(redis, wait_time, supervisor_repo)

        result = await repo.get()
        assert len(result) == 2
        names = {r["name"] for r in result}
        assert names == {"high", "low"}

        high = next(r for r in result if r["name"] == "high")
        low = next(r for r in result if r["name"] == "low")
        assert high["length"] == 4
        assert low["length"] == 2

    @pytest.mark.asyncio
    async def test_queue_length_resilient_to_redis_errors(self, supervisor_repo):
        """If Redis LLEN fails, queue length falls back to 0."""

        class BrokenRedis:
            async def llen(self, key):
                raise ConnectionError("Redis down")

        metrics = FakeMetrics(queues=["default"], runtimes={"default": 100.0})
        wait_time = WaitTimeCalculator(metrics)
        repo = RedisWorkloadRepository(BrokenRedis(), wait_time, supervisor_repo)

        result = await repo.get()
        assert result[0]["length"] == 0
