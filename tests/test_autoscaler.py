import pytest

from taskiq_horizon.autoscaler import AutoScaler
from taskiq_horizon.supervisor import SupervisorOptions


class MockSupervisor:
    def __init__(
        self,
        queue: str,
        max_processes: int = 10,
        min_processes: int = 1,
        balance: str = "auto",
        auto_scale_by_num_jobs: bool = False,
    ):
        self.options = SupervisorOptions(
            name="test-supervisor",
            queue=queue,
            max_processes=max_processes,
            min_processes=min_processes,
            balance=balance,
            auto_scale_by_num_jobs=auto_scale_by_num_jobs,
        )


class MockMetrics:
    def __init__(self, runtimes):
        self.runtimes = runtimes

    async def runtime_for_queue(self, queue):
        return self.runtimes.get(queue, 0.0)


class MockWaitTime:
    def __init__(self, lengths):
        self.lengths = lengths

    async def _get_queue_length(self, queue):
        return self.lengths.get(queue, 0)


@pytest.fixture
def autoscaler_factory():
    def _build(runtimes, lengths):
        return AutoScaler(MockMetrics(runtimes), MockWaitTime(lengths))

    return _build


@pytest.mark.asyncio
async def test_simple_strategy_scales_by_queue_depth(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 5.0}, {"default": 3})
    supervisor = MockSupervisor("default", max_processes=10, min_processes=1, balance="simple")

    assert await autoscaler.scale(supervisor) == 3


@pytest.mark.asyncio
async def test_auto_strategy_scales_by_time_to_clear(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 2500.0}, {"default": 3})
    supervisor = MockSupervisor("default", max_processes=10, min_processes=1, balance="auto")

    assert await autoscaler.scale(supervisor) == 8


@pytest.mark.asyncio
async def test_num_jobs_strategy_scales_by_total_jobs(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 999.0}, {"default": 7})
    supervisor = MockSupervisor(
        "default",
        max_processes=10,
        min_processes=1,
        balance="auto",
        auto_scale_by_num_jobs=True,
    )

    assert await autoscaler.scale(supervisor) == 7


@pytest.mark.asyncio
async def test_multi_queue_supervisor_sums_backlog(autoscaler_factory):
    autoscaler = autoscaler_factory(
        {"default": 500.0, "high": 500.0},
        {"default": 2, "high": 3},
    )
    supervisor = MockSupervisor("default,high", max_processes=10, min_processes=1, balance="simple")

    assert await autoscaler.scale(supervisor) == 5


@pytest.mark.asyncio
async def test_autoscaler_respects_min_and_max(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 5000.0}, {"default": 0})
    supervisor = MockSupervisor("default", max_processes=4, min_processes=2, balance="auto")

    assert await autoscaler.scale(supervisor) == 2
