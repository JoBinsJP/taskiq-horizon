import pytest

from taskiq_horizon.autoscaler import AutoScaler
from taskiq_horizon.supervisor import SupervisorOptions


class MockPool:
    def __init__(self, queue: str, active_process_count: int = 0):
        self.queue = queue
        self.active_process_count = active_process_count
        self.terminating_count = 0
        self.scale_calls: list[int] = []

    @property
    def total_process_count(self) -> int:
        return self.active_process_count + self.terminating_count

    def scale(self, target: int) -> None:
        self.scale_calls.append(target)
        self.active_process_count = target

    def prune_terminating(self, timeout: int = 60) -> None:
        self.terminating_count = 0


class MockSupervisor:
    def __init__(
        self,
        queue: str,
        *,
        max_processes: int = 10,
        min_processes: int = 1,
        balance: str = "auto",
        auto_scale_by_num_jobs: bool = False,
        auto_scaling_strategy: str = "time",
        balance_max_shift: int = 10,
    ):
        self.options = SupervisorOptions(
            name="test-supervisor",
            queue=queue,
            max_processes=max_processes,
            min_processes=min_processes,
            balance=balance,
            auto_scale_by_num_jobs=auto_scale_by_num_jobs,
            auto_scaling_strategy=auto_scaling_strategy,
            balance_max_shift=balance_max_shift,
        )
        queues = self.options.queue_names if self.options.balancing else [self.options.queue]

        self.process_pools = {queue_name: MockPool(queue_name) for queue_name in queues}

    def prune_terminating_processes(self) -> None:
        for pool in self.process_pools.values():
            pool.prune_terminating(self.options.timeout)

    @property
    def total_process_count(self) -> int:
        return sum(pool.total_process_count for pool in self.process_pools.values())


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
async def test_simple_strategy_balances_evenly_across_queues(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 5.0, "high": 5.0}, {"default": 3, "high": 9})
    supervisor = MockSupervisor("default,high", max_processes=6, min_processes=1, balance="simple")

    await autoscaler.scale(supervisor)

    assert supervisor.process_pools["default"].active_process_count == 3
    assert supervisor.process_pools["high"].active_process_count == 3


@pytest.mark.asyncio
async def test_auto_strategy_scales_by_time_share(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 1000.0, "high": 3000.0}, {"default": 1, "high": 1})
    supervisor = MockSupervisor("default,high", max_processes=8, min_processes=1, balance="auto")

    await autoscaler.scale(supervisor)

    assert supervisor.process_pools["default"].active_process_count == 2
    assert supervisor.process_pools["high"].active_process_count == 6


@pytest.mark.asyncio
async def test_size_strategy_scales_by_job_count_share(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 999.0, "high": 1.0}, {"default": 2, "high": 6})
    supervisor = MockSupervisor(
        "default,high",
        max_processes=8,
        min_processes=1,
        balance="auto",
        auto_scaling_strategy="size",
    )

    await autoscaler.scale(supervisor)

    assert supervisor.process_pools["default"].active_process_count == 2
    assert supervisor.process_pools["high"].active_process_count == 6


@pytest.mark.asyncio
async def test_zero_backlog_scales_idle_queues_to_minimum_when_capacity_allows(autoscaler_factory):
    autoscaler = autoscaler_factory({"default": 5000.0, "high": 5000.0}, {"default": 0, "high": 0})
    supervisor = MockSupervisor("default,high", max_processes=4, min_processes=1, balance="auto")

    await autoscaler.scale(supervisor)

    assert supervisor.process_pools["default"].active_process_count == 1
    assert supervisor.process_pools["high"].active_process_count == 1


@pytest.mark.asyncio
async def test_non_balancing_supervisor_scales_single_pool_by_queue_depth(autoscaler_factory):
    """balance='false' supervisors scale their single pool between min and max by backlog size."""
    autoscaler = autoscaler_factory({"default": 100.0}, {"default": 5})
    supervisor = MockSupervisor("default", max_processes=10, min_processes=1, balance="false")

    await autoscaler.scale(supervisor)

    assert supervisor.process_pools["default"].active_process_count == 5


@pytest.mark.asyncio
async def test_non_balancing_supervisor_respects_max_processes_cap(autoscaler_factory):
    """balance='false' never exceeds max_processes and never drops below min_processes."""
    autoscaler = autoscaler_factory({"default": 100.0}, {"default": 500})
    supervisor = MockSupervisor("default", max_processes=4, min_processes=2, balance="false")

    await autoscaler.scale(supervisor)
    assert supervisor.process_pools["default"].active_process_count == 4

    # Backlog drains → scale back down to min_processes
    autoscaler_empty = autoscaler_factory({"default": 100.0}, {"default": 0})
    await autoscaler_empty.scale(supervisor)
    assert supervisor.process_pools["default"].active_process_count == 2


@pytest.mark.asyncio
async def test_global_max_process_cap_is_respected_across_many_queues(autoscaler_factory):
    autoscaler = autoscaler_factory(
        {"default": 100.0, "high": 100.0, "low": 100.0},
        {"default": 0, "high": 0, "low": 0},
    )
    supervisor = MockSupervisor("default,high,low", max_processes=2, min_processes=1, balance="auto")

    await autoscaler.scale(supervisor)

    total = sum(pool.active_process_count for pool in supervisor.process_pools.values())
    assert total == 2
    assert sorted(pool.active_process_count for pool in supervisor.process_pools.values()) == [0, 1, 1]
