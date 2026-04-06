import pytest
from unittest.mock import AsyncMock

from taskiq_horizon.autoscaler import AutoScaler


class MockSupervisor:
    def __init__(self, queues, max_processes=10, min_processes=1, balance_max_shift=1, 
                 balance_strategy='auto', auto_scale_by_num_jobs=False):
        self.queues = queues
        self.max_processes = max_processes
        self.min_processes = min_processes
        self.balance_max_shift = balance_max_shift
        self.balance_strategy = balance_strategy
        self.auto_scale_by_num_jobs = auto_scale_by_num_jobs
        
        # workers mapping: queue name to a dummy list of processes
        self.workers = {q: [] for q in queues}
        self.scale_queue = AsyncMock()


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
def run_scale():
    async def _run(supervisor, runtimes, lengths):
        autoscaler = AutoScaler(MockMetrics(runtimes), MockWaitTime(lengths))
        await autoscaler.scale(supervisor)
        return supervisor.scale_queue.call_args_list
    return _run


@pytest.mark.asyncio
async def test_autoscaler_simple_strategy(run_scale):
    supervisor = MockSupervisor(["default", "high"], max_processes=10, min_processes=1, balance_strategy="simple")
    # In simple strategy, it caps at size, maxing at max_processes / num_pools... wait! 
    # Our auto scaler code for simple:
    # target = min(max_processes, max(min_processes, metrics['size']))
    
    calls = await run_scale(supervisor, runtimes={"default": 1.0, "high": 1.0}, lengths={"default": 5, "high": 20})
    
    # Default size = 5. Shift up gradually by max_shift=1
    # Current workers is 0 for both.
    # desired is min(0 + 1, 5, 10) = 1
    # For high, desired is min(0 + 1, 10, 10) = 1
    call_args = {c.args[0]: c.args[1] for c in calls}
    assert call_args["default"] == 1
    assert call_args["high"] == 1


@pytest.mark.asyncio
async def test_autoscaler_time_based_allocates_proportionally(run_scale):
    # default has 10 jobs * 0.1s = 1s
    # high has 2 jobs * 4.5s = 9s
    # Total runtime = 10s. Default fraction = 0.1, High fraction = 0.9.
    # Target default = 0.1 * 10 = 1
    # Target high = 0.9 * 10 = 9
    supervisor = MockSupervisor(
        ["default", "high"], 
        max_processes=10, 
        balance_max_shift=10  # Allow immediate scaling for test
    )
    calls = await run_scale(supervisor, runtimes={"default": 0.1, "high": 4.5}, lengths={"default": 10, "high": 2})
    
    call_args = {c.args[0]: c.args[1] for c in calls}
    assert call_args["default"] == 1
    assert call_args["high"] == 9


@pytest.mark.asyncio
async def test_autoscaler_num_jobs_based_allocates_proportionally(run_scale):
    # default has 90 jobs
    # high has 10 jobs
    # Total jobs = 100. Default fraction = 0.9, High fraction = 0.1
    # Target default = 0.9 * 10 = 9
    # Target high = 0.1 * 10 = 1
    supervisor = MockSupervisor(
        ["default", "high"], 
        max_processes=10, 
        balance_max_shift=10,
        auto_scale_by_num_jobs=True
    )
    calls = await run_scale(supervisor, runtimes={"default": 999.0, "high": 999.0}, lengths={"default": 90, "high": 10})
    
    call_args = {c.args[0]: c.args[1] for c in calls}
    assert call_args["default"] == 9
    assert call_args["high"] == 1


@pytest.mark.asyncio
async def test_autoscaler_never_breaches_global_max_processes(run_scale):
    # We set 3 queues. 
    # With floating point ceiling math in Target allocations, it could breach max_processes.
    # Fractional targets: 1.2, 1.2, 7.6.
    # ceil -> 2, 2, 8 = 12 breaching max 10.
    supervisor = MockSupervisor(
        ["q1", "q2", "q3"], 
        max_processes=10, 
        balance_max_shift=10 # allow massive shifting 
    )
    # Total time = 100s
    # q1: 12%, q2: 12%, q3: 76%
    runtimes = {"q1": 1.0, "q2": 1.0, "q3": 1.0}
    lengths = {"q1": 12, "q2": 12, "q3": 76}
    
    # Pre-populate workers to mimic existing scale so we test dynamic cap!
    supervisor.workers["q1"] = [1, 2] # 2 workers
    supervisor.workers["q2"] = [3, 4] # 2 workers
    supervisor.workers["q3"] = [5, 6, 7, 8, 9, 10] # 6 workers
    # Total currently = 10 workers! Supervisor is completely AT max!
    
    # Target for q3 is ceil(7.6) = 8
    # Since total is 10, shift UP max_shift should be max(0, 10 - 10) = 0!
    # q3 should NOT be allowed to scale UP.
    calls = await run_scale(supervisor, runtimes, lengths)
    
    # Even though target > current, max_up_shift is 0, so desired = current, so no scaling UP call is fired!
    call_args = {c.args[0]: c.args[1] for c in calls}
    
    assert "q3" not in call_args # No scale UP occurred due to global limit!
