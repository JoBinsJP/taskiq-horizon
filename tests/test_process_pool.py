"""Tests for ProcessPool — scaling, monitoring, and draining."""

import time
from unittest.mock import patch

from taskiq_horizon.process_pool import ProcessPool
from taskiq_horizon.worker_process import WorkerProcess


class MockWorkerProcess:
    """Fake WorkerProcess for testing pool logic without real subprocesses."""

    def __init__(self, alive=True, pid=None, memory_mb=50.0):
        self.pid = pid or id(self) % 99999
        self._alive = alive
        self._memory_mb = memory_mb
        self.terminated = False
        self.killed = False
        self.broker_path = "test:broker"
        self.queue = "default"

    def start(self):
        self._alive = True

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self._alive = False

    def is_alive(self):
        return self._alive

    def memory_usage_mb(self):
        return self._memory_mb


class TestProcessPool:
    """Unit tests for ProcessPool."""

    def _make_pool(self, workers=None, queue="default"):
        """Create a pool with pre-injected mock workers."""
        pool = ProcessPool("test:broker", queue)
        if workers:
            pool.workers = workers
        return pool

    def test_scale_up_creates_workers(self):
        """scale() up increases the number of active workers."""
        pool = self._make_pool()

        with patch.object(WorkerProcess, "start"):
            pool.scale(3)

        assert pool.active_process_count == 3

    def test_scale_down_moves_to_terminating(self):
        """scale() down moves workers from active to terminating."""
        workers = [MockWorkerProcess(pid=i) for i in range(5)]
        pool = self._make_pool(workers)

        pool.scale(2)

        assert pool.active_process_count == 2
        assert len(pool.terminating) == 3
        # Verify the terminated workers got terminate() called
        for worker, _ in pool.terminating:
            assert worker.terminated is True

    def test_scale_to_same_is_noop(self):
        """scale() to current count does nothing."""
        workers = [MockWorkerProcess(pid=i) for i in range(3)]
        pool = self._make_pool(workers)

        pool.scale(3)

        assert pool.active_process_count == 3
        assert len(pool.terminating) == 0

    def test_total_includes_terminating(self):
        """total_process_count includes both active and draining."""
        workers = [MockWorkerProcess(pid=i) for i in range(5)]
        pool = self._make_pool(workers)

        pool.scale(3)

        assert pool.total_process_count == 5  # 3 active + 2 terminating

    def test_prune_removes_dead_terminating(self):
        """prune_terminating() removes workers that have exited."""
        dead_worker = MockWorkerProcess(alive=False, pid=1)
        alive_worker = MockWorkerProcess(alive=True, pid=2)

        pool = self._make_pool()
        now = time.time()
        pool.terminating = [(dead_worker, now), (alive_worker, now)]

        pool.prune_terminating(timeout=60)

        assert len(pool.terminating) == 1
        assert pool.terminating[0][0].pid == 2

    def test_prune_force_kills_on_timeout(self):
        """prune_terminating() force-kills workers that exceed the timeout."""
        stuck_worker = MockWorkerProcess(alive=True, pid=1)

        pool = self._make_pool()
        # Terminated 120 seconds ago (exceeds 60s timeout)
        pool.terminating = [(stuck_worker, time.time() - 120)]

        pool.prune_terminating(timeout=60)

        assert stuck_worker.killed is True
        assert len(pool.terminating) == 0

    def test_monitor_respawns_dead_workers(self):
        """monitor() detects dead workers and spawns replacements."""
        alive = MockWorkerProcess(alive=True, pid=1)
        dead = MockWorkerProcess(alive=False, pid=2)

        pool = self._make_pool([alive, dead])

        with patch.object(WorkerProcess, "start"):
            pool.monitor(memory_limit_mb=128)

        # Original alive worker stays, dead one is replaced
        assert pool.active_process_count == 2
        assert pool.workers[0].pid == 1  # original alive worker kept

    def test_monitor_terminates_memory_hogs(self):
        """monitor() terminates workers exceeding the memory limit."""
        normal = MockWorkerProcess(alive=True, pid=1, memory_mb=50)
        hog = MockWorkerProcess(alive=True, pid=2, memory_mb=256)

        pool = self._make_pool([normal, hog])

        with patch.object(WorkerProcess, "start"):
            pool.monitor(memory_limit_mb=128)

        # Hog should be terminated and moved to draining
        assert hog.terminated is True
        assert len(pool.terminating) == 1
        # But a replacement was spawned, so still 2 active
        assert pool.active_process_count == 2

    def test_terminate_all_moves_everything_to_draining(self):
        """terminate_all() moves all workers to terminating list."""
        workers = [MockWorkerProcess(pid=i) for i in range(3)]
        pool = self._make_pool(workers)

        pool.terminate_all()

        assert pool.active_process_count == 0
        assert len(pool.terminating) == 3

    def test_kill_all_clears_everything(self):
        """kill_all() force-kills all workers and empties both lists."""
        workers = [MockWorkerProcess(pid=i) for i in range(3)]
        pool = self._make_pool(workers)

        pool.kill_all()

        assert pool.active_process_count == 0
        assert len(pool.terminating) == 0
        for w in workers:
            assert w.killed is True
