"""Tests for Supervisor — command processing and state management."""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

import taskiq_horizon.supervisor as supervisor_module
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.supervisor import Supervisor, SupervisorOptions


class FakeProcessPool:
    def __init__(self, broker_path: str, queue: str = "default", worker_args=None, queue_env_var: str = "QUEUE_NAME"):
        self.broker_path = broker_path
        self.queue = queue
        self.worker_args = worker_args or []
        self.queue_env_var = queue_env_var
        self.workers = 0
        self.terminating = 0
        self.monitor_calls = 0
        self.restart_calls = 0
        self.kill_all_calls = 0
        self.paused = False
        self.pause_calls = 0
        self.resume_calls = 0

    @property
    def active_process_count(self) -> int:
        return self.workers

    @property
    def total_process_count(self) -> int:
        return self.workers + self.terminating

    def scale(self, target: int) -> None:
        self.workers = max(0, int(target))

    def monitor(self, memory_limit_mb: int = 128) -> None:
        self.monitor_calls += 1

    def prune_terminating(self, timeout: int = 60) -> None:
        self.terminating = 0

    def terminate_all(self) -> None:
        self.terminating += self.workers
        self.workers = 0

    def restart(self) -> None:
        self.restart_calls += 1

    def pause(self) -> None:
        self.paused = True
        self.pause_calls += 1

    def resume(self) -> None:
        self.paused = False
        self.resume_calls += 1

    def kill_all(self) -> None:
        self.kill_all_calls += 1
        self.workers = 0
        self.terminating = 0


@pytest.fixture(autouse=True)
def patch_process_pool(monkeypatch):
    monkeypatch.setattr(supervisor_module, "ProcessPool", FakeProcessPool)


class TestSupervisorOptions:
    """Tests for SupervisorOptions configuration."""

    def test_defaults(self):
        opts = SupervisorOptions(name="test")
        assert opts.queue == "default"
        assert opts.balance == "auto"
        assert opts.max_processes == 10
        assert opts.min_processes == 1
        assert opts.memory == 128

    def test_balancing_enabled_for_auto(self):
        opts = SupervisorOptions(name="test", balance="auto")
        assert opts.balancing is True
        assert opts.auto_scaling is True

    def test_balancing_enabled_for_simple(self):
        opts = SupervisorOptions(name="test", balance="simple")
        assert opts.balancing is True
        assert opts.auto_scaling is False

    def test_balancing_disabled_for_false(self):
        opts = SupervisorOptions(name="test", balance="false")
        assert opts.balancing is False

    def test_queue_names_are_split_and_trimmed(self):
        opts = SupervisorOptions(name="test", queue=" default, high ,low ")
        assert opts.queue_names == ["default", "high", "low"]


class TestSupervisor:
    """Tests for Supervisor state management and commands."""

    def _make_supervisor(
        self,
        name="test-supervisor",
        queue="default",
        min_processes=1,
        max_processes=10,
        balance="auto",
        autoscaler=None,
    ):
        opts = SupervisorOptions(
            name=name,
            broker_path="test:broker",
            queue=queue,
            min_processes=min_processes,
            max_processes=max_processes,
            balance=balance,
        )
        command_queue = AsyncMock()
        command_queue.pending.return_value = []
        command_queue.flush.return_value = None

        dispatcher = EventDispatcher()
        sup = Supervisor(opts, command_queue, dispatcher, autoscaler=autoscaler)
        return sup

    def test_initial_state(self):
        """Supervisor starts in running + working state."""
        sup = self._make_supervisor()
        assert sup.status == "running"
        assert sup.working is True
        assert sup.running is True

    def test_pause(self):
        """pause() sets working=False, status changes to 'paused', and pools are paused."""
        sup = self._make_supervisor()
        sup.pause()
        assert sup.working is False
        assert sup.status == "paused"
        for pool in sup.process_pools.values():
            assert pool.paused is True
            assert pool.pause_calls == 1

    def test_continue_working(self):
        """continue_working() sets working=True and resumes the pools."""
        sup = self._make_supervisor()
        sup.pause()
        assert sup.status == "paused"

        sup.continue_working()
        assert sup.working is True
        assert sup.status == "running"
        for pool in sup.process_pools.values():
            assert pool.paused is False
            assert pool.resume_calls == 1

    def test_terminate(self):
        """terminate() sets both running and working to False."""
        sup = self._make_supervisor()
        sup.terminate(0)
        assert sup.running is False
        assert sup.working is False
        assert sup.status == "stopped"

    def test_scale_distributes_workers_across_balanced_queues(self):
        """scale() spreads manual worker targets across per-queue pools."""
        sup = self._make_supervisor(queue="default,high,low", balance="auto")

        sup.scale(5)

        assert sup.process_pools["default"].active_process_count == 2
        assert sup.process_pools["high"].active_process_count == 2
        assert sup.process_pools["low"].active_process_count == 1

    def test_scale_single_pool_uses_exact_target(self):
        """Manual scale() applies the requested target on non-balancing supervisors."""
        sup = self._make_supervisor(queue="default", balance="false", min_processes=2, max_processes=5)

        sup.scale(1)

        assert sup.pool.active_process_count == 1

    def test_restart_restarts_all_pools(self):
        """restart() delegates to each queue-specific pool."""
        sup = self._make_supervisor(queue="default,high", balance="auto")

        sup.restart()

        assert sup.process_pools["default"].restart_calls == 1
        assert sup.process_pools["high"].restart_calls == 1

    def test_to_dict_serialization_uses_per_queue_process_counts(self):
        """to_dict() produces correct JSON-serializable output."""
        sup = self._make_supervisor(queue="default,high", balance="auto")
        sup.process_pools["default"].scale(2)
        sup.process_pools["high"].scale(1)
        d = sup.to_dict()

        assert d["name"] == "test-supervisor"
        assert d["status"] == "running"
        assert "pid" in d
        assert d["processes"] == {"default": 2, "high": 1}
        assert d["options"]["queue"] == "default,high"
        assert d["options"]["maxProcesses"] == 10

    @pytest.mark.asyncio
    async def test_process_commands_dispatches(self):
        """_process_commands() resolves and executes queued commands."""
        sup = self._make_supervisor()
        sup.command_queue.pending.return_value = [
            {"command": "pause", "options": {}},
        ]

        await sup._process_commands()

        assert sup.working is False  # pause was executed

    @pytest.mark.asyncio
    async def test_monitor_autoscales_non_balancing_supervisors(self):
        """balance='false' supervisors still run the autoscaler (single-pool scaling)."""
        autoscaler = AsyncMock()

        async def terminate_on_scale(supervisor):
            supervisor.terminate(0)

        autoscaler.scale.side_effect = terminate_on_scale
        sup = self._make_supervisor(balance="false", autoscaler=autoscaler)

        await asyncio.wait_for(sup.monitor(), timeout=5)

        autoscaler.scale.assert_awaited_with(sup)

    @pytest.mark.asyncio
    async def test_autoscale_honors_immediate_balance_requests(self):
        """Balance commands bypass the cooldown window on the next loop."""
        autoscaler = AsyncMock()
        sup = self._make_supervisor(queue="default,high", balance="auto", autoscaler=autoscaler)
        sup._last_autoscale = time.time()
        sup._needs_balance = True

        await sup._autoscale()

        autoscaler.scale.assert_awaited_once_with(sup)
        assert sup._needs_balance is False
