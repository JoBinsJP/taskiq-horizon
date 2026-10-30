"""Tests for trim job listeners — frequency throttle and delegation."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.events import (
    MasterSupervisorLooped,
    MasterSupervisorOutOfMemory,
    SupervisorLooped,
    SupervisorOutOfMemory,
)
from taskiq_horizon.listeners.monitor_master_memory import MonitorMasterSupervisorMemoryListener
from taskiq_horizon.listeners.monitor_supervisor_memory import MonitorSupervisorMemoryListener
from taskiq_horizon.listeners.trim_failed_jobs import TrimFailedJobsListener
from taskiq_horizon.listeners.trim_monitored_jobs import TrimMonitoredJobsListener
from taskiq_horizon.listeners.trim_recent_jobs import TrimRecentJobsListener
from taskiq_horizon.master_supervisor import MasterSupervisor


class TestTrimRecentJobsListener:
    """Tests for TrimRecentJobsListener."""

    @pytest.mark.asyncio
    async def test_first_call_always_trims(self):
        """The first call should always trigger a trim (no last_trimmed yet)."""
        jobs = AsyncMock()
        listener = TrimRecentJobsListener(jobs, frequency=60)

        await listener.handle(MasterSupervisorLooped(master=None))

        jobs.trim_recent_jobs.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_second_call_within_frequency_skips(self):
        """A second call within the frequency window should NOT trim."""
        jobs = AsyncMock()
        listener = TrimRecentJobsListener(jobs, frequency=60)

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_recent_jobs.reset_mock()

        # Second call immediately — should skip
        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_recent_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_call_after_frequency_trims_again(self):
        """After the frequency interval, trimming should fire again."""
        jobs = AsyncMock()
        listener = TrimRecentJobsListener(jobs, frequency=1)

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_recent_jobs.reset_mock()

        # Manually age the last_trimmed
        listener.last_trimmed = time.time() - 2

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_recent_jobs.assert_awaited_once()


class TestTrimFailedJobsListener:
    """Tests for TrimFailedJobsListener."""

    @pytest.mark.asyncio
    async def test_trims_on_first_call(self):
        jobs = AsyncMock()
        listener = TrimFailedJobsListener(jobs, frequency=60)

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_failed_jobs.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_throttles_within_frequency(self):
        jobs = AsyncMock()
        listener = TrimFailedJobsListener(jobs, frequency=60)

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_failed_jobs.reset_mock()

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_failed_jobs.assert_not_awaited()


class TestTrimMonitoredJobsListener:
    """Tests for TrimMonitoredJobsListener."""

    @pytest.mark.asyncio
    async def test_trims_on_first_call(self):
        jobs = AsyncMock()
        listener = TrimMonitoredJobsListener(jobs, frequency=60)

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_monitored_jobs.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_throttles_within_frequency(self):
        jobs = AsyncMock()
        listener = TrimMonitoredJobsListener(jobs, frequency=60)

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_monitored_jobs.reset_mock()

        await listener.handle(MasterSupervisorLooped(master=None))
        jobs.trim_monitored_jobs.assert_not_awaited()


def _mem_process(rss_mb: float):
    """Build a psutil.Process stand-in reporting the given RSS."""
    proc = MagicMock()
    proc.memory_info.return_value = SimpleNamespace(rss=int(rss_mb * 1024 * 1024))
    return proc


class TestMonitorMasterSupervisorMemoryListener:
    """Master memory monitor sets should_quit so the master loop terminates."""

    @pytest.mark.asyncio
    async def test_over_limit_sets_should_quit_and_fires_event(self):
        dispatcher = AsyncMock()
        listener = MonitorMasterSupervisorMemoryListener(dispatcher, memory_limit_mb=100)
        master = SimpleNamespace(should_quit=False)

        with patch("taskiq_horizon.listeners.monitor_master_memory.psutil.Process", return_value=_mem_process(150)):
            await listener.handle(MasterSupervisorLooped(master=master))

        assert master.should_quit is True
        dispatcher.fire.assert_awaited_once()
        assert isinstance(dispatcher.fire.await_args.args[0], MasterSupervisorOutOfMemory)

    @pytest.mark.asyncio
    async def test_under_limit_does_nothing(self):
        dispatcher = AsyncMock()
        listener = MonitorMasterSupervisorMemoryListener(dispatcher, memory_limit_mb=100)
        master = SimpleNamespace(should_quit=False)

        with patch("taskiq_horizon.listeners.monitor_master_memory.psutil.Process", return_value=_mem_process(50)):
            await listener.handle(MasterSupervisorLooped(master=master))

        assert master.should_quit is False
        dispatcher.fire.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_master_loop_honors_should_quit(self, redis, config):
        """A should_quit request makes the master's monitor loop shut down gracefully."""
        master = MasterSupervisor(config, redis, EventDispatcher(), supervisor_options=[])
        master.should_quit = True

        await asyncio.wait_for(master.monitor(), timeout=5)

        assert master.running is False
        assert master.status == "stopped"


class TestMonitorSupervisorMemoryListener:
    """Supervisor memory = sum of its pools' worker RSS; over-limit restarts the pools."""

    def _supervisor(self, worker_pids):
        pool = MagicMock()
        pool.workers = [SimpleNamespace(pid=pid) for pid in worker_pids]
        return SimpleNamespace(name="test-supervisor", process_pools={"default": pool}), pool

    @pytest.mark.asyncio
    async def test_sums_worker_rss_and_restarts_pools_when_over_limit(self):
        dispatcher = AsyncMock()
        listener = MonitorSupervisorMemoryListener(dispatcher, memory_limit_mb=100)
        supervisor, pool = self._supervisor([101, 102])

        # Two workers at 60MB each → 120MB total, over the 100MB limit
        with patch(
            "taskiq_horizon.listeners.monitor_supervisor_memory.psutil.Process",
            side_effect=lambda pid: _mem_process(60),
        ):
            await listener.handle(SupervisorLooped(supervisor=supervisor))

        pool.restart.assert_called_once()
        dispatcher.fire.assert_awaited_once()
        event = dispatcher.fire.await_args.args[0]
        assert isinstance(event, SupervisorOutOfMemory)
        assert event.memory_usage_mb == pytest.approx(120.0)

    @pytest.mark.asyncio
    async def test_under_limit_leaves_workers_alone(self):
        dispatcher = AsyncMock()
        listener = MonitorSupervisorMemoryListener(dispatcher, memory_limit_mb=200)
        supervisor, pool = self._supervisor([101, 102])

        with patch(
            "taskiq_horizon.listeners.monitor_supervisor_memory.psutil.Process",
            side_effect=lambda pid: _mem_process(60),
        ):
            await listener.handle(SupervisorLooped(supervisor=supervisor))

        pool.restart.assert_not_called()
        dispatcher.fire.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dead_worker_pids_are_skipped(self):
        import psutil as psutil_module

        dispatcher = AsyncMock()
        listener = MonitorSupervisorMemoryListener(dispatcher, memory_limit_mb=100)
        supervisor, pool = self._supervisor([101])

        with patch(
            "taskiq_horizon.listeners.monitor_supervisor_memory.psutil.Process",
            side_effect=psutil_module.NoSuchProcess(101),
        ):
            await listener.handle(SupervisorLooped(supervisor=supervisor))

        pool.restart.assert_not_called()


@pytest.mark.asyncio
async def test_wait_alerts_preserve_queue_name_and_backend_connection(config):
    from taskiq_horizon.listeners.monitor_wait_times import MonitorWaitTimesListener

    wait = SimpleNamespace(
        connection="database", metrics_repo=None, calculate=AsyncMock(return_value={"tenant:mail": 100})
    )
    dispatcher = AsyncMock()
    await MonitorWaitTimesListener(wait, dispatcher, config).handle(SupervisorLooped(supervisor=None))
    event = dispatcher.fire.call_args.args[0]
    assert event.connection == "database"
    assert event.queue == "tenant:mail"
