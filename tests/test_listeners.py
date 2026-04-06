"""Tests for trim job listeners — frequency throttle and delegation."""

import time
from unittest.mock import AsyncMock

import pytest

from taskiq_horizon.events import MasterSupervisorLooped
from taskiq_horizon.listeners.trim_recent_jobs import TrimRecentJobsListener
from taskiq_horizon.listeners.trim_failed_jobs import TrimFailedJobsListener
from taskiq_horizon.listeners.trim_monitored_jobs import TrimMonitoredJobsListener


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
