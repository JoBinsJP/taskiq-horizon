"""Tests for EventMap — default listener registration."""

from unittest.mock import AsyncMock

import pytest

from taskiq_horizon.event_map import register_default_listeners
from taskiq_horizon.events import (
    LongWaitDetected,
    MasterSupervisorLooped,
    SupervisorLooped,
)


class TestEventMap:
    """Tests for register_default_listeners()."""

    def test_registers_master_looped_listeners(self, dispatcher, config):
        """MasterSupervisorLooped should have 4 listeners after registration."""
        job_repo = AsyncMock()
        master_repo = AsyncMock()
        supervisor_repo = AsyncMock()

        register_default_listeners(
            dispatcher,
            job_repo=job_repo,
            master_repo=master_repo,
            supervisor_repo=supervisor_repo,
            config=config,
        )

        assert dispatcher.has_listeners(MasterSupervisorLooped)
        # 3 trim listeners + 1 expire listener + 1 memory listener = 5
        assert len(dispatcher._listeners[MasterSupervisorLooped]) == 5

    def test_registers_supervisor_looped_listeners(self, dispatcher, config):
        """SupervisorLooped should have 1 listener (MonitorWaitTimes)."""
        wait_time_calculator = AsyncMock()

        register_default_listeners(
            dispatcher,
            wait_time_calculator=wait_time_calculator,
            config=config,
        )

        assert dispatcher.has_listeners(SupervisorLooped)
        # 1 wait time listener + 1 memory listener = 2
        assert len(dispatcher._listeners[SupervisorLooped]) == 2

    def test_registers_long_wait_detected_listener(self, dispatcher, config):
        """LongWaitDetected should always have the notification listener."""
        register_default_listeners(dispatcher, config=config)

        assert dispatcher.has_listeners(LongWaitDetected)
        assert len(dispatcher._listeners[LongWaitDetected]) == 1

    def test_skips_listeners_when_repos_not_provided(self, dispatcher, config):
        """If repos are None, their listeners are not registered."""
        register_default_listeners(
            dispatcher,
            job_repo=None,
            master_repo=None,
            supervisor_repo=None,
            config=config,
        )

        # Memory listeners always get registered
        assert dispatcher.has_listeners(MasterSupervisorLooped)
        assert len(dispatcher._listeners[MasterSupervisorLooped]) == 1

        assert dispatcher.has_listeners(SupervisorLooped)
        assert len(dispatcher._listeners[SupervisorLooped]) == 1

        assert dispatcher.has_listeners(LongWaitDetected)

    @pytest.mark.asyncio
    async def test_full_event_chain_fires(self, dispatcher, config):
        """Integration: fire MasterSupervisorLooped → trim listeners execute."""
        job_repo = AsyncMock()
        master_repo = AsyncMock()
        supervisor_repo = AsyncMock()

        register_default_listeners(
            dispatcher,
            job_repo=job_repo,
            master_repo=master_repo,
            supervisor_repo=supervisor_repo,
            config=config,
        )

        await dispatcher.fire(MasterSupervisorLooped(master=None))

        # Trim listeners fire on first call (no throttle gate yet)
        job_repo.trim_recent_jobs.assert_awaited_once()
        job_repo.trim_failed_jobs.assert_awaited_once()
        job_repo.trim_monitored_jobs.assert_awaited_once()

        # Expire supervisors fires too
        master_repo.flush_expired.assert_awaited_once()
        supervisor_repo.flush_expired.assert_awaited_once()
