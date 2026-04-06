"""Tests for Supervisor — command processing and state management."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.supervisor import Supervisor, SupervisorOptions


class TestSupervisorOptions:
    """Tests for SupervisorOptions configuration."""

    def test_defaults(self):
        opts = SupervisorOptions(name="test")
        assert opts.connection == "redis"
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


class TestSupervisor:
    """Tests for Supervisor state management and commands."""

    def _make_supervisor(self, name="test-supervisor", min_processes=1, max_processes=10, balance="auto"):
        opts = SupervisorOptions(
            name=name,
            broker_path="test:broker",
            min_processes=min_processes,
            max_processes=max_processes,
            balance=balance,
        )
        command_queue = AsyncMock()
        command_queue.pending.return_value = []
        command_queue.flush.return_value = None

        dispatcher = EventDispatcher()
        sup = Supervisor(opts, command_queue, dispatcher)
        return sup

    def test_initial_state(self):
        """Supervisor starts in running + working state."""
        sup = self._make_supervisor()
        assert sup.status == "running"
        assert sup.working is True
        assert sup.running is True

    def test_pause(self):
        """pause() sets working=False, status changes to 'paused'."""
        sup = self._make_supervisor()
        sup.pause()
        assert sup.working is False
        assert sup.status == "paused"

    def test_continue_working(self):
        """continue_working() sets working=True."""
        sup = self._make_supervisor()
        sup.pause()
        assert sup.status == "paused"

        sup.continue_working()
        assert sup.working is True
        assert sup.status == "running"

    def test_terminate(self):
        """terminate() sets both running and working to False."""
        sup = self._make_supervisor()
        sup.terminate(0)
        assert sup.running is False
        assert sup.working is False
        assert sup.status == "stopped"

    def test_scale_respects_min_max(self):
        """scale() clamps to min_processes and max_processes."""
        sup = self._make_supervisor(min_processes=2, max_processes=5)

        sup.scale(1)  # below min
        assert sup.pool.active_process_count == 2

    def test_restart_terminates_and_respawns(self):
        """restart() kills all workers and re-scales to min."""
        sup = self._make_supervisor(min_processes=2)
        sup.pool.scale(4)  # start with 4

        sup.restart()

        # Workers moved to terminating, then re-scaled to min
        assert sup.pool.active_process_count == 2

    def test_to_dict_serialization(self):
        """to_dict() produces correct JSON-serializable output."""
        sup = self._make_supervisor()
        d = sup.to_dict()

        assert d["name"] == "test-supervisor"
        assert d["status"] == "running"
        assert "pid" in d
        assert "default" in d["processes"]
        assert d["options"]["queue"] == "default"
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
