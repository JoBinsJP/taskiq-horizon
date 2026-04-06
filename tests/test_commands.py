"""Tests for Supervisor command classes."""

from unittest.mock import MagicMock

import pytest

from taskiq_horizon.commands.pause import PauseCommand
from taskiq_horizon.commands.continue_working import ContinueWorkingCommand
from taskiq_horizon.commands.restart import RestartCommand
from taskiq_horizon.commands.terminate import TerminateCommand
from taskiq_horizon.commands.scale import ScaleCommand
from taskiq_horizon.commands import COMMAND_MAP


class TestCommands:
    """Test that each command mutates supervisor state correctly."""

    def _mock_supervisor(self):
        sup = MagicMock()
        sup.name = "test-supervisor"
        return sup

    def test_pause_command(self):
        sup = self._mock_supervisor()
        PauseCommand().process(sup)
        sup.pause.assert_called_once()

    def test_continue_working_command(self):
        sup = self._mock_supervisor()
        ContinueWorkingCommand().process(sup)
        sup.continue_working.assert_called_once()

    def test_restart_command(self):
        sup = self._mock_supervisor()
        RestartCommand().process(sup)
        sup.restart.assert_called_once()

    def test_terminate_command_default_status(self):
        sup = self._mock_supervisor()
        TerminateCommand().process(sup)
        sup.terminate.assert_called_once_with(0)

    def test_terminate_command_with_status(self):
        sup = self._mock_supervisor()
        TerminateCommand().process(sup, {"status": 1})
        sup.terminate.assert_called_once_with(1)

    def test_scale_command_default(self):
        sup = self._mock_supervisor()
        ScaleCommand().process(sup)
        sup.scale.assert_called_once_with(1)

    def test_scale_command_with_target(self):
        sup = self._mock_supervisor()
        ScaleCommand().process(sup, {"scale": 7})
        sup.scale.assert_called_once_with(7)

    def test_command_map_has_all_commands(self):
        """The command registry contains all expected command names."""
        assert set(COMMAND_MAP.keys()) == {
            "pause", "continue", "restart", "terminate", "scale", "balance"
        }

    def test_command_map_resolves_to_correct_classes(self):
        """Each command name maps to the correct handler class."""
        assert COMMAND_MAP["pause"] is PauseCommand
        assert COMMAND_MAP["continue"] is ContinueWorkingCommand
        assert COMMAND_MAP["restart"] is RestartCommand
        assert COMMAND_MAP["terminate"] is TerminateCommand
        assert COMMAND_MAP["scale"] is ScaleCommand
