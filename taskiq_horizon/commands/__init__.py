"""
Supervisor Commands — mirrors Laravel Horizon's SupervisorCommands/ directory.

Each command is a simple class with a `process(supervisor, options)` method.
Commands are consumed from the RedisCommandQueue on each supervisor loop.
"""

from taskiq_horizon.commands.balance import BalanceCommand
from taskiq_horizon.commands.continue_working import ContinueWorkingCommand
from taskiq_horizon.commands.pause import PauseCommand
from taskiq_horizon.commands.restart import RestartCommand
from taskiq_horizon.commands.scale import ScaleCommand
from taskiq_horizon.commands.terminate import TerminateCommand

# Command name → handler class mapping (used by Supervisor._resolve_command)
COMMAND_MAP: dict[str, type] = {
    "pause": PauseCommand,
    "continue": ContinueWorkingCommand,
    "restart": RestartCommand,
    "terminate": TerminateCommand,
    "scale": ScaleCommand,
    "balance": BalanceCommand,
}

__all__ = [
    "COMMAND_MAP",
    "BalanceCommand",
    "ContinueWorkingCommand",
    "PauseCommand",
    "RestartCommand",
    "ScaleCommand",
    "TerminateCommand",
]
